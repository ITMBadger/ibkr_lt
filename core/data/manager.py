"""DataManager — per-instrument 1-min bar store with backfill/stream merge.

Dedup policy:
  - Prior-session offline history wins when timestamps already exist.
  - Live split-feed runs can make the live provider authoritative for the
    current local session so CSV and broker/provider volume are not mixed.
  - Live 1-min bars: latest writer wins (keep='last').

One DataManager per instrument. The engine creates managers for strategy data
instruments and, during simulated replay, execution instruments needed for fills.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from ..types import Bar, Instrument
from ..engine.timeframes import Timeframe
from .resampler import Resampler

_OHLCV = ["open", "high", "low", "close", "volume"]
_BAR_COLUMNS = [*_OHLCV, "source"]
_OFFLINE_SOURCES = {"csv"}
MERGE_POLICY_PRESERVE = "preserve"
MERGE_POLICY_LIVE_PROVIDER_WINS = "live_provider_wins"


class DataManager:
    def __init__(
        self,
        instrument: Instrument,
        lookback_days: int = 500,
        session_tz: str = "America/New_York",
    ) -> None:
        self._instrument = instrument
        self._lookback_days = lookback_days
        self._session_tz = session_tz

        self._bars: pd.DataFrame = _empty_frame()
        self._pending_rows: list[tuple[pd.Timestamp, list[object]]] = []
        self._pending_flush_size = 512
        self._revision: int = 0
        self._lock = threading.RLock()

        self._resampler = Resampler()
        self._resample_cache: dict[tuple, tuple[int, pd.DataFrame]] = {}  # key → (revision, df)
        self._last_merge_report: dict[str, object] = {
            "policy": MERGE_POLICY_PRESERVE,
            "input_bars": 0,
            "added": 0,
            "replaced_session_rows": 0,
            "dropped_current_session_offline": 0,
            "source_counts": {},
        }

    # ------------------------------------------------------------------
    # Startup: CSV load + backfill
    # ------------------------------------------------------------------

    def load_csv(self, path: str | Path, tz: str | None = None) -> int:
        """Load 1-min bars from a CSV file. Returns number of bars loaded.

        The CSV must have columns: timestamp (or index), open, high, low, close, volume.
        Timestamps are parsed as tz-aware UTC.
        """
        path = Path(path)
        if not path.exists():
            return 0
        try:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
        except Exception:
            return 0

        df.columns = [c.lower() for c in df.columns]
        df = df[[c for c in _OHLCV if c in df.columns]]
        if df.empty:
            return 0

        df.index = _normalize_index(df.index, tz or self._session_tz)
        df = df.sort_index()
        df = df[~df.index.duplicated(keep="last")]
        df["source"] = "csv"

        with self._lock:
            self._pending_rows.clear()
            self._bars = df
            self._revision += 1
            self._trim_lookback()
        return len(df)

    def merge_backfill(
        self,
        bars: list[Bar],
        live_session_date: date | None = None,
        *,
        current_session_source_policy: str = MERGE_POLICY_PRESERVE,
    ) -> int:
        """Merge historical backfill bars.

        Prior sessions preserve existing offline rows. Live split-feed startup
        should pass ``current_session_source_policy="live_provider_wins"`` and
        the current local session date so live-provider history replaces current
        session CSV rows before streaming begins.
        """
        if not bars:
            self._last_merge_report = {
                "policy": current_session_source_policy,
                "input_bars": 0,
                "added": 0,
                "replaced_session_rows": 0,
                "dropped_current_session_offline": 0,
                "source_counts": {},
            }
            return 0

        new_rows: list[dict] = []
        source_counts: dict[str, int] = {}
        dropped_current_session_offline = 0
        replaced_session_rows = 0
        policy = str(current_session_source_policy or MERGE_POLICY_PRESERVE)

        with self._lock:
            self._flush_pending()
            live_provider_policy = (
                live_session_date is not None
                and policy == MERGE_POLICY_LIVE_PROVIDER_WINS
            )
            if live_provider_policy:
                if not self._bars.empty:
                    local_dates = self._bars.index.tz_convert(self._session_tz).date
                    if "source" in self._bars.columns:
                        offline = (
                            self._bars["source"]
                            .fillna("")
                            .astype(str)
                            .isin(_OFFLINE_SOURCES)
                        )
                    else:
                        offline = pd.Series(True, index=self._bars.index)
                    replace = (local_dates == live_session_date) & offline.to_numpy()
                    keep = ~replace
                    replaced_session_rows = int((~keep).sum())
                    if replaced_session_rows:
                        self._bars = self._bars[keep]

            existing = set(self._bars.index)
            for bar in bars:
                ts = _bar_timestamp_utc(bar.timestamp)
                source = str(bar.source or "")
                source_counts[source] = source_counts.get(source, 0) + 1
                is_current_session = (
                    live_session_date is not None
                    and _local_session_date(ts, self._session_tz) == live_session_date
                )
                if live_provider_policy and is_current_session and source in _OFFLINE_SOURCES:
                    dropped_current_session_offline += 1
                    continue
                if (
                    policy == MERGE_POLICY_PRESERVE
                    and live_session_date is not None
                    and _local_session_date(ts, self._session_tz) >= live_session_date
                ):
                    continue
                if ts in existing:
                    continue  # CSV wins for prior sessions
                new_rows.append({
                    "timestamp": ts,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "source": source,
                })

            if not new_rows:
                if replaced_session_rows:
                    self._revision += 1
                    self._trim_lookback()
                self._last_merge_report = {
                    "policy": policy,
                    "input_bars": len(bars),
                    "added": 0,
                    "replaced_session_rows": replaced_session_rows,
                    "dropped_current_session_offline": dropped_current_session_offline,
                    "source_counts": source_counts,
                }
                return 0

            new_df = pd.DataFrame(new_rows).set_index("timestamp")
            new_df.index = _normalize_index(new_df.index, "UTC")
            self._bars = (
                pd.concat([self._bars, new_df])
                .sort_index()
            )
            self._bars = self._bars[~self._bars.index.duplicated(keep="last")]
            self._revision += 1
            self._trim_lookback()
            self._last_merge_report = {
                "policy": policy,
                "input_bars": len(bars),
                "added": len(new_rows),
                "replaced_session_rows": replaced_session_rows,
                "dropped_current_session_offline": dropped_current_session_offline,
                "source_counts": source_counts,
            }
            return len(new_rows)

    # ------------------------------------------------------------------
    # Live streaming
    # ------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> Bar | None:
        """Ingest a completed 1-min bar from the live stream.

        Returns the bar if it was new/updated (revision incremented),
        or None if it was duplicate/ignored.

        Live bars overwrite matching timestamps and append new timestamps.
        """
        ts = bar.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts = pd.Timestamp(ts).tz_convert("UTC")
        values = [bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source]

        with self._lock:
            latest_pending = self._pending_rows[-1][0] if self._pending_rows else None
            latest_stored = self._bars.index[-1] if not self._bars.empty else None
            latest = latest_pending if latest_pending is not None else latest_stored
            if latest is None or ts > latest:
                self._pending_rows.append((ts, values))
                if len(self._pending_rows) >= self._pending_flush_size:
                    self._flush_pending()
            else:
                self._flush_pending()
                row = pd.DataFrame(
                    [values],
                    index=pd.DatetimeIndex([ts], tz="UTC", name="timestamp"),
                    columns=_BAR_COLUMNS,
                )
                self._bars = (
                    pd.concat([self._bars, row])
                    .sort_index()
                )
                self._bars = self._bars[~self._bars.index.duplicated(keep="last")]
            self._revision += 1
            return bar

    # ------------------------------------------------------------------
    # Data access (called by FeatureRegistry)
    # ------------------------------------------------------------------

    def bars_1m(self, lookback_days: int = 0) -> pd.DataFrame:
        with self._lock:
            self._flush_pending()
            if lookback_days > 0:
                cutoff = self._lookback_cutoff(lookback_days)
                return self._bars[self._bars.index >= cutoff].copy()
            return self._bars.copy()

    def resampled(self, target_tf: Timeframe, lookback_bars: int = 0) -> pd.DataFrame:
        """Return cached resample. Cache is invalidated on revision change."""
        cache_key = (target_tf.label, lookback_bars)
        with self._lock:
            self._flush_pending()
            cached_rev, cached_df = self._resample_cache.get(cache_key, (-1, None))
            if cached_rev == self._revision and cached_df is not None:
                return cached_df.copy()
            result = self._resampler.resample(self._bars, target_tf, lookback_bars)
            self._resample_cache[cache_key] = (self._revision, result)
            return result.copy()

    @property
    def revision(self) -> int:
        return self._revision

    def bar_count(self) -> int:
        """Return the number of stored 1-minute bars without copying them."""
        with self._lock:
            return len(self._bars) + len(self._pending_rows)

    def session_count(self) -> int:
        """Return the number of stored sessions without copying OHLCV rows."""
        with self._lock:
            self._flush_pending()
            if self._bars.empty:
                return 0
            local_index = self._bars.index.tz_convert(self._session_tz)
            return int(local_index.normalize().nunique())

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    def latest_timestamp(self) -> datetime | None:
        """Return the timestamp of the most recent bar, or None if empty."""
        with self._lock:
            if self._pending_rows:
                return self._pending_rows[-1][0].to_pydatetime()
            if self._bars.empty:
                return None
            return self._bars.index[-1].to_pydatetime()

    def data_quality(self) -> dict[str, object]:
        """Return compact read-only data-source diagnostics for operators."""
        with self._lock:
            self._flush_pending()
            source_counts: dict[str, int] = {}
            if "source" in self._bars.columns:
                source_counts = {
                    str(source): int(count)
                    for source, count in self._bars["source"].fillna("").value_counts().items()
                }
            latest = self.latest_timestamp()
            return {
                "bar_count": len(self._bars),
                "latest_timestamp": latest.isoformat() if latest is not None else None,
                "source_counts": source_counts,
                "last_merge": dict(self._last_merge_report),
            }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _trim_lookback(self) -> None:
        if self._lookback_days <= 0 or self._bars.empty:
            return
        cutoff = self._lookback_cutoff(self._lookback_days)
        if self._bars.index[0] >= cutoff:
            return
        trim_at = self._bars.index.searchsorted(pd.Timestamp(cutoff), side="left")
        if trim_at > 0:
            self._bars = self._bars.iloc[int(trim_at):]

    def _flush_pending(self) -> None:
        if not self._pending_rows:
            return
        index = pd.DatetimeIndex(
            [ts for ts, _ in self._pending_rows],
            tz="UTC",
            name="timestamp",
        )
        values = [row for _, row in self._pending_rows]
        pending = pd.DataFrame(values, index=index, columns=_BAR_COLUMNS)
        self._pending_rows.clear()

        if self._bars.empty:
            self._bars = pending
        elif pending.index[0] > self._bars.index[-1]:
            self._bars = pd.concat([self._bars, pending])
        else:
            self._bars = pd.concat([self._bars, pending]).sort_index()
            self._bars = self._bars[~self._bars.index.duplicated(keep="last")]
        self._trim_lookback()

    def _lookback_cutoff(self, lookback_days: int) -> datetime:
        if self._bars.empty:
            return datetime.min.replace(tzinfo=timezone.utc)
        latest = self._bars.index[-1]
        if latest.tzinfo is None:
            latest = latest.tz_localize(timezone.utc)
        return latest.to_pydatetime() - timedelta(days=lookback_days)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_frame() -> pd.DataFrame:
    df = pd.DataFrame(columns=_BAR_COLUMNS)
    df.index = pd.DatetimeIndex([], tz="UTC", name="timestamp")
    return df


def _bar_timestamp_utc(timestamp: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    return ts.tz_convert("UTC")


def _local_session_date(timestamp: datetime | pd.Timestamp, session_tz: str) -> date:
    return _bar_timestamp_utc(timestamp).tz_convert(ZoneInfo(session_tz)).date()


def _normalize_index(index: pd.Index, tz: str) -> pd.DatetimeIndex:
    """Ensure DatetimeIndex is tz-aware UTC."""
    import pytz
    try:
        dti = pd.DatetimeIndex(index)
    except ValueError as exc:
        if "Mixed timezones" not in str(exc):
            raise
        return pd.DatetimeIndex(pd.to_datetime(index, utc=True))
    if dti.tz is None:
        # Assume the timestamps are in `tz` (local session time)
        local_tz = pytz.timezone(tz)
        dti = dti.tz_localize(local_tz, ambiguous="infer", nonexistent="shift_forward")
    return dti.tz_convert("UTC")

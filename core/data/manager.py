"""DataManager — per-instrument 1-min bar store with backfill/stream merge.

Dedup policy (ported exactly from legacy/core/data_manager.py):
  - Prior sessions: CSV wins (single source of truth for historical data).
  - Today: live stream is authoritative. When the first live bar for today
    arrives, today's CSV bars are purged so IBKR volume is the only source
    for VWAP and intraday indicators.
  - Live 1-min bars: latest writer wins (keep='last').

One DataManager per instrument. The engine creates one per unique instrument
in all registered strategies' primary+reference sets.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from ..types import Bar, Instrument
from ..engine.timeframes import Timeframe
from .resampler import Resampler

_OHLCV = ["open", "high", "low", "close", "volume"]


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
        self._revision: int = 0
        self._lock = threading.RLock()
        self._today_purged: bool = False  # True once live-today bars evict CSV today-bars

        self._resampler = Resampler()
        self._resample_cache: dict[tuple, tuple[int, pd.DataFrame]] = {}  # key → (revision, df)

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

        with self._lock:
            self._bars = df
            self._revision += 1
            self._trim_lookback()
        return len(df)

    def merge_backfill(self, bars: list[Bar], live_session_date: date | None = None) -> int:
        """Merge IBKR historical backfill bars.

        Prior sessions: CSV wins (skip if timestamp already in _bars).
        If live_session_date is provided, skip that session and later because
        the live stream is authoritative for the current live session.
        Gap fills: add bars not already present.
        Returns number of new bars added.
        """
        if not bars:
            return 0

        new_rows: list[dict] = []

        with self._lock:
            existing = set(self._bars.index)
            for bar in bars:
                ts = bar.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if live_session_date is not None and ts.date() >= live_session_date:
                    continue  # live stream owns current live session
                if ts in existing:
                    continue  # CSV wins for prior sessions
                new_rows.append({
                    "timestamp": ts,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                })

            if not new_rows:
                return 0

            new_df = pd.DataFrame(new_rows).set_index("timestamp")
            new_df.index = pd.DatetimeIndex(new_df.index, tz="UTC")
            self._bars = (
                pd.concat([self._bars, new_df])
                .sort_index()
            )
            self._bars = self._bars[~self._bars.index.duplicated(keep="last")]
            self._revision += 1
            self._trim_lookback()
            return len(new_rows)

    # ------------------------------------------------------------------
    # Live streaming
    # ------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> Bar | None:
        """Ingest a completed 1-min bar from the live stream.

        Returns the bar if it was new/updated (revision incremented),
        or None if it was duplicate/ignored.

        First live bar for today triggers a purge of today's CSV bars so
        IBKR volume is the sole source for VWAP.
        """
        ts = bar.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        with self._lock:
            # Purge same-session historical bars on first live bar.
            if not self._today_purged:
                self._purge_session(ts.date())
                self._today_purged = True

            row = pd.DataFrame(
                [[bar.open, bar.high, bar.low, bar.close, bar.volume]],
                index=pd.DatetimeIndex([ts], tz="UTC"),
                columns=_OHLCV,
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
            if lookback_days > 0:
                cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
                return self._bars[self._bars.index >= cutoff].copy()
            return self._bars.copy()

    def resampled(self, target_tf: Timeframe, lookback_bars: int = 0) -> pd.DataFrame:
        """Return cached resample. Cache is invalidated on revision change."""
        cache_key = (target_tf.label, lookback_bars)
        with self._lock:
            cached_rev, cached_df = self._resample_cache.get(cache_key, (-1, None))
            if cached_rev == self._revision and cached_df is not None:
                return cached_df.copy()
            result = self._resampler.resample(self._bars, target_tf, lookback_bars)
            self._resample_cache[cache_key] = (self._revision, result)
            return result.copy()

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    def latest_timestamp(self) -> datetime | None:
        """Return the timestamp of the most recent bar, or None if empty."""
        with self._lock:
            if self._bars.empty:
                return None
            return self._bars.index[-1].to_pydatetime()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _purge_session(self, session_date: date) -> None:
        """Remove bars for the live bar's UTC session before first live write."""
        self._bars = self._bars[
            self._bars.index.normalize().date < session_date
        ] if len(self._bars) else self._bars

    def _trim_lookback(self) -> None:
        if self._lookback_days <= 0 or self._bars.empty:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)
        self._bars = self._bars[self._bars.index >= cutoff]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_frame() -> pd.DataFrame:
    df = pd.DataFrame(columns=_OHLCV)
    df.index = pd.DatetimeIndex([], tz="UTC", name="timestamp")
    return df


def _normalize_index(index: pd.Index, tz: str) -> pd.DatetimeIndex:
    """Ensure DatetimeIndex is tz-aware UTC."""
    import pytz
    dti = pd.DatetimeIndex(index)
    if dti.tz is None:
        # Assume the timestamps are in `tz` (local session time)
        local_tz = pytz.timezone(tz)
        dti = dti.tz_localize(local_tz, ambiguous="infer", nonexistent="shift_forward")
    return dti.tz_convert("UTC")

"""Multi-instrument feature registry.

The registry centralises common indicator computation across strategies.
Strategies can request public/common indicators at runtime without declaring
every feature in StrategySpec, which keeps protected strategy metadata lean.
"""

from __future__ import annotations

import threading
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from ..data.manager import DataManager
from ..engine.timeframes import Timeframe
from ..types import Bar, Instrument
from . import indicators as ind
from .ids import parse_indicator_id

_OHLCV = ["open", "high", "low", "close", "volume"]


class FeatureRegistry:
    """Shared indicator cache across all instruments and strategies."""

    def __init__(
        self,
        managers: Mapping[Instrument, DataManager],
        max_cache_size: int = 1024,
    ) -> None:
        self._managers = dict(managers)
        self._max = max_cache_size
        self._cache: OrderedDict[tuple, Any] = OrderedDict()
        self._source_bars: dict[Instrument, pd.DataFrame] = {}
        self._source_versions: dict[Instrument, int] = {}
        self._compute_counts: dict[tuple, int] = {}
        self._lock = threading.RLock()

    def as_of(self, timestamp: datetime) -> "FeatureView":
        """Return a timestamp-bound accessor that prevents future leakage."""
        return FeatureView(self, timestamp)

    def preload_from_managers(self) -> None:
        """Seed feature sources from currently backfilled DataManager bars."""
        with self._lock:
            for instrument, manager in self._managers.items():
                self._set_source(instrument, manager.bars_1m())

    def preload_bars(self, bars: Iterable[Bar]) -> None:
        """Merge replay/future bars into feature sources for vectorized backtests."""
        grouped: dict[Instrument, list[Bar]] = {}
        for bar in bars:
            grouped.setdefault(bar.instrument, []).append(bar)

        with self._lock:
            for instrument, manager in self._managers.items():
                frames = [manager.bars_1m()]
                extra = _bars_to_frame(grouped.get(instrument, []))
                if not extra.empty:
                    frames.append(extra)
                self._set_source(instrument, _combine_frames(frames))

    def on_bar(self, bar: Bar) -> None:
        """Merge a live/replay bar into the feature source when needed."""
        with self._lock:
            existing = self._source_bars.get(bar.instrument)
            if existing is None:
                return
            row = _bars_to_frame([bar])
            if row.empty:
                return
            ts = row.index[0]
            if ts in existing.index and _rows_equal(existing.loc[ts], row.iloc[0]):
                return
            self._set_source(bar.instrument, _combine_frames([existing, row]))

    def get(
        self,
        name: str,
        instrument: Instrument,
        timeframe: str = "1m",
        **params: Any,
    ) -> Any:
        """Return a cached public/common indicator.

        Example:
            ctx.features.get("ema", SPY, "1d", period=20)
            ctx.features.get("bollinger", SPY, "1d", period=200)

        The same request from multiple strategies on the same bar computes once.
        When vectorized sources are preloaded, timestamp-bound feature views
        slice the precomputed series to the current bar.
        """
        normalized_name = _normalize_name(name)
        normalized_params = _normalize_params(params)
        return self._get(
            normalized_name,
            instrument,
            timeframe,
            normalized_params,
            params,
            as_of=None,
        )

    def bars(
        self,
        instrument: Instrument,
        timeframe: str = "1m",
        *,
        as_of: datetime | None = None,
        start: datetime | pd.Timestamp | None = None,
        end: datetime | pd.Timestamp | None = None,
        lookback_bars: int = 0,
    ) -> pd.DataFrame:
        """Return cached timestamp-bound bars for strategy-private calculations."""
        manager = self._manager_for(instrument)
        with self._lock:
            if instrument in self._source_bars:
                source_version = self._source_versions[instrument]
                bars = self._cached_source_timeframe_bars(
                    instrument,
                    timeframe,
                    source_version,
                )
                return _slice_bars(
                    bars,
                    timeframe,
                    as_of=as_of,
                    start=start,
                    end=end,
                    lookback_bars=lookback_bars,
                )

        bars = self._bars(manager, timeframe)
        return _slice_bars(
            bars,
            timeframe,
            as_of=as_of,
            start=start,
            end=end,
            lookback_bars=lookback_bars,
        )

    def latest_bar(
        self,
        instrument: Instrument,
        timeframe: str = "1m",
        *,
        as_of: datetime | None = None,
    ) -> pd.Series | None:
        """Return the latest timestamp-safe OHLCV bar without a DataFrame slice."""
        manager = self._manager_for(instrument)
        with self._lock:
            if instrument in self._source_bars:
                source_version = self._source_versions[instrument]
                bars = self._cached_source_timeframe_bars(
                    instrument,
                    timeframe,
                    source_version,
                )
                return _latest_bar_from_bars(bars, timeframe, as_of=as_of)

        bars = self._bars(manager, timeframe)
        return _latest_bar_from_bars(bars, timeframe, as_of=as_of)

    def _get(
        self,
        normalized_name: str,
        instrument: Instrument,
        timeframe: str,
        normalized_params: tuple[tuple[str, Any], ...],
        params: Mapping[str, Any],
        *,
        as_of: datetime | None,
    ) -> Any:
        manager = self._manager_for(instrument)
        with self._lock:
            if instrument in self._source_bars:
                return self._get_from_source(
                    normalized_name,
                    instrument,
                    timeframe,
                    normalized_params,
                    params,
                    as_of=as_of,
                )

        revision = manager.revision
        key = (
            normalized_name,
            instrument,
            timeframe,
            normalized_params,
            revision,
        )

        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                return _slice_to_as_of(cached, timeframe, as_of)

            bars = self._bars(manager, timeframe)
            value = self._compute(normalized_name, bars, params)
            self._cache[key] = value
            compute_key = (
                normalized_name,
                instrument,
                timeframe,
                normalized_params,
            )
            self._compute_counts[compute_key] = self._compute_counts.get(compute_key, 0) + 1
            if len(self._cache) > self._max:
                self._cache.popitem(last=False)
            return _slice_to_as_of(value, timeframe, as_of)

    def get_id(self, indicator_id: str, *, as_of: datetime | None = None) -> Any:
        """Compatibility helper for ids like ``ema_20@QQQ.3m``."""
        name, symbol, timeframe = parse_indicator_id(indicator_id)
        instrument = self._instrument_for_symbol(symbol)
        public_name, params = _name_to_request(name)
        normalized_name = _normalize_name(public_name)
        return self._get(
            normalized_name,
            instrument,
            timeframe or "1m",
            _normalize_params(params),
            params,
            as_of=as_of,
        )

    def invalidate(self, instrument: Instrument | None = None) -> None:
        """Clear cache entries.

        Revision-aware keys prevent stale reads; this method is used to cap
        memory and is safe to call after each live bar.
        """
        with self._lock:
            if instrument is not None and instrument in self._source_bars:
                return
            if instrument is None:
                self._cache.clear()
                return
            for key in list(self._cache):
                if key[1] == instrument:
                    del self._cache[key]

    def compute_count(
        self,
        name: str,
        instrument: Instrument,
        timeframe: str = "1m",
        **params: Any,
    ) -> int:
        """Return compute count for tests and diagnostics."""
        key = (
            _normalize_name(name),
            instrument,
            timeframe,
            _normalize_params(params),
        )
        return self._compute_counts.get(key, 0)

    def _get_from_source(
        self,
        normalized_name: str,
        instrument: Instrument,
        timeframe: str,
        normalized_params: tuple[tuple[str, Any], ...],
        params: Mapping[str, Any],
        *,
        as_of: datetime | None,
    ) -> Any:
        source_version = self._source_versions[instrument]
        key = (
            "feature",
            normalized_name,
            instrument,
            timeframe,
            normalized_params,
            source_version,
        )
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return _slice_to_as_of(cached, timeframe, as_of)

        bars = self._source_timeframe_bars(instrument, timeframe, source_version)
        value = self._compute(normalized_name, bars, params)
        self._cache[key] = value
        compute_key = (
            normalized_name,
            instrument,
            timeframe,
            normalized_params,
        )
        self._compute_counts[compute_key] = self._compute_counts.get(compute_key, 0) + 1
        if len(self._cache) > self._max:
            self._cache.popitem(last=False)
        return _slice_to_as_of(value, timeframe, as_of)

    def _source_timeframe_bars(
        self,
        instrument: Instrument,
        timeframe: str,
        source_version: int,
    ) -> pd.DataFrame:
        return self._cached_source_timeframe_bars(
            instrument,
            timeframe,
            source_version,
        ).copy()

    def _cached_source_timeframe_bars(
        self,
        instrument: Instrument,
        timeframe: str,
        source_version: int,
    ) -> pd.DataFrame:
        key = ("bars", instrument, timeframe, source_version)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached

        source = self._source_bars[instrument]
        if timeframe == "1m":
            result = source.copy()
        else:
            result = managerless_resample(source, Timeframe.parse(timeframe))
        self._cache[key] = result
        if len(self._cache) > self._max:
            self._cache.popitem(last=False)
        return result

    def _set_source(self, instrument: Instrument, bars: pd.DataFrame) -> None:
        clean = bars[[col for col in _OHLCV if col in bars.columns]].copy()
        if clean.empty:
            clean.index = pd.DatetimeIndex([], tz="UTC", name="timestamp")
        else:
            clean.index = _normalize_index(clean.index)
            clean = clean.sort_index()
            clean = clean[~clean.index.duplicated(keep="last")]
        existing = self._source_bars.get(instrument)
        if existing is not None and clean.equals(existing):
            return
        self._source_bars[instrument] = clean
        self._source_versions[instrument] = self._source_versions.get(instrument, 0) + 1

    def _manager_for(self, instrument: Instrument) -> DataManager:
        manager = self._managers.get(instrument)
        if manager is None:
            raise KeyError(f"No DataManager for instrument {instrument.symbol!r}")
        return manager

    def _instrument_for_symbol(self, symbol: str) -> Instrument:
        for instrument in self._managers:
            if instrument.symbol == symbol:
                return instrument
        raise KeyError(f"No DataManager for symbol {symbol!r}")

    def _bars(self, manager: DataManager, timeframe: str) -> pd.DataFrame:
        if timeframe == "1m":
            return manager.bars_1m()
        return manager.resampled(Timeframe.parse(timeframe))

    def _compute(self, name: str, bars: pd.DataFrame, params: Mapping[str, Any]) -> Any:
        if bars.empty:
            return pd.Series(dtype=float)

        if name == "ema":
            return ind.ema(
                bars,
                int(params.get("period", 20)),
                str(params.get("col", "close")),
            )
        if name == "sma":
            return ind.sma(
                bars,
                int(params.get("period", 20)),
                str(params.get("col", "close")),
            )
        if name == "stddev":
            return ind.stddev(
                bars,
                int(params.get("period", 20)),
                str(params.get("col", "close")),
            )
        if name == "rsi":
            return ind.rsi(
                bars,
                int(params.get("period", 14)),
                str(params.get("col", "close")),
            )
        if name == "macd":
            return ind.macd(
                bars,
                int(params.get("fast", 12)),
                int(params.get("slow", 26)),
                int(params.get("signal", 9)),
                str(params.get("col", "close")),
            )
        if name == "stoch":
            return ind.stoch(
                bars,
                int(params.get("fastk_period", params.get("fastk", 14))),
                int(params.get("slowk_period", params.get("slowk", 3))),
                int(params.get("slowd_period", params.get("slowd", 3))),
            )
        if name == "atr":
            return ind.atr(bars, int(params.get("period", 14)))
        if name == "adx":
            return ind.adx(bars, int(params.get("period", 14)))
        if name in {"bollinger", "bollinger_bands", "bb"}:
            return ind.bollinger_bands(
                bars,
                int(params.get("period", 20)),
                float(params.get("nbdev", 2.0)),
                str(params.get("col", "close")),
            )
        if name == "session_vwap":
            return ind.session_vwap(bars)
        if name == "session_open":
            return ind.session_open_values(bars)
        if name == "heikin_ashi":
            return ind.heikin_ashi(bars)
        if name == "range_ratio":
            return ind.range_ratio_by_session(
                bars,
                int(params.get("window", 20)),
                int(params.get("min_periods", 5)),
                int(params.get("shift", 1)),
            )
        if name in {"daily", "daily_ohlcv"}:
            return ind.daily_ohlcv(bars)
        raise ValueError(f"Unknown feature: {name!r}")


def _normalize_name(name: str) -> str:
    aliases = {
        "bb": "bollinger",
        "bollinger_bands": "bollinger",
        "ha": "heikin_ashi",
        "daily_ohlcv": "daily",
    }
    normalized = name.lower()
    return aliases.get(normalized, normalized)


def _normalize_params(params: Mapping[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(params.items()))


def _copy(value: Any) -> Any:
    return value.copy() if hasattr(value, "copy") else value


class FeatureView:
    """Timestamp-bound feature accessor used inside MarketContext."""

    def __init__(self, registry: FeatureRegistry, timestamp: datetime) -> None:
        self._registry = registry
        self._timestamp = timestamp

    def get(
        self,
        name: str,
        instrument: Instrument,
        timeframe: str = "1m",
        **params: Any,
    ) -> Any:
        normalized_name = _normalize_name(name)
        return self._registry._get(
            normalized_name,
            instrument,
            timeframe,
            _normalize_params(params),
            params,
            as_of=self._timestamp,
        )

    def get_id(self, indicator_id: str) -> Any:
        return self._registry.get_id(indicator_id, as_of=self._timestamp)

    def bars(
        self,
        instrument: Instrument,
        timeframe: str = "1m",
        *,
        start: datetime | pd.Timestamp | None = None,
        end: datetime | pd.Timestamp | None = None,
        lookback_bars: int = 0,
    ) -> pd.DataFrame:
        return self._registry.bars(
            instrument,
            timeframe,
            as_of=self._timestamp,
            start=start,
            end=end,
            lookback_bars=lookback_bars,
        )

    def latest_bar(
        self,
        instrument: Instrument,
        timeframe: str = "1m",
    ) -> pd.Series | None:
        return self._registry.latest_bar(
            instrument,
            timeframe,
            as_of=self._timestamp,
        )


def managerless_resample(bars_1m: pd.DataFrame, target_tf: Timeframe) -> pd.DataFrame:
    from ..data.resampler import Resampler

    return Resampler().resample(bars_1m, target_tf)


def _slice_to_as_of(value: Any, timeframe: str, as_of: datetime | None) -> Any:
    if as_of is None or not hasattr(value, "index"):
        return _copy(value)
    if not isinstance(value.index, pd.DatetimeIndex):
        return _copy(value)
    if value.empty:
        return _copy(value)

    ts = pd.Timestamp(as_of)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    tf = Timeframe.parse(timeframe)
    if tf.seconds <= 60:
        cutoff = ts
        stop = value.index.searchsorted(cutoff, side="right")
    else:
        cutoff = ts.floor(_to_pandas_offset(tf))
        stop = value.index.searchsorted(cutoff, side="left")
    return value.iloc[: int(stop)].copy()


def _slice_bars(
    bars: pd.DataFrame,
    timeframe: str,
    *,
    as_of: datetime | None,
    start: datetime | pd.Timestamp | None,
    end: datetime | pd.Timestamp | None,
    lookback_bars: int,
) -> pd.DataFrame:
    if bars.empty:
        return bars.copy()
    index = bars.index
    start_pos = 0
    stop_pos = len(index)
    if as_of is not None:
        ts = pd.Timestamp(as_of)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        tf = Timeframe.parse(timeframe)
        if tf.seconds <= 60:
            cutoff = ts
            stop_pos = min(stop_pos, int(index.searchsorted(cutoff, side="right")))
        else:
            cutoff = ts.floor(_to_pandas_offset(tf))
            stop_pos = min(stop_pos, int(index.searchsorted(cutoff, side="left")))
    if start is not None:
        start_pos = max(start_pos, int(index.searchsorted(_normalize_bound(start), side="left")))
    if end is not None:
        stop_pos = min(stop_pos, int(index.searchsorted(_normalize_bound(end), side="right")))
    if start_pos >= stop_pos:
        return bars.iloc[0:0].copy()
    result = bars.iloc[start_pos:stop_pos]
    if lookback_bars > 0:
        result = result.iloc[-lookback_bars:]
    return result.copy()


def _latest_bar_from_bars(
    bars: pd.DataFrame,
    timeframe: str,
    *,
    as_of: datetime | None,
) -> pd.Series | None:
    if bars.empty:
        return None
    if as_of is None:
        return bars.iloc[-1].copy()

    ts = pd.Timestamp(as_of)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    tf = Timeframe.parse(timeframe)
    if tf.seconds <= 60:
        cutoff = ts
        pos = int(bars.index.searchsorted(cutoff, side="right")) - 1
    else:
        cutoff = ts.floor(_to_pandas_offset(tf))
        pos = int(bars.index.searchsorted(cutoff, side="left")) - 1
    if pos < 0:
        return None
    return bars.iloc[pos].copy()


def _normalize_bound(value: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _to_pandas_offset(tf: Timeframe) -> str:
    unit = tf.label[-1]
    qty = tf.label[:-1]
    aliases = {"s": "s", "m": "min", "h": "h", "d": "D", "w": "W"}
    return f"{qty}{aliases[unit]}"


def _bars_to_frame(bars: Iterable[Bar]) -> pd.DataFrame:
    rows = []
    for bar in bars:
        ts = bar.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        rows.append({
            "timestamp": pd.Timestamp(ts).tz_convert("UTC"),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        })
    if not rows:
        df = pd.DataFrame(columns=_OHLCV)
        df.index = pd.DatetimeIndex([], tz="UTC", name="timestamp")
        return df
    return pd.DataFrame(rows).set_index("timestamp")


def _combine_frames(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid:
        df = pd.DataFrame(columns=_OHLCV)
        df.index = pd.DatetimeIndex([], tz="UTC", name="timestamp")
        return df
    result = pd.concat(valid).sort_index()
    result.index = _normalize_index(result.index)
    result = result[~result.index.duplicated(keep="last")]
    return result


def _normalize_index(index: pd.Index) -> pd.DatetimeIndex:
    dti = pd.DatetimeIndex(index)
    if dti.tz is None:
        dti = dti.tz_localize("UTC")
    return dti.tz_convert("UTC")


def _rows_equal(left: pd.Series, right: pd.Series) -> bool:
    try:
        if isinstance(left, pd.DataFrame):
            left = left.iloc[-1]
        left_values = pd.to_numeric(left[_OHLCV], errors="coerce").to_numpy(dtype=float)
        right_values = pd.to_numeric(right[_OHLCV], errors="coerce").to_numpy(dtype=float)
        return bool(np.allclose(left_values, right_values, equal_nan=True))
    except Exception:
        return False


def _name_to_request(name: str) -> tuple[str, dict[str, Any]]:
    compact = re.match(r"^([a-z]+)(\d+)$", name)
    if compact:
        base = compact.group(1)
        period = int(compact.group(2))
        if base in {"ema", "sma", "rsi", "atr", "adx", "stddev"}:
            return base, {"period": period}

    parts = name.split("_")
    base = parts[0]
    params = [int(p) for p in parts[1:] if p.isdigit()]

    if base in {"ema", "sma", "rsi", "atr", "adx"}:
        return base, {"period": params[0]} if params else {}
    if base == "stddev":
        return base, {"period": params[0]} if params else {}
    if base == "macd":
        result: dict[str, Any] = {}
        if len(params) > 0:
            result["fast"] = params[0]
        if len(params) > 1:
            result["slow"] = params[1]
        if len(params) > 2:
            result["signal"] = params[2]
        return base, result
    if base == "stoch":
        result = {}
        if len(params) > 0:
            result["fastk"] = params[0]
        if len(params) > 1:
            result["slowk"] = params[1]
        if len(params) > 2:
            result["slowd"] = params[2]
        return base, result
    if name.startswith("bollinger"):
        result = {}
        if params:
            result["period"] = params[0]
        if len(params) > 1:
            result["nbdev"] = float(params[1])
        return "bollinger", result
    if name.startswith("range_ratio"):
        return "range_ratio", {"window": params[0]} if params else {}
    if name in {"session_vwap", "session_open", "heikin_ashi", "range_ratio", "daily_ohlcv"}:
        return name, {}
    return name, {}

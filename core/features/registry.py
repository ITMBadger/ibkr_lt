"""Multi-instrument feature registry.

The registry centralises common indicator computation across strategies.
Strategies can request public/common indicators at runtime without declaring
every feature in StrategySpec, which keeps protected strategy metadata lean.
"""

from __future__ import annotations

import threading
import re
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

import pandas as pd

from ..data.manager import DataManager
from ..engine.timeframes import Timeframe
from ..types import Instrument
from . import indicators as ind
from .ids import parse_indicator_id


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
        self._compute_counts: dict[tuple, int] = {}
        self._lock = threading.RLock()

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

        The cache is keyed by indicator name, instrument, timeframe, params,
        and the instrument DataManager revision. The same request from multiple
        strategies on the same bar computes once.
        """
        manager = self._manager_for(instrument)
        normalized_name = _normalize_name(name)
        normalized_params = _normalize_params(params)
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
                return _copy(cached)

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
            return _copy(value)

    def get_id(self, indicator_id: str) -> Any:
        """Compatibility helper for ids like ``ema_20@QQQ.3m``."""
        name, symbol, timeframe = parse_indicator_id(indicator_id)
        instrument = self._instrument_for_symbol(symbol)
        public_name, params = _name_to_request(name)
        return self.get(public_name, instrument, timeframe or "1m", **params)

    def invalidate(self, instrument: Instrument | None = None) -> None:
        """Clear cache entries.

        Revision-aware keys prevent stale reads; this method is used to cap
        memory and is safe to call after each live bar.
        """
        with self._lock:
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

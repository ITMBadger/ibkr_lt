"""Scheduler — maps bar-close events to matching strategies.

When a bar closes for an instrument, the Scheduler finds all registered
strategies whose primary_instrument matches, builds a MarketContext
(primary + reference bars + shared feature accessor), and returns
(kernel, ctx, state) tuples for the Engine to dispatch via run_in_executor.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

from ..data.manager import DataManager
from ..features.registry import FeatureRegistry
from .timeframes import TF_1D, TF_1M, Timeframe
from ..interfaces.strategy import StrategyKernel, StrategySpec
from ..types import Bar, Instrument, MarketContext

log = logging.getLogger(__name__)


class Scheduler:
    """Maintains the strategy registry and builds MarketContext on bar-close."""

    def __init__(
        self,
        features: FeatureRegistry | None = None,
        options=None,
    ) -> None:
        self._by_primary: dict[Instrument, list[tuple[StrategyKernel, dict]]] = {}
        # All instruments that need strategy MarketContext data.
        self._all_instruments: set[Instrument] = set()
        self._features = features
        self._options = options

    def register(
        self,
        kernel: StrategyKernel,
        state: dict,
    ) -> None:
        """Register a strategy kernel with its per-instance state dict."""
        primary = kernel.SPEC.primary_instrument
        if primary not in self._by_primary:
            self._by_primary[primary] = []
        self._by_primary[primary].append((kernel, state))
        self._all_instruments.add(primary)
        for ref in kernel.SPEC.reference_instruments:
            self._all_instruments.add(ref)
        log.debug("Registered strategy %s on primary %s", kernel.SPEC.id, primary.symbol)

    def all_instruments(self) -> set[Instrument]:
        return set(self._all_instruments)

    def on_bar(
        self,
        bar: Bar,
        managers: dict[Instrument, DataManager],
        include: Callable[[StrategyKernel, dict], bool] | None = None,
    ) -> list[tuple[StrategyKernel, MarketContext, dict]]:
        """Return (kernel, ctx, state) for all strategies matching bar.instrument."""
        matching = self._by_primary.get(bar.instrument, [])
        if not matching:
            return []

        result = []
        for kernel, state in matching:
            spec = kernel.SPEC
            if include is not None and not include(kernel, state):
                continue
            if not self._warmup_satisfied(spec, managers):
                continue
            ctx = self._build_context(bar, spec, managers)
            result.append((kernel, ctx, state))
        return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _warmup_satisfied(
        self,
        spec: StrategySpec,
        managers: dict[Instrument, DataManager],
    ) -> bool:
        dm = managers.get(spec.primary_instrument)
        if dm is None:
            return False
        one_minute_count = dm.bar_count()
        session_count = None
        for tf_label, min_bars in spec.warmup_bars.items():
            if min_bars <= 0:
                continue
            try:
                tf = Timeframe.parse(tf_label)
            except Exception:
                tf = TF_1M
            if tf.seconds <= TF_1M.seconds:
                available = max(0, one_minute_count - 1)
            elif tf.seconds >= TF_1D.seconds:
                if session_count is None:
                    session_count = dm.session_count()
                available = session_count
            else:
                bars_per_window = max(1, (tf.seconds + TF_1M.seconds - 1) // TF_1M.seconds)
                available = max(0, one_minute_count - 1) // bars_per_window
            if available < min_bars:
                return False
        return True

    def _build_context(
        self,
        bar: Bar,
        spec: StrategySpec,
        managers: dict[Instrument, DataManager],
    ) -> MarketContext:
        """Build MarketContext with primary + reference bars + shared features."""
        bars_map: dict[Instrument, dict[str, pd.DataFrame]] = {}

        all_instr = [spec.primary_instrument] + list(spec.reference_instruments)
        for instr in all_instr:
            dm = managers.get(instr)
            if dm is None:
                bars_map[instr] = {}
                continue
            bars_map[instr] = _LazyTimeframeBars(dm, spec.timeframes)

        # Pre-compute declared indicators
        indicators: dict[str, Any] = {}
        feature_view = self._features.as_of(bar.timestamp) if self._features is not None else None
        if self._features is not None:
            for indicator_id in spec.indicators:
                try:
                    indicators[indicator_id] = feature_view.get_id(indicator_id)
                except Exception as e:
                    log.debug("Could not compute indicator %s: %s", indicator_id, e)

        return MarketContext(
            primary=spec.primary_instrument,
            timestamp=bar.timestamp,
            bars=bars_map,
            indicators=indicators,
            features=feature_view,
            options=self._options,
        )


class _LazyTimeframeBars(dict):
    """Mapping that loads timeframe DataFrames only when a strategy asks."""

    def __init__(self, manager: DataManager, declared_timeframes: tuple[str, ...]) -> None:
        super().__init__()
        self._manager = manager
        self._allowed = {"1m", *declared_timeframes}

    def __contains__(self, key: object) -> bool:
        return str(key) in self._allowed

    def __getitem__(self, key: str) -> pd.DataFrame:
        label = str(key)
        if label not in self._allowed:
            raise KeyError(label)
        if not dict.__contains__(self, label):
            dict.__setitem__(self, label, self._load(label))
        return dict.__getitem__(self, label)

    def get(self, key: str, default=None):
        label = str(key)
        if label not in self._allowed:
            return default
        try:
            return self[label]
        except Exception:
            return default

    def keys(self):
        return self._allowed

    def _load(self, label: str) -> pd.DataFrame:
        if label == "1m":
            return self._manager.bars_1m()
        tf = Timeframe.parse(label)
        return self._manager.resampled(tf)

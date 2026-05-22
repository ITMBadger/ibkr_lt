"""Scheduler — maps bar-close events to matching strategies.

When a bar closes for an instrument, the Scheduler finds all registered
strategies whose primary_instrument matches, builds a MarketContext
(primary + reference bars + shared feature accessor), and returns
(kernel, ctx, state) tuples for the Engine to dispatch via run_in_executor.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from ..data.manager import DataManager
from ..features.registry import FeatureRegistry
from .timeframes import Timeframe
from ..interfaces.strategy import StrategyKernel, StrategySpec
from ..types import Bar, Instrument, MarketContext

log = logging.getLogger(__name__)


class Scheduler:
    """Maintains the strategy registry and builds MarketContext on bar-close."""

    def __init__(self, features: FeatureRegistry | None = None) -> None:
        self._by_primary: dict[Instrument, list[tuple[StrategyKernel, dict]]] = {}
        # All instruments that need strategy MarketContext data.
        self._all_instruments: set[Instrument] = set()
        self._features = features

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
    ) -> list[tuple[StrategyKernel, MarketContext, dict]]:
        """Return (kernel, ctx, state) for all strategies matching bar.instrument."""
        matching = self._by_primary.get(bar.instrument, [])
        if not matching:
            return []

        result = []
        for kernel, state in matching:
            spec = kernel.SPEC
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
        for tf_label, min_bars in spec.warmup_bars.items():
            dm = managers.get(spec.primary_instrument)
            if dm is None:
                return False
            try:
                tf = Timeframe.parse(tf_label)
                bars = dm.resampled(tf)
            except Exception:
                bars = dm.bars_1m()
            if len(bars) < min_bars:
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
            tf_map: dict[str, pd.DataFrame] = {}
            tf_map["1m"] = dm.bars_1m()
            for tf_label in spec.timeframes:
                if tf_label == "1m":
                    continue
                try:
                    tf = Timeframe.parse(tf_label)
                    tf_map[tf_label] = dm.resampled(tf)
                except Exception:
                    pass
            bars_map[instr] = tf_map

        # Pre-compute declared indicators
        indicators: dict[str, Any] = {}
        if self._features is not None:
            for indicator_id in spec.indicators:
                try:
                    indicators[indicator_id] = self._features.get_id(indicator_id)
                except Exception as e:
                    log.debug("Could not compute indicator %s: %s", indicator_id, e)

        return MarketContext(
            primary=spec.primary_instrument,
            timestamp=bar.timestamp,
            bars=bars_map,
            indicators=indicators,
            features=self._features,
        )

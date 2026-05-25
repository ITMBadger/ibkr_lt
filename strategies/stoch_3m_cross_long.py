"""Stoch 3m Cross Long -- simple QQQ stochastic-D cross sample.

Signal: QQQ 3-minute Stochastic slowD crosses from below 20 to above 20.
Entry window: 10:00-15:30 America/New_York, end-exclusive.
Execution: QQQ long. Protection: broker-side 1.5% stop after entry fill.

This is a public sample strategy for framework/testing workflows. It is not
approved for live capital deployment.
"""

from __future__ import annotations

from datetime import timezone
from zoneinfo import ZoneInfo

import numpy as np

try:
    import talib
except ImportError as exc:
    raise ImportError(
        "stoch_3m_cross_long requires TA-Lib. Install the TA-Lib C library "
        "and Python package before loading this strategy."
    ) from exc

from core import register_strategy
from core.audit import DecisionTrace, record_decision
from core.interfaces.strategy import (
    ENTRY_FREQUENCY_ONE_PER_DAY,
    POSITION_MODE_SINGLE,
    PositionPolicy,
    ProtectiveStopSpec,
    StrategyKernel,
    StrategySpec,
)
from core.types import Instrument, MarketContext, Signal


QQQ = Instrument(asset_class="equity", symbol="QQQ")
MARKET_TZ = ZoneInfo("America/New_York")


@register_strategy
class Stoch3mCrossLong(StrategyKernel):
    """Sample: buy QQQ when 3-minute stochastic slowD crosses up through 20."""

    _BAR_SIZE = "3m"
    _PARALLEL_BACKTEST_SAFE = True

    SPEC = StrategySpec(
        id="stoch_3m_cross_long",
        primary_instrument=QQQ,
        execution_instrument=QQQ,
        timeframes=("1m", "3m"),
        warmup_bars={"3m": 30},
        protective_stop=ProtectiveStopSpec(pct=0.015, reference="fill_price"),
        position_policy=PositionPolicy(
            position_mode=POSITION_MODE_SINGLE,
            entry_frequency=ENTRY_FREQUENCY_ONE_PER_DAY,
        ),
    )

    _FASTK_PERIOD = 14
    _SLOWK_PERIOD = 3
    _SLOWD_PERIOD = 3
    _THRESHOLD = 20.0
    _ENTRY_START_HHMM = 1000
    _ENTRY_END_HHMM = 1530

    def on_start(self, state: dict) -> None:
        state["last_evaluated_3m_bar"] = None

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        trace = DecisionTrace.entry(ctx, self.SPEC.id)

        def finish(decision: str, reason: str, signal: Signal | None = None) -> Signal | None:
            trace.set_decision(decision, reason=reason, signal=signal)
            record_decision(state, trace)
            return signal

        bars_3m = ctx.bars.get(QQQ, {}).get("3m")
        if bars_3m is None or len(bars_3m) < self.SPEC.warmup_bars["3m"]:
            trace.add_metric("bars_3m_count", 0 if bars_3m is None else len(bars_3m))
            return finish("no_signal", "insufficient_3m_bars")

        p = self.params
        now = ctx.timestamp
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        market_now = now.astimezone(MARKET_TZ)
        trace.add_bar("qqq_3m_current", QQQ, "3m", bars_3m.iloc[-1])
        if len(bars_3m) >= 2:
            trace.add_bar("qqq_3m_prior", QQQ, "3m", bars_3m.iloc[-2])

        fastk_period = int(p.get("fastk_period", self._FASTK_PERIOD))
        slowk_period = int(p.get("slowk_period", self._SLOWK_PERIOD))
        slowd_period = int(p.get("slowd_period", self._SLOWD_PERIOD))
        threshold = float(p.get("threshold", self._THRESHOLD))

        _, slowd = talib.STOCH(
            bars_3m["high"].astype(float).to_numpy(),
            bars_3m["low"].astype(float).to_numpy(),
            bars_3m["close"].astype(float).to_numpy(),
            fastk_period=fastk_period,
            slowk_period=slowk_period,
            slowk_matype=0,
            slowd_period=slowd_period,
            slowd_matype=0,
        )

        if len(slowd) < 2:
            trace.add_metric("slowd_count", len(slowd))
            return finish("no_signal", "insufficient_slowd")

        slowd_series = bars_3m["close"].copy()
        slowd_series[:] = slowd
        cross_series = (
            np.isfinite(slowd_series.shift(1))
            & np.isfinite(slowd_series)
            & (slowd_series.shift(1) < threshold)
            & (slowd_series > threshold)
        )
        table_3m = bars_3m.tail(5).copy()
        table_3m["stoch_d"] = slowd_series.reindex(table_3m.index)
        table_3m["threshold"] = threshold
        table_3m["condition_stoch_d_cross_above_threshold"] = cross_series.reindex(table_3m.index).fillna(False)
        trace.add_table("qqq_3m", QQQ, "3m", table_3m)

        prev_d = float(slowd[-2])
        current_d = float(slowd[-1])
        trace.add_indicator("stoch_d_prior", prev_d, instrument=QQQ, timeframe="3m")
        trace.add_indicator("stoch_d_current", current_d, instrument=QQQ, timeframe="3m")
        trace.add_metric("fastk_period", fastk_period)
        trace.add_metric("slowk_period", slowk_period)
        trace.add_metric("slowd_period", slowd_period)
        trace.add_metric("threshold", threshold)

        crossed_up = bool(cross_series.iloc[-1])

        entry_start = int(p.get("entry_start_hhmm", self._ENTRY_START_HHMM))
        entry_end = int(p.get("entry_end_hhmm", self._ENTRY_END_HHMM))
        market_hhmm = market_now.hour * 100 + market_now.minute
        in_entry_window = entry_start <= market_hhmm < entry_end
        trace.add_metric("market_time_hhmm", market_hhmm)
        trace.add_condition(
            "entry_window",
            in_entry_window,
            lhs=market_hhmm,
            op="start_inclusive_end_exclusive",
            rhs={"start": entry_start, "end": entry_end, "timezone": str(MARKET_TZ)},
        )
        if not in_entry_window:
            return finish("no_signal", "outside_entry_window")

        current_3m_bar = bars_3m.index[-1]
        already_evaluated_bar = state.get("last_evaluated_3m_bar") == current_3m_bar
        trace.add_condition(
            "one_evaluation_per_completed_3m_bar",
            not already_evaluated_bar,
            lhs=state.get("last_evaluated_3m_bar"),
            op="!=",
            rhs=current_3m_bar,
        )
        if already_evaluated_bar:
            return finish("no_signal", "already_evaluated_3m_bar")
        state["last_evaluated_3m_bar"] = current_3m_bar

        trace.add_condition(
            "stoch_d_cross_above_threshold",
            crossed_up,
            lhs={"prior": prev_d, "current": current_d},
            op="cross_above",
            rhs=threshold,
        )
        if not crossed_up:
            return finish("no_signal", "stoch_d_not_crossed")

        signal = Signal(instrument=self.SPEC.execution_instrument, side="long")
        return finish("signal", "stoch_d_crossed_above_threshold", signal)


# end of file

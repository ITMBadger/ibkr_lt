"""Sample strategy scaffold.

Copy this file to `strategies/my_strategy.py`, rename the class and SPEC.id,
then replace the placeholder entry/exit logic.

The leading underscore is intentional: `load_strategies()` skips underscore
modules, so this sample will not be auto-loaded as a real strategy.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

from core import register_strategy
from core.audit import DecisionTrace, record_decision
from core.interfaces.strategy import (
    ENTRY_FREQUENCY_ONE_PER_DAY,
    ENTRY_FREQUENCY_UNLIMITED,
    POSITION_MODE_MULTI,
    POSITION_MODE_SINGLE,
    PositionPolicy,
    ProtectiveStopSpec,
    StrategyKernel,
    StrategySpec,
)
from core.types import Instrument, MarketContext, Position, Signal


# Keep shared objects at module level. This avoids re-creating Instrument and
# timezone objects on every bar.
QQQ = Instrument(asset_class="equity", symbol="QQQ")
MARKET_TZ = ZoneInfo("America/New_York")


@register_strategy
class SampleStrategy(StrategyKernel):
    """Copy-only example showing the standard strategy shape."""

    # Entry evaluation cadence. fast-event backtests use this to decide when
    # generate() needs to run. Open-position exits still run on every 1m bar.
    _BAR_SIZE = "3m"
    _PARALLEL_BACKTEST_SAFE = True

    SPEC = StrategySpec(
        # Stable runtime id. Config, logs, API metadata, and position adoption
        # use this value, so do not rely on the filename as the strategy id.
        id="sample_strategy",

        # primary_instrument is the stream that wakes this strategy up.
        # execution_instrument is the symbol the returned Signal will trade.
        primary_instrument=QQQ,
        execution_instrument=QQQ,

        # Declare every raw bar timeframe the strategy depends on. This lets
        # the engine warm up/cache/resample shared bars once, and lets
        # fast-event auto-detect the entry cadence from _BAR_SIZE.
        # Keep proprietary indicator details out of SPEC.indicators when
        # possible; use ctx.features.get(...) inside strategy logic instead.
        timeframes=("1m", "3m"),
        # Warmup must cover the largest entry/exit lookback the strategy needs.
        warmup_bars={"3m": 30},

        # Optional broker-side protective stop submitted after entry fill.
        # Remove this field if the strategy owns all exits in on_exit().
        protective_stop=ProtectiveStopSpec(pct=0.015, reference="fill_price"),

        # Position policy is public metadata. It should describe trade
        # ownership and entry throttling, not proprietary signal logic.
        position_policy=PositionPolicy(
            # single_position:
            #   One open strategy position per execution instrument. While
            #   open, Engine calls on_exit() instead of generate().
            #
            # multi_position:
            #   Future strategies may hold independent logical lots on the
            #   same instrument. Each Signal should provide a unique trade_id
            #   when the strategy needs per-lot state.
            position_mode=POSITION_MODE_SINGLE,

            # one_per_day:
            #   At most one entry signal per market date.
            # one_per_session:
            #   Same current behavior as market date; reserved for richer
            #   session calendars later.
            # unlimited:
            #   No framework entry-frequency throttle.
            entry_frequency=ENTRY_FREQUENCY_ONE_PER_DAY,

            # For single_position keep this as 1. For multi_position set a
            # real cap unless there is a deliberate reason to leave it None.
            max_concurrent_positions=1,
        ),
    )

    # Strategy constants are okay here. In private strategies, keep sensitive
    # formulas and thresholds inside the private file, not in shared modules,
    # public docs, tests, config examples, or comments.
    _ENTRY_START_HHMM = 1000
    _ENTRY_END_HHMM = 1530

    def on_start(self, state: dict) -> None:
        """Initialize runtime memory.

        Use `state` for deterministic strategy memory. Do not store mutable
        runtime state on `self`; live strategy calls may run in worker threads.
        """
        state["last_evaluated_3m_bar"] = None

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        """Return an entry Signal or None.

        Rules for generate():
        - no broker calls
        - no file, network, or database I/O
        - no logging of proprietary formulas
        - no mutation outside the provided state dict
        - no manual resampling inside the strategy hot path
        - use ctx.features.get(...) for common indicators
        - use ctx.features.bars(..., lookback_bars=N) for private bar windows
        - use ctx.features.latest_bar(...) when only the current bar is needed
        """
        trace = DecisionTrace.entry(ctx, self.SPEC.id)

        def finish(
            decision: str,
            reason: str,
            signal: Signal | None = None,
        ) -> Signal | None:
            trace.set_decision(decision, reason=reason, signal=signal)
            record_decision(state, trace)
            return signal

        required_3m = self.SPEC.warmup_bars["3m"]
        if ctx.features:
            bars_3m = ctx.features.bars(QQQ, "3m", lookback_bars=required_3m)
        else:
            bars_3m = ctx.bars.get(QQQ, {}).get("3m")
            if bars_3m is not None:
                bars_3m = bars_3m.iloc[-required_3m:]
        if bars_3m is None or len(bars_3m) < required_3m:
            trace.add_metric("bars_3m_count", 0 if bars_3m is None else len(bars_3m))
            return finish("no_signal", "insufficient_3m_bars")

        now = ctx.timestamp
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        market_now = now.astimezone(MARKET_TZ)
        market_hhmm = market_now.hour * 100 + market_now.minute

        in_entry_window = self._ENTRY_START_HHMM <= market_hhmm < self._ENTRY_END_HHMM
        trace.add_condition(
            "entry_window",
            in_entry_window,
            lhs=market_hhmm,
            op="start_inclusive_end_exclusive",
            rhs={"start": self._ENTRY_START_HHMM, "end": self._ENTRY_END_HHMM},
        )
        if not in_entry_window:
            # Dashboard contract: expose only sanitized operator telemetry.
            # This percent is useful for monitoring but does not reveal which
            # private rule failed. Raw condition names stay in audit traces.
            trace.set_entry_readiness(
                [in_entry_window, False],
                label="Waiting for setup",
            )
            return finish("no_signal", "outside_entry_window")

        # Use latest_bar() when only the current completed bar is needed. It is
        # cheaper than allocating a one-row DataFrame slice, especially during
        # open-position exit checks.
        latest_3m = ctx.features.latest_bar(QQQ, "3m") if ctx.features else bars_3m.iloc[-1]
        if latest_3m is None:
            return finish("no_signal", "missing_latest_3m_bar")
        current_3m_bar = latest_3m.name
        already_evaluated = state.get("last_evaluated_3m_bar") == current_3m_bar
        trace.add_condition(
            "one_evaluation_per_completed_3m_bar",
            not already_evaluated,
            lhs=state.get("last_evaluated_3m_bar"),
            op="!=",
            rhs=current_3m_bar,
        )
        if already_evaluated:
            return finish("no_signal", "already_evaluated_3m_bar")
        state["last_evaluated_3m_bar"] = current_3m_bar

        # For multi-bar setup/confirmation patterns, preserve the research
        # runner's candidate lifecycle exactly. If a setup is consumed by its
        # first confirmation attempt, do not let a later bar reuse that same
        # setup just because final filters rejected the first candidate.
        #
        # Dashboard contract: when the strategy has raw same-day trigger/setup
        # action times, pass only the timestamps with set_trigger_times().
        # Do not pass rule names, counts, thresholds, scores, or formulas; the
        # dashboard uses these times only to show whether a possible trigger
        # occurred today. For completed 3m bars, action time is bar start + 3m.
        raw_trigger_times = []

        # Preferred shared-indicator style. This keeps SPEC minimal and avoids
        # advertising every private feature dependency through metadata.
        ema_fast = ctx.features.get("ema", QQQ, "3m", period=9) if ctx.features else None
        ema_slow = ctx.features.get("ema", QQQ, "3m", period=21) if ctx.features else None
        if ema_fast is None or ema_slow is None or len(ema_fast) == 0 or len(ema_slow) == 0:
            return finish("no_signal", "missing_features")

        # Placeholder condition. Replace this with the real strategy logic.
        condition_ok = bool(ema_fast.iloc[-1] > ema_slow.iloc[-1])
        if condition_ok:
            raw_trigger_times.append(current_3m_bar + timedelta(minutes=3))
        trace.set_trigger_times(raw_trigger_times)
        trace.add_condition(
            "sample_condition",
            condition_ok,
            lhs=float(ema_fast.iloc[-1]),
            op=">",
            rhs=float(ema_slow.iloc[-1]),
        )
        if not condition_ok:
            # Dashboard contract: count only the entry gates this strategy
            # deliberately wants represented as a percent. Do not derive this
            # from every trace.add_condition(); some conditions are diagnostics
            # or context and should not affect the operator-facing percent.
            trace.set_entry_readiness(
                [in_entry_window, not already_evaluated, condition_ok],
                label="Entry filters not met",
            )
            return finish("no_signal", "sample_condition_failed")

        # Dashboard contract: if a strategy computes a per-entry runtime stop
        # such as ATR, add these metrics and pass protective_stop_pct on the
        # Signal. Otherwise the dashboard will show the static SPEC fallback.
        #
        # Example:
        # entry_stop_pct = 0.0125
        # trace.add_metric("entry_stop_pct", entry_stop_pct)
        # trace.add_metric("entry_stop_pct_source", "atr")
        # signal = Signal(
        #     instrument=self.SPEC.execution_instrument,
        #     side="long",
        #     protective_stop_pct=entry_stop_pct,
        # )

        # For multi_position strategies that need independent exit state, pass
        # a deterministic trade_id, for example:
        # Signal(instrument=QQQ, side="long", trade_id=f"entry_{current_3m_bar:%Y%m%d_%H%M}")
        signal = Signal(instrument=self.SPEC.execution_instrument, side="long")
        return finish("signal", "sample_condition_passed", signal)

    def on_exit(
        self,
        ctx: MarketContext,
        position: Position,
        state: dict,
    ) -> str | None:
        """Return an exit reason string, or None to hold.

        In multi_position mode, `position.trade_id` identifies the logical lot
        currently being evaluated. Store per-lot state under that key.
        on_exit() is called every 1m bar for each open lot, so avoid rebuilding
        DataFrames or indicators here unless the exit rule truly needs them.
        """
        latest_1m = ctx.features.latest_bar(QQQ, "1m") if ctx.features else None
        # Use latest_1m for stop/target checks that only need the current bar.
        _ = ctx, position, state
        _ = latest_1m
        return None


# These imported constants are intentionally referenced in comments above.
# Keeping the aliases here makes copy/paste changes obvious for new strategies.
_POLICY_EXAMPLES = (
    POSITION_MODE_MULTI,
    ENTRY_FREQUENCY_UNLIMITED,
)

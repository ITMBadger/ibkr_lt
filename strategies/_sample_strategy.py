"""Sample strategy scaffold.

Copy this file to `strategies/my_strategy.py`, rename the class and SPEC.id,
then replace the placeholder entry/exit logic.

The leading underscore is intentional: `load_strategies()` skips underscore
modules, so this sample will not be auto-loaded as a real strategy.
"""

from __future__ import annotations

from datetime import timezone
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

    SPEC = StrategySpec(
        # Stable runtime id. Config, logs, API metadata, and position adoption
        # use this value, so do not rely on the filename as the strategy id.
        id="sample_strategy",

        # primary_instrument is the stream that wakes this strategy up.
        # execution_instrument is the symbol the returned Signal will trade.
        primary_instrument=QQQ,
        execution_instrument=QQQ,

        # Declare only timeframes the engine must build before generate().
        # Keep proprietary indicator details out of SPEC.indicators when
        # possible; use ctx.features.get(...) inside strategy logic instead.
        timeframes=("1m", "3m"),
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

        bars_3m = ctx.bars.get(QQQ, {}).get("3m")
        if bars_3m is None or len(bars_3m) < self.SPEC.warmup_bars["3m"]:
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
            return finish("no_signal", "outside_entry_window")

        current_3m_bar = bars_3m.index[-1]
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

        # Preferred shared-indicator style. This keeps SPEC minimal and avoids
        # advertising every private feature dependency through metadata.
        ema_fast = ctx.features.get("ema", QQQ, "3m", period=9) if ctx.features else None
        ema_slow = ctx.features.get("ema", QQQ, "3m", period=21) if ctx.features else None
        if ema_fast is None or ema_slow is None or len(ema_fast) == 0 or len(ema_slow) == 0:
            return finish("no_signal", "missing_features")

        # Placeholder condition. Replace this with the real strategy logic.
        condition_ok = bool(ema_fast.iloc[-1] > ema_slow.iloc[-1])
        trace.add_condition(
            "sample_condition",
            condition_ok,
            lhs=float(ema_fast.iloc[-1]),
            op=">",
            rhs=float(ema_slow.iloc[-1]),
        )
        if not condition_ok:
            return finish("no_signal", "sample_condition_failed")

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
        """
        _ = ctx, position, state
        return None


# These imported constants are intentionally referenced in comments above.
# Keeping the aliases here makes copy/paste changes obvious for new strategies.
_POLICY_EXAMPLES = (
    POSITION_MODE_MULTI,
    ENTRY_FREQUENCY_UNLIMITED,
)

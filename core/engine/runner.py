"""Engine — single async implementation for live trading and backtesting.

ONE _run() coroutine. run_live() and run_backtest() are thin entry points
that inject different adapters and clock into the same engine code.

Live:     WallClock + IBKRBroker + IBKRDataProvider + thread_pool_workers=4+
Backtest: SimulatedClock + PaperBroker + ReplayDataProvider + thread_pool_workers=1

The Phase 4 acceptance test asserts byte-identical signal logs from both paths
on the same replay data, proving there is one engine, not two.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from ..audit.serialize import to_jsonable
from ..data.bar_builder import BarBuilder
from ..data.feed import DataFeed
from ..data.manager import DataManager
from ..orders.order_manager import OrderManager
from ..orders.strategy_modes import strategy_mode_map
from ..features.registry import FeatureRegistry
from ..audit import AuditLogger, pop_decision
from .timeframes import TF_1M, TF_5S, Timeframe
from ..portfolio.state import PortfolioState
from ..interfaces.strategy import (
    ENTRY_FREQUENCY_ONE_PER_DAY,
    ENTRY_FREQUENCY_ONE_PER_SESSION,
    ENTRY_FREQUENCY_UNLIMITED,
    POSITION_MODE_MULTI,
    StrategyKernel,
)
from ..risk.policy import RiskPolicy
from ..types import Bar, Fill, Instrument, Position, PositionAdoption, Signal
from .clock import Clock, SimulatedClock, WallClock
from .scheduler import Scheduler

if TYPE_CHECKING:
    from ..interfaces.broker import BrokerAdapter
    from ..interfaces.data import HistoricalDataProvider, StreamingDataProvider

log = logging.getLogger(__name__)

_ORDER_TASK_YIELD_SECONDS = 0.001
_DISPATCH_MODE_EVENT = "event"
_DISPATCH_MODE_FAST_EVENT = "fast_event"
_DISPATCH_MODE_PARALLEL = "parallel"
_PHASE_AWAITING_STARTUP_MAPPING = "awaiting_startup_mapping"


class Engine:
    """Parameterised trading engine.

    Inject adapters at construction time; call run_live() or run_backtest().
    """

    def __init__(
        self,
        broker: "BrokerAdapter",
        streaming: "StreamingDataProvider | None" = None,
        historical: "HistoricalDataProvider | None" = None,
        data_feed: DataFeed | None = None,
        clock: Clock | None = None,
        strategies: list[tuple[StrategyKernel, dict]] | None = None,
        risk: RiskPolicy | None = None,
        strategy_risk: Mapping[str, RiskPolicy] | None = None,
        thread_pool_workers: int = 4,
        lookback_days: int = 500,
        session_tz: str = "America/New_York",
        adopted_position_map: dict[Instrument, str] | None = None,
        audit_logger: AuditLogger | None = None,
        strategy_modes: Mapping[str, str] | None = None,
        dispatch_mode: str = _DISPATCH_MODE_EVENT,
        evaluation_timeframes: Mapping[str, str] | None = None,
        precomputed_entry_signals: Mapping[str, Sequence[tuple[datetime, Signal]]] | None = None,
        feature_preload_bars: list[Bar] | None = None,
        progress_enabled: bool = False,
        progress_total_bars: int | None = None,
        progress_interval_bars: int = 1000,
        progress_interval_seconds: float = 30.0,
        startup_position_gate_enabled: bool = False,
    ) -> None:
        self._broker = broker
        if data_feed is not None:
            self._data_feed = data_feed
        else:
            if streaming is None:
                raise ValueError("Engine requires either streaming or data_feed")
            self._data_feed = DataFeed(historical, streaming)
        self._clock = clock or WallClock()
        self._strategies = strategies or []
        strategy_ids = [kernel.SPEC.id for kernel, _ in self._strategies]
        self._strategy_modes = strategy_mode_map(strategy_modes, strategy_ids)
        self._dispatch_mode = _normalize_dispatch_mode(dispatch_mode)
        self._evaluation_timeframes = _normalize_evaluation_timeframes(
            evaluation_timeframes
        )
        self._precomputed_entry_signals = _normalize_precomputed_entry_signals(
            precomputed_entry_signals
        )
        self._feature_preload_bars = list(feature_preload_bars or [])
        self._progress = _EngineProgress(
            enabled=progress_enabled,
            total_bars=progress_total_bars,
            interval_bars=progress_interval_bars,
            interval_seconds=progress_interval_seconds,
        )
        self._risk = risk or RiskPolicy()
        self._strategy_risk = dict(strategy_risk or {})
        self._pool_workers = thread_pool_workers
        self._lookback_days = lookback_days
        self._session_tz = session_tz
        self._adopted_position_map = adopted_position_map or {}
        self._audit = audit_logger
        self._state_lock = RLock()
        self._phase = "initialized"
        self._broker_connected = False
        self._data_connected = False
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None
        self._last_error: str | None = None
        self._last_bar: dict[str, Any] | None = None
        self._bar_count = 0
        self._managers: dict[Instrument, DataManager] = {}
        self._portfolio: PortfolioState | None = None
        self._recent_events: list[dict[str, Any]] = []
        self._startup_position_gate_enabled = bool(startup_position_gate_enabled)
        self._startup_gate_status: dict[str, Any] = {
            "enabled": self._startup_position_gate_enabled,
            "phase": "inactive",
            "message": "",
            "positions": [],
            "allocations": [],
            "unmanaged": [],
            "last_error": None,
        }
        self._startup_gate_event: asyncio.Event | None = None
        self._startup_gate_loop: asyncio.AbstractEventLoop | None = None
        self._startup_gate_action: str | None = None
        self._startup_gate_submitted_allocations: list[dict[str, Any]] | None = None

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_live(self) -> None:
        """Start live trading. Blocks until interrupted."""
        asyncio.run(self._run())

    def run_backtest(self) -> None:
        """Run backtest to completion. Blocks until all replay bars are consumed."""
        asyncio.run(self._run())

    def snapshot_state(self) -> dict[str, Any]:
        """Return a JSON-safe read-only runtime snapshot for control APIs."""
        now = datetime.now(tz=timezone.utc)
        with self._state_lock:
            managers = dict(self._managers)
            portfolio = self._portfolio
            state = {
                "timestamp_utc": now.isoformat(),
                "phase": self._phase,
                "running": self._phase in {
                    "starting",
                    "running",
                    _PHASE_AWAITING_STARTUP_MAPPING,
                },
                "started_at": to_jsonable(self._started_at),
                "stopped_at": to_jsonable(self._stopped_at),
                "last_error": self._last_error,
                "connection": {
                    "broker_connected": self._broker_connected,
                    "data_connected": self._data_connected,
                    "connected": self._broker_connected and self._data_connected,
                },
                "broker": {
                    "name": getattr(self._broker, "name", self._broker.__class__.__name__),
                    "capabilities": to_jsonable(getattr(self._broker, "capabilities", None)),
                },
                "data": {
                    "capabilities": to_jsonable(getattr(self._data_feed, "capabilities", None)),
                    "instruments": [
                        to_jsonable(instrument)
                        for instrument in sorted(managers, key=lambda item: item.symbol)
                    ],
                    "latest_bars": {
                        instrument.symbol: to_jsonable(manager.latest_timestamp())
                        for instrument, manager in sorted(
                            managers.items(),
                            key=lambda item: item[0].symbol,
                        )
                    },
                    "bar_count": self._bar_count,
                    "last_bar": to_jsonable(self._last_bar),
                },
                "strategies": [
                    {
                        "id": kernel.SPEC.id,
                        "primary_instrument": to_jsonable(kernel.SPEC.primary_instrument),
                        "execution_instrument": to_jsonable(kernel.SPEC.execution_instrument),
                        "reference_instruments": to_jsonable(kernel.SPEC.reference_instruments),
                        "timeframes": list(kernel.SPEC.timeframes),
                        "warmup_bars": dict(kernel.SPEC.warmup_bars),
                        "protective_stop": to_jsonable(kernel.SPEC.protective_stop),
                        "position_policy": to_jsonable(kernel.SPEC.position_policy),
                        "mode": self._strategy_modes.get(kernel.SPEC.id, "live"),
                        "evaluation_timeframe": (
                            self._evaluation_timeframes[kernel.SPEC.id].label
                            if kernel.SPEC.id in self._evaluation_timeframes
                            else None
                        ),
                        "precomputed_entry_signals": sum(
                            len(signals)
                            for signals in self._precomputed_entry_signals
                            .get(kernel.SPEC.id, {})
                            .values()
                        ),
                    }
                    for kernel, _ in self._strategies
                ],
                "dispatch_mode": self._dispatch_mode,
                "risk": {
                    "position_size_shares": self._risk.position_size_shares,
                    "max_order_quantity": self._risk.max_order_quantity,
                    "strategy_risk": {
                        strategy_id: {
                            "position_size_shares": risk.position_size_shares,
                            "max_order_quantity": risk.max_order_quantity,
                        }
                        for strategy_id, risk in sorted(self._strategy_risk.items())
                    },
                },
                "startup_gate": to_jsonable(dict(self._startup_gate_status)),
                "recent_events": list(self._recent_events),
            }

        state["positions"] = {
            "broker": to_jsonable(portfolio.positions()) if portfolio is not None else [],
            "strategy": [
                {"strategy_id": sid, "position": to_jsonable(position)}
                for sid, position in portfolio.strategy_positions()
            ] if portfolio is not None else [],
            "strategy_lots": [
                {"strategy_id": sid, "position": to_jsonable(position)}
                for sid, position in portfolio.strategy_position_lots()
            ] if portfolio is not None else [],
            "net_liquidation": portfolio.net_liquidation() if portfolio is not None else 0.0,
        }
        progress = self._progress.snapshot()
        if progress:
            state["progress"] = progress
        return state

    # ------------------------------------------------------------------
    # Core coroutine
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        self._set_runtime_state(
            phase="starting",
            started_at=datetime.now(tz=timezone.utc),
            stopped_at=None,
            last_error=None,
        )
        self._progress.start()
        loop = asyncio.get_running_loop()
        is_simulated = isinstance(self._clock, SimulatedClock)

        # Build DataManagers for strategy data. Simulated paper replay also
        # needs execution-instrument bars so PaperBroker can resolve fills.
        strategy_data_instruments: set[Instrument] = set()
        execution_instruments: set[Instrument] = set()
        for kernel, _ in self._strategies:
            strategy_data_instruments.add(kernel.SPEC.primary_instrument)
            strategy_data_instruments.update(kernel.SPEC.reference_instruments)
            execution_instruments.add(kernel.SPEC.execution_instrument)
        all_instruments = set(strategy_data_instruments)
        if is_simulated and hasattr(self._broker, "on_bar"):
            all_instruments.update(execution_instruments)

        managers: dict[Instrument, DataManager] = {
            instr: DataManager(instr, self._lookback_days, self._session_tz)
            for instr in all_instruments
        }
        self._set_managers(managers)
        features = FeatureRegistry(managers)
        bar_builders: dict[Instrument, BarBuilder] = {}
        strategy_entries: list[tuple[StrategyKernel, dict]] = []
        for kernel, initial_state in self._strategies:
            state: dict = dict(initial_state)
            kernel.on_start(state)
            strategy_entries.append((kernel, state))

        self._progress.info(
            "Engine setup starting strategies=%s dispatch_mode=%s progress_total_bars=%s",
            [kernel.SPEC.id for kernel, _ in self._strategies],
            self._dispatch_mode,
            self._progress.total_bars,
        )

        # Connect broker
        setup_t0 = time.perf_counter()
        await self._broker.connect()
        self._set_runtime_state(broker_connected=True)
        await self._data_feed.connect()
        self._set_runtime_state(data_connected=True)
        self._progress.add_timing("setup_connect", time.perf_counter() - setup_t0)

        # Portfolio state and order manager
        portfolio = PortfolioState()
        self._set_portfolio(portfolio)
        account_snapshot = await self._broker.get_account()
        sizing_account = _MarkToMarketSizingState(account_snapshot.net_liquidation)
        adopted = await self._broker.get_positions()
        if adopted:
            if self._startup_position_gate_enabled:
                portfolio.adopt_positions(adopted)
                allocations = await self._run_startup_position_gate(
                    adopted,
                    strategy_entries,
                )
                self._adopt_startup_position_allocations(
                    allocations,
                    adopted,
                    strategy_entries,
                    portfolio,
                )
            else:
                strategy_map = self._resolve_adopted_position_map(adopted)
                portfolio.adopt_positions(adopted, strategy_map)
                for position in adopted:
                    sid = strategy_map.get(position.instrument)
                    owner = sid if sid else "unmapped"
                    log.warning(
                        "Adopted broker position: %s %.4f avg_cost=%.4f owner=%s",
                        position.instrument.symbol,
                        position.quantity,
                        position.avg_cost,
                        owner,
                    )
        elif self._startup_position_gate_enabled:
            self._set_startup_gate_status({
                "enabled": True,
                "phase": "clear",
                "message": "No broker positions found; startup can continue.",
                "positions": [],
                "allocations": [],
                "unmanaged": [],
                "last_error": None,
            })
        protective_stops = {
            kernel.SPEC.id: kernel.SPEC.protective_stop
            for kernel, _ in self._strategies
            if kernel.SPEC.protective_stop is not None
        }
        position_policies = {
            kernel.SPEC.id: kernel.SPEC.position_policy
            for kernel, _ in self._strategies
        }
        order_manager = OrderManager(
            self._broker,
            portfolio,
            self._risk,
            self._audit,
            protective_stops=protective_stops,
            strategy_modes=self._strategy_modes,
            position_policies=position_policies,
            sizing_price_provider=sizing_account.latest_price,
            sizing_equity_provider=sizing_account.equity,
            fill_listener=sizing_account.apply_fill,
        )

        # Backfill historical data. Split feeds may load offline history first
        # and supplement the gap with broker historical bars before live starts.
        end = self._clock.now()
        if isinstance(self._clock, SimulatedClock) and end.year == 1:
            end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=self._lookback_days)
        for instr, dm in managers.items():
            try:
                fetch_t0 = time.perf_counter()
                self._progress.info(
                    "Backfill fetch starting instrument=%s lookback_days=%d start=%s end=%s",
                    instr.symbol,
                    self._lookback_days,
                    start.isoformat(),
                    end.isoformat(),
                )
                bars = await self._data_feed.fetch(instr, TF_1M, start, end)
                self._progress.add_timing("setup_backfill_fetch", time.perf_counter() - fetch_t0)
                merge_t0 = time.perf_counter()
                dm.merge_backfill(bars)
                self._progress.add_timing("setup_backfill_merge", time.perf_counter() - merge_t0)
                log.info("Backfilled %d bars for %s", len(bars), instr.symbol)
                self._record_event(
                    "data",
                    "backfill_complete",
                    instrument=instr.symbol,
                    bars=len(bars),
                )
            except Exception as e:
                log.warning("Backfill failed for %s: %s", instr.symbol, e)
                self._record_event(
                    "data",
                    "backfill_failed",
                    instrument=instr.symbol,
                    error=str(e),
                )

        preload_t0 = time.perf_counter()
        features.preload_from_managers()
        self._progress.add_timing("setup_feature_preload_managers", time.perf_counter() - preload_t0)
        if self._feature_preload_bars:
            preload_t0 = time.perf_counter()
            features.preload_bars(self._feature_preload_bars)
            self._progress.add_timing("setup_feature_preload_replay", time.perf_counter() - preload_t0)
            self._progress.info(
                "Feature replay preload complete bars=%d",
                len(self._feature_preload_bars),
            )
        features_are_replay_preloaded = is_simulated and bool(self._feature_preload_bars)

        # Subscribe streaming
        for instr in all_instruments:
            subscribe_t0 = time.perf_counter()
            native_tfs = self._data_feed.capabilities.native_timeframes
            if native_tfs and TF_5S in native_tfs:
                # IBKR: subscribe at 5s, build 1m
                await self._data_feed.subscribe(instr, TF_5S)
                bar_builders[instr] = BarBuilder(instr, TF_5S, TF_1M)
            else:
                # Replay / moomoo: subscribe at 1m directly
                await self._data_feed.subscribe(instr, TF_1M)
            self._record_event("data", "subscribed", instrument=instr.symbol)
            self._progress.add_timing("setup_subscribe", time.perf_counter() - subscribe_t0)

        # Register strategies in scheduler
        scheduler = Scheduler(features)
        for kernel, state in strategy_entries:
            scheduler.register(kernel, state)
        last_evaluation_bars: dict[str, datetime] = {}

        # Start background tasks
        pool = ThreadPoolExecutor(max_workers=self._pool_workers)
        drain_orders_task = None
        drain_fills_task = None
        drain_order_updates_task = None
        if not is_simulated:
            drain_orders_task = loop.create_task(order_manager.drain_orders())
            drain_fills_task = loop.create_task(order_manager.drain_fills())
            drain_order_updates_task = loop.create_task(order_manager.drain_order_updates())
        self._set_runtime_state(phase="running")

        async def evaluate_exit(
            kernel: StrategyKernel,
            ctx,
            state: dict,
            position: Position,
        ) -> None:
            self._progress.count("exit_evals")
            strategy_t0 = time.perf_counter()
            if is_simulated:
                reason = kernel.on_exit(ctx, position, state)
            else:
                reason = await loop.run_in_executor(
                    pool, kernel.on_exit, ctx, position, state
                )
            elapsed = time.perf_counter() - strategy_t0
            self._progress.add_strategy_timing(kernel.SPEC.id, "exit", elapsed)
            audit_t0 = time.perf_counter()
            self._write_decision_trace(state)
            self._progress.add_timing("audit_decision", time.perf_counter() - audit_t0)
            if not reason:
                return

            self._progress.count("exit_signals")
            order_t0 = time.perf_counter()
            self._write_signal_event(
                kernel.SPEC.id,
                "exit",
                ctx.timestamp,
                reason=reason,
                instrument=position.instrument,
                side=position.side,
                trade_id=position.trade_id,
            )
            await order_manager.submit_close(
                kernel.SPEC.id,
                position,
                reason,
            )
            self._progress.add_timing("signal_order", time.perf_counter() - order_t0)

        async def evaluate_entry(kernel: StrategyKernel, ctx, state: dict) -> None:
            if self._dispatch_mode == _DISPATCH_MODE_PARALLEL:
                signals = self._precomputed_signals_at(kernel.SPEC.id, ctx.timestamp)
                if not signals:
                    self._progress.count("parallel_entry_misses")
                    return
                for signal in signals:
                    if not self._entry_frequency_allows(kernel, state, ctx.timestamp):
                        self._progress.count("entry_frequency_skips")
                        continue
                    self._progress.count("entry_evals")
                    self._mark_entry_frequency(kernel, state, ctx.timestamp)
                    self._progress.count("entry_signals")
                    order_t0 = time.perf_counter()
                    self._write_signal_event(
                        kernel.SPEC.id,
                        "entry",
                        ctx.timestamp,
                        signal=signal,
                        trade_id=signal.trade_id,
                        source="precomputed",
                    )
                    await order_manager.submit(signal, kernel.SPEC.id)
                    if is_simulated:
                        await order_manager.drain_ready_orders()
                        await order_manager.drain_ready_order_updates()
                    else:
                        await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                    self._progress.add_timing("signal_order", time.perf_counter() - order_t0)
                return

            if not self._entry_frequency_allows(kernel, state, ctx.timestamp):
                self._progress.count("entry_frequency_skips")
                return

            self._progress.count("entry_evals")
            strategy_t0 = time.perf_counter()
            if is_simulated:
                signal = kernel.generate(ctx, state)
            else:
                signal = await loop.run_in_executor(
                    pool, kernel.generate, ctx, state
                )
            elapsed = time.perf_counter() - strategy_t0
            self._progress.add_strategy_timing(kernel.SPEC.id, "entry", elapsed)
            audit_t0 = time.perf_counter()
            self._write_decision_trace(state)
            self._progress.add_timing("audit_decision", time.perf_counter() - audit_t0)
            if signal is None:
                return

            self._mark_entry_frequency(kernel, state, ctx.timestamp)
            self._progress.count("entry_signals")
            order_t0 = time.perf_counter()
            self._write_signal_event(
                kernel.SPEC.id,
                "entry",
                ctx.timestamp,
                signal=signal,
                trade_id=signal.trade_id,
            )
            await order_manager.submit(signal, kernel.SPEC.id)
            if is_simulated:
                await order_manager.drain_ready_orders()
                await order_manager.drain_ready_order_updates()
            else:
                await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
            self._progress.add_timing("signal_order", time.perf_counter() - order_t0)

        try:
            async for raw_bar in self._data_feed.bars():
                bar_t0 = time.perf_counter()
                # Advance simulated clock before any processing
                if is_simulated:
                    self._clock.advance_to(raw_bar.timestamp)

                # Resolve to 1-min bar (BarBuilder or pass-through)
                builder = bar_builders.get(raw_bar.instrument)
                if builder is not None:
                    bar_1m = builder.on_bar(raw_bar)
                    if bar_1m is None:
                        continue
                else:
                    bar_1m = raw_bar
                self._progress.add_timing("bar_prepare", time.perf_counter() - bar_t0)

                # Update DataManager
                dm = managers.get(bar_1m.instrument)
                if dm is None:
                    continue
                stage_t0 = time.perf_counter()
                dm.on_bar(bar_1m)
                self._record_bar(bar_1m)
                sizing_account.update_bar(bar_1m)
                if not features_are_replay_preloaded:
                    features.on_bar(bar_1m)
                self._progress.add_timing("data_update", time.perf_counter() - stage_t0)

                # Let PaperBroker resolve pending orders on new bar
                if hasattr(self._broker, "on_bar"):
                    stage_t0 = time.perf_counter()
                    await self._broker.on_bar(bar_1m)
                    if is_simulated:
                        await order_manager.drain_ready_fills()
                        await order_manager.drain_ready_order_updates()
                    else:
                        await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                    self._progress.add_timing("broker_on_bar", time.perf_counter() - stage_t0)

                # Invalidate feature caches for this instrument
                stage_t0 = time.perf_counter()
                features.invalidate(bar_1m.instrument)
                self._progress.add_timing("feature_invalidate", time.perf_counter() - stage_t0)

                # Dispatch to strategies via thread pool. Fast-event mode still
                # evaluates open-position exits on every primary bar, but it
                # skips flat entry context builds until the evaluation bar rolls.
                include_strategy = None
                if self._dispatch_mode == _DISPATCH_MODE_FAST_EVENT:

                    def include_strategy(
                        kernel: StrategyKernel,
                        _state: dict,
                    ) -> bool:
                        if kernel.SPEC.position_policy.position_mode == POSITION_MODE_MULTI:
                            positions = portfolio.get_strategy_positions(
                                kernel.SPEC.id,
                                kernel.SPEC.execution_instrument,
                            )
                        else:
                            position = portfolio.get_strategy_position(
                                kernel.SPEC.id,
                                kernel.SPEC.execution_instrument,
                            )
                            positions = [position] if position is not None else []
                        if positions:
                            self._progress.count("fast_event_exit_contexts")
                            return True
                        should_generate = self._should_generate_entry(
                            kernel,
                            managers,
                            last_evaluation_bars,
                        )
                        if should_generate:
                            self._progress.count("fast_event_entry_contexts")
                        else:
                            self._progress.count("fast_event_entry_skips")
                        return should_generate
                elif self._dispatch_mode == _DISPATCH_MODE_PARALLEL:

                    def include_strategy(
                        kernel: StrategyKernel,
                        _state: dict,
                    ) -> bool:
                        if kernel.SPEC.position_policy.position_mode == POSITION_MODE_MULTI:
                            positions = portfolio.get_strategy_positions(
                                kernel.SPEC.id,
                                kernel.SPEC.execution_instrument,
                            )
                        else:
                            position = portfolio.get_strategy_position(
                                kernel.SPEC.id,
                                kernel.SPEC.execution_instrument,
                            )
                            positions = [position] if position is not None else []
                        if positions:
                            self._progress.count("parallel_exit_contexts")
                            return True
                        if self._has_precomputed_signal_at(kernel.SPEC.id, bar_1m.timestamp):
                            self._progress.count("parallel_entry_contexts")
                            return True
                        self._progress.count("parallel_entry_skips")
                        return False

                stage_t0 = time.perf_counter()
                dispatch_results = scheduler.on_bar(
                    bar_1m,
                    managers,
                    include=include_strategy,
                )
                self._progress.add_timing("scheduler_context", time.perf_counter() - stage_t0)
                self._progress.count("dispatch_contexts", len(dispatch_results))
                for kernel, ctx, state in dispatch_results:
                    try:
                        policy = kernel.SPEC.position_policy
                        if policy.position_mode == POSITION_MODE_MULTI:
                            positions = portfolio.get_strategy_positions(
                                kernel.SPEC.id,
                                kernel.SPEC.execution_instrument,
                            )
                            had_positions = bool(positions)
                            for position in positions:
                                await evaluate_exit(kernel, ctx, state, position)
                            if not self._has_entry_capacity(kernel, len(positions)):
                                self._progress.count("entry_capacity_skips")
                                continue
                            if had_positions and not self._should_generate_entry(
                                kernel,
                                managers,
                                last_evaluation_bars,
                            ):
                                self._progress.count("fast_event_entry_skips")
                                continue
                            await evaluate_entry(kernel, ctx, state)
                            continue

                        position = portfolio.get_strategy_position(
                            kernel.SPEC.id,
                            kernel.SPEC.execution_instrument,
                        )
                        if position is not None:
                            await evaluate_exit(kernel, ctx, state, position)
                            continue
                        await evaluate_entry(kernel, ctx, state)
                    except Exception as e:
                        self._progress.count("strategy_errors")
                        log.exception("Strategy %s raised: %s", kernel.SPEC.id, e)
                        self._record_event(
                            "strategy",
                            "strategy_error",
                            strategy_id=kernel.SPEC.id,
                            error=str(e),
                        )
                        self._write_signal_event(
                            kernel.SPEC.id,
                            "error",
                            ctx.timestamp,
                            error=str(e),
                        )
                self._progress.count_bar(bar_1m.timestamp)
                self._progress.maybe_report()

        except Exception as e:
            log.exception("Engine _run error: %s", e)
            self._set_runtime_state(phase="error", last_error=str(e))
            self._record_event("engine", "engine_error", error=str(e))
        finally:
            if drain_orders_task is not None:
                drain_orders_task.cancel()
            if drain_fills_task is not None:
                drain_fills_task.cancel()
            if drain_order_updates_task is not None:
                drain_order_updates_task.cancel()
            pool.shutdown(wait=False)
            await self._data_feed.disconnect()
            self._set_runtime_state(data_connected=False)
            await self._broker.disconnect()
            self._progress.finish()
            phase = "error" if self._last_error else "stopped"
            self._set_runtime_state(
                phase=phase,
                broker_connected=False,
                stopped_at=datetime.now(tz=timezone.utc),
            )

    def _resolve_adopted_position_map(
        self,
        positions: list[Position],
    ) -> dict[Instrument, str]:
        resolved: dict[Instrument, str] = {}
        by_execution: dict[Instrument, list[str]] = {}
        for kernel, _ in self._strategies:
            by_execution.setdefault(kernel.SPEC.execution_instrument, []).append(kernel.SPEC.id)

        for position in positions:
            configured = self._adopted_position_map.get(position.instrument)
            if configured:
                resolved[position.instrument] = configured
                continue
            matches = by_execution.get(position.instrument, [])
            if len(matches) == 1:
                resolved[position.instrument] = matches[0]
            elif len(matches) > 1:
                log.warning(
                    "Position %s is ambiguous across strategies %s; add adopted_position_map",
                    position.instrument.symbol,
                    matches,
                )
        return resolved

    def _should_generate_entry(
        self,
        kernel: StrategyKernel,
        managers: Mapping[Instrument, DataManager],
        last_evaluation_bars: dict[str, datetime],
    ) -> bool:
        if self._dispatch_mode != _DISPATCH_MODE_FAST_EVENT:
            return True

        timeframe = self._evaluation_timeframes.get(kernel.SPEC.id)
        if timeframe is None or timeframe.seconds <= TF_1M.seconds:
            return True

        manager = managers.get(kernel.SPEC.primary_instrument)
        if manager is None:
            return True

        latest_ts = manager.latest_timestamp()
        if latest_ts is None:
            return False

        latest_bar = _completed_timeframe_bar_start(latest_ts, timeframe)
        if latest_bar == last_evaluation_bars.get(kernel.SPEC.id):
            return False

        last_evaluation_bars[kernel.SPEC.id] = latest_bar
        return True

    def _has_precomputed_signal_at(self, strategy_id: str, timestamp: datetime) -> bool:
        return bool(self._precomputed_signals_at(strategy_id, timestamp))

    def _precomputed_signals_at(
        self,
        strategy_id: str,
        timestamp: datetime,
    ) -> list[Signal]:
        by_timestamp = self._precomputed_entry_signals.get(strategy_id)
        if not by_timestamp:
            return []
        return list(by_timestamp.get(_normalize_precomputed_timestamp(timestamp), ()))

    def startup_gate_status(self) -> dict[str, Any]:
        with self._state_lock:
            return to_jsonable(dict(self._startup_gate_status))

    def submit_startup_mappings(self, allocations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        with self._state_lock:
            try:
                normalized = self._validate_startup_allocations_locked(allocations)
            except ValueError as exc:
                self._startup_gate_status["last_error"] = str(exc)
                self._write_startup_order_event(
                    "startup_position_mapping_rejected",
                    reason=str(exc),
                    allocations=list(allocations),
                )
                raise
            self._startup_gate_submitted_allocations = normalized
            self._startup_gate_action = "continue"
            self._startup_gate_status["allocations"] = normalized
            self._startup_gate_status["last_error"] = None
            event = self._startup_gate_event
            loop = self._startup_gate_loop
        if event is not None and loop is not None:
            loop.call_soon_threadsafe(event.set)
        return self.startup_gate_status()

    def request_startup_gate_refresh(self) -> dict[str, Any]:
        with self._state_lock:
            self._startup_gate_action = "refresh"
            self._startup_gate_status["message"] = "Startup position refresh requested."
            event = self._startup_gate_event
            loop = self._startup_gate_loop
        if event is not None and loop is not None:
            loop.call_soon_threadsafe(event.set)
        return self.startup_gate_status()

    def _has_entry_capacity(self, kernel: StrategyKernel, open_positions: int) -> bool:
        limit = kernel.SPEC.position_policy.max_concurrent_positions
        if limit is None:
            return True
        return open_positions < limit

    def _entry_frequency_allows(
        self,
        kernel: StrategyKernel,
        state: dict,
        timestamp: datetime,
    ) -> bool:
        frequency = kernel.SPEC.position_policy.entry_frequency
        if frequency == ENTRY_FREQUENCY_UNLIMITED:
            return True

        key = self._entry_frequency_key(frequency, timestamp)
        return state.get("_policy_last_entry_frequency_key") != key

    def _mark_entry_frequency(
        self,
        kernel: StrategyKernel,
        state: dict,
        timestamp: datetime,
    ) -> None:
        frequency = kernel.SPEC.position_policy.entry_frequency
        if frequency == ENTRY_FREQUENCY_UNLIMITED:
            return
        state["_policy_last_entry_frequency_key"] = self._entry_frequency_key(
            frequency,
            timestamp,
        )

    def _entry_frequency_key(self, frequency: str, timestamp: datetime) -> str:
        ts = timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        market_tz = getattr(
            getattr(self._broker, "capabilities", None),
            "market_timezone",
            self._session_tz,
        )
        try:
            local_ts = ts.astimezone(ZoneInfo(str(market_tz)))
        except Exception:
            local_ts = ts.astimezone(ZoneInfo(self._session_tz))

        if frequency == ENTRY_FREQUENCY_ONE_PER_DAY:
            return f"day:{local_ts.date().isoformat()}"
        if frequency == ENTRY_FREQUENCY_ONE_PER_SESSION:
            return f"session:{local_ts.date().isoformat()}"
        return "unlimited"

    def _write_decision_trace(self, state: dict) -> None:
        if self._audit is None:
            return
        trace = pop_decision(state)
        if trace is not None:
            self._audit.decision(trace)

    def _write_signal_event(
        self,
        strategy_id: str,
        event: str,
        timestamp: datetime,
        **fields: Any,
    ) -> None:
        if self._audit is None:
            return
        self._audit.signal({
            "event": event,
            "strategy_id": strategy_id,
            "timestamp": timestamp,
            **fields,
        })

    def _write_startup_order_event(self, event: str, **fields: Any) -> None:
        if self._audit is not None:
            self._audit.order({"event": event, **fields})

    def _set_managers(self, managers: dict[Instrument, DataManager]) -> None:
        with self._state_lock:
            self._managers = dict(managers)

    def _set_portfolio(self, portfolio: PortfolioState) -> None:
        with self._state_lock:
            self._portfolio = portfolio

    def _set_runtime_state(self, **fields: Any) -> None:
        with self._state_lock:
            if "phase" in fields:
                self._phase = str(fields["phase"])
            if "broker_connected" in fields:
                self._broker_connected = bool(fields["broker_connected"])
            if "data_connected" in fields:
                self._data_connected = bool(fields["data_connected"])
            if "started_at" in fields:
                self._started_at = fields["started_at"]
            if "stopped_at" in fields:
                self._stopped_at = fields["stopped_at"]
            if "last_error" in fields:
                self._last_error = fields["last_error"]
        if "phase" in fields:
            self._record_event("engine", f"phase_{self._phase}", phase=self._phase)

    def _record_bar(self, bar) -> None:
        with self._state_lock:
            self._bar_count += 1
            self._last_bar = {
                "instrument": bar.instrument,
                "timeframe": bar.timeframe.label,
                "timestamp": bar.timestamp,
                "source": bar.source,
            }

    def _record_event(self, source: str, message: str, **fields: Any) -> None:
        event = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "source": source,
            "message": message,
            **to_jsonable(fields),
        }
        with self._state_lock:
            self._recent_events.append(event)
            self._recent_events = self._recent_events[-200:]

    def _set_startup_gate_status(self, status: Mapping[str, Any]) -> None:
        with self._state_lock:
            self._startup_gate_status = to_jsonable(dict(status))

    async def _run_startup_position_gate(
        self,
        positions: list[Position],
        strategy_entries: list[tuple[StrategyKernel, dict]],
    ) -> list[dict[str, Any]]:
        self._startup_gate_loop = asyncio.get_running_loop()
        self._startup_gate_event = asyncio.Event()
        current_positions = list(positions)
        while True:
            status = self._build_startup_gate_status(current_positions, strategy_entries)
            self._set_startup_gate_status(status)
            self._log_startup_gate_status(status)
            if status["phase"] == "clear":
                return []

            self._set_runtime_state(phase=_PHASE_AWAITING_STARTUP_MAPPING)
            log.warning(
                "Startup paused: %s",
                status.get("message", "broker positions require mapping"),
            )
            while True:
                await self._startup_gate_event.wait()
                self._startup_gate_event.clear()
                with self._state_lock:
                    action = self._startup_gate_action
                    self._startup_gate_action = None
                    allocations = list(self._startup_gate_submitted_allocations or [])
                    if action == "continue":
                        self._startup_gate_submitted_allocations = None
                if action == "refresh":
                    current_positions = await self._broker.get_positions()
                    break
                if action == "continue":
                    self._write_startup_order_event(
                        "startup_gate_released",
                        allocations=allocations,
                    )
                    self._set_runtime_state(phase="starting")
                    return allocations

    def _build_startup_gate_status(
        self,
        positions: list[Position],
        strategy_entries: list[tuple[StrategyKernel, dict]],
    ) -> dict[str, Any]:
        candidates_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for kernel, _ in strategy_entries:
            strategy_id = kernel.SPEC.id
            risk = self._risk_for_strategy(strategy_id)
            candidates_by_key[_instrument_match_key(kernel.SPEC.execution_instrument)].append({
                "strategy_id": strategy_id,
                "position_mode": kernel.SPEC.position_policy.position_mode,
                "mode": self._strategy_modes.get(strategy_id, "live"),
                "supports_position_adoption": (
                    kernel.SPEC.position_policy.supports_position_adoption
                ),
                "required_fields": list(kernel.POSITION_ADOPTION_REQUIRED_FIELDS),
                "position_size_shares": float(risk.position_size_shares),
                "max_order_quantity": risk.max_order_quantity,
            })

        required: list[dict[str, Any]] = []
        unmanaged: list[dict[str, Any]] = []
        for position in positions:
            item = _position_gate_item(position)
            candidates = candidates_by_key.get(_instrument_match_key(position.instrument), [])
            if not candidates:
                unmanaged.append({**item, "reason": "instrument_not_used_by_enabled_strategies"})
                continue
            required.append({**item, "candidates": candidates})

        phase = "awaiting_mapping" if required else "clear"
        message = (
            "Broker positions require API mapping before live startup can continue."
            if required
            else "No broker positions match enabled strategy execution instruments."
        )
        return {
            "enabled": True,
            "phase": phase,
            "message": message,
            "positions": required,
            "allocations": [],
            "unmanaged": unmanaged,
            "last_error": None,
        }

    def _log_startup_gate_status(self, status: Mapping[str, Any]) -> None:
        for item in status.get("unmanaged", []) or []:
            self._write_startup_order_event("startup_position_unmanaged", position=item)
            log.warning(
                "Startup broker position unmanaged: %s %.4f reason=%s",
                item.get("symbol"),
                item.get("quantity"),
                item.get("reason"),
            )
        for item in status.get("positions", []) or []:
            self._write_startup_order_event("startup_position_mapping_required", position=item)
            log.warning(
                "Startup broker position requires mapping: %s %.4f candidates=%s",
                item.get("symbol"),
                item.get("quantity"),
                [c.get("strategy_id") for c in item.get("candidates", [])],
            )

    def _validate_startup_allocations_locked(
        self,
        allocations: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        if self._startup_gate_status.get("phase") != "awaiting_mapping":
            raise ValueError("startup gate is not awaiting position mappings")
        if not allocations:
            raise ValueError("at least one allocation is required")

        positions = {
            item["position_id"]: item
            for item in self._startup_gate_status.get("positions", [])
        }
        allocated_by_position: dict[str, float] = defaultdict(float)
        allocated_by_strategy_position: dict[tuple[str, str], int] = defaultdict(int)
        normalized: list[dict[str, Any]] = []

        for raw in allocations:
            position_id = str(raw.get("position_id", "")).strip()
            strategy_id = str(raw.get("strategy_id", "")).strip()
            if position_id not in positions:
                raise ValueError(f"unknown startup position_id: {position_id!r}")
            position = positions[position_id]
            candidates = {
                item["strategy_id"]: item
                for item in position.get("candidates", [])
            }
            candidate = candidates.get(strategy_id)
            if candidate is None:
                raise ValueError(
                    f"strategy {strategy_id!r} cannot adopt position {position_id!r}"
                )
            if candidate.get("mode") == "dry_run":
                raise ValueError(f"dry-run strategy {strategy_id!r} cannot adopt live positions")
            if not candidate.get("supports_position_adoption"):
                raise ValueError(f"strategy {strategy_id!r} does not support position adoption")

            required_fields = set(candidate.get("required_fields", []))
            entry_ts = raw.get("entry_ts")
            parsed_entry_ts = _parse_optional_timestamp(entry_ts)
            if "entry_ts" in required_fields and parsed_entry_ts is None:
                raise ValueError(f"strategy {strategy_id!r} requires entry_ts for adoption")

            quantity = float(candidate.get("position_size_shares", 0.0))
            if quantity <= 0:
                raise ValueError(f"strategy {strategy_id!r} has invalid position size {quantity}")
            allocated_by_position[position_id] += quantity
            available = abs(float(position.get("quantity", 0.0)))
            if allocated_by_position[position_id] - available > 1e-9:
                raise ValueError(
                    f"allocated quantity {allocated_by_position[position_id]} exceeds "
                    f"broker quantity {available} for {position_id}"
                )

            strategy_position_key = (position_id, strategy_id)
            allocated_by_strategy_position[strategy_position_key] += 1
            if (
                allocated_by_strategy_position[strategy_position_key] > 1
                and candidate.get("position_mode") != POSITION_MODE_MULTI
            ):
                raise ValueError(
                    f"strategy {strategy_id!r} is not multi-position and cannot adopt multiple lots"
                )

            normalized.append({
                "position_id": position_id,
                "strategy_id": strategy_id,
                "quantity": quantity,
                "entry_ts": parsed_entry_ts.isoformat() if parsed_entry_ts else None,
                "trade_id": str(raw.get("trade_id") or "") or None,
            })
        return normalized

    def _adopt_startup_position_allocations(
        self,
        allocations: Sequence[Mapping[str, Any]],
        broker_positions: list[Position],
        strategy_entries: list[tuple[StrategyKernel, dict]],
        portfolio: PortfolioState,
    ) -> None:
        if not allocations:
            return
        positions_by_id = {
            _position_id(position): position
            for position in broker_positions
        }
        strategy_by_id = {
            kernel.SPEC.id: (kernel, state)
            for kernel, state in strategy_entries
        }
        allocated_by_position: dict[str, float] = defaultdict(float)
        for allocation in allocations:
            position_id = str(allocation["position_id"])
            strategy_id = str(allocation["strategy_id"])
            broker_position = positions_by_id[position_id]
            kernel, state = strategy_by_id[strategy_id]
            quantity = float(allocation["quantity"])
            signed_quantity = quantity if broker_position.quantity > 0 else -quantity
            entry_ts = _parse_optional_timestamp(allocation.get("entry_ts"))
            adoption = PositionAdoption(
                strategy_id=strategy_id,
                quantity=quantity,
                entry_ts=entry_ts,
                trade_id=allocation.get("trade_id"),
                source_position_id=position_id,
            )
            candidate_position = Position(
                instrument=broker_position.instrument,
                quantity=signed_quantity,
                avg_cost=broker_position.avg_cost,
                trade_id=adoption.trade_id,
            )
            adopted_position = kernel.on_adopt_position(candidate_position, adoption, state)
            if adopted_position is None:
                raise RuntimeError(f"strategy {strategy_id!r} rejected adopted position")
            portfolio.adopt_strategy_position(strategy_id, adopted_position)
            allocated_by_position[position_id] += quantity
            self._write_startup_order_event(
                "startup_position_mapped",
                strategy_id=strategy_id,
                position=adopted_position,
                adoption=adoption,
            )
            log.warning(
                "Startup mapped broker position: %s %.4f to %s trade_id=%s",
                adopted_position.instrument.symbol,
                adopted_position.quantity,
                strategy_id,
                adopted_position.trade_id,
            )

        for position_id, position in positions_by_id.items():
            remaining = abs(position.quantity) - allocated_by_position.get(position_id, 0.0)
            if remaining > 1e-9:
                self._write_startup_order_event(
                    "startup_position_unmanaged",
                    position=_position_gate_item(position),
                    unmanaged_quantity=remaining,
                    reason="unallocated_remainder",
                )
                log.warning(
                    "Startup broker position remainder unmanaged: %s %.4f",
                    position.instrument.symbol,
                    remaining,
                )

    def _risk_for_strategy(self, strategy_id: str) -> RiskPolicy:
        return self._strategy_risk.get(strategy_id, self._risk)


class _EngineProgress:
    """Lightweight timing reporter for long simulated engine runs."""

    def __init__(
        self,
        *,
        enabled: bool,
        total_bars: int | None,
        interval_bars: int,
        interval_seconds: float,
    ) -> None:
        self.enabled = bool(enabled)
        self.total_bars = total_bars if total_bars and total_bars > 0 else None
        self.interval_bars = max(1, int(interval_bars or 1000))
        self.interval_seconds = max(1.0, float(interval_seconds or 30.0))
        self.started_perf: float | None = None
        self.finished_perf: float | None = None
        self.last_report_perf: float | None = None
        self.last_report_bar = 0
        self.bars = 0
        self.last_replay_ts: datetime | None = None
        self.timings: dict[str, float] = defaultdict(float)
        self.timing_counts: dict[str, int] = defaultdict(int)
        self.last_report_timings: dict[str, float] = defaultdict(float)
        self.last_report_counts: dict[str, int] = defaultdict(int)
        self.counts: dict[str, int] = defaultdict(int)
        self.strategy_timings: dict[tuple[str, str], float] = defaultdict(float)
        self.strategy_counts: dict[tuple[str, str], int] = defaultdict(int)
        self.strategy_max: dict[tuple[str, str], float] = defaultdict(float)

    def start(self) -> None:
        if not self.enabled:
            return
        now = time.perf_counter()
        self.started_perf = now
        self.last_report_perf = now
        log.info(
            "Backtest progress enabled total_bars=%s interval_bars=%d interval_seconds=%.1f",
            self.total_bars,
            self.interval_bars,
            self.interval_seconds,
        )

    def finish(self) -> None:
        if not self.enabled:
            return
        self.finished_perf = time.perf_counter()
        self.report(force=True, final=True)

    def info(self, message: str, *args: Any) -> None:
        if self.enabled:
            log.info(message, *args)

    def add_timing(self, stage: str, seconds: float) -> None:
        if not self.enabled:
            return
        self.timings[stage] += max(0.0, float(seconds))
        self.timing_counts[stage] += 1

    def add_strategy_timing(self, strategy_id: str, phase: str, seconds: float) -> None:
        if not self.enabled:
            return
        key = (str(strategy_id), str(phase))
        elapsed = max(0.0, float(seconds))
        self.strategy_timings[key] += elapsed
        self.strategy_counts[key] += 1
        self.strategy_max[key] = max(self.strategy_max[key], elapsed)
        self.add_timing(f"strategy_{phase}", elapsed)

    def count(self, name: str, amount: int = 1) -> None:
        if self.enabled:
            self.counts[name] += int(amount)

    def count_bar(self, replay_ts: datetime) -> None:
        if not self.enabled:
            return
        self.bars += 1
        self.last_replay_ts = replay_ts

    def maybe_report(self) -> None:
        if not self.enabled or self.started_perf is None:
            return
        now = time.perf_counter()
        by_bars = (self.bars - self.last_report_bar) >= self.interval_bars
        by_time = self.last_report_perf is None or (now - self.last_report_perf) >= self.interval_seconds
        if by_bars or by_time:
            self.report()

    def report(self, *, force: bool = False, final: bool = False) -> None:
        if not self.enabled or self.started_perf is None:
            return
        now = self.finished_perf or time.perf_counter()
        if not force and self.bars == self.last_report_bar:
            return

        elapsed = max(now - self.started_perf, 1e-9)
        interval_elapsed = (
            max(now - self.last_report_perf, 1e-9)
            if self.last_report_perf is not None
            else elapsed
        )
        interval_bars = self.bars - self.last_report_bar
        pct = (self.bars / self.total_bars * 100.0) if self.total_bars else None
        rate = self.bars / elapsed
        interval_rate = interval_bars / interval_elapsed
        slow_total = self._format_top_stages(self.timings)
        slow_interval = self._format_top_interval_stages()
        prefix = "Backtest final progress" if final else "Backtest progress"
        pct_text = f" {pct:.1f}%" if pct is not None else ""
        log.info(
            (
                "%s bars=%d/%s%s replay_ts=%s elapsed=%.1fs rate=%.1f bars/s "
                "interval=%d bars %.1f bars/s dispatch=%d entry_evals=%d exit_evals=%d "
                "entry_signals=%d exit_signals=%d fast_skips=%d slow_total=%s slow_interval=%s"
            ),
            prefix,
            self.bars,
            self.total_bars or "?",
            pct_text,
            self.last_replay_ts.isoformat() if self.last_replay_ts else None,
            elapsed,
            rate,
            interval_bars,
            interval_rate,
            self.counts.get("dispatch_contexts", 0),
            self.counts.get("entry_evals", 0),
            self.counts.get("exit_evals", 0),
            self.counts.get("entry_signals", 0),
            self.counts.get("exit_signals", 0),
            self.counts.get("fast_event_entry_skips", 0),
            slow_total,
            slow_interval,
        )
        strategy_line = self._format_strategy_timings()
        if strategy_line:
            log.info("Backtest strategy timing %s", strategy_line)

        self.last_report_bar = self.bars
        self.last_report_perf = now
        self.last_report_timings = defaultdict(float, self.timings)
        self.last_report_counts = defaultdict(int, self.timing_counts)

    def snapshot(self) -> dict[str, Any]:
        if not self.enabled:
            return {}
        elapsed = None
        if self.started_perf is not None:
            end = self.finished_perf or time.perf_counter()
            elapsed = end - self.started_perf
        return {
            "enabled": True,
            "bars": self.bars,
            "total_bars": self.total_bars,
            "elapsed_seconds": elapsed,
            "last_replay_ts": self.last_replay_ts,
            "counts": dict(self.counts),
            "timings_seconds": dict(self.timings),
            "timing_counts": dict(self.timing_counts),
            "strategy_timings": [
                {
                    "strategy_id": strategy_id,
                    "phase": phase,
                    "calls": self.strategy_counts[(strategy_id, phase)],
                    "total_seconds": total,
                    "avg_seconds": total / max(self.strategy_counts[(strategy_id, phase)], 1),
                    "max_seconds": self.strategy_max[(strategy_id, phase)],
                }
                for (strategy_id, phase), total in sorted(self.strategy_timings.items())
            ],
        }

    def _format_top_stages(self, values: Mapping[str, float], limit: int = 4) -> str:
        if not values:
            return "none"
        top = sorted(values.items(), key=lambda item: item[1], reverse=True)[:limit]
        return ",".join(f"{name}:{seconds:.1f}s" for name, seconds in top)

    def _format_top_interval_stages(self, limit: int = 4) -> str:
        deltas = {
            name: seconds - self.last_report_timings.get(name, 0.0)
            for name, seconds in self.timings.items()
        }
        deltas = {name: seconds for name, seconds in deltas.items() if seconds > 0}
        return self._format_top_stages(deltas, limit=limit)

    def _format_strategy_timings(self, limit: int = 6) -> str:
        if not self.strategy_timings:
            return ""
        rows: list[tuple[float, str]] = []
        for key, total in self.strategy_timings.items():
            strategy_id, phase = key
            calls = self.strategy_counts[key]
            avg = total / max(calls, 1)
            max_seconds = self.strategy_max[key]
            rows.append(
                (
                    total,
                    (
                        f"{strategy_id}.{phase}:calls={calls},"
                        f"total={total:.1f}s,avg={avg:.3f}s,max={max_seconds:.3f}s"
                    ),
                )
            )
        rows.sort(reverse=True)
        return " ".join(text for _, text in rows[:limit])


class _MarkToMarketSizingState:
    """Small account model used only for sizing decisions inside the engine."""

    def __init__(self, initial_equity: float) -> None:
        self._cash = float(initial_equity)
        self._latest_prices: dict[Instrument, float] = {}
        self._positions: dict[Instrument, float] = defaultdict(float)

    def update_bar(self, bar: Bar) -> None:
        self._latest_prices[bar.instrument] = float(bar.close)

    def latest_price(self, instrument: Instrument) -> float | None:
        return self._latest_prices.get(instrument)

    def equity(self) -> float:
        equity = self._cash
        for instrument, quantity in self._positions.items():
            price = self._latest_prices.get(instrument)
            if price is not None:
                equity += quantity * price * float(instrument.multiplier or 1.0)
        return equity

    def apply_fill(self, fill: Fill) -> None:
        signed_qty = _signed_fill_quantity(fill)
        multiplier = float(fill.instrument.multiplier or 1.0)
        self._cash -= signed_qty * float(fill.price) * multiplier
        new_qty = self._positions.get(fill.instrument, 0.0) + signed_qty
        if abs(new_qty) < 1e-9:
            self._positions.pop(fill.instrument, None)
        else:
            self._positions[fill.instrument] = new_qty
            self._latest_prices.setdefault(fill.instrument, float(fill.price))


def _instrument_match_key(instrument: Instrument) -> tuple[str, str]:
    return (str(instrument.asset_class).lower(), str(instrument.symbol).upper())


def _position_id(position: Position) -> str:
    return (
        f"position:{position.instrument.asset_class}:"
        f"{str(position.instrument.symbol).upper()}:{position.side}"
    )


def _position_gate_item(position: Position) -> dict[str, Any]:
    return {
        "position_id": _position_id(position),
        "asset_class": position.instrument.asset_class,
        "symbol": position.instrument.symbol,
        "side": position.side,
        "quantity": position.quantity,
        "avg_cost": position.avg_cost,
        "instrument": position.instrument,
    }


def _parse_optional_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value))
        except ValueError as exc:
            raise ValueError(f"invalid entry_ts: {value!r}") from exc
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _signed_fill_quantity(fill: Fill) -> float:
    if fill.side.upper() in {"BOT", "BUY", "B", "LONG"}:
        return float(fill.quantity)
    return -float(fill.quantity)


def _normalize_dispatch_mode(mode: str) -> str:
    normalized = str(mode or _DISPATCH_MODE_EVENT).strip().lower().replace("-", "_")
    if normalized not in {_DISPATCH_MODE_EVENT, _DISPATCH_MODE_FAST_EVENT, _DISPATCH_MODE_PARALLEL}:
        raise ValueError(
            "dispatch_mode must be 'event', 'fast-event', or 'parallel'; "
            f"got {mode!r}"
        )
    return normalized


def _normalize_evaluation_timeframes(
    evaluation_timeframes: Mapping[str, str] | None,
) -> dict[str, Timeframe]:
    result: dict[str, Timeframe] = {}
    for strategy_id, label in dict(evaluation_timeframes or {}).items():
        result[str(strategy_id)] = Timeframe.parse(str(label))
    return result


def _normalize_precomputed_entry_signals(
    signals: Mapping[str, Sequence[tuple[datetime, Signal]]] | None,
) -> dict[str, dict[datetime, list[Signal]]]:
    result: dict[str, dict[datetime, list[Signal]]] = {}
    for strategy_id, entries in dict(signals or {}).items():
        by_timestamp = result.setdefault(str(strategy_id), {})
        for timestamp, signal in entries:
            key = _normalize_precomputed_timestamp(timestamp)
            by_timestamp.setdefault(key, []).append(signal)
    return result


def _normalize_precomputed_timestamp(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _completed_timeframe_bar_start(latest_ts: datetime, timeframe: Timeframe) -> datetime:
    """Return the start of the latest completed resample bucket.

    This mirrors the engine resampler's left-labeled, left-closed buckets with
    the last in-progress bucket dropped, without rebuilding a full DataFrame on
    every fast-event skip check.
    """
    latest = latest_ts.astimezone(timezone.utc) if latest_ts.tzinfo else latest_ts.replace(tzinfo=timezone.utc)
    candidate = latest - timedelta(seconds=timeframe.seconds)
    anchor = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((candidate - anchor).total_seconds())
    bucket = (elapsed // timeframe.seconds) * timeframe.seconds
    return anchor + timedelta(seconds=bucket)

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
import math
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, time as dt_time, timedelta, timezone
from threading import RLock
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from ..audit.serialize import to_jsonable
from ..data.bar_builder import BarBuilder
from ..data.feed import DataFeed
from ..data.manager import DataManager
from ..data.options import OptionDataCache
from ..orders.approvals import ApprovalStore
from ..orders.order_manager import OrderManager
from ..orders.strategy_modes import strategy_mode_map
from ..privacy import is_customer_profile, redact_payload, safe_strategy_id
from ..features.registry import FeatureRegistry
from ..audit import AuditLogger, pop_decision
from ..startup import (
    PositionOwnershipLedger,
    StartupPositionGateController,
    apply_startup_position_allocations,
    position_id,
    resolve_adopted_position_map,
)
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
from ..types import Bar, Fill, Instrument, MarketContext, OptionDataRequest, Position, Signal
from .clock import Clock, SimulatedClock, WallClock
from .market_summary import (
    DEFAULT_MARKET_SUMMARY_POINTS,
    build_market_summary,
    unavailable_market_summary,
)
from .scheduler import Scheduler

if TYPE_CHECKING:
    from ..interfaces.broker import BrokerAdapter
    from ..interfaces.data import HistoricalDataProvider, StreamingDataProvider
    from ..interfaces.instruments import InstrumentResolver
    from ..interfaces.options import OptionDataProvider
    from ..startup import StrategyStateStore

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
        startup_position_allocations: Sequence[Mapping[str, Any]] | None = None,
        startup_position_mapping_enabled: bool = True,
        ownership_ledger: PositionOwnershipLedger | None = None,
        strategy_state_store: "StrategyStateStore | None" = None,
        audit_logger: AuditLogger | None = None,
        strategy_modes: Mapping[str, str] | None = None,
        metadata_profile: str = "owner",
        strategy_aliases: Mapping[str, str] | None = None,
        dispatch_mode: str = _DISPATCH_MODE_EVENT,
        evaluation_timeframes: Mapping[str, str] | None = None,
        precomputed_entry_signals: Mapping[str, Sequence[tuple[datetime, Signal]]] | None = None,
        feature_preload_bars: list[Bar] | None = None,
        progress_enabled: bool = False,
        progress_total_bars: int | None = None,
        progress_interval_bars: int = 1000,
        progress_interval_seconds: float = 30.0,
        startup_position_gate_enabled: bool = False,
        instrument_resolver: "InstrumentResolver | None" = None,
        option_data_provider: "OptionDataProvider | None" = None,
        live_health: Mapping[str, Any] | None = None,
        runtime_reconciliation: Mapping[str, Any] | None = None,
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
        self._instrument_resolver = instrument_resolver
        self._option_data_provider = option_data_provider
        self._option_cache = OptionDataCache()
        self._approval_store = ApprovalStore()
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
        self._ownership_ledger = ownership_ledger
        self._strategy_state_store = strategy_state_store
        self._audit = audit_logger
        self._metadata_profile = str(metadata_profile or "owner")
        self._strategy_aliases = dict(strategy_aliases or {})
        self._state_lock = RLock()
        self._phase = "initialized"
        self._broker_connected = False
        self._data_connected = False
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None
        self._last_error: str | None = None
        self._last_bar: dict[str, Any] | None = None
        self._last_bar_received_at: datetime | None = None
        self._bar_count = 0
        self._managers: dict[Instrument, DataManager] = {}
        self._strategy_market_cache: dict[
            tuple[Instrument, int, str, int],
            dict[str, Any],
        ] = {}
        self._last_strategy_decisions: dict[str, dict[str, Any]] = {}
        self._portfolio: PortfolioState | None = None
        self._order_manager: OrderManager | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._recent_events: list[dict[str, Any]] = []
        self._entry_block_reason: str | None = None
        self._runtime_health: dict[str, Any] = {
            "entry_blocked": False,
            "block_reason": None,
            "last_check": None,
            "last_bar_received_at": None,
        }
        self._reconciliation_state: dict[str, Any] = {
            "enabled": False,
            "last_check": None,
            "status": "not_started",
            "block_reason": None,
        }
        self._live_health_config = dict(live_health or {})
        self._runtime_reconciliation_config = dict(runtime_reconciliation or {})
        self._latest_account_snapshot = None
        self._last_session_stop_cancel_date = None
        self._startup_position_gate_enabled = bool(startup_position_gate_enabled)
        self._startup_gate = StartupPositionGateController(
            enabled=self._startup_position_gate_enabled,
            mapping_enabled=startup_position_mapping_enabled,
            default_risk=self._risk,
            strategy_risk=self._strategy_risk,
            strategy_modes=self._strategy_modes,
            configured_allocations=startup_position_allocations,
            ownership_ledger=self._ownership_ledger,
            strategy_state_store=self._strategy_state_store,
            write_event=self._write_startup_order_event,
        )

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_live(self) -> None:
        """Start live trading. Blocks until interrupted."""
        asyncio.run(self._run())

    def run_backtest(self) -> None:
        """Run backtest to completion. Blocks until all replay bars are consumed."""
        asyncio.run(self._run())

    def set_startup_position_mapping_enabled(self, enabled: bool) -> None:
        """Enable/disable live startup ownership mapping interfaces."""
        self._startup_gate.set_mapping_enabled(enabled)

    def snapshot_state(self) -> dict[str, Any]:
        """Return a JSON-safe read-only runtime snapshot for control APIs."""
        now = datetime.now(tz=timezone.utc)
        startup_gate_status = self._startup_gate.status()
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
                "runtime_health": dict(self._runtime_health),
                "reconciliation": dict(self._reconciliation_state),
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
                "strategies": self._snapshot_strategies(),
                "last_decisions": dict(self._last_strategy_decisions),
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
                "startup_gate": startup_gate_status,
                "approvals": to_jsonable(self._approval_store.list()),
                "recent_events": list(self._recent_events),
            }

        state["strategy_market"] = self._snapshot_strategy_market(managers)

        broker_positions: list[dict[str, Any]] = []
        if portfolio is not None:
            for position in portfolio.positions():
                item = to_jsonable(position)
                if isinstance(item, dict):
                    item["position_id"] = position_id(position)
                    broker_positions.append(item)

        state["positions"] = {
            "broker": broker_positions,
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
        if is_customer_profile(self._metadata_profile):
            state = redact_payload(
                state,
                profile=self._metadata_profile,
                aliases=self._strategy_aliases,
            )
        elif self._strategy_state_store is not None:
            state["strategy_state"] = self._strategy_state_store.summary()
        return state

    def _snapshot_strategy_market(
        self,
        managers: Mapping[Instrument, DataManager],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for kernel, _ in self._strategies:
            strategy_id = kernel.SPEC.id
            instrument = kernel.SPEC.primary_instrument
            manager = managers.get(instrument)
            if manager is None:
                result[strategy_id] = unavailable_market_summary(instrument)
                continue
            result[strategy_id] = self._cached_market_summary(instrument, manager)
        return result

    def _cached_market_summary(
        self,
        instrument: Instrument,
        manager: DataManager,
    ) -> dict[str, Any]:
        max_points = DEFAULT_MARKET_SUMMARY_POINTS
        cache_key = (instrument, manager.revision, self._session_tz, max_points)
        with self._state_lock:
            cached = self._strategy_market_cache.get(cache_key)
        if cached is not None:
            return cached

        summary = build_market_summary(
            instrument,
            manager.bars_1m(lookback_days=10),
            session_tz=self._session_tz,
            max_points=max_points,
        )
        with self._state_lock:
            self._strategy_market_cache = {
                key: value
                for key, value in self._strategy_market_cache.items()
                if key[0] != instrument
            }
            self._strategy_market_cache[cache_key] = summary
        return summary

    def _snapshot_strategies(self) -> list[dict[str, Any]]:
        if is_customer_profile(self._metadata_profile):
            return [
                {
                    "id": safe_strategy_id(
                        kernel.SPEC.id,
                        profile=self._metadata_profile,
                        aliases=self._strategy_aliases,
                    ),
                    "mode": self._strategy_modes.get(kernel.SPEC.id, "live"),
                    "status": "loaded",
                }
                for kernel, _ in self._strategies
            ]
        return [
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
        ]

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
        self._runtime_loop = loop
        is_simulated = isinstance(self._clock, SimulatedClock)

        pool = None
        drain_orders_task = None
        drain_fills_task = None
        drain_order_updates_task = None
        health_task = None
        reconciliation_task = None
        try:
            # Connect before instrument resolution so adapter-backed resolvers can
            # query broker contract details without strategies importing adapters.
            setup_t0 = time.perf_counter()
            await self._broker.connect()
            self._set_runtime_state(broker_connected=True)
            await self._data_feed.connect()
            self._set_runtime_state(data_connected=True)
            if self._option_data_provider is not None:
                await self._option_data_provider.connect()
            self._progress.add_timing("setup_connect", time.perf_counter() - setup_t0)

            await self._resolve_runtime_instruments(
                require_resolved_futures=(
                    not is_simulated
                    and str(getattr(self._broker, "name", "")).lower() == "ibkr"
                )
            )

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
                if self._strategy_state_store is not None:
                    state.update(self._strategy_state_store.load_state(kernel.SPEC.id))
                strategy_entries.append((kernel, state))

            self._progress.info(
                "Engine setup starting strategies=%s dispatch_mode=%s progress_total_bars=%s",
                [kernel.SPEC.id for kernel, _ in self._strategies],
                self._dispatch_mode,
                self._progress.total_bars,
            )

            # Portfolio state and order manager
            portfolio = PortfolioState()
            self._set_portfolio(portfolio)
            account_snapshot = await self._broker.get_account()
            self._set_latest_account_snapshot(account_snapshot)
            portfolio.update_account(account_snapshot)
            sizing_account = _MarkToMarketSizingState(account_snapshot.net_liquidation)
            adopted = await self._broker.get_positions()
            if self._startup_position_gate_enabled:
                portfolio.adopt_positions(adopted)
                if adopted:
                    gate_result = await self._startup_gate.run(
                        adopted,
                        strategy_entries,
                        refresh_positions=self._broker.get_positions,
                        on_awaiting_mapping=lambda: self._set_runtime_state(
                            phase=_PHASE_AWAITING_STARTUP_MAPPING,
                        ),
                        on_released=lambda: self._set_runtime_state(phase="starting"),
                    )
                    apply_startup_position_allocations(
                        gate_result.allocations,
                        gate_result.positions,
                        strategy_entries,
                        portfolio,
                        unmanaged_remainder_acknowledgements=(
                            gate_result.unmanaged_remainder_acknowledgements
                        ),
                        ownership_ledger=self._ownership_ledger,
                        write_event=self._write_startup_order_event,
                    )
                else:
                    self._startup_gate.mark_clear(
                        "No broker positions found; startup can continue.",
                    )
            elif adopted:
                strategy_map = resolve_adopted_position_map(
                    adopted,
                    strategy_entries,
                    self._adopted_position_map,
                )
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
            self._persist_strategy_states(strategy_entries, portfolio)
            protective_stops = {
                kernel.SPEC.id: kernel.SPEC.protective_stop
                for kernel, _ in self._strategies
                if kernel.SPEC.protective_stop is not None
            }
            position_policies = {
                kernel.SPEC.id: kernel.SPEC.position_policy
                for kernel, _ in self._strategies
            }
            strategy_execution_instruments = {
                kernel.SPEC.id: kernel.SPEC.execution_instrument
                for kernel, _ in self._strategies
            }

            def on_fill_applied(fill: Fill) -> None:
                sizing_account.apply_fill(fill)
                self._persist_strategy_states(strategy_entries, portfolio)

            order_manager = OrderManager(
                self._broker,
                portfolio,
                self._risk,
                self._audit,
                protective_stops=protective_stops,
                strategy_modes=self._strategy_modes,
                position_policies=position_policies,
                strategy_risk=self._strategy_risk,
                ownership_ledger=self._ownership_ledger,
                metadata_profile=self._metadata_profile,
                strategy_aliases=self._strategy_aliases,
                sizing_price_provider=sizing_account.latest_price,
                sizing_equity_provider=sizing_account.equity,
                fill_listener=on_fill_applied,
                strategy_execution_instruments=strategy_execution_instruments,
                approval_store=self._approval_store,
                entry_block_provider=self._order_entry_block_reason,
                account_snapshot_provider=self._latest_account_for_risk,
            )
            self._order_manager = order_manager

            if not is_simulated:
                await self._startup_open_order_gate(all_instruments)

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

            await self._seed_startup_protective_stops(
                strategy_entries,
                portfolio,
                managers,
                features,
                order_manager,
            )

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
            scheduler = Scheduler(features, options=self._option_cache)
            for kernel, state in strategy_entries:
                scheduler.register(kernel, state)
            last_evaluation_bars: dict[str, datetime] = {}

            # Start background tasks
            pool = ThreadPoolExecutor(max_workers=self._pool_workers)
            inline_order_drain = _supports_inline_order_drain(self._broker)
            inline_strategy_calls = is_simulated or inline_order_drain
            drain_orders_task = None
            drain_fills_task = None
            drain_order_updates_task = None
            health_task = None
            reconciliation_task = None
            if not is_simulated and not inline_order_drain:
                drain_orders_task = loop.create_task(order_manager.drain_orders())
                drain_fills_task = loop.create_task(order_manager.drain_fills())
                drain_order_updates_task = loop.create_task(order_manager.drain_order_updates())
            if not is_simulated:
                health_task = loop.create_task(self._live_health_monitor(all_instruments))
                reconciliation_task = loop.create_task(
                    self._runtime_reconciliation_loop(
                        portfolio,
                        order_manager,
                        all_instruments,
                    )
                )
            self._set_runtime_state(phase="running")

            async def evaluate_exit(
                kernel: StrategyKernel,
                ctx,
                state: dict,
                position: Position,
            ) -> None:
                ctx = self._context_with_positions(ctx, kernel, portfolio)
                self._progress.count("exit_evals")
                strategy_t0 = time.perf_counter()
                if inline_strategy_calls:
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
                self._persist_strategy_states(strategy_entries, portfolio)
                if not reason:
                    strategy_t0 = time.perf_counter()
                    if inline_strategy_calls:
                        stop_update = kernel.on_protective_stop_update(ctx, position, state)
                    else:
                        stop_update = await loop.run_in_executor(
                            pool, kernel.on_protective_stop_update, ctx, position, state
                        )
                    elapsed = time.perf_counter() - strategy_t0
                    self._progress.add_strategy_timing(kernel.SPEC.id, "protective_stop", elapsed)
                    audit_t0 = time.perf_counter()
                    self._write_decision_trace(state)
                    self._progress.add_timing("audit_decision", time.perf_counter() - audit_t0)
                    self._persist_strategy_states(strategy_entries, portfolio)
                    if stop_update is None:
                        return

                    self._progress.count("protective_stop_updates")
                    order_t0 = time.perf_counter()
                    self._write_signal_event(
                        kernel.SPEC.id,
                        "protective_stop_update",
                        ctx.timestamp,
                        stop_update=stop_update,
                        instrument=position.instrument,
                        side=position.side,
                        trade_id=position.trade_id,
                    )
                    await order_manager.ensure_protective_stop(
                        kernel.SPEC.id,
                        position,
                        stop_update.stop_price,
                        stop_update.reason,
                    )
                    if is_simulated or inline_order_drain:
                        await order_manager.drain_ready_order_updates()
                    else:
                        await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                    self._persist_strategy_states(strategy_entries, portfolio)
                    self._progress.add_timing("signal_order", time.perf_counter() - order_t0)
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
                self._persist_strategy_states(strategy_entries, portfolio)
                self._progress.add_timing("signal_order", time.perf_counter() - order_t0)

            async def refresh_option_data(
                kernel: StrategyKernel,
                ctx,
                state: dict,
            ) -> None:
                if self._option_data_provider is None:
                    return
                seen: set[tuple] = set()
                for _ in range(2):
                    if inline_strategy_calls:
                        requests = kernel.option_data_requests(ctx, state)
                    else:
                        requests = await loop.run_in_executor(
                            pool,
                            kernel.option_data_requests,
                            ctx,
                            state,
                        )
                    fresh = [
                        request for request in requests or ()
                        if _option_request_key(request) not in seen
                    ]
                    if not fresh:
                        break
                    for request in fresh:
                        seen.add(_option_request_key(request))
                        await self._refresh_option_data_request(request)

            async def evaluate_entry(kernel: StrategyKernel, ctx, state: dict) -> None:
                ctx = self._context_with_positions(ctx, kernel, portfolio)
                block_reason = self._order_entry_block_reason(
                    kernel.SPEC.id,
                    kernel.SPEC.execution_instrument,
                )
                if block_reason is not None:
                    self._progress.count("entry_blocked")
                    self._write_signal_event(
                        kernel.SPEC.id,
                        "entry_blocked",
                        ctx.timestamp,
                        reason=block_reason,
                        instrument=kernel.SPEC.execution_instrument,
                    )
                    return
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
                        if is_simulated or inline_order_drain:
                            await order_manager.drain_ready_orders()
                            await order_manager.drain_ready_order_updates()
                        else:
                            await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                        self._persist_strategy_states(strategy_entries, portfolio)
                        self._progress.add_timing("signal_order", time.perf_counter() - order_t0)
                    return

                if not self._entry_frequency_allows(kernel, state, ctx.timestamp):
                    self._progress.count("entry_frequency_skips")
                    return

                self._progress.count("entry_evals")
                await refresh_option_data(kernel, ctx, state)
                strategy_t0 = time.perf_counter()
                if inline_strategy_calls:
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
                if inline_strategy_calls:
                    intents = kernel.generate_intents(ctx, state)
                else:
                    intents = await loop.run_in_executor(
                        pool,
                        kernel.generate_intents,
                        ctx,
                        state,
                    )
                audit_t0 = time.perf_counter()
                self._write_decision_trace(state)
                self._progress.add_timing("audit_decision", time.perf_counter() - audit_t0)
                if signal is None and not intents:
                    self._persist_strategy_states(strategy_entries, portfolio)
                    return

                self._mark_entry_frequency(kernel, state, ctx.timestamp)
                order_t0 = time.perf_counter()
                if signal is not None:
                    self._progress.count("entry_signals")
                    self._write_signal_event(
                        kernel.SPEC.id,
                        "entry",
                        ctx.timestamp,
                        signal=signal,
                        trade_id=signal.trade_id,
                    )
                    await order_manager.submit(signal, kernel.SPEC.id)
                for intent in intents or ():
                    self._progress.count("entry_signals")
                    self._write_signal_event(
                        kernel.SPEC.id,
                        "entry_intent",
                        ctx.timestamp,
                        intent=intent,
                        trade_id=intent.trade_id,
                    )
                    await order_manager.submit_intent(intent, kernel.SPEC.id)
                if is_simulated or inline_order_drain:
                    await order_manager.drain_ready_orders()
                    await order_manager.drain_ready_order_updates()
                else:
                    await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                self._persist_strategy_states(strategy_entries, portfolio)
                self._progress.add_timing("signal_order", time.perf_counter() - order_t0)

        except Exception as e:
            log.exception("Engine setup error: %s", e)
            self._set_runtime_state(phase="error", last_error=str(e))
            self._record_event("engine", "engine_setup_error", error=str(e))
            for task in (
                drain_orders_task,
                drain_fills_task,
                drain_order_updates_task,
                health_task,
                reconciliation_task,
            ):
                if task is not None:
                    task.cancel()
            self._order_manager = None
            self._runtime_loop = None
            if pool is not None:
                pool.shutdown(wait=True, cancel_futures=True)
            if self._option_data_provider is not None:
                try:
                    await self._option_data_provider.disconnect()
                except Exception as disconnect_exc:
                    log.warning("Option provider cleanup failed: %s", disconnect_exc)
            try:
                await self._data_feed.disconnect()
            except Exception as disconnect_exc:
                log.warning("Data feed cleanup failed: %s", disconnect_exc)
            self._set_runtime_state(data_connected=False)
            try:
                await self._broker.disconnect()
            except Exception as disconnect_exc:
                log.warning("Broker cleanup failed: %s", disconnect_exc)
            self._progress.finish()
            self._set_runtime_state(
                phase="error",
                broker_connected=False,
                stopped_at=datetime.now(tz=timezone.utc),
            )
            raise

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
                    if is_simulated or inline_order_drain:
                        await order_manager.drain_ready_fills()
                        await order_manager.drain_ready_order_updates()
                    else:
                        await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                    self._progress.add_timing("broker_on_bar", time.perf_counter() - stage_t0)

                if not is_simulated:
                    await self._maybe_cancel_session_protective_stops(
                        order_manager,
                        bar_1m.timestamp,
                    )

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
            if health_task is not None:
                health_task.cancel()
            if reconciliation_task is not None:
                reconciliation_task.cancel()
            self._order_manager = None
            self._runtime_loop = None
            pool.shutdown(wait=True, cancel_futures=True)
            if self._option_data_provider is not None:
                await self._option_data_provider.disconnect()
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

    async def _resolve_runtime_instruments(
        self,
        *,
        require_resolved_futures: bool,
    ) -> None:
        cache: dict[Instrument, Instrument] = {}

        async def resolve(instrument: Instrument) -> Instrument:
            if instrument in cache:
                return cache[instrument]
            resolved = instrument
            if self._instrument_resolver is not None:
                resolved = await self._instrument_resolver.resolve(instrument)
            elif require_resolved_futures and _future_requires_resolution(instrument):
                raise RuntimeError(
                    "unresolved future instrument requires an instrument resolver: "
                    f"{instrument.symbol}"
                )
            if require_resolved_futures and _future_requires_resolution(resolved):
                raise RuntimeError(
                    "future instrument was not resolved to a concrete contract: "
                    f"{resolved.symbol}"
                )
            cache[instrument] = resolved
            if resolved != instrument:
                self._record_event(
                    "engine",
                    "instrument_resolved",
                    original=instrument,
                    resolved=resolved,
                )
                log.info(
                    "Resolved instrument %s -> %s expiry=%s",
                    instrument.symbol,
                    resolved.symbol,
                    resolved.expiry,
                )
            return resolved

        for kernel, _ in self._strategies:
            spec = kernel.SPEC
            primary = await resolve(spec.primary_instrument)
            execution = await resolve(spec.execution_instrument)
            resolved_references: list[Instrument] = []
            for instrument in spec.reference_instruments:
                resolved_references.append(await resolve(instrument))
            references = tuple(resolved_references)
            if (
                primary != spec.primary_instrument
                or execution != spec.execution_instrument
                or references != spec.reference_instruments
            ):
                kernel.SPEC = replace(
                    spec,
                    primary_instrument=primary,
                    execution_instrument=execution,
                    reference_instruments=references,
                )

    async def _seed_startup_protective_stops(
        self,
        strategy_entries: Sequence[tuple[StrategyKernel, dict]],
        portfolio: PortfolioState,
        managers: Mapping[Instrument, DataManager],
        features: FeatureRegistry,
        order_manager: OrderManager,
    ) -> None:
        strategy_lots = portfolio.strategy_position_lots()
        lot_strategy_ids = {strategy_id for strategy_id, _ in strategy_lots}
        strategy_positions = [
            (strategy_id, position)
            for strategy_id, position in portfolio.strategy_positions()
            if strategy_id not in lot_strategy_ids
        ]
        startup_positions = [*strategy_lots, *strategy_positions]
        if not startup_positions:
            return

        entries_by_strategy = {
            kernel.SPEC.id: (kernel, state)
            for kernel, state in strategy_entries
        }
        for strategy_id, position in startup_positions:
            entry = entries_by_strategy.get(strategy_id)
            if entry is None:
                continue
            kernel, state = entry
            if kernel.SPEC.protective_stop is None:
                continue
            ctx = self._startup_context(kernel, managers, features)
            stop_update = kernel.on_protective_stop_update(ctx, position, state)
            self._write_decision_trace(state)
            if stop_update is None:
                raise RuntimeError(
                    "startup adopted position is missing protective stop update: "
                    f"strategy={strategy_id} instrument={position.instrument.symbol} "
                    f"trade_id={position.trade_id}"
                )
            self._write_signal_event(
                strategy_id,
                "startup_protective_stop_update",
                ctx.timestamp,
                stop_update=stop_update,
                instrument=position.instrument,
                side=position.side,
                trade_id=position.trade_id,
            )
            submitted = await order_manager.ensure_protective_stop(
                strategy_id,
                position,
                stop_update.stop_price,
                stop_update.reason,
            )
            if not submitted:
                raise RuntimeError(
                    "startup adopted position protective stop was not accepted: "
                    f"strategy={strategy_id} instrument={position.instrument.symbol} "
                    f"trade_id={position.trade_id}"
            )
            self._write_startup_order_event(
                "startup_protective_stop_seeded",
                strategy_id=strategy_id,
                position=position,
                stop_update=stop_update,
            )
            self._persist_strategy_states(strategy_entries, portfolio)

    def _persist_strategy_states(
        self,
        strategy_entries: Sequence[tuple[StrategyKernel, dict]],
        portfolio: PortfolioState,
    ) -> None:
        if self._strategy_state_store is None:
            return
        positions_by_strategy = {
            kernel.SPEC.id: portfolio.get_all_strategy_positions(kernel.SPEC.id)
            for kernel, _ in strategy_entries
        }
        run_id = self._audit.run_id if self._audit is not None else None
        try:
            self._strategy_state_store.save_all(
                strategy_entries,
                positions_by_strategy,
                run_id=run_id,
            )
        except Exception as exc:
            log.warning("Strategy state persistence failed: %s", exc)

    async def _refresh_option_data_request(self, request: OptionDataRequest) -> None:
        if self._option_data_provider is None:
            return
        if request.request_type == "chain":
            snapshot = await self._option_data_provider.option_chain(request.underlying)
            self._option_cache.update_chain(snapshot)
            return
        if request.request_type == "quote" and request.instrument is not None:
            quote = await self._option_data_provider.option_quote(request.instrument)
            self._option_cache.update_quote(quote)
            return
        self._record_event(
            "data",
            "option_data_request_dropped",
            request=request,
            reason="invalid_request",
        )

    def _startup_context(
        self,
        kernel: StrategyKernel,
        managers: Mapping[Instrument, DataManager],
        features: FeatureRegistry,
    ) -> MarketContext:
        primary_manager = managers.get(kernel.SPEC.primary_instrument)
        if primary_manager is None:
            raise RuntimeError(
                f"startup context missing data manager for {kernel.SPEC.primary_instrument.symbol}"
            )
        timestamp = primary_manager.latest_timestamp()
        if timestamp is None:
            raise RuntimeError(
                f"startup context has no bars for {kernel.SPEC.primary_instrument.symbol}"
            )
        bars: dict[Instrument, dict[str, Any]] = {}
        for instrument in [
            kernel.SPEC.primary_instrument,
            *list(kernel.SPEC.reference_instruments),
        ]:
            manager = managers.get(instrument)
            if manager is None:
                bars[instrument] = {}
                continue
            instrument_bars: dict[str, Any] = {"1m": manager.bars_1m()}
            for label in kernel.SPEC.timeframes:
                if label == "1m":
                    continue
                instrument_bars[label] = manager.resampled(Timeframe.parse(label))
            bars[instrument] = instrument_bars
        feature_view = features.as_of(timestamp)
        indicators: dict[str, Any] = {}
        for indicator_id in kernel.SPEC.indicators:
            try:
                indicators[indicator_id] = feature_view.get_id(indicator_id)
            except Exception:
                log.debug("Could not compute startup indicator %s", indicator_id)
        return MarketContext(
            primary=kernel.SPEC.primary_instrument,
            timestamp=timestamp,
            bars=bars,
            indicators=indicators,
            features=feature_view,
            options=self._option_cache,
        )

    def _has_precomputed_signal_at(self, strategy_id: str, timestamp: datetime) -> bool:
        return bool(self._precomputed_signals_at(strategy_id, timestamp))

    def _context_with_positions(
        self,
        ctx: MarketContext,
        kernel: StrategyKernel,
        portfolio: PortfolioState,
    ) -> MarketContext:
        return replace(
            ctx,
            positions={
                "strategy": portfolio.get_all_strategy_positions(kernel.SPEC.id),
                "broker": portfolio.positions(),
            },
        )

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
        return self._startup_gate.status()

    def submit_startup_mappings(
        self,
        allocations: Sequence[Mapping[str, Any]],
        *,
        ack_unmanaged_remainders: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self._startup_gate.submit_mappings(
            allocations,
            ack_unmanaged_remainders=ack_unmanaged_remainders,
        )

    def request_startup_gate_refresh(self) -> dict[str, Any]:
        return self._startup_gate.request_refresh()

    def pending_approvals(self) -> list[dict[str, Any]]:
        return to_jsonable(self._approval_store.list())

    def approve_pending_action(
        self,
        approval_id: str,
        *,
        operator_note: str | None = None,
    ) -> dict[str, Any]:
        approval = self._approval_store.approve(
            approval_id,
            operator_note=operator_note,
        )
        self._submit_approved_action(approval.approval_id)
        self._record_event(
            "operator",
            "approval_marked_approved",
            approval_id=approval.approval_id,
            strategy_id=approval.strategy_id,
        )
        return to_jsonable(approval)

    def reject_pending_action(
        self,
        approval_id: str,
        *,
        operator_note: str | None = None,
    ) -> dict[str, Any]:
        approval = self._approval_store.reject(
            approval_id,
            operator_note=operator_note,
        )
        self._record_event(
            "operator",
            "approval_marked_rejected",
            approval_id=approval.approval_id,
            strategy_id=approval.strategy_id,
        )
        return to_jsonable(approval)

    def _submit_approved_action(self, approval_id: str) -> None:
        approval = self._approval_store.get(approval_id)
        if approval is None:
            return
        order_manager = self._order_manager
        loop = self._runtime_loop
        if order_manager is None or loop is None or not loop.is_running():
            self._record_event(
                "operator",
                "approval_submission_deferred",
                approval_id=approval_id,
                strategy_id=approval.strategy_id,
                reason="engine_not_running",
            )
            return

        async def submit() -> None:
            await order_manager.submit_intent(approval.intent, approval.strategy_id)

        loop.call_soon_threadsafe(lambda: loop.create_task(submit()))

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
        trace = pop_decision(state)
        if trace is None:
            return
        self._record_last_decision(trace.to_event())
        if self._audit is not None:
            self._audit.decision(trace)

    def _record_last_decision(self, event: dict[str, Any]) -> None:
        strategy_id = event.get("strategy_id")
        if not strategy_id:
            return

        with self._state_lock:
            previous_decision = self._last_strategy_decisions.get(str(strategy_id))

        summary: dict[str, Any] = {
            "strategy_id": strategy_id,
            "timestamp": event.get("timestamp"),
            "phase": event.get("phase"),
            "decision": event.get("decision"),
        }
        operator_summary = event.get("operator_summary")
        if isinstance(operator_summary, dict):
            readiness_pct = _percentage_or_none(operator_summary.get("entry_readiness_pct"))
            readiness_label = operator_summary.get("entry_readiness_label")
            if (
                readiness_pct is None
                and event.get("phase") == "entry"
                and isinstance(previous_decision, dict)
                and _same_local_date(
                    previous_decision.get("timestamp"),
                    event.get("timestamp"),
                    self._session_tz,
                )
            ):
                previous_pct = _percentage_or_none(previous_decision.get("entry_readiness_pct"))
                if previous_pct is not None:
                    readiness_pct = previous_pct
                    readiness_label = previous_decision.get("entry_readiness_label")
            summary.update({
                "entry_readiness_pct": readiness_pct,
                "entry_readiness_label": readiness_label,
            })
        if not is_customer_profile(self._metadata_profile):
            if isinstance(operator_summary, dict):
                trigger_times = operator_summary.get("trigger_times")
                if isinstance(trigger_times, list):
                    summary["trigger_times"] = to_jsonable(trigger_times)
            metrics = event.get("metrics")
            if not isinstance(metrics, dict):
                metrics = {}
            signal = event.get("signal")
            if not isinstance(signal, dict):
                signal = {}
            instrument = signal.get("instrument")
            if not isinstance(instrument, dict):
                instrument = {}
            summary.update({
                "signal_side": signal.get("side"),
                "signal_symbol": instrument.get("symbol"),
                "trade_id": signal.get("trade_id"),
                "entry_stop_pct": _first_present(
                    metrics,
                    "entry_stop_pct",
                    "atr_trailing_stop_pct",
                ),
                "entry_stop_pct_source": metrics.get("entry_stop_pct_source"),
                "protective_stop_mode": metrics.get("protective_stop_mode"),
            })
        with self._state_lock:
            self._last_strategy_decisions[str(strategy_id)] = to_jsonable(summary)

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

    async def _startup_open_order_gate(
        self,
        enabled_instruments: set[Instrument],
    ) -> None:
        get_open_orders = getattr(self._broker, "get_open_orders", None)
        if not callable(get_open_orders):
            return
        open_orders = await get_open_orders()
        relevant = [
            order for order in open_orders
            if _matches_any_instrument(order.instrument, enabled_instruments)
        ]
        self._set_reconciliation_state(
            enabled=True,
            status="startup_checked",
            last_check=datetime.now(tz=timezone.utc).isoformat(),
            open_order_count=len(open_orders),
            relevant_open_order_count=len(relevant),
        )
        if not relevant:
            return
        reason = "startup_open_orders_require_operator_review"
        self._set_entry_block(reason)
        self._record_event(
            "reconciliation",
            "startup_open_orders_blocked",
            reason=reason,
            open_orders=relevant,
        )
        raise RuntimeError(
            "Open broker orders exist for enabled instruments; resolve them in "
            "TWS/Gateway before live startup or add an explicit adopt/cancel workflow."
        )

    async def _live_health_monitor(self, enabled_instruments: set[Instrument]) -> None:
        interval = float(self._live_health_config.get("check_interval_seconds", 15.0))
        stale_seconds = float(self._live_health_config.get("stale_bar_seconds", 120.0))
        reconnect = bool(self._live_health_config.get("reconnect", True))
        while True:
            await asyncio.sleep(max(1.0, interval))
            try:
                now = datetime.now(tz=timezone.utc)
                broker_connected = self._broker_is_connected()
                data_connected = self._data_feed_is_connected()
                self._set_runtime_state(
                    broker_connected=broker_connected,
                    data_connected=data_connected,
                )
                stale_bar = False
                last_received = self._last_bar_received_at
                if last_received is not None:
                    stale_bar = (now - last_received).total_seconds() > stale_seconds
                reason = None
                if not broker_connected:
                    reason = "broker_disconnected"
                elif not data_connected:
                    reason = "data_disconnected"
                elif stale_bar:
                    reason = "stale_market_data"

                self._set_runtime_health(
                    last_check=now.isoformat(),
                    broker_connected=broker_connected,
                    data_connected=data_connected,
                    stale_bar=stale_bar,
                    block_reason=reason,
                )
                if reason is None:
                    self._clear_entry_block_if_health_owned()
                    continue
                self._set_entry_block(reason)
                self._record_event("health", reason, stale_seconds=stale_seconds)
                if reconnect and reason in {"broker_disconnected", "data_disconnected"}:
                    await self._attempt_reconnect(enabled_instruments, reason)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_entry_block("health_monitor_error")
                self._record_event("health", "health_monitor_error", error=str(exc))

    async def _attempt_reconnect(
        self,
        enabled_instruments: set[Instrument],
        reason: str,
    ) -> None:
        try:
            self._record_event("health", "reconnect_attempt", reason=reason)
            await self._broker.connect()
            await self._data_feed.connect()
            await self._data_feed.resubscribe_all()
            self._set_runtime_state(
                broker_connected=self._broker_is_connected(),
                data_connected=self._data_feed_is_connected(),
            )
            self._record_event(
                "health",
                "reconnect_complete",
                instruments=[instrument.symbol for instrument in enabled_instruments],
            )
        except Exception as exc:
            self._record_event("health", "reconnect_failed", error=str(exc))

    async def _runtime_reconciliation_loop(
        self,
        portfolio: PortfolioState,
        order_manager: OrderManager,
        enabled_instruments: set[Instrument],
    ) -> None:
        cfg = self._runtime_reconciliation_config
        enabled = bool(cfg.get("enabled", True))
        interval = float(cfg.get("interval_seconds", 60.0))
        self._set_reconciliation_state(enabled=enabled, status="idle")
        if not enabled:
            return
        while True:
            await asyncio.sleep(max(5.0, interval))
            try:
                await self._run_reconciliation_snapshot(
                    portfolio,
                    order_manager,
                    enabled_instruments,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_entry_block("reconciliation_error")
                self._set_reconciliation_state(
                    status="error",
                    block_reason="reconciliation_error",
                    last_error=str(exc),
                    last_check=datetime.now(tz=timezone.utc).isoformat(),
                )
                self._record_event("reconciliation", "runtime_reconciliation_error", error=str(exc))

    async def _run_reconciliation_snapshot(
        self,
        portfolio: PortfolioState,
        order_manager: OrderManager,
        enabled_instruments: set[Instrument],
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        account = await self._broker.get_account()
        self._set_latest_account_snapshot(account)
        broker_positions = await self._broker.get_positions()
        get_open_orders = getattr(self._broker, "get_open_orders", None)
        open_orders = await get_open_orders() if callable(get_open_orders) else []
        relevant_open_orders = [
            order for order in open_orders
            if _matches_any_instrument(order.instrument, enabled_instruments)
        ]
        unknown_order_ids = [
            order.broker_order_id
            for order in relevant_open_orders
            if order.broker_order_id not in order_manager.tracked_open_order_ids()
        ]
        broker_qty = _position_quantities(broker_positions, enabled_instruments)
        local_qty = _position_quantities(portfolio.positions(), enabled_instruments)
        position_drift = broker_qty != local_qty
        status = "ok"
        block_reason = None
        if unknown_order_ids:
            status = "blocked"
            block_reason = "unknown_open_orders"
        elif position_drift:
            status = "blocked"
            block_reason = "position_drift"
        self._set_reconciliation_state(
            enabled=True,
            status=status,
            last_check=now.isoformat(),
            block_reason=block_reason,
            unknown_order_ids=unknown_order_ids,
            broker_positions=broker_qty,
            local_positions=local_qty,
        )
        if block_reason is not None:
            self._set_entry_block(f"reconciliation_{block_reason}")
            self._record_event(
                "reconciliation",
                "runtime_reconciliation_blocked",
                reason=block_reason,
                unknown_order_ids=unknown_order_ids,
                broker_positions=broker_qty,
                local_positions=local_qty,
            )
        elif self._entry_block_reason and self._entry_block_reason.startswith("reconciliation_"):
            self._set_entry_block(None)

    async def _maybe_cancel_session_protective_stops(
        self,
        order_manager: OrderManager,
        timestamp: datetime,
    ) -> None:
        ts = timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        try:
            local_ts = ts.astimezone(ZoneInfo(self._session_tz))
        except Exception:
            local_ts = ts.astimezone(timezone.utc)
        if local_ts.time() < dt_time(15, 59):
            return
        if self._last_session_stop_cancel_date == local_ts.date():
            return
        self._last_session_stop_cancel_date = local_ts.date()
        await order_manager.cancel_session_protective_stops(reason="rth_session_end")

    def _set_managers(self, managers: dict[Instrument, DataManager]) -> None:
        with self._state_lock:
            self._managers = dict(managers)

    def _set_portfolio(self, portfolio: PortfolioState) -> None:
        with self._state_lock:
            self._portfolio = portfolio

    def _set_latest_account_snapshot(self, account) -> None:
        with self._state_lock:
            self._latest_account_snapshot = account

    def _latest_account_for_risk(self):
        with self._state_lock:
            return self._latest_account_snapshot

    def _broker_is_connected(self) -> bool:
        connected = getattr(self._broker, "is_connected", None)
        if callable(connected):
            return bool(connected())
        return bool(self._broker_connected)

    def _data_feed_is_connected(self) -> bool:
        connected = getattr(self._data_feed, "is_connected", None)
        if callable(connected):
            return bool(connected())
        return bool(self._data_connected)

    def _order_entry_block_reason(
        self,
        _strategy_id: str,
        _instrument: Instrument,
    ) -> str | None:
        with self._state_lock:
            return self._entry_block_reason

    def _set_entry_block(self, reason: str | None) -> None:
        with self._state_lock:
            previous = self._entry_block_reason
            self._entry_block_reason = reason
            self._runtime_health["entry_blocked"] = reason is not None
            self._runtime_health["block_reason"] = reason
        if reason and reason != previous:
            self._record_event("health", "entries_blocked", reason=reason)
        elif reason is None and previous is not None:
            self._record_event("health", "entries_unblocked", previous_reason=previous)

    def _clear_entry_block_if_health_owned(self) -> None:
        with self._state_lock:
            reason = self._entry_block_reason
        if reason in {"broker_disconnected", "data_disconnected", "stale_market_data"}:
            self._set_entry_block(None)

    def _set_runtime_health(self, **fields: Any) -> None:
        with self._state_lock:
            self._runtime_health.update(to_jsonable(fields))

    def _set_reconciliation_state(self, **fields: Any) -> None:
        with self._state_lock:
            self._reconciliation_state.update(to_jsonable(fields))

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
        received_at = datetime.now(tz=timezone.utc)
        with self._state_lock:
            self._bar_count += 1
            self._last_bar_received_at = received_at
            self._last_bar = {
                "instrument": bar.instrument,
                "timeframe": bar.timeframe.label,
                "timestamp": bar.timestamp,
                "source": bar.source,
            }
            self._runtime_health["last_bar_received_at"] = received_at.isoformat()

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


def _signed_fill_quantity(fill: Fill) -> float:
    if fill.side.upper() in {"BOT", "BUY", "B", "LONG"}:
        return float(fill.quantity)
    return -float(fill.quantity)


def _option_request_key(request: OptionDataRequest) -> tuple:
    option = request.instrument
    option_key = None
    if option is not None:
        option_key = (
            option.asset_class,
            option.symbol,
            option.expiry,
            option.strike,
            option.right,
            option.multiplier,
        )
    return (
        request.request_type,
        request.underlying.asset_class,
        request.underlying.symbol,
        request.underlying.exchange,
        request.underlying.currency,
        option_key,
    )


def _future_requires_resolution(instrument: Instrument) -> bool:
    return (
        str(instrument.asset_class).lower() == "future"
        and instrument.expiry is None
    )


def _matches_any_instrument(
    instrument: Instrument,
    candidates: set[Instrument],
) -> bool:
    return any(_instrument_match(instrument, candidate) for candidate in candidates)


def _instrument_match(left: Instrument, right: Instrument) -> bool:
    return (
        str(left.asset_class).lower() == str(right.asset_class).lower()
        and left.symbol.upper() == right.symbol.upper()
        and (left.expiry == right.expiry or left.expiry is None or right.expiry is None)
        and (left.exchange == right.exchange or not left.exchange or not right.exchange)
        and (left.currency == right.currency or not left.currency or not right.currency)
    )


def _position_quantities(
    positions: Sequence[Position],
    instruments: set[Instrument],
) -> dict[str, float]:
    result: dict[str, float] = {}
    for position in positions:
        if not _matches_any_instrument(position.instrument, instruments):
            continue
        key = _instrument_quantity_key(position.instrument)
        result[key] = round(result.get(key, 0.0) + float(position.quantity), 8)
    return {key: qty for key, qty in sorted(result.items()) if abs(qty) > 1e-8}


def _instrument_quantity_key(instrument: Instrument) -> str:
    expiry = instrument.expiry.isoformat() if instrument.expiry is not None else ""
    return "|".join([
        str(instrument.asset_class).lower(),
        instrument.symbol.upper(),
        expiry,
        str(instrument.exchange or ""),
        str(instrument.currency or ""),
    ])


def _supports_inline_order_drain(broker: Any) -> bool:
    return (
        callable(getattr(broker, "ready_fills", None))
        and callable(getattr(broker, "ready_order_updates", None))
    )


def _first_present(values: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = values.get(key)
        if value is not None:
            return value
    return None


def _percentage_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(pct):
        return None
    return round(max(0.0, min(100.0, pct)), 2)


def _same_local_date(left: Any, right: Any, tz_name: str) -> bool:
    left_dt = _datetime_or_none(left)
    right_dt = _datetime_or_none(right)
    if left_dt is None or right_dt is None:
        return False
    try:
        tz = ZoneInfo(str(tz_name))
    except Exception:
        tz = timezone.utc
    return left_dt.astimezone(tz).date() == right_dt.astimezone(tz).date()


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


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

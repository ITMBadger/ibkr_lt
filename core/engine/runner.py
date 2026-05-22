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
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import TYPE_CHECKING, Any

from ..audit.serialize import to_jsonable
from ..data.bar_builder import BarBuilder
from ..data.feed import DataFeed
from ..data.manager import DataManager
from ..orders.order_manager import OrderManager
from ..orders.strategy_modes import strategy_mode_map
from ..features.registry import FeatureRegistry
from ..audit import AuditLogger, pop_decision
from .timeframes import TF_1M, TF_5S
from ..portfolio.state import PortfolioState
from ..interfaces.strategy import StrategyKernel
from ..risk.policy import RiskPolicy
from ..types import Instrument, Position, Signal
from .clock import Clock, SimulatedClock, WallClock
from .scheduler import Scheduler

if TYPE_CHECKING:
    from ..interfaces.broker import BrokerAdapter
    from ..interfaces.data import HistoricalDataProvider, StreamingDataProvider

log = logging.getLogger(__name__)

_ORDER_TASK_YIELD_SECONDS = 0.001


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
        thread_pool_workers: int = 4,
        lookback_days: int = 500,
        session_tz: str = "America/New_York",
        adopted_position_map: dict[Instrument, str] | None = None,
        audit_logger: AuditLogger | None = None,
        strategy_modes: Mapping[str, str] | None = None,
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
        self._risk = risk or RiskPolicy()
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
                "running": self._phase in {"starting", "running"},
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
                        "mode": self._strategy_modes.get(kernel.SPEC.id, "live"),
                    }
                    for kernel, _ in self._strategies
                ],
                "risk": {
                    "position_size_shares": self._risk.position_size_shares,
                    "max_order_quantity": self._risk.max_order_quantity,
                },
                "recent_events": list(self._recent_events),
            }

        state["positions"] = {
            "broker": to_jsonable(portfolio.positions()) if portfolio is not None else [],
            "strategy": [
                {"strategy_id": sid, "position": to_jsonable(position)}
                for sid, position in portfolio.strategy_positions()
            ] if portfolio is not None else [],
            "net_liquidation": portfolio.net_liquidation() if portfolio is not None else 0.0,
        }
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

        # Connect broker
        await self._broker.connect()
        self._set_runtime_state(broker_connected=True)
        await self._data_feed.connect()
        self._set_runtime_state(data_connected=True)

        # Portfolio state and order manager
        portfolio = PortfolioState()
        self._set_portfolio(portfolio)
        adopted = await self._broker.get_positions()
        if adopted:
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
        protective_stops = {
            kernel.SPEC.id: kernel.SPEC.protective_stop
            for kernel, _ in self._strategies
            if kernel.SPEC.protective_stop is not None
        }
        order_manager = OrderManager(
            self._broker,
            portfolio,
            self._risk,
            self._audit,
            protective_stops=protective_stops,
            strategy_modes=self._strategy_modes,
        )

        # Backfill historical data. Split feeds may load offline history first
        # and supplement the gap with broker historical bars before live starts.
        end = self._clock.now()
        if isinstance(self._clock, SimulatedClock) and end.year == 1:
            end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=self._lookback_days)
        for instr, dm in managers.items():
            try:
                bars = await self._data_feed.fetch(instr, TF_1M, start, end)
                dm.merge_backfill(bars)
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

        # Subscribe streaming
        for instr in all_instruments:
            native_tfs = self._data_feed.capabilities.native_timeframes
            if native_tfs and TF_5S in native_tfs:
                # IBKR: subscribe at 5s, build 1m
                await self._data_feed.subscribe(instr, TF_5S)
                bar_builders[instr] = BarBuilder(instr, TF_5S, TF_1M)
            else:
                # Replay / moomoo: subscribe at 1m directly
                await self._data_feed.subscribe(instr, TF_1M)
            self._record_event("data", "subscribed", instrument=instr.symbol)

        # Register strategies in scheduler
        scheduler = Scheduler(features)
        for kernel, initial_state in self._strategies:
            state: dict = dict(initial_state)
            scheduler.register(kernel, state)
            kernel.on_start(state)

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

        try:
            async for raw_bar in self._data_feed.bars():
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

                # Update DataManager
                dm = managers.get(bar_1m.instrument)
                if dm is None:
                    continue
                dm.on_bar(bar_1m)
                self._record_bar(bar_1m)

                # Let PaperBroker resolve pending orders on new bar
                if hasattr(self._broker, "on_bar"):
                    await self._broker.on_bar(bar_1m)
                    if is_simulated:
                        await order_manager.drain_ready_fills()
                        await order_manager.drain_ready_order_updates()
                    else:
                        await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)

                # Invalidate feature caches for this instrument
                features.invalidate(bar_1m.instrument)

                # Dispatch to strategies via thread pool
                dispatch_results = scheduler.on_bar(bar_1m, managers)
                for kernel, ctx, state in dispatch_results:
                    try:
                        position = portfolio.get_strategy_position(
                            kernel.SPEC.id,
                            kernel.SPEC.execution_instrument,
                        )
                        if position is not None:
                            if is_simulated:
                                reason = kernel.on_exit(ctx, position, state)
                            else:
                                reason = await loop.run_in_executor(
                                    pool, kernel.on_exit, ctx, position, state
                                )
                            self._write_decision_trace(state)
                            if reason:
                                self._write_signal_event(
                                    kernel.SPEC.id,
                                    "exit",
                                    ctx.timestamp,
                                    reason=reason,
                                    instrument=position.instrument,
                                    side=position.side,
                                )
                                await order_manager.submit_close(
                                    kernel.SPEC.id,
                                    position,
                                    reason,
                                )
                            continue
                        if is_simulated:
                            signal = kernel.generate(ctx, state)
                        else:
                            signal = await loop.run_in_executor(
                                pool, kernel.generate, ctx, state
                            )
                        self._write_decision_trace(state)
                        if signal is not None:
                            self._write_signal_event(
                                kernel.SPEC.id,
                                "entry",
                                ctx.timestamp,
                                signal=signal,
                            )
                            await order_manager.submit(signal, kernel.SPEC.id)
                            if is_simulated:
                                await order_manager.drain_ready_orders()
                                await order_manager.drain_ready_order_updates()
                            else:
                                await asyncio.sleep(_ORDER_TASK_YIELD_SECONDS)
                    except Exception as e:
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

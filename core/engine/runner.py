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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ..data.bar_builder import BarBuilder
from ..data.feed import DataFeed
from ..data.manager import DataManager
from ..orders.order_manager import OrderManager
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
        self._risk = risk or RiskPolicy()
        self._pool_workers = thread_pool_workers
        self._lookback_days = lookback_days
        self._session_tz = session_tz
        self._adopted_position_map = adopted_position_map or {}
        self._audit = audit_logger

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_live(self) -> None:
        """Start live trading. Blocks until interrupted."""
        asyncio.run(self._run())

    def run_backtest(self) -> None:
        """Run backtest to completion. Blocks until all replay bars are consumed."""
        asyncio.run(self._run())

    # ------------------------------------------------------------------
    # Core coroutine
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        is_simulated = isinstance(self._clock, SimulatedClock)

        # Build DataManagers for every instrument requested as strategy data.
        all_instruments: set[Instrument] = set()
        for kernel, _ in self._strategies:
            all_instruments.add(kernel.SPEC.primary_instrument)
            all_instruments.update(kernel.SPEC.reference_instruments)

        managers: dict[Instrument, DataManager] = {
            instr: DataManager(instr, self._lookback_days, self._session_tz)
            for instr in all_instruments
        }
        features = FeatureRegistry(managers)
        bar_builders: dict[Instrument, BarBuilder] = {}

        # Connect broker
        await self._broker.connect()
        await self._data_feed.connect()

        # Portfolio state and order manager
        portfolio = PortfolioState()
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
        order_manager = OrderManager(self._broker, portfolio, self._risk, self._audit)

        # Backfill historical data. The live session date is excluded so live
        # bars remain authoritative for the current session.
        end = self._clock.now()
        if isinstance(self._clock, SimulatedClock) and end.year == 1:
            end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=self._lookback_days)
        for instr, dm in managers.items():
            try:
                bars = await self._data_feed.fetch(instr, TF_1M, start, end)
                dm.merge_backfill(bars, live_session_date=end.date())
                log.info("Backfilled %d bars for %s", len(bars), instr.symbol)
            except Exception as e:
                log.warning("Backfill failed for %s: %s", instr.symbol, e)

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

        # Register strategies in scheduler
        scheduler = Scheduler(features)
        for kernel, initial_state in self._strategies:
            state: dict = dict(initial_state)
            scheduler.register(kernel, state)
            kernel.on_start(state)

        # Start background tasks
        pool = ThreadPoolExecutor(max_workers=self._pool_workers)
        drain_orders_task = loop.create_task(order_manager.drain_orders())
        drain_fills_task = loop.create_task(order_manager.drain_fills())

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

                # Let PaperBroker resolve pending orders on new bar
                if hasattr(self._broker, "on_bar"):
                    await self._broker.on_bar(bar_1m)
                    await asyncio.sleep(0)

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
                            reason: str | None = await loop.run_in_executor(
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
                        signal: Signal | None = await loop.run_in_executor(
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
                            await asyncio.sleep(0)
                    except Exception as e:
                        log.exception("Strategy %s raised: %s", kernel.SPEC.id, e)
                        self._write_signal_event(
                            kernel.SPEC.id,
                            "error",
                            ctx.timestamp,
                            error=str(e),
                        )

        except Exception as e:
            log.exception("Engine _run error: %s", e)
        finally:
            drain_orders_task.cancel()
            drain_fills_task.cancel()
            pool.shutdown(wait=False)
            await self._data_feed.disconnect()
            await self._broker.disconnect()

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

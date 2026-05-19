"""Phase 6 unit tests: adapter hardening, dry-run, exits, split data feed."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core import DataFeed, Engine, SimulatedClock
from core.audit import AuditLogger, DecisionTrace, record_decision
from core.adapters.dry_run import DryRunBroker
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.engine.timeframes import TF_1M
from core.interfaces.strategy import StrategyKernel, StrategySpec
from core.orders.order_manager import OrderManager
from core.portfolio.state import PortfolioState
from core.risk.policy import RiskPolicy
from core.types import Bar, Instrument, MarketContext, OrderRequest, Position, Signal

QQQ = Instrument(asset_class="equity", symbol="QQQ")
SPY = Instrument(asset_class="equity", symbol="SPY")
MNQ = Instrument(asset_class="future", symbol="MNQ", multiplier=2.0)


def _bars(instrument: Instrument, n: int = 3) -> list[Bar]:
    base = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
    return [
        Bar(
            instrument=instrument,
            timeframe=TF_1M,
            timestamp=base + timedelta(minutes=i),
            open=100.0 + i,
            high=101.0 + i,
            low=99.0 + i,
            close=100.5 + i,
            volume=1000.0,
            is_closed=True,
            source="test",
        )
        for i in range(n)
    ]


class _CountingBroker(PaperBroker):
    def __init__(self) -> None:
        super().__init__()
        self.submit_calls = 0

    async def submit_order(self, order: OrderRequest):
        self.submit_calls += 1
        return await super().submit_order(order)


class _RecordingReplay(ReplayDataProvider):
    def __init__(self, bars: list[Bar]) -> None:
        super().__init__(bars)
        self.subscriptions: list[Instrument] = []

    async def subscribe(self, instrument: Instrument, timeframe) -> None:
        self.subscriptions.append(instrument)
        await super().subscribe(instrument, timeframe)


class _OneShotStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_phase6_one_shot",
        primary_instrument=QQQ,
        execution_instrument=MNQ,
        timeframes=("1m",),
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        if not state.get("fired"):
            state["fired"] = True
            return Signal(instrument=MNQ, side="long")
        return None


class _ExitStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_phase6_exit",
        primary_instrument=QQQ,
        execution_instrument=MNQ,
        timeframes=("1m",),
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        if not state.get("entered"):
            state["entered"] = True
            return Signal(instrument=MNQ, side="long")
        return None

    def on_exit(self, ctx: MarketContext, position: Position, state: dict) -> str | None:
        return "test_exit"


class _FeatureStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_phase6_feature",
        primary_instrument=QQQ,
        execution_instrument=MNQ,
        timeframes=("1m",),
    )

    def __init__(self) -> None:
        super().__init__()
        self.feature_seen = False

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        ema = ctx.features.get("ema", QQQ, "1m", period=2) if ctx.features else None
        self.feature_seen = ema is not None and len(ema) > 0
        return None


class _TraceStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_phase6_trace",
        primary_instrument=QQQ,
        execution_instrument=MNQ,
        timeframes=("1m",),
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        trace = DecisionTrace.entry(ctx, self.SPEC.id)
        bars = ctx.bars[QQQ]["1m"]
        trace.add_bar("qqq_1m_current", QQQ, "1m", bars.iloc[-1])
        trace.add_condition("always_false", False, lhs=1, op=">", rhs=2)
        trace.set_decision("no_signal", reason="test")
        record_decision(state, trace)
        return None


class _StaticHistorical:
    def __init__(self, bars: list[Bar]) -> None:
        self._bars = bars
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def fetch(self, instrument, timeframe, start, end) -> list[Bar]:
        return [b for b in self._bars if b.instrument == instrument]


class _StaticLive(ReplayDataProvider):
    def __init__(self, bars: list[Bar]) -> None:
        super().__init__(bars)
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True


def test_dry_run_never_calls_native_submit():
    async def run():
        native = _CountingBroker()
        dry = DryRunBroker(native)
        order = OrderRequest(MNQ, "long", 1.0, "market")
        status = await dry.submit_order(order)
        assert status.status == "dry_run"
        assert native.submit_calls == 0
        assert dry.intended_orders == [order]

    import asyncio
    asyncio.run(run())


def test_data_feed_splits_historical_and_live():
    async def run():
        hist = _StaticHistorical(_bars(QQQ, 2))
        live = _StaticLive(_bars(SPY, 1))
        feed = DataFeed(hist, live)
        await feed.connect()
        fetched = await feed.fetch(QQQ, TF_1M, datetime.min.replace(tzinfo=timezone.utc), datetime.max.replace(tzinfo=timezone.utc))
        await feed.subscribe(SPY, TF_1M)
        emitted = []
        async for bar in feed.bars():
            emitted.append(bar)
            break
        await feed.disconnect()
        assert hist.connected and hist.disconnected
        assert live.connected and live.disconnected
        assert [b.instrument for b in fetched] == [QQQ, QQQ]
        assert emitted[0].instrument == SPY

    import asyncio
    asyncio.run(run())


def test_engine_does_not_subscribe_execution_instrument_as_data():
    provider = _RecordingReplay(_bars(QQQ, 1))
    broker = PaperBroker()
    engine = Engine(
        broker=broker,
        streaming=provider,
        historical=None,
        clock=SimulatedClock(),
        strategies=[(_OneShotStrategy(), {})],
        risk=RiskPolicy(position_size_shares=1, max_order_quantity=2),
        thread_pool_workers=1,
        lookback_days=10,
    )
    engine.run_backtest()
    assert QQQ in provider.subscriptions
    assert MNQ not in provider.subscriptions


def test_engine_on_exit_submits_market_close():
    provider = ReplayDataProvider(_bars(QQQ, 4))
    broker = PaperBroker()
    engine = Engine(
        broker=broker,
        streaming=provider,
        historical=None,
        clock=SimulatedClock(),
        strategies=[(_ExitStrategy(), {})],
        risk=RiskPolicy(position_size_shares=1, max_order_quantity=2),
        thread_pool_workers=1,
        lookback_days=10,
    )
    engine.run_backtest()

    async def positions():
        return await broker.get_positions()

    import asyncio
    assert asyncio.run(positions()) == []


def test_engine_attaches_shared_feature_registry_to_context():
    provider = ReplayDataProvider(_bars(QQQ, 3))
    broker = PaperBroker()
    strategy = _FeatureStrategy()
    engine = Engine(
        broker=broker,
        streaming=provider,
        historical=None,
        clock=SimulatedClock(),
        strategies=[(strategy, {})],
        risk=RiskPolicy(position_size_shares=1, max_order_quantity=2),
        thread_pool_workers=1,
        lookback_days=10,
    )
    engine.run_backtest()
    assert strategy.feature_seen


def test_engine_writes_strategy_decision_trace(tmp_path):
    provider = ReplayDataProvider(_bars(QQQ, 2))
    broker = PaperBroker()
    audit = AuditLogger(log_dir=tmp_path)
    engine = Engine(
        broker=broker,
        streaming=provider,
        historical=None,
        clock=SimulatedClock(),
        strategies=[(_TraceStrategy(), {})],
        risk=RiskPolicy(position_size_shares=1, max_order_quantity=2),
        thread_pool_workers=1,
        lookback_days=10,
        audit_logger=audit,
    )
    engine.run_backtest()
    lines = (tmp_path / "strategy_decisions.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert '"strategy_id":"_phase6_trace"' in lines[0]
    assert '"always_false"' in lines[0]


def test_order_manager_writes_order_and_fill_audit(tmp_path):
    async def run():
        broker = PaperBroker()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(broker, PortfolioState(), RiskPolicy(), audit)
        await om._process_signal(Signal(MNQ, "long"), "_phase6")
        import asyncio
        task = asyncio.create_task(om.drain_fills())
        await broker.on_bar(_bars(MNQ, 1)[0])
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    import asyncio
    asyncio.run(run())
    assert (tmp_path / "orders.jsonl").exists()
    assert "order_submitted" in (tmp_path / "orders.jsonl").read_text(encoding="utf-8")
    assert (tmp_path / "fills.jsonl").exists()


def test_order_manager_rejects_unsupported_short_entry():
    async def run():
        broker = _CountingBroker()
        broker.capabilities = type(broker).capabilities.__class__(
            asset_classes=type(broker).capabilities.asset_classes,
            order_types=type(broker).capabilities.order_types,
            quantity_rules=type(broker).capabilities.quantity_rules,
            supports_short=False,
        )
        om = OrderManager(broker, PortfolioState(), RiskPolicy())
        await om._process_signal(Signal(MNQ, "short"), "_phase6")
        assert broker.submit_calls == 0

    import asyncio
    asyncio.run(run())

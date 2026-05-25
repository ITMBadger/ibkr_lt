"""Phase 6 unit tests: adapter hardening, strategy dry-run, exits, split data feed."""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from core import DataFeed, Engine, SimulatedClock, WallClock
from core.audit import AuditLogger, DecisionTrace, record_decision
from core.adapters.ibkr.data import IBKRDataProvider
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.engine.loader import get_registry, load_strategies
from core.engine.timeframes import TF_1M, TF_5S
from core.interfaces.strategy import (
    POSITION_MODE_MULTI,
    PositionPolicy,
    ProtectiveStopSpec,
    StrategyKernel,
    StrategySpec,
)
from core.orders.order_manager import OrderManager
from core.portfolio.state import PortfolioState
from core.risk.policy import RiskPolicy
from core.startup import PositionOwnershipLedger
from core.types import Bar, Fill, Instrument, MarketContext, OrderRequest, OrderStatus, Position, Signal

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
        self.cancel_calls = 0
        self.submitted_orders: list[OrderRequest] = []
        self.cancelled_order_ids: list[str] = []

    async def submit_order(self, order: OrderRequest):
        self.submit_calls += 1
        self.submitted_orders.append(order)
        return await super().submit_order(order)

    async def cancel_order(self, broker_order_id: str) -> None:
        self.cancel_calls += 1
        self.cancelled_order_ids.append(broker_order_id)
        await super().cancel_order(broker_order_id)


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


class _AdoptableQqqStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_adoptable_qqq",
        primary_instrument=QQQ,
        execution_instrument=QQQ,
        timeframes=("1m",),
        position_policy=PositionPolicy(supports_position_adoption=True),
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        return None

    def on_adopt_position(self, position, adoption, state):
        state["adopted_entry_ts"] = adoption.entry_ts
        return position


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


class _FakeIBKRDataClient:
    def __init__(self, hist_items: list[dict] | None = None) -> None:
        self.bar_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.hist_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.hist_items = hist_items or []
        self.historical_calls: list[dict] = []
        self.realtime_calls: list[dict] = []
        self.cancelled_realtime: list[int] = []

    def is_ready(self) -> bool:
        return True

    def reqHistoricalData(
        self,
        req_id,
        contract,
        end_str,
        duration_str,
        bar_size,
        what_to_show,
        use_rth,
        format_date,
        keep_up_to_date,
        chart_options,
    ) -> None:
        self.historical_calls.append({
            "req_id": req_id,
            "end_str": end_str,
            "duration_str": duration_str,
            "bar_size": bar_size,
            "what_to_show": what_to_show,
        })
        for item in self.hist_items:
            payload = {"req_id": req_id, **item}
            self.hist_queue.put_nowait(payload)

    def reqRealTimeBars(
        self,
        req_id,
        contract,
        bar_size,
        what_to_show,
        use_rth,
        realtime_bar_options,
    ) -> None:
        self.realtime_calls.append({
            "req_id": req_id,
            "bar_size": bar_size,
            "what_to_show": what_to_show,
        })

    def cancelRealTimeBars(self, req_id) -> None:
        self.cancelled_realtime.append(req_id)


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


def test_data_feed_supplements_historical_gap_from_live_provider():
    async def run():
        bars = _bars(QQQ, 4)
        hist = _StaticHistorical([bars[0]])
        live = _StaticLive(bars[1:])
        feed = DataFeed(hist, live)

        fetched = await feed.fetch(
            QQQ,
            TF_1M,
            bars[0].timestamp,
            bars[-1].timestamp,
        )

        assert [bar.timestamp for bar in fetched] == [bar.timestamp for bar in bars]
        assert fetched[0].source == "test"
        assert fetched[-1].source == "test"

    import asyncio
    asyncio.run(run())


def test_ibkr_fetch_caps_1m_duration_and_uses_midpoint_for_fx(monkeypatch):
    async def run():
        monkeypatch.setattr(
            "core.adapters.ibkr.data.instrument_to_contract",
            lambda instrument: object(),
        )
        client = _FakeIBKRDataClient(hist_items=[{"done": True}])
        provider = IBKRDataProvider(client)
        await provider.fetch(
            Instrument(asset_class="fx", symbol="EUR"),
            TF_1M,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 5, 1, tzinfo=timezone.utc),
        )

        call = client.historical_calls[0]
        assert call["duration_str"] == "10 D"
        assert call["bar_size"] == "1 min"
        assert call["what_to_show"] == "MIDPOINT"

    asyncio.run(run())


def test_ibkr_fetch_logs_unparsable_dates(monkeypatch, caplog):
    async def run():
        monkeypatch.setattr(
            "core.adapters.ibkr.data.instrument_to_contract",
            lambda instrument: object(),
        )
        client = _FakeIBKRDataClient(hist_items=[
            {
                "date": "not-a-date",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
                "volume": 1000,
            },
            {"done": True},
        ])
        provider = IBKRDataProvider(client)
        return await provider.fetch(
            QQQ,
            TF_1M,
            datetime(2026, 5, 1, tzinfo=timezone.utc),
            datetime(2026, 5, 2, tzinfo=timezone.utc),
        )

    caplog.set_level(logging.WARNING, logger="core.adapters.ibkr.data")
    assert asyncio.run(run()) == []
    assert "unparsable IBKR date" in caplog.text


def test_ibkr_bars_uses_live_subscription_lookup(monkeypatch):
    async def run():
        monkeypatch.setattr(
            "core.adapters.ibkr.data.instrument_to_contract",
            lambda instrument: object(),
        )
        client = _FakeIBKRDataClient()
        provider = IBKRDataProvider(client)
        next_bar = asyncio.create_task(anext(provider.bars()))
        await asyncio.sleep(0)

        await provider.subscribe(QQQ, TF_5S)
        req_id = client.realtime_calls[0]["req_id"]
        await client.bar_queue.put({
            "req_id": req_id,
            "timestamp": datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10.0,
        })
        bar = await asyncio.wait_for(next_bar, timeout=1)
        assert bar.instrument == QQQ
        assert bar.volume == 1000.0

    asyncio.run(run())


def test_simulated_engine_subscribes_execution_instrument_for_paper_fills():
    provider = _RecordingReplay([*_bars(QQQ, 1), *_bars(MNQ, 1)])
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
    assert MNQ in provider.subscriptions


def test_non_simulated_engine_does_not_subscribe_execution_instrument_as_data():
    provider = _RecordingReplay(_bars(QQQ, 1))
    broker = PaperBroker()
    engine = Engine(
        broker=broker,
        streaming=provider,
        historical=None,
        clock=WallClock(),
        strategies=[(_OneShotStrategy(), {})],
        risk=RiskPolicy(position_size_shares=1, max_order_quantity=2),
        thread_pool_workers=1,
        lookback_days=10,
    )
    engine.run_live()
    assert QQQ in provider.subscriptions
    assert MNQ not in provider.subscriptions


def test_engine_on_exit_submits_market_close():
    provider = ReplayDataProvider([*_bars(QQQ, 4), *_bars(MNQ, 4)])
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
    decision_files = sorted(tmp_path.glob("strategy_eval__phase6_trace_*_et/decision.csv"))
    assert len(decision_files) == 2
    with decision_files[0].open("r", encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))
    assert row["strategy_id"] == "_phase6_trace"
    assert row["condition_always_false"] == "False"


def test_startup_gate_ignores_unrelated_positions():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableQqqStrategy(), {})],
        startup_position_gate_enabled=True,
    )

    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(SPY, quantity=5, avg_cost=100.0)],
        [(_AdoptableQqqStrategy(), {})],
    )

    assert status["phase"] == "clear"
    assert status["positions"] == []
    assert status["unmanaged"][0]["symbol"] == "SPY"


def test_startup_gate_uses_operator_quantity():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableQqqStrategy(), {})],
        strategy_risk={"_adoptable_qqq": RiskPolicy(position_size_shares=2)},
        startup_position_gate_enabled=True,
    )
    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(QQQ, quantity=5, avg_cost=100.0)],
        [(_AdoptableQqqStrategy(), {})],
    )
    engine._set_startup_gate_status(status)  # noqa: SLF001

    position_id = status["positions"][0]["position_id"]
    result = engine.submit_startup_mappings([
        {
            "position_id": position_id,
            "strategy_id": "_adoptable_qqq",
            "quantity": 3,
        }
    ])

    assert result["phase"] == "mapped"
    assert result["allocations"][0]["quantity"] == 3.0


def test_startup_gate_rejects_insufficient_broker_quantity():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableQqqStrategy(), {})],
        strategy_risk={"_adoptable_qqq": RiskPolicy(position_size_shares=2)},
        startup_position_gate_enabled=True,
    )
    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(QQQ, quantity=1, avg_cost=100.0)],
        [(_AdoptableQqqStrategy(), {})],
    )
    engine._set_startup_gate_status(status)  # noqa: SLF001

    with pytest.raises(ValueError, match="exceeds broker quantity"):
        engine.submit_startup_mappings([
            {
                "position_id": status["positions"][0]["position_id"],
                "strategy_id": "_adoptable_qqq",
                "quantity": 2,
            }
        ])


def test_startup_gate_rejects_missing_allocation_quantity():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableQqqStrategy(), {})],
        startup_position_gate_enabled=True,
    )
    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(QQQ, quantity=1, avg_cost=100.0)],
        [(_AdoptableQqqStrategy(), {})],
    )
    engine._set_startup_gate_status(status)  # noqa: SLF001

    with pytest.raises(ValueError, match="must include quantity"):
        engine.submit_startup_mappings([
            {
                "position_id": status["positions"][0]["position_id"],
                "strategy_id": "_adoptable_qqq",
            }
        ])


def test_startup_gate_blocks_derivative_contract_mismatch():
    strategy_instrument = Instrument(
        asset_class="future",
        symbol="MNQ",
        exchange="CME",
        currency="USD",
        expiry=date(2026, 6, 19),
        multiplier=2.0,
    )
    broker_instrument = Instrument(
        asset_class="future",
        symbol="MNQ",
        exchange="CME",
        currency="USD",
        expiry=date(2026, 9, 18),
        multiplier=2.0,
    )

    class _AdoptableFutureStrategy(StrategyKernel):
        SPEC = StrategySpec(
            id="_adoptable_future",
            primary_instrument=strategy_instrument,
            execution_instrument=strategy_instrument,
            timeframes=("1m",),
            position_policy=PositionPolicy(supports_position_adoption=True),
        )

        def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
            return None

    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableFutureStrategy(), {})],
        startup_position_gate_enabled=True,
    )
    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(broker_instrument, quantity=1, avg_cost=100.0)],
        [(_AdoptableFutureStrategy(), {})],
    )

    assert status["phase"] == "blocked"
    assert status["positions"][0]["reason"] == "instrument_contract_not_exactly_declared"


def test_startup_gate_uses_configured_adopted_position_mapping():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_AdoptableQqqStrategy(), {})],
        startup_position_gate_enabled=True,
        startup_position_allocations=[
            {
                "symbol": "QQQ",
                "asset_class": "equity",
                "strategy_id": "_adoptable_qqq",
                "quantity": 1,
                "source": "config",
            }
        ],
    )
    status = engine._build_startup_gate_status(  # noqa: SLF001
        [Position(QQQ, quantity=1, avg_cost=100.0)],
        [(_AdoptableQqqStrategy(), {})],
    )

    assert status["phase"] == "clear"
    assert status["allocations"][0]["strategy_id"] == "_adoptable_qqq"
    assert status["allocations"][0]["source"] == "config"


def test_startup_gate_fails_fast_without_mapping_interface():
    async def run():
        engine = Engine(
            broker=PaperBroker(),
            data_feed=DataFeed(None, ReplayDataProvider([])),
            strategies=[(_AdoptableQqqStrategy(), {})],
            startup_position_gate_enabled=True,
            startup_position_mapping_enabled=False,
        )
        with pytest.raises(RuntimeError, match="no mapping interface"):
            await engine._run_startup_position_gate(  # noqa: SLF001
                [Position(QQQ, quantity=1, avg_cost=100.0)],
                [(_AdoptableQqqStrategy(), {})],
            )

    asyncio.run(run())


def test_position_ownership_ledger_recovers_fill_allocation(tmp_path):
    ledger = PositionOwnershipLedger(tmp_path / "ownership.json")
    ledger.apply_fill(
        Fill(
            broker_order_id="1",
            instrument=QQQ,
            side="long",
            quantity=2,
            price=100.0,
            timestamp=datetime(2026, 5, 25, 14, 18, tzinfo=timezone.utc),
        ),
        strategy_id="_adoptable_qqq",
        role="entry",
        trade_id="lot_a",
    )

    allocations = ledger.open_allocations()

    assert allocations == [
        {
            "strategy_id": "_adoptable_qqq",
            "quantity": 2.0,
            "entry_ts": "2026-05-25T14:18:00+00:00",
            "trade_id": "lot_a",
            "source": "ownership_ledger",
            "side": "long",
            "instrument": {
                "asset_class": "equity",
                "symbol": "QQQ",
                "exchange": None,
                "currency": None,
                "expiry": None,
                "strike": None,
                "right": None,
                "multiplier": 1.0,
            },
        }
    ]


def test_load_strategies_accepts_protected_package(tmp_path, monkeypatch):
    package_dir = tmp_path / "protected_pkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "demo.py").write_text(
        "\n".join([
            "from core import Instrument, MarketContext, Signal",
            "from core.engine.loader import register_strategy",
            "from core.interfaces.strategy import StrategyKernel, StrategySpec",
            "",
            "QQQ = Instrument(asset_class='equity', symbol='QQQ')",
            "",
            "@register_strategy",
            "class ProtectedLoaderTestStrategy(StrategyKernel):",
            "    SPEC = StrategySpec(",
            "        id='_protected_loader_test',",
            "        primary_instrument=QQQ,",
            "        execution_instrument=QQQ,",
            "        timeframes=('1m',),",
            "    )",
            "",
            "    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:",
            "        return None",
        ]),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    load_strategies(["protected_pkg"])

    assert "_protected_loader_test" in get_registry()


def test_audit_run_subdir_uses_et_minute_and_unique_suffix(tmp_path):
    ts = datetime(2026, 5, 20, 14, 3, 45, tzinfo=timezone.utc)
    audit = AuditLogger(log_dir=tmp_path, run_subdir=True, run_started_at=ts)
    duplicate = AuditLogger(log_dir=tmp_path, run_subdir=True, run_started_at=ts)

    assert audit.log_dir == tmp_path / "20260520_1003_et"
    assert duplicate.log_dir == tmp_path / "20260520_1003_et_2"

    audit.signal({"event": "started"})
    assert (audit.log_dir / "signals.jsonl").exists()
    assert not (tmp_path / "signals.jsonl").exists()


def test_audit_from_config_defaults_to_run_subdir(tmp_path):
    audit = AuditLogger.from_config({"logging": {"log_dir": tmp_path}})

    assert audit is not None
    assert audit.run_subdir is True
    assert audit.log_dir.parent == tmp_path
    assert audit.log_dir.name.endswith("_et")


def _full_decision_trace(ts: datetime, decision: str = "no_signal") -> DecisionTrace:
    trace = DecisionTrace(phase="entry", strategy_id="_phase6_trace", timestamp=ts)
    trace.add_bar(
        "qqq_3m_current",
        QQQ,
        "3m",
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000.0},
    )
    trace.add_indicator("stoch_d_current", 21.0, instrument=QQQ, timeframe="3m")
    trace.add_condition(
        "stoch_d_cross_above_threshold",
        decision == "signal",
        lhs={"prior": 18.0, "current": 21.0},
        op="cross_above",
        rhs=20.0,
    )
    signal = Signal(QQQ, "long") if decision == "signal" else None
    reason = "stoch_d_crossed_above_threshold" if signal else "stoch_d_not_crossed"
    trace.set_decision(decision, reason=reason, signal=signal)
    return trace


def test_audit_trigger_and_interval_decision_scope(tmp_path):
    audit = AuditLogger(
        log_dir=tmp_path,
        decision_scope="trigger_and_interval",
        decision_interval_minutes=30,
    )

    audit.decision(_full_decision_trace(datetime(2026, 5, 20, 14, 3, tzinfo=timezone.utc)))
    first_interval_dir = tmp_path / "strategy_30m__phase6_trace_20260520_1000_et"
    assert first_interval_dir.exists()
    first_csv = (first_interval_dir / "decision.csv").read_text(encoding="utf-8")
    assert "condition_stoch_d_cross_above_threshold" in first_csv

    audit.decision(_full_decision_trace(datetime(2026, 5, 20, 14, 15, tzinfo=timezone.utc)))
    assert len(list(tmp_path.glob("strategy_30m__phase6_trace_*_et"))) == 1
    assert (first_interval_dir / "decision.csv").read_text(encoding="utf-8") == first_csv

    audit.decision(_full_decision_trace(
        datetime(2026, 5, 20, 14, 18, tzinfo=timezone.utc),
        decision="signal",
    ))
    trigger_dir = tmp_path / "strategy_trigger__phase6_trace_20260520_1018_et"
    assert trigger_dir.exists()
    assert (first_interval_dir / "decision.csv").read_text(encoding="utf-8") == first_csv

    audit.decision(_full_decision_trace(datetime(2026, 5, 20, 14, 31, tzinfo=timezone.utc)))
    second_interval_dir = tmp_path / "strategy_30m__phase6_trace_20260520_1030_et"
    assert second_interval_dir.exists()
    second_csv = (second_interval_dir / "decision.csv").read_text(encoding="utf-8")
    assert second_csv != first_csv
    assert "2026-05-20T10:31:00-04:00" in second_csv


def test_audit_interval_csv_flattens_multitimeframe_trace(tmp_path):
    audit = AuditLogger(
        log_dir=tmp_path,
        decision_scope="trigger_and_interval",
        decision_interval_minutes=30,
    )
    trace = DecisionTrace(
        phase="entry",
        strategy_id="_phase6_multi_tf",
        timestamp=datetime(2026, 5, 20, 14, 3, tzinfo=timezone.utc),
    )
    trace.add_bar(
        "qqq_3m_current",
        QQQ,
        "3m",
        {
            "timestamp": "2026-05-20T14:03:00+00:00",
            "open": 101.0,
            "high": 102.0,
            "low": 100.5,
            "close": 101.5,
            "volume": 1000.0,
        },
    )
    trace.add_bar(
        "qqq_15m_current",
        QQQ,
        "15m",
        {
            "timestamp": "2026-05-20T14:00:00+00:00",
            "open": 99.0,
            "high": 101.8,
            "low": 98.9,
            "close": 101.1,
            "volume": 5400.0,
        },
    )
    trace.add_bar(
        "qqq_30m_current",
        QQQ,
        "30m",
        {
            "timestamp": "2026-05-20T13:30:00+00:00",
            "open": 98.4,
            "high": 101.9,
            "low": 98.2,
            "close": 101.0,
            "volume": 9900.0,
        },
    )
    trace.add_indicator("ema20_3m", 101.2, instrument=QQQ, timeframe="3m")
    trace.add_indicator("ema20_15m", 100.7, instrument=QQQ, timeframe="15m")
    trace.add_indicator("ema20_30m", 100.1, instrument=QQQ, timeframe="30m")
    trace.add_condition("entry_window", True, lhs=1003, op="in", rhs="[1000,1530)")
    trace.add_condition("mtf_alignment", False, lhs={"3m": 1, "15m": 1, "30m": -1}, op="all_same", rhs=True)
    trace.set_decision("no_signal", reason="mtf_alignment_failed")

    audit.decision(trace)

    trace_dir = tmp_path / "strategy_30m__phase6_multi_tf_20260520_1000_et"
    assert trace_dir.exists()
    with (trace_dir / "decision.csv").open("r", encoding="utf-8", newline="") as fh:
        row = next(csv.DictReader(fh))

    assert list(row)[:5] == ["eval_datetime_et", "strategy_id", "phase", "decision", "reason"]
    assert row["eval_datetime_et"] == "2026-05-20T10:03:00-04:00"
    assert row["qqq_3m_current_datetime_et"] == "2026-05-20T10:03:00-04:00"
    assert row["qqq_3m_current_open"] == "101.0"
    assert row["qqq_3m_current_high"] == "102.0"
    assert row["qqq_3m_current_low"] == "100.5"
    assert row["qqq_3m_current_close"] == "101.5"
    assert row["qqq_3m_current_volume"] == "1000.0"
    assert row["qqq_15m_current_datetime_et"] == "2026-05-20T10:00:00-04:00"
    assert row["qqq_30m_current_datetime_et"] == "2026-05-20T09:30:00-04:00"
    assert row["ema20_3m"] == "101.2"
    assert row["ema20_15m"] == "100.7"
    assert row["ema20_30m"] == "100.1"
    assert row["condition_entry_window"] == "True"
    assert row["condition_mtf_alignment"] == "False"
    assert row["reason"] == "mtf_alignment_failed"


def test_audit_decision_trace_writes_5_row_table_snapshot(tmp_path):
    audit = AuditLogger(
        log_dir=tmp_path,
        decision_scope="trigger_and_interval",
        decision_interval_minutes=30,
    )
    trace = DecisionTrace(
        phase="entry",
        strategy_id="_phase6_table",
        timestamp=datetime(2026, 5, 20, 14, 15, tzinfo=timezone.utc),
    )
    index = pd.date_range("2026-05-20T14:03:00+00:00", periods=5, freq="3min")
    frame = pd.DataFrame(
        {
            "open": [100.11111, 101.0, 102.0, 103.0, 104.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
            "volume": [1000, 1100, 1200, 1300, 1400],
            "stoch_d": [18.0, 19.0, 19.5, 20.5, 21.123456],
            "condition_stoch_d_cross_above_threshold": [False, False, False, True, False],
        },
        index=index,
    )
    trace.add_bar("qqq_3m_current", QQQ, "3m", frame.iloc[-1])
    trace.add_table("qqq_3m", QQQ, "3m", frame)
    trace.add_indicator("stoch_d_current", 21.123456, instrument=QQQ, timeframe="3m")
    trace.add_condition("stoch_d_cross_above_threshold", False)
    trace.set_decision("no_signal", reason="stoch_d_not_crossed")

    audit.decision(trace)

    trace_dir = tmp_path / "strategy_30m__phase6_table_20260520_1000_et"
    with (trace_dir / "qqq_3m.csv").open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 5
    assert rows[0]["time"] == "2026-05-20T10:03:00-04:00"
    assert rows[0]["bar_offset"] == "-4"
    assert rows[-1]["bar_offset"] == "0"
    assert rows[-1]["stoch_d"] == "21.1235"
    assert rows[-2]["condition_stoch_d_cross_above_threshold"] == "True"


def test_audit_interval_accepts_table_only_decision_detail(tmp_path):
    audit = AuditLogger(
        log_dir=tmp_path,
        decision_scope="trigger_and_interval",
        decision_interval_minutes=30,
    )
    trace = DecisionTrace(
        phase="entry",
        strategy_id="_phase6_table_only",
        timestamp=datetime(2026, 5, 20, 14, 15, tzinfo=timezone.utc),
    )
    trace.add_table(
        "qqq_3m",
        QQQ,
        "3m",
        [
            {
                "timestamp": "2026-05-20T14:15:00+00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
                "condition_entry": False,
            }
        ],
    )
    trace.add_condition("entry", False)
    trace.set_decision("no_signal", reason="entry_failed")

    audit.decision(trace)

    trace_dir = tmp_path / "strategy_30m__phase6_table_only_20260520_1000_et"
    assert (trace_dir / "decision.csv").exists()
    assert (trace_dir / "qqq_3m.csv").exists()


def test_audit_csv_naive_datetimes_are_marked_et(tmp_path):
    audit = AuditLogger(log_dir=tmp_path, decision_scope="every_eval")
    trace = DecisionTrace(
        phase="entry",
        strategy_id="_phase6_naive_time",
        timestamp=datetime(2026, 5, 20, 10, 15),
    )
    trace.add_bar(
        "qqq_3m_current",
        QQQ,
        "3m",
        {
            "timestamp": datetime(2026, 5, 20, 10, 15),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
        },
    )
    trace.add_condition("entry", False)
    trace.set_decision("no_signal", reason="entry_failed")

    audit.decision(trace)

    with (tmp_path / "strategy_eval__phase6_naive_time_20260520_1015_et" / "decision.csv").open(
        "r",
        encoding="utf-8",
        newline="",
    ) as fh:
        row = next(csv.DictReader(fh))

    assert row["eval_datetime_et"] == "2026-05-20T10:15:00-04:00"
    assert row["qqq_3m_current_datetime_et"] == "2026-05-20T10:15:00-04:00"


def test_audit_trigger_trace_dirs_do_not_overwrite(tmp_path):
    audit = AuditLogger(log_dir=tmp_path, decision_scope="trigger_and_interval")
    audit.decision(_full_decision_trace(
        datetime(2026, 5, 20, 14, 3, tzinfo=timezone.utc),
        decision="signal",
    ))
    audit.decision(_full_decision_trace(
        datetime(2026, 5, 20, 14, 3, 30, tzinfo=timezone.utc),
        decision="signal",
    ))

    assert (tmp_path / "strategy_trigger__phase6_trace_20260520_1003_et").exists()
    assert (tmp_path / "strategy_trigger__phase6_trace_20260520_1003_et_2").exists()


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


def test_order_manager_strategy_dry_run_does_not_submit_entry(tmp_path):
    async def run():
        broker = _CountingBroker()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(
            broker,
            PortfolioState(),
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            audit,
            strategy_modes={"_phase6": "dry_run"},
        )
        await om._process_signal(Signal(MNQ, "long"), "_phase6")
        assert broker.submit_calls == 0

    import asyncio
    asyncio.run(run())
    text = (tmp_path / "orders.jsonl").read_text(encoding="utf-8")
    assert "order_intent" in text
    assert "order_dry_run" in text
    assert "order_submitted" not in text


def test_order_manager_strategy_dry_run_does_not_submit_close(tmp_path):
    async def run():
        broker = _CountingBroker()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(
            broker,
            PortfolioState(),
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            audit,
            strategy_modes={"_phase6": "dry_run"},
        )
        await om.submit_close("_phase6", Position(QQQ, 1, 100.0), "test_exit")
        assert broker.submit_calls == 0

    import asyncio
    asyncio.run(run())
    text = (tmp_path / "orders.jsonl").read_text(encoding="utf-8")
    assert "close_intent" in text
    assert "close_dry_run" in text
    assert "close_submitted" not in text


def test_order_manager_drops_duplicate_pending_close(tmp_path):
    async def run():
        broker = _CountingBroker()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(
            broker,
            PortfolioState(),
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            audit,
        )
        position = Position(QQQ, 1, 100.0)

        await om.submit_close("_phase6", position, "first_exit")
        await om.submit_close("_phase6", position, "second_exit")

        assert broker.submit_calls == 1

    import asyncio
    asyncio.run(run())
    text = (tmp_path / "orders.jsonl").read_text(encoding="utf-8")
    assert "close_submitted" in text
    assert "close_already_pending" in text


def test_order_manager_keeps_close_pending_until_fill_after_filled_update():
    async def run():
        broker = _CountingBroker()
        om = OrderManager(
            broker,
            PortfolioState(),
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
        )
        position = Position(QQQ, 1, 100.0)

        await om.submit_close("_phase6", position, "test_exit")
        close_order_id = broker.submitted_orders[0].idempotency_key
        om._handle_order_update(OrderStatus(close_order_id, "filled", filled_qty=1))
        await om.submit_close("_phase6", position, "test_exit_again")

        assert broker.submit_calls == 1

    import asyncio
    asyncio.run(run())


def test_order_manager_live_strategy_still_submits_with_other_dry_run_strategy():
    async def run():
        broker = _CountingBroker()
        om = OrderManager(
            broker,
            PortfolioState(),
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            strategy_modes={"_dry_strategy": "dry_run"},
        )
        await om._process_signal(Signal(MNQ, "long"), "_live_strategy")
        assert broker.submit_calls == 1
        assert broker.submitted_orders[0].strategy_id == "_live_strategy"

    import asyncio
    asyncio.run(run())


def test_order_manager_multi_position_policy_allows_independent_lots():
    async def run():
        broker = _CountingBroker()
        portfolio = PortfolioState()
        om = OrderManager(
            broker,
            portfolio,
            RiskPolicy(position_size_shares=1, max_order_quantity=5),
            position_policies={
                "_multi": PositionPolicy(position_mode=POSITION_MODE_MULTI),
            },
        )

        bars = _bars(QQQ, 2)
        await om._process_signal(Signal(QQQ, "long", trade_id="lot_a"), "_multi")
        await broker.on_bar(bars[0])
        await om.drain_ready_fills()

        await om._process_signal(Signal(QQQ, "long", trade_id="lot_b"), "_multi")
        await broker.on_bar(bars[1])
        await om.drain_ready_fills()

        lots = portfolio.get_strategy_positions("_multi", QQQ)
        assert broker.submit_calls == 2
        assert sorted(pos.trade_id for pos in lots) == ["lot_a", "lot_b"]
        assert portfolio.get_strategy_position("_multi", QQQ).quantity == 2

    import asyncio
    asyncio.run(run())


def test_order_manager_submits_fill_price_protective_stop():
    async def run():
        broker = _CountingBroker()
        portfolio = PortfolioState()
        om = OrderManager(
            broker,
            portfolio,
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            protective_stops={
                "_phase6": ProtectiveStopSpec(pct=0.015, reference="fill_price"),
            },
        )
        await om._process_signal(Signal(QQQ, "long"), "_phase6")

        task = asyncio.create_task(om.drain_fills())
        await broker.on_bar(_bars(QQQ, 1)[0])
        await asyncio.sleep(0)

        assert len(broker.submitted_orders) == 2
        stop = broker.submitted_orders[1]
        assert stop.instrument == QQQ
        assert stop.side == "short"
        assert stop.order_type == "stop"
        assert stop.quantity == 1
        assert stop.stop_price == 98.5

        await broker.on_bar(Bar(
            instrument=QQQ,
            timeframe=TF_1M,
            timestamp=datetime(2026, 5, 1, 13, 31, tzinfo=timezone.utc),
            open=99.0,
            high=99.5,
            low=98.0,
            close=98.5,
            volume=1000.0,
            is_closed=True,
            source="test",
        ))
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert portfolio.get_strategy_position("_phase6", QQQ) is None

    import asyncio
    asyncio.run(run())


def test_order_manager_cancels_protective_stop_when_strategy_close_submits(tmp_path):
    async def run():
        broker = _CountingBroker()
        portfolio = PortfolioState()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(
            broker,
            portfolio,
            RiskPolicy(position_size_shares=1, max_order_quantity=2),
            audit,
            protective_stops={
                "_phase6": ProtectiveStopSpec(pct=0.015, reference="fill_price"),
            },
        )
        await om._process_signal(Signal(QQQ, "long"), "_phase6")
        await broker.on_bar(_bars(QQQ, 1)[0])
        await om.drain_ready_fills()
        await om.drain_ready_order_updates()

        position = portfolio.get_strategy_position("_phase6", QQQ)
        assert position is not None
        stop_order_id = broker.submitted_orders[1].idempotency_key

        await om.submit_close("_phase6", position, "target_exit")

        assert broker.cancel_calls == 1
        assert broker.cancelled_order_ids == [stop_order_id]

        await broker.on_bar(Bar(
            instrument=QQQ,
            timeframe=TF_1M,
            timestamp=datetime(2026, 5, 1, 13, 31, tzinfo=timezone.utc),
            open=102.0,
            high=103.0,
            low=90.0,
            close=91.0,
            volume=1000.0,
            is_closed=True,
            source="test",
        ))
        await om.drain_ready_fills()
        await om.drain_ready_order_updates()

        assert portfolio.get_strategy_position("_phase6", QQQ) is None

    import asyncio
    asyncio.run(run())
    text = (tmp_path / "orders.jsonl").read_text(encoding="utf-8")
    assert "protective_stop_cancel_requested" in text


def test_order_manager_drains_order_updates(tmp_path):
    async def run():
        broker = PaperBroker()
        audit = AuditLogger(log_dir=tmp_path)
        om = OrderManager(broker, PortfolioState(), RiskPolicy(), audit)
        await om._process_signal(Signal(QQQ, "long"), "_phase6")

        task = asyncio.create_task(om.drain_order_updates())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert broker._order_update_queue.empty()

    import asyncio
    asyncio.run(run())
    assert "order_update" in (tmp_path / "orders.jsonl").read_text(encoding="utf-8")


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

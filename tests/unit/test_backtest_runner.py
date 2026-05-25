from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backtest.config import (
    BacktestSettings,
    parse_boundary,
    resolve_settings,
    resolve_strategy_ids,
)
from backtest.loaders import required_instruments, resolve_evaluation_timeframes
from backtest.run import run_backtest
from core import DataFeed, Engine, SimulatedClock
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.audit.logger import AuditLogger
from core.interfaces.strategy import StrategyKernel, StrategySpec
from core.types import Bar, Instrument, MarketContext, Signal
from core.engine.timeframes import TF_1M

SPY = Instrument(asset_class="equity", symbol="SPY")
MES = Instrument(asset_class="future", symbol="MES", multiplier=5.0)


class _CrossInstrumentStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_bt_cross",
        primary_instrument=SPY,
        execution_instrument=MES,
        timeframes=("1m",),
        warmup_bars={"1m": 2},
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        if state.get("fired"):
            return None
        if len(ctx.bars[SPY]["1m"]) < 2:
            return None
        state["fired"] = True
        return Signal(instrument=MES, side="long")


class _ThreeMinuteCountingStrategy(StrategyKernel):
    _BAR_SIZE = "3m"
    SPEC = StrategySpec(
        id="_bt_3m_count",
        primary_instrument=SPY,
        execution_instrument=SPY,
        timeframes=("1m", "3m"),
        warmup_bars={},
    )

    def __init__(self) -> None:
        self.generate_calls = 0

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        self.generate_calls += 1
        return None


class _ParallelPrecomputedStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="_bt_parallel_precomputed",
        primary_instrument=SPY,
        execution_instrument=SPY,
        timeframes=("1m",),
        warmup_bars={},
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        raise AssertionError("parallel mode must not call generate()")


def test_parse_boundary_date_only_uses_session_timezone():
    start = parse_boundary("2026-05-01", "America/New_York", is_end=False)
    end = parse_boundary("2026-05-01", "America/New_York", is_end=True)
    assert start == datetime(2026, 5, 1, 4, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 2, 3, 59, 59, 999999, tzinfo=timezone.utc)


def test_resolve_strategy_ids_supports_repeated_and_comma_values():
    result = resolve_strategy_ids(
        override_values=["a,b", "c"],
        configured=None,
        known_strategy_ids=["a", "b", "c"],
    )
    assert result == ["a", "b", "c"]


def test_required_instruments_includes_execution_instrument():
    instruments = required_instruments([(_CrossInstrumentStrategy(), {})])
    assert instruments == [MES, SPY]


def test_resolve_evaluation_timeframes_uses_strategy_bar_size():
    strategies = [(_ThreeMinuteCountingStrategy(), {})]

    assert resolve_evaluation_timeframes(strategies) == {"_bt_3m_count": "3m"}
    assert resolve_evaluation_timeframes(strategies, "5m") == {"_bt_3m_count": "5m"}


def test_audit_log_dir_template_uses_runtime_mode(tmp_path):
    audit = AuditLogger.from_config({
        "mode": "paper",
        "logging": {
            "enabled": False,
            "log_dir": str(tmp_path / "runs" / "{mode}"),
        },
    })

    assert audit is not None
    assert audit.base_log_dir == tmp_path / "runs" / "paper"


def test_resolve_settings_defaults_to_runs_backtests(tmp_path):
    args = Namespace(
        config="config.yaml",
        csv=tmp_path / "QQQ.csv",
        start="2026-05-01",
        end="2026-05-01",
        strategy=None,
        lookback_days=None,
        output_dir=None,
        mode=None,
        eval_timeframe=None,
        all_live=False,
        thread_pool_workers=None,
        no_progress=False,
        progress_interval_bars=None,
        progress_interval_seconds=None,
        max_parallel_workers=None,
    )

    settings = resolve_settings(
        args=args,
        config={"data": {"historical": {"provider": "csv"}}},
        known_strategy_ids=["demo_strategy"],
    )

    assert settings.output_dir == Path("runs/backtests")


def test_resolve_settings_accepts_parallel_mode(tmp_path):
    args = Namespace(
        config="config.yaml",
        csv=tmp_path / "QQQ.csv",
        start="2026-05-01",
        end="2026-05-01",
        strategy=None,
        lookback_days=None,
        output_dir=None,
        mode="parallel",
        eval_timeframe=None,
        all_live=False,
        thread_pool_workers=None,
        no_progress=False,
        progress_interval_bars=None,
        progress_interval_seconds=None,
        max_parallel_workers=2,
    )

    settings = resolve_settings(
        args=args,
        config={"data": {"historical": {"provider": "csv"}}},
        known_strategy_ids=["demo_strategy"],
    )

    assert settings.mode == "parallel"
    assert settings.max_parallel_workers == 2


def test_resolve_settings_accepts_full_equity_sizing(tmp_path):
    args = Namespace(
        config="config.yaml",
        csv=tmp_path / "QQQ.csv",
        start="2026-05-01",
        end="2026-05-01",
        strategy=None,
        lookback_days=None,
        output_dir=None,
        mode=None,
        eval_timeframe=None,
        all_live=False,
        thread_pool_workers=None,
        no_progress=False,
        progress_interval_bars=None,
        progress_interval_seconds=None,
        max_parallel_workers=None,
    )

    settings = resolve_settings(
        args=args,
        config={
            "data": {"historical": {"provider": "csv"}},
            "backtest": {
                "sizing": {
                    "mode": "full-account",
                    "equity_fraction": 0.75,
                },
            },
        },
        known_strategy_ids=["demo_strategy"],
    )

    assert settings.sizing_mode == "full_equity"
    assert settings.sizing_equity_fraction == 0.75
    assert settings.sizing_max_order_quantity is None


def test_run_backtest_uses_execution_instrument_bars_for_paper_fills(tmp_path):
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    _write_csv(csv_dir / "SPY.csv", 100.0)
    _write_csv(csv_dir / "MES.csv", 5000.0)

    settings = BacktestSettings(
        config_path=tmp_path / "config.yaml",
        csv_path=csv_dir,
        start=datetime(2026, 5, 1, 13, 29, tzinfo=timezone.utc),
        end=datetime(2026, 5, 1, 13, 33, tzinfo=timezone.utc),
        lookback_days=12,
        session_tz="UTC",
        strategy_ids=["_bt_cross"],
        strategy_modes={"_bt_cross": "live"},
        position_size_shares=1,
        max_order_quantity=2,
        thread_pool_workers=1,
        output_dir=tmp_path / "runs",
        audit_enabled=True,
        logging={"strategy_decisions": "off"},
        rth_only=False,
        market_open="00:00",
        market_close="23:59",
    )

    result = run_backtest(settings, [(_CrossInstrumentStrategy(), {})])

    assert result.processed_bars == 8
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["audit_stats"]["fills"]["count"] == 1
    assert result.report_path is not None
    assert result.report_path.exists()
    assert summary["report"]["path"].endswith("report.html")
    assert summary["report"]["metrics"]["open_positions"] == 1
    fill = json.loads((result.run_dir / "fills.jsonl").read_text(encoding="utf-8"))
    assert fill["fill"]["instrument"]["symbol"] == "MES"
    assert fill["fill"]["price"] == 5003.0


def test_run_backtest_full_equity_sizing_ignores_legacy_fixed_share_cap(tmp_path):
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    _write_csv(csv_dir / "SPY.csv", 100.0)
    _write_csv(csv_dir / "MES.csv", 5000.0)

    settings = BacktestSettings(
        config_path=tmp_path / "config.yaml",
        csv_path=csv_dir,
        start=datetime(2026, 5, 1, 13, 29, tzinfo=timezone.utc),
        end=datetime(2026, 5, 1, 13, 33, tzinfo=timezone.utc),
        lookback_days=12,
        session_tz="UTC",
        strategy_ids=["_bt_cross"],
        strategy_modes={"_bt_cross": "live"},
        position_size_shares=1,
        max_order_quantity=2,
        thread_pool_workers=1,
        output_dir=tmp_path / "runs",
        audit_enabled=True,
        logging={"strategy_decisions": "off"},
        rth_only=False,
        market_open="00:00",
        market_close="23:59",
        sizing_mode="full_equity",
        sizing_equity_fraction=1.0,
        sizing_max_order_quantity=None,
    )

    result = run_backtest(settings, [(_CrossInstrumentStrategy(), {})])

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    fill = json.loads((result.run_dir / "fills.jsonl").read_text(encoding="utf-8"))
    assert summary["sizing"] == {
        "mode": "full_equity",
        "equity_fraction": 1.0,
        "max_order_quantity": None,
        "position_size_shares": 1,
    }
    assert fill["fill"]["quantity"] > 2


def test_fast_event_mode_throttles_flat_entry_generation(tmp_path):
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    _write_csv(csv_dir / "SPY.csv", 100.0, minutes=10)

    settings = BacktestSettings(
        config_path=tmp_path / "config.yaml",
        csv_path=csv_dir,
        start=datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
        end=datetime(2026, 5, 1, 13, 39, tzinfo=timezone.utc),
        lookback_days=0,
        session_tz="UTC",
        strategy_ids=["_bt_3m_count"],
        strategy_modes={"_bt_3m_count": "live"},
        position_size_shares=1,
        max_order_quantity=2,
        thread_pool_workers=1,
        output_dir=tmp_path / "event_runs",
        audit_enabled=False,
        logging={},
        rth_only=False,
        market_open="00:00",
        market_close="23:59",
    )

    event_strategy = _ThreeMinuteCountingStrategy()
    fast_strategy = _ThreeMinuteCountingStrategy()

    event_result = run_backtest(settings, [(event_strategy, {})])
    fast_result = run_backtest(
        replace(settings, output_dir=tmp_path / "fast_runs", mode="fast-event"),
        [(fast_strategy, {})],
    )

    assert event_result.processed_bars == 10
    assert fast_result.processed_bars == 10
    assert event_strategy.generate_calls == 10
    assert 0 < fast_strategy.generate_calls < event_strategy.generate_calls


def test_engine_parallel_mode_uses_precomputed_entry_without_generate():
    bars = _bars(SPY, minutes=3)
    provider = ReplayDataProvider(bars)
    clock = SimulatedClock()
    clock.advance_to(bars[0].timestamp)
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(provider, provider),
        clock=clock,
        strategies=[(_ParallelPrecomputedStrategy(), {})],
        lookback_days=0,
        dispatch_mode="parallel",
        precomputed_entry_signals={
            "_bt_parallel_precomputed": [
                (bars[0].timestamp, Signal(instrument=SPY, side="long")),
            ],
        },
        progress_enabled=False,
    )

    engine.run_backtest()

    positions = engine.snapshot_state()["positions"]["strategy"]
    assert len(positions) == 1
    assert positions[0]["strategy_id"] == "_bt_parallel_precomputed"
    assert positions[0]["position"]["quantity"] == 1.0


def _write_csv(path, base_price: float, minutes: int = 4) -> None:
    base = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
    lines = ["timestamp,open,high,low,close,volume"]
    for index in range(minutes):
        ts = base + timedelta(minutes=index)
        price = base_price + index
        lines.append(
            f"{ts.isoformat()},{price},{price + 1},{price - 1},{price + 0.5},1000"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _bars(instrument: Instrument, minutes: int) -> list[Bar]:
    base = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
    return [
        Bar(
            instrument=instrument,
            timeframe=TF_1M,
            timestamp=base + timedelta(minutes=index),
            open=100.0 + index,
            high=101.0 + index,
            low=99.0 + index,
            close=100.5 + index,
            volume=1000.0,
            is_closed=True,
            source="test",
        )
        for index in range(minutes)
    ]

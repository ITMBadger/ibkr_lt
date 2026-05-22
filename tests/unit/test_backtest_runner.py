from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from backtest.config import BacktestSettings, parse_boundary, resolve_strategy_ids
from backtest.loaders import required_instruments
from backtest.run import run_backtest
from core.interfaces.strategy import StrategyKernel, StrategySpec
from core.types import Instrument, MarketContext, Signal

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
        lookback_days=0,
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
    fill = json.loads((result.run_dir / "fills.jsonl").read_text(encoding="utf-8"))
    assert fill["fill"]["instrument"]["symbol"] == "MES"
    assert fill["fill"]["price"] == 5003.0


def _write_csv(path, base_price: float) -> None:
    base = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
    lines = ["timestamp,open,high,low,close,volume"]
    for index in range(4):
        ts = base + timedelta(minutes=index)
        price = base_price + index
        lines.append(
            f"{ts.isoformat()},{price},{price + 1},{price - 1},{price + 0.5},1000"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

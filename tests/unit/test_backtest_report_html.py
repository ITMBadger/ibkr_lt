from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from backtest.report_html import write_html_report
from core.engine.timeframes import TF_1M
from core.types import Bar, Instrument


SPY = Instrument(asset_class="equity", symbol="SPY")


def test_html_report_reconstructs_closed_trade_equity(tmp_path):
    timestamps = [
        datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 14, 31, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 14, 32, tzinfo=timezone.utc),
    ]
    bars = [
        _bar(timestamps[0], close=101.0),
        _bar(timestamps[1], close=105.0),
        _bar(timestamps[2], close=110.0),
    ]
    _write_fill(
        tmp_path,
        timestamp=timestamps[0],
        role="entry",
        side="long",
        quantity=10,
        price=100.0,
        trade_id="trade-1",
    )
    _write_fill(
        tmp_path,
        timestamp=timestamps[2],
        role="close",
        side="short",
        quantity=10,
        price=110.0,
        trade_id="trade-1",
    )

    result = write_html_report(
        run_dir=tmp_path,
        summary=_summary(timestamps[0], timestamps[-1]),
        replay_bars=bars,
        initial_equity=100_000.0,
    )

    assert result.report_path.exists()
    assert result.metrics["closed_trades"] == 1
    assert result.metrics["open_positions"] == 0
    assert result.metrics["final_equity"] == pytest.approx(100_100.0)
    assert result.metrics["strategy_return"] == pytest.approx(0.001)
    assert result.metrics["equity_return"] == pytest.approx(0.001)
    assert result.metrics["sum_trade_return"] == pytest.approx(0.10)
    assert result.metrics["max_concurrent_open_lots"] == 1
    assert result.metrics["max_exposure_multiple"] > 0
    html = result.report_path.read_text(encoding="utf-8")
    assert "Strategy Performance" in html
    assert "Closed Trades" in html
    assert "Max Open Lots" in html
    assert "Max Exposure" in html
    assert "chart.umd.min.js" in html
    assert "<script" in html


def test_html_report_marks_open_position_to_latest_bar(tmp_path):
    timestamps = [
        datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 14, 31, tzinfo=timezone.utc),
    ]
    bars = [
        _bar(timestamps[0], close=100.0),
        _bar(timestamps[1], close=90.0),
    ]
    _write_fill(
        tmp_path,
        timestamp=timestamps[0],
        role="entry",
        side="long",
        quantity=10,
        price=100.0,
        trade_id="trade-1",
    )

    result = write_html_report(
        run_dir=tmp_path,
        summary=_summary(timestamps[0], timestamps[-1]),
        replay_bars=bars,
        initial_equity=100_000.0,
    )

    assert result.metrics["closed_trades"] == 0
    assert result.metrics["open_positions"] == 1
    assert result.metrics["max_concurrent_open_lots"] == 1
    assert result.metrics["final_equity"] == pytest.approx(99_900.0)
    assert result.metrics["strategy_return"] == pytest.approx(-0.001)
    assert result.metrics["equity_return"] == pytest.approx(-0.001)
    assert result.metrics["max_drawdown"] < 0


def _bar(timestamp: datetime, *, close: float) -> Bar:
    return Bar(
        instrument=SPY,
        timeframe=TF_1M,
        timestamp=timestamp,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1000.0,
        is_closed=True,
        source="test",
    )


def _summary(start: datetime, end: datetime) -> dict:
    return {
        "start": start,
        "end": end,
        "session_timezone": "UTC",
        "strategies": ["demo"],
        "mode": "event",
        "replay_bars": 2,
    }


def _write_fill(
    run_dir,
    *,
    timestamp: datetime,
    role: str,
    side: str,
    quantity: float,
    price: float,
    trade_id: str,
) -> None:
    event = {
        "event": "fill",
        "strategy_id": "demo",
        "role": role,
        "trade_id": trade_id,
        "fill": {
            "broker_order_id": f"order-{role}",
            "instrument": {
                "asset_class": "equity",
                "symbol": "SPY",
                "exchange": None,
                "currency": None,
                "expiry": None,
                "strike": None,
                "right": None,
                "multiplier": 1.0,
            },
            "side": side,
            "quantity": quantity,
            "price": price,
            "timestamp": timestamp.isoformat(),
        },
    }
    with (run_dir / "fills.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, separators=(",", ":")) + "\n")

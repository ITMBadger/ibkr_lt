"""Phase 3 unit tests: PortfolioState, PaperBroker, QuantityRules rounding."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from core.types import (
    Bar, Fill, Instrument, OrderRequest, QuantityRules,
)
from core.engine.timeframes import TF_1M
from core.portfolio.state import PortfolioState
from core.adapters.paper.broker import PaperBroker

QQQ = Instrument(asset_class="equity", symbol="QQQ")
MES = Instrument(asset_class="future", symbol="MES", multiplier=5.0)
BTC = Instrument(asset_class="crypto_perp", symbol="BTC")


def _bar(instrument=QQQ, price=100.0):
    return Bar(
        instrument=instrument, timeframe=TF_1M,
        timestamp=datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
        open=price, high=price + 1, low=price - 1, close=price,
        volume=1000.0, is_closed=True, source="paper",
    )


def _order(instrument=QQQ, side="long", qty=1.0, order_type="market"):
    return OrderRequest(
        instrument=instrument,
        side=side,
        quantity=qty,
        order_type=order_type,
        idempotency_key="test-001",
    )


# ---------------------------------------------------------------------------
# QuantityRules
# ---------------------------------------------------------------------------

class TestQuantityRulesRounding:
    def test_equity_round_to_integer(self):
        rules = QuantityRules(1.0, 1.0, 0)
        assert rules.round(3.7) == 4.0
        assert rules.round(0.3) == 1.0  # clamped to min

    def test_future_integer(self):
        rules = QuantityRules(1.0, 1.0, 0)
        assert rules.round(1.5) == 2.0

    def test_crypto_step(self):
        rules = QuantityRules(0.001, 0.001, 3)
        assert rules.round(0.0015) == pytest.approx(0.002)
        assert rules.round(0.0004) == pytest.approx(0.001)  # clamped to min


# ---------------------------------------------------------------------------
# PortfolioState
# ---------------------------------------------------------------------------

class TestPortfolioState:
    def _fill(self, side="long", qty=1.0, price=100.0):
        return Fill(
            broker_order_id="test",
            instrument=QQQ,
            side="BOT" if side == "long" else "SLD",
            quantity=qty,
            price=price,
            timestamp=datetime.now(tz=timezone.utc),
        )

    def test_long_position(self):
        ps = PortfolioState()
        ps.apply_fill(self._fill("long", 2.0, 100.0))
        pos = ps.get_position(QQQ)
        assert pos is not None
        assert pos.quantity == pytest.approx(2.0)
        assert pos.side == "long"

    def test_flat_after_close(self):
        ps = PortfolioState()
        ps.apply_fill(self._fill("long", 1.0))
        ps.apply_fill(self._fill("short", 1.0))
        assert ps.get_position(QQQ) is None
        assert ps.is_flat(QQQ)

    def test_positions_excludes_flat(self):
        ps = PortfolioState()
        ps.apply_fill(self._fill("long", 1.0))
        ps.apply_fill(self._fill("short", 1.0))
        assert ps.positions() == []

    def test_short_position(self):
        ps = PortfolioState()
        ps.apply_fill(self._fill("short", 3.0, 100.0))
        pos = ps.get_position(QQQ)
        assert pos is not None
        assert pos.quantity == pytest.approx(-3.0)
        assert pos.side == "short"

    def test_paper_long_side_is_buy(self):
        ps = PortfolioState()
        ps.apply_fill(Fill(
            broker_order_id="paper",
            instrument=QQQ,
            side="long",
            quantity=2.0,
            price=100.0,
            timestamp=datetime.now(tz=timezone.utc),
        ))
        pos = ps.get_position(QQQ)
        assert pos is not None
        assert pos.quantity == pytest.approx(2.0)

    def test_paper_short_side_is_sell(self):
        ps = PortfolioState()
        ps.apply_fill(Fill(
            broker_order_id="paper",
            instrument=QQQ,
            side="short",
            quantity=2.0,
            price=100.0,
            timestamp=datetime.now(tz=timezone.utc),
        ))
        pos = ps.get_position(QQQ)
        assert pos is not None
        assert pos.quantity == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# PaperBroker
# ---------------------------------------------------------------------------

class TestPaperBroker:
    def test_submit_and_fill_on_next_bar(self):
        broker = PaperBroker()

        async def run():
            status = await broker.submit_order(_order(QQQ, "long", 2.0))
            assert status.status == "pending"
            # Resolve fills
            bar = _bar(QQQ, price=105.0)
            await broker.on_bar(bar)
            fill = await asyncio.wait_for(broker._fill_queue.get(), timeout=1)
            assert fill.quantity == pytest.approx(2.0)
            assert fill.price == pytest.approx(105.0)
            assert fill.side == "long"

        asyncio.run(run())

    def test_cancel_removes_pending(self):
        broker = PaperBroker()

        async def run():
            status = await broker.submit_order(_order(QQQ, "long", 1.0))
            await broker.cancel_order(status.broker_order_id)
            await broker.on_bar(_bar(QQQ, 100.0))
            assert broker._fill_queue.empty()

        asyncio.run(run())

    def test_get_account(self):
        broker = PaperBroker()

        async def run():
            acct = await broker.get_account()
            assert acct.net_liquidation == pytest.approx(100_000.0)

        asyncio.run(run())

    def test_capabilities_include_future(self):
        assert "future" in PaperBroker.capabilities.asset_classes
        assert "market" in PaperBroker.capabilities.order_types

    def test_quantity_rules_crypto(self):
        rules = PaperBroker.capabilities.quantity_rules["crypto_perp"]
        assert rules.quantity_step == pytest.approx(0.001)

    def test_multiple_fills_sequential(self):
        broker = PaperBroker()

        async def run():
            await broker.submit_order(_order(QQQ, "long", 1.0))
            await broker.submit_order(OrderRequest(QQQ, "long", 2.0, "market", idempotency_key="test-002"))
            await broker.on_bar(_bar(QQQ, 100.0))
            fills = []
            for _ in range(2):
                f = await asyncio.wait_for(broker._fill_queue.get(), timeout=1)
                fills.append(f)
            assert sum(f.quantity for f in fills) == pytest.approx(3.0)

        asyncio.run(run())

"""IBKR paper order-path tests.

The market-order test opens one paper share and then closes only the delta it
created. It skips unless IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1 and US equity RTH
is open.
"""

from __future__ import annotations

import asyncio

import pytest

from core.audit import AuditLogger
from core.engine.timeframes import TF_1M
from core.interfaces.strategy import ProtectiveStopSpec
from core.orders.order_manager import OrderManager
from core.portfolio.state import PortfolioState
from core.risk.policy import RiskPolicy
from core.types import OrderRequest, Signal

from .conftest import (
    actual_position_qty,
    cancel_quietly,
    detect_paper_account,
    flatten_delta,
    require_paper_market_orders,
    utc_now,
    wait_for_file,
    wait_for_manager_role,
    wait_for_position,
)

pytestmark = pytest.mark.paper


def test_ibkr_paper_limit_order_submit_and_cancel(
    run_paper_scenario,
    paper_config,
    qqq,
) -> None:
    async def scenario(stack) -> None:
        await detect_paper_account(stack.broker, paper_config.account)
        end = utc_now()
        bars = await stack.data.fetch(qqq, TF_1M, end, end)
        assert bars
        # Deliberately far from market so this exercises order submit/cancel
        # without intentionally creating a fill.
        away_price = round(max(0.01, bars[-1].close * 0.5), 2)
        order = OrderRequest(
            instrument=qqq,
            side="long",
            quantity=1,
            order_type="limit",
            limit_price=away_price,
            strategy_id="paper_order_cancel_test",
            idempotency_key=f"paper-order-cancel-{qqq.symbol}",
        )
        status = await stack.broker.submit_order(order)
        try:
            assert status.broker_order_id
            assert status.status == "pending"
        finally:
            await cancel_quietly(stack.broker, status.broker_order_id)

    run_paper_scenario(scenario, client_id_offset=2)


def test_ibkr_paper_order_manager_entry_fill_stop_and_cleanup(
    run_paper_scenario,
    paper_config,
    qqq,
    tmp_path,
) -> None:
    require_paper_market_orders(paper_config)

    async def scenario(stack) -> None:
        await detect_paper_account(stack.broker, paper_config.account)
        initial_qty = await actual_position_qty(stack.broker, qqq)
        audit = AuditLogger(enabled=True, log_dir=tmp_path, profile="owner")
        portfolio = PortfolioState()
        manager = OrderManager(
            stack.broker,
            portfolio,
            RiskPolicy(position_size_shares=1, max_order_quantity=1),
            audit_logger=audit,
            protective_stops={
                "paper_entry_stop_test": ProtectiveStopSpec(pct=0.015, reference="fill_price")
            },
        )
        tasks = [
            asyncio.create_task(manager.drain_orders()),
            asyncio.create_task(manager.drain_fills()),
            asyncio.create_task(manager.drain_order_updates()),
        ]
        protective_ids: list[str] = []
        try:
            await manager.submit(Signal(instrument=qqq, side="long"), "paper_entry_stop_test")
            await wait_for_manager_role(manager, "entry", timeout=paper_config.order_timeout)
            await wait_for_position(
                stack.broker,
                qqq,
                lambda qty: qty >= initial_qty + 1,
                timeout=paper_config.order_timeout,
            )
            protective_ids = await wait_for_manager_role(
                manager,
                "protective_stop",
                timeout=paper_config.order_timeout,
            )

            assert portfolio.get_strategy_position("paper_entry_stop_test", qqq) is not None
            await wait_for_file(
                tmp_path / "orders.jsonl",
                "protective_stop_submitted",
                timeout=paper_config.order_timeout,
            )
            await wait_for_file(
                tmp_path / "fills.jsonl",
                '"role":"entry"',
                timeout=paper_config.order_timeout,
            )
        finally:
            for order_id in protective_ids:
                await cancel_quietly(stack.broker, order_id)
            await flatten_delta(
                stack.broker,
                qqq,
                initial_qty,
                strategy_id="paper_entry_stop_test_cleanup",
            )
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    run_paper_scenario(scenario, client_id_offset=3)

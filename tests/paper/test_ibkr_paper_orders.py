"""IBKR paper order-path tests.

The market-order test opens one paper share and then closes only the delta it
created. It skips unless IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1 and US equity RTH
is open.

The micro-futures tests resolve MNQ/MES to actual front-month FUT contracts
before any data request or order submission. Continuous futures are data-only
and are not used for trading tests.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import pytest

from core.audit import AuditLogger
from core.adapters.ibkr.contracts import resolve_front_month_future
from core.engine.timeframes import TF_1M, TF_5S
from core.interfaces.strategy import ProtectiveStopSpec
from core.orders.order_manager import OrderManager
from core.portfolio.state import PortfolioState
from core.risk.policy import RiskPolicy
from core.types import Instrument, OrderRequest, Signal

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

_MICRO_EQUITY_INDEX_FUTURES = (
    pytest.param("MNQ", 0, id="MNQ"),
    pytest.param("MES", 1, id="MES"),
)
_FUTURES_EXCHANGE = "CME"
_FUTURES_TICK = 0.25


def _future_underlying(symbol: str) -> Instrument:
    return Instrument(
        asset_class="future",
        symbol=symbol,
        exchange=_FUTURES_EXCHANGE,
        currency="USD",
    )


async def _resolve_front_month(stack, symbol: str) -> Instrument:
    instrument = await resolve_front_month_future(
        stack.client,
        _future_underlying(symbol),
        min_days_to_expiry=7,
        lookahead_contracts=2,
    )
    assert instrument.asset_class == "future"
    assert instrument.symbol == symbol
    assert instrument.expiry is not None
    assert instrument.exchange
    assert instrument.currency == "USD"
    return instrument


def _away_buy_limit(price: float) -> float:
    raw = max(_FUTURES_TICK, price * 0.5)
    return round(round(raw / _FUTURES_TICK) * _FUTURES_TICK, 2)


def _require_futures_market_orders() -> None:
    if os.getenv("IBKR_LT_ALLOW_PAPER_FUTURES_MARKET_ORDERS") != "1":
        pytest.skip(
            "set IBKR_LT_ALLOW_PAPER_FUTURES_MARKET_ORDERS=1 to place futures "
            "paper market orders"
        )


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


@pytest.mark.parametrize(("symbol", "case_offset"), _MICRO_EQUITY_INDEX_FUTURES)
def test_ibkr_paper_micro_future_front_month_history_and_realtime(
    run_paper_scenario,
    paper_config,
    symbol,
    case_offset,
) -> None:
    async def scenario(stack) -> None:
        await detect_paper_account(stack.broker, paper_config.account)
        future = await _resolve_front_month(stack, symbol)

        end = utc_now()
        bars = await stack.data.fetch(future, TF_1M, end - timedelta(days=5), end)
        assert bars
        assert all(bar.instrument == future for bar in bars)
        assert all(bar.timeframe == TF_1M for bar in bars)
        assert all(bar.source == "ibkr" for bar in bars)

        await stack.data.subscribe(future, TF_5S)
        stream = stack.data.bars()
        try:
            bar = await asyncio.wait_for(stream.__anext__(), timeout=paper_config.realtime_timeout)
        except TimeoutError:
            pytest.skip(f"no realtime IBKR 5-second bar arrived for {symbol} before timeout")

        assert bar.instrument == future
        assert bar.timeframe == TF_5S
        assert bar.source == "ibkr"

    run_paper_scenario(scenario, client_id_offset=100 + case_offset)


@pytest.mark.parametrize(("symbol", "case_offset"), _MICRO_EQUITY_INDEX_FUTURES)
def test_ibkr_paper_micro_future_limit_order_submit_and_cancel(
    run_paper_scenario,
    paper_config,
    symbol,
    case_offset,
) -> None:
    async def scenario(stack) -> None:
        await detect_paper_account(stack.broker, paper_config.account)
        future = await _resolve_front_month(stack, symbol)
        end = utc_now()
        bars = await stack.data.fetch(future, TF_1M, end - timedelta(days=5), end)
        assert bars

        order = OrderRequest(
            instrument=future,
            side="long",
            quantity=1,
            order_type="limit",
            limit_price=_away_buy_limit(bars[-1].close),
            strategy_id=f"paper_{symbol.lower()}_fut_limit_cancel_test",
            idempotency_key=f"paper-fut-limit-cancel-{symbol}",
        )
        status = await stack.broker.submit_order(order)
        try:
            assert status.broker_order_id
            assert status.status == "pending"
        finally:
            await cancel_quietly(stack.broker, status.broker_order_id)

    run_paper_scenario(scenario, client_id_offset=110 + case_offset)


@pytest.mark.parametrize(("symbol", "case_offset"), _MICRO_EQUITY_INDEX_FUTURES)
def test_ibkr_paper_micro_future_market_entry_and_cleanup(
    run_paper_scenario,
    paper_config,
    symbol,
    case_offset,
) -> None:
    _require_futures_market_orders()

    async def scenario(stack) -> None:
        await detect_paper_account(stack.broker, paper_config.account)
        future = await _resolve_front_month(stack, symbol)
        initial_qty = await actual_position_qty(stack.broker, future)
        strategy_id = f"paper_{symbol.lower()}_fut_entry_cleanup_test"
        entry_order_id: str | None = None

        order = OrderRequest(
            instrument=future,
            side="long",
            quantity=1,
            order_type="market",
            strategy_id=strategy_id,
            idempotency_key=f"{strategy_id}-{symbol}-entry",
        )
        try:
            status = await stack.broker.submit_order(order)
            entry_order_id = status.broker_order_id
            assert entry_order_id
            await wait_for_position(
                stack.broker,
                future,
                lambda qty: qty >= initial_qty + 1,
                timeout=paper_config.order_timeout,
            )
        finally:
            await cancel_quietly(stack.broker, entry_order_id)
            await flatten_delta(
                stack.broker,
                future,
                initial_qty,
                strategy_id=f"{strategy_id}_cleanup",
            )

    run_paper_scenario(scenario, client_id_offset=120 + case_offset)

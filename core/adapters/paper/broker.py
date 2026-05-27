"""PaperBroker — full async broker for backtests.

Orders resolve at the open of the next bar after submission.
Fills are pushed to an asyncio.Queue and drained by OrderManager.
Pairs with ReplayDataProvider + SimulatedClock.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from asyncio import QueueEmpty
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator

from ...types import (
    AccountSnapshot,
    Bar,
    BrokerCapabilities,
    Fill,
    Instrument,
    OpenOrder,
    OrderRequest,
    OrderStatus,
    Position,
    QuantityRules,
)
log = logging.getLogger(__name__)


class PaperBroker:
    """Simulated broker. Fills market orders at next bar open with optional slippage."""

    name = "paper"
    capabilities = BrokerCapabilities(
        asset_classes=frozenset({
            "equity", "future", "option", "fx",
            "crypto_spot", "crypto_perp", "index",
        }),
        order_types=frozenset({"market", "limit", "stop", "stop_limit"}),
        quantity_rules={
            "equity":      QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "future":      QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "option":      QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "fx":          QuantityRules(min_quantity=1000.0, quantity_step=1.0, quantity_precision=0),
            "crypto_spot": QuantityRules(min_quantity=0.001, quantity_step=0.001, quantity_precision=3),
            "crypto_perp": QuantityRules(min_quantity=0.001, quantity_step=0.001, quantity_precision=3),
            "index":       QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
        },
    )

    def __init__(self, slippage_ticks: int = 0) -> None:
        self._slippage_ticks = slippage_ticks
        self._pending: list[_PendingOrder] = []
        self._fill_queue: asyncio.Queue[Fill] = asyncio.Queue()
        self._order_update_queue: asyncio.Queue[OrderStatus] = asyncio.Queue()
        self._positions: dict[Instrument, float] = {}
        self._cash: float = 100_000.0
        self._net_liq: float = 100_000.0
        self._account_id: str = "PAPER-001"
        self._last_seen_timestamp: datetime | None = None

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def get_account(self) -> AccountSnapshot:
        return AccountSnapshot(
            account_id=self._account_id,
            net_liquidation=self._net_liq,
            buying_power=self._cash,
            available_funds=self._cash,
        )

    async def get_positions(self) -> list[Position]:
        return [
            Position(instrument=inst, quantity=qty, avg_cost=0.0)
            for inst, qty in self._positions.items()
            if qty != 0
        ]

    async def get_open_orders(self) -> list[OpenOrder]:
        return [
            OpenOrder(
                broker_order_id=pending.broker_id,
                instrument=pending.order.instrument,
                side=pending.order.side,
                quantity=pending.order.quantity,
                order_type=pending.order.order_type,
                status="pending",
                limit_price=pending.order.limit_price,
                stop_price=pending.order.stop_price,
                tif=pending.order.tif,
                order_ref=pending.order.idempotency_key or None,
                metadata=pending.order.metadata,
            )
            for pending in self._pending
        ]

    def is_connected(self) -> bool:
        return True

    async def submit_order(self, order: OrderRequest) -> OrderStatus:
        """Queue order for fill at next bar. Returns pending status immediately."""
        broker_id = order.client_order_id or order.idempotency_key or str(uuid.uuid4())
        self._pending.append(
            _PendingOrder(
                order=order,
                broker_id=broker_id,
                submitted_after=self._last_seen_timestamp,
            )
        )
        status = OrderStatus(
            broker_order_id=broker_id,
            status="pending",
            filled_qty=0.0,
        )
        await self._order_update_queue.put(status)
        return status

    async def modify_order(self, broker_order_id: str, order: OrderRequest) -> OrderStatus:
        """Replace a pending order in place, preserving the broker id."""
        for index, pending in enumerate(self._pending):
            if pending.broker_id != broker_order_id:
                continue
            self._pending[index] = _PendingOrder(
                order=order,
                broker_id=broker_order_id,
                submitted_after=pending.submitted_after,
            )
            status = OrderStatus(
                broker_order_id=broker_order_id,
                status="pending",
                filled_qty=0.0,
            )
            await self._order_update_queue.put(status)
            return status
        raise ValueError(f"Cannot modify unknown pending paper order: {broker_order_id}")

    async def cancel_order(self, broker_order_id: str) -> None:
        self._pending = [
            pending for pending in self._pending
            if pending.broker_id != broker_order_id
        ]

    # ------------------------------------------------------------------
    # Called by Engine on each new bar — resolves pending orders
    # ------------------------------------------------------------------

    async def on_bar(self, bar: Bar) -> None:
        """Resolve pending orders on a new bar."""
        bar_ts = _ensure_aware(bar.timestamp)
        if self._last_seen_timestamp is None or bar_ts > self._last_seen_timestamp:
            self._last_seen_timestamp = bar_ts
        if not self._pending:
            return
        to_fill = list(self._pending)
        self._pending.clear()
        still_pending: list[_PendingOrder] = []
        for pending in to_fill:
            order = pending.order
            broker_id = pending.broker_id
            if order.instrument != bar.instrument:
                still_pending.append(pending)
                continue
            if pending.submitted_after is not None and bar_ts <= pending.submitted_after:
                still_pending.append(pending)
                continue
            should_fill, fill_price = _resolve_fill(order, bar)
            if not should_fill:
                still_pending.append(pending)
                continue
            fill = Fill(
                broker_order_id=broker_id,
                instrument=order.instrument,
                side=order.side,
                quantity=order.quantity,
                price=fill_price,
                timestamp=bar.timestamp,
            )
            await self._fill_queue.put(fill)
            completed = OrderStatus(
                broker_order_id=broker_id,
                status="filled",
                filled_qty=order.quantity,
                avg_fill_price=fill_price,
            )
            await self._order_update_queue.put(completed)
            signed = order.quantity if order.side == "long" else -order.quantity
            self._positions[order.instrument] = self._positions.get(order.instrument, 0.0) + signed
        self._pending.extend(still_pending)

    # ------------------------------------------------------------------
    # AsyncIterator streams
    # ------------------------------------------------------------------

    async def fills(self) -> AsyncIterator[Fill]:
        while True:
            fill = await self._fill_queue.get()
            yield fill

    async def order_updates(self) -> AsyncIterator[OrderStatus]:
        while True:
            status = await self._order_update_queue.get()
            yield status

    def ready_fills(self) -> list[Fill]:
        fills: list[Fill] = []
        while True:
            try:
                fills.append(self._fill_queue.get_nowait())
            except QueueEmpty:
                return fills

    def ready_order_updates(self) -> list[OrderStatus]:
        updates: list[OrderStatus] = []
        while True:
            try:
                updates.append(self._order_update_queue.get_nowait())
            except QueueEmpty:
                return updates


def _resolve_fill(order: OrderRequest, bar: Bar) -> tuple[bool, float]:
    if order.order_type == "stop":
        if order.stop_price is None:
            return False, 0.0
        if order.side == "short":
            return bar.low <= order.stop_price, order.stop_price
        return bar.high >= order.stop_price, order.stop_price
    return True, bar.open


@dataclass(frozen=True)
class _PendingOrder:
    order: OrderRequest
    broker_id: str
    submitted_after: datetime | None


def _ensure_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)

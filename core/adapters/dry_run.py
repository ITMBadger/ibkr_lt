"""Dry-run broker wrapper.

Wraps a real broker for account/position visibility while ensuring native
order submission is never called.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from typing import AsyncIterator

from ..interfaces.broker import BrokerAdapter
from ..types import AccountSnapshot, Fill, OrderRequest, OrderStatus, Position

log = logging.getLogger(__name__)


class DryRunBroker:
    """BrokerAdapter wrapper that records intended orders without placing them."""

    def __init__(self, broker: BrokerAdapter) -> None:
        self._broker = broker
        self._counter = itertools.count(1)
        self._updates: asyncio.Queue[OrderStatus] = asyncio.Queue()
        self.intended_orders: list[OrderRequest] = []

    @property
    def name(self) -> str:
        return f"dry_run:{self._broker.name}"

    @property
    def capabilities(self):
        return self._broker.capabilities

    async def connect(self) -> None:
        await self._broker.connect()

    async def disconnect(self) -> None:
        await self._broker.disconnect()

    async def get_account(self) -> AccountSnapshot:
        return await self._broker.get_account()

    async def get_positions(self) -> list[Position]:
        return await self._broker.get_positions()

    async def submit_order(self, order: OrderRequest) -> OrderStatus:
        self.intended_orders.append(order)
        broker_id = f"dry-run-{next(self._counter)}"
        status = OrderStatus(
            broker_order_id=broker_id,
            status="dry_run",
            filled_qty=0.0,
        )
        await self._updates.put(status)
        log.info(
            "DRY RUN: would submit %s %s %.4f %s",
            order.side,
            order.instrument.symbol,
            order.quantity,
            order.order_type,
        )
        return status

    async def cancel_order(self, broker_order_id: str) -> None:
        log.info("DRY RUN: would cancel order_id=%s", broker_order_id)

    async def order_updates(self) -> AsyncIterator[OrderStatus]:
        while True:
            yield await self._updates.get()

    async def fills(self) -> AsyncIterator[Fill]:
        async for fill in self._broker.fills():
            yield fill

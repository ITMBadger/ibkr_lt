"""BrokerAdapter port — the only interface between the engine and a broker."""

from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from ..types import (
    AccountSnapshot,
    BrokerCapabilities,
    Fill,
    OrderRequest,
    OrderStatus,
    Position,
)


@runtime_checkable
class BrokerAdapter(Protocol):
    """Async interface every broker adapter must satisfy."""

    name: str
    capabilities: BrokerCapabilities

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_account(self) -> AccountSnapshot: ...
    async def get_positions(self) -> list[Position]: ...
    async def submit_order(self, order: OrderRequest) -> OrderStatus: ...
    async def modify_order(self, broker_order_id: str, order: OrderRequest) -> OrderStatus: ...
    async def cancel_order(self, broker_order_id: str) -> None: ...
    async def order_updates(self) -> AsyncIterator[OrderStatus]: ...
    async def fills(self) -> AsyncIterator[Fill]: ...

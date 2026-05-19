"""OrderManager — single-writer asyncio task for all broker order submissions.

All broker.submit_order calls go through here. Strategies submit signals;
the OrderManager sizes them, validates capabilities, and drains the queue
serially so there is never concurrent order submission to the broker.

Single-writer is free: one asyncio task owns the queue drain loop. No locking needed.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ..portfolio.state import PortfolioState
from ..risk.policy import RiskPolicy
from ..types import OrderRequest, Position, Signal
from ..audit import AuditLogger

if TYPE_CHECKING:
    from ..interfaces.broker import BrokerAdapter

log = logging.getLogger(__name__)


class OrderManager:
    """Enqueues signals; drains them serially to the broker."""

    def __init__(
        self,
        broker: "BrokerAdapter",
        portfolio: PortfolioState,
        risk: RiskPolicy,
        audit_logger: AuditLogger | None = None,
    ) -> None:
        self._broker = broker
        self._portfolio = portfolio
        self._risk = risk
        self._audit = audit_logger
        self._queue: asyncio.Queue[tuple[Signal, str]] = asyncio.Queue()
        self._signal_log: list[tuple[str, Signal]] = []  # (strategy_id, signal)
        self._order_owner: dict[str, str] = {}  # broker_order_id → strategy_id

    async def submit(self, signal: Signal, strategy_id: str) -> None:
        """Enqueue a signal for the order drain loop to process."""
        await self._queue.put((signal, strategy_id))

    async def submit_close(
        self,
        strategy_id: str,
        position: Position,
        reason: str,
    ) -> None:
        """Submit an opposite-side market order to close a managed position."""
        qty = abs(position.quantity)
        if qty <= 0:
            self._write_order_event(
                "close_dropped",
                strategy_id=strategy_id,
                reason="flat_position",
                position=position,
            )
            return
        side = "short" if position.quantity > 0 else "long"
        order = OrderRequest(
            instrument=position.instrument,
            side=side,
            quantity=qty,
            order_type="market",
            strategy_id=strategy_id,
            idempotency_key=f"{strategy_id}-{position.instrument.symbol}-close-{reason}",
        )
        if not self._validate_order(order, is_entry=False):
            self._write_order_event(
                "close_dropped",
                strategy_id=strategy_id,
                reason="validation_failed",
                order=order,
            )
            return
        self._write_order_event(
            "close_intent",
            strategy_id=strategy_id,
            reason=reason,
            order=order,
        )
        status = await self._broker.submit_order(order)
        self._order_owner[status.broker_order_id] = strategy_id
        self._write_order_event(
            "close_submitted",
            strategy_id=strategy_id,
            reason=reason,
            order=order,
            status=status,
        )
        log.info(
            "Submitted close for %s: %s %s %.0f reason=%s → order_id=%s status=%s",
            strategy_id,
            side,
            position.instrument.symbol,
            qty,
            reason,
            status.broker_order_id,
            status.status,
        )

    # ------------------------------------------------------------------
    # Drain tasks (run as asyncio tasks by Engine)
    # ------------------------------------------------------------------

    async def drain_orders(self) -> None:
        """Continuously dequeue signals and submit orders to the broker."""
        while True:
            signal, strategy_id = await self._queue.get()
            try:
                await self._process_signal(signal, strategy_id)
            except Exception as e:
                log.exception("OrderManager: error processing signal from %s: %s", strategy_id, e)

    async def drain_fills(self) -> None:
        """Continuously drain fills from the broker and update PortfolioState."""
        async for fill in self._broker.fills():
            try:
                strategy_id = self._order_owner.get(fill.broker_order_id)
                self._portfolio.apply_fill(fill, strategy_id=strategy_id)
                self._write_fill_event(fill, strategy_id)
                log.info(
                    "Fill: %s %s %.0f @ %.4f",
                    fill.side, fill.instrument.symbol, fill.quantity, fill.price,
                )
            except Exception as e:
                log.exception("OrderManager: error applying fill: %s", e)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _process_signal(self, signal: Signal, strategy_id: str) -> None:
        if signal.side == "flat":
            self._write_order_event(
                "signal_dropped",
                strategy_id=strategy_id,
                reason="flat_signal",
                signal=signal,
            )
            return  # exit signals handled separately

        if signal.side == "short" and not self._broker.capabilities.supports_short:
            log.warning("Broker %s does not support short entries; dropping signal", self._broker.name)
            self._write_order_event(
                "signal_dropped",
                strategy_id=strategy_id,
                reason="short_not_supported",
                signal=signal,
                broker=self._broker.name,
            )
            return

        # Size the order
        qty_float = self._risk.size_order(signal, self._portfolio)
        if qty_float <= 0:
            self._write_order_event(
                "signal_dropped",
                strategy_id=strategy_id,
                reason="risk_size_zero",
                signal=signal,
                quantity=qty_float,
            )
            return

        ac = signal.instrument.asset_class
        rules = self._broker.capabilities.quantity_rules.get(ac)
        qty = rules.round(float(qty_float)) if rules else float(qty_float)
        if qty <= 0:
            self._write_order_event(
                "signal_dropped",
                strategy_id=strategy_id,
                reason="rounded_quantity_zero",
                signal=signal,
                raw_quantity=qty_float,
                quantity=qty,
            )
            return

        order = OrderRequest(
            instrument=signal.instrument,
            side=signal.side,
            quantity=qty,
            order_type="market",
            strategy_id=strategy_id,
            idempotency_key=f"{strategy_id}-{signal.instrument.symbol}-{signal.side}",
        )
        if not self._validate_order(order, is_entry=True):
            self._write_order_event(
                "order_dropped",
                strategy_id=strategy_id,
                reason="validation_failed",
                order=order,
            )
            return

        self._signal_log.append((strategy_id, signal))
        self._write_order_event(
            "order_intent",
            strategy_id=strategy_id,
            signal=signal,
            order=order,
            raw_quantity=qty_float,
        )
        status = await self._broker.submit_order(order)
        self._order_owner[status.broker_order_id] = strategy_id
        self._write_order_event(
            "order_submitted",
            strategy_id=strategy_id,
            signal=signal,
            order=order,
            status=status,
        )
        log.info(
            "Submitted %s %s %.0f → order_id=%s status=%s",
            signal.side, signal.instrument.symbol, qty,
            status.broker_order_id, status.status,
        )

    @property
    def signal_log(self) -> list[tuple[str, Signal]]:
        """Immutable copy of the signal log (used in tests)."""
        return list(self._signal_log)

    def _validate_order(self, order: OrderRequest, is_entry: bool) -> bool:
        ac = order.instrument.asset_class
        caps = self._broker.capabilities
        if ac not in caps.asset_classes:
            log.warning(
                "Broker %s does not support asset_class %r; dropping order",
                self._broker.name,
                ac,
            )
            return False
        if order.order_type not in caps.order_types:
            log.warning(
                "Broker %s does not support order_type %r; dropping order",
                self._broker.name,
                order.order_type,
            )
            return False
        if is_entry and order.side == "short" and not caps.supports_short:
            log.warning("Broker %s does not support short entries; dropping order", self._broker.name)
            return False
        if not caps.supports_fractional and not float(order.quantity).is_integer():
            log.warning(
                "Broker %s does not support fractional quantity %.4f; dropping order",
                self._broker.name,
                order.quantity,
            )
            return False
        return True

    def _write_order_event(self, event: str, **fields) -> None:
        if self._audit is not None:
            self._audit.order({"event": event, **fields})

    def _write_fill_event(self, fill, strategy_id: str | None) -> None:
        if self._audit is not None:
            self._audit.fill({
                "event": "fill",
                "strategy_id": strategy_id,
                "fill": fill,
            })

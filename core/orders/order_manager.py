"""OrderManager — centralized broker order submission and fill handling.

Strategies submit signals; the OrderManager sizes them, validates broker
capabilities, applies fills to PortfolioState, drains order-status updates,
and submits configured protective stops.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from .strategy_modes import STRATEGY_MODE_DRY_RUN, strategy_mode_map
from ..portfolio.state import PortfolioState
from ..risk.policy import RiskPolicy
from ..interfaces.strategy import POSITION_MODE_MULTI, PositionPolicy, ProtectiveStopSpec
from ..types import Fill, OrderRequest, OrderStatus, Position, Signal
from ..audit import AuditLogger

if TYPE_CHECKING:
    from ..interfaces.broker import BrokerAdapter

log = logging.getLogger(__name__)

_BUY_FILL_SIDES = {"BOT", "BUY", "B", "LONG"}


class OrderManager:
    """Enqueues signals; drains them serially to the broker."""

    def __init__(
        self,
        broker: "BrokerAdapter",
        portfolio: PortfolioState,
        risk: RiskPolicy,
        audit_logger: AuditLogger | None = None,
        protective_stops: Mapping[str, ProtectiveStopSpec] | None = None,
        strategy_modes: Mapping[str, str] | None = None,
        position_policies: Mapping[str, PositionPolicy] | None = None,
    ) -> None:
        self._broker = broker
        self._portfolio = portfolio
        self._risk = risk
        self._audit = audit_logger
        self._queue: asyncio.Queue[tuple[Signal, str]] = asyncio.Queue()
        self._signal_log: list[tuple[str, Signal]] = []  # (strategy_id, signal)
        self._order_owner: dict[str, str] = {}  # broker_order_id → strategy_id
        self._order_role: dict[str, str] = {}  # broker_order_id → entry/close/protective_stop
        self._protective_stops = dict(protective_stops or {})
        strategy_ids = set(self._protective_stops) | set(strategy_modes or {})
        self._strategy_modes = strategy_mode_map(strategy_modes, strategy_ids)
        self._position_policies = dict(position_policies or {})
        self._dry_run_counter = itertools.count(1)
        self._trade_counter = itertools.count(1)
        self._order_trade_id: dict[str, str | None] = {}

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
        close_key = f"{strategy_id}-{position.instrument.symbol}-close-{reason}"
        if position.trade_id:
            close_key = f"{close_key}-{position.trade_id}"
        order = OrderRequest(
            instrument=position.instrument,
            side=side,
            quantity=qty,
            order_type="market",
            strategy_id=strategy_id,
            idempotency_key=close_key,
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
        if self._is_dry_run_strategy(strategy_id):
            self._write_dry_run_order_event(
                "close_dry_run",
                strategy_id=strategy_id,
                order=order,
                reason=reason,
            )
            log.info(
                "Dry-run strategy %s: would close %s %s %.0f reason=%s",
                strategy_id,
                side,
                position.instrument.symbol,
                qty,
                reason,
            )
            return
        status = await self._broker.submit_order(order)
        self._order_owner[status.broker_order_id] = strategy_id
        self._order_role[status.broker_order_id] = "close"
        self._order_trade_id[status.broker_order_id] = position.trade_id
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

    async def drain_ready_orders(self) -> None:
        """Drain currently queued orders without waiting for more."""
        while not self._queue.empty():
            signal, strategy_id = self._queue.get_nowait()
            try:
                await self._process_signal(signal, strategy_id)
            except Exception as e:
                log.exception("OrderManager: error processing signal from %s: %s", strategy_id, e)

    async def drain_fills(self) -> None:
        """Continuously drain fills from the broker and update PortfolioState."""
        async for fill in self._broker.fills():
            try:
                await self._handle_fill(fill)
            except Exception as e:
                log.exception("OrderManager: error applying fill: %s", e)

    async def drain_ready_fills(self) -> None:
        """Drain currently ready broker fills from adapters that expose them."""
        ready_fills = getattr(self._broker, "ready_fills", None)
        if not callable(ready_fills):
            return
        for fill in ready_fills():
            try:
                await self._handle_fill(fill)
            except Exception as e:
                log.exception("OrderManager: error applying fill: %s", e)

    async def drain_order_updates(self) -> None:
        """Continuously drain broker order status updates and audit/log them."""
        async for status in self._broker.order_updates():
            try:
                self._handle_order_update(status)
            except Exception as e:
                log.exception("OrderManager: error handling order update: %s", e)

    async def drain_ready_order_updates(self) -> None:
        """Drain currently ready order status updates from adapters that expose them."""
        ready_order_updates = getattr(self._broker, "ready_order_updates", None)
        if not callable(ready_order_updates):
            return
        for status in ready_order_updates():
            try:
                self._handle_order_update(status)
            except Exception as e:
                log.exception("OrderManager: error handling order update: %s", e)

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
            log.warning(
                "Broker %s does not support short entries; dropping signal",
                self._broker.name,
            )
            self._write_order_event(
                "signal_dropped",
                strategy_id=strategy_id,
                reason="short_not_supported",
                signal=signal,
                broker=self._broker.name,
            )
            return

        # Size the order
        trade_id = self._resolve_trade_id(signal, strategy_id)
        policy = self._position_policies.get(strategy_id)
        allow_existing_position = (
            policy is not None and policy.position_mode == POSITION_MODE_MULTI
        )
        qty_float = self._risk.size_order(
            signal,
            self._portfolio,
            allow_existing_position=allow_existing_position,
        )
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

        entry_key = f"{strategy_id}-{signal.instrument.symbol}-{signal.side}"
        if trade_id:
            entry_key = f"{entry_key}-{trade_id}"
        order = OrderRequest(
            instrument=signal.instrument,
            side=signal.side,
            quantity=qty,
            order_type="market",
            strategy_id=strategy_id,
            idempotency_key=entry_key,
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
        if self._is_dry_run_strategy(strategy_id):
            self._write_dry_run_order_event(
                "order_dry_run",
                strategy_id=strategy_id,
                order=order,
                signal=signal,
            )
            log.info(
                "Dry-run strategy %s: would submit %s %s %.0f",
                strategy_id,
                signal.side,
                signal.instrument.symbol,
                qty,
            )
            return
        status = await self._broker.submit_order(order)
        self._order_owner[status.broker_order_id] = strategy_id
        self._order_role[status.broker_order_id] = "entry"
        self._order_trade_id[status.broker_order_id] = trade_id
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

    async def _handle_fill(self, fill: Fill) -> None:
        strategy_id = self._order_owner.get(fill.broker_order_id)
        role = self._order_role.get(fill.broker_order_id, "unknown")
        trade_id = self._order_trade_id.get(fill.broker_order_id)
        self._portfolio.apply_fill(fill, strategy_id=strategy_id, trade_id=trade_id)
        self._write_fill_event(fill, strategy_id, role, trade_id)
        log.info(
            "Fill: %s %s %.0f @ %.4f",
            fill.side, fill.instrument.symbol, fill.quantity, fill.price,
        )
        if strategy_id is not None and role == "entry":
            await self._submit_protective_stop(fill, strategy_id)

    def _handle_order_update(self, status: OrderStatus) -> None:
        strategy_id = self._order_owner.get(status.broker_order_id)
        role = self._order_role.get(status.broker_order_id, "unknown")
        self._write_order_event(
            "order_update",
            strategy_id=strategy_id,
            role=role,
            status=status,
        )
        log.info(
            "Order update: order_id=%s status=%s filled=%.4f avg_fill_price=%s role=%s",
            status.broker_order_id,
            status.status,
            status.filled_qty,
            status.avg_fill_price,
            role,
        )

    async def _submit_protective_stop(self, fill: Fill, strategy_id: str) -> None:
        spec = self._protective_stops.get(strategy_id)
        if spec is None:
            return
        if self._is_dry_run_strategy(strategy_id):
            self._write_order_event(
                "protective_stop_dropped",
                strategy_id=strategy_id,
                reason="strategy_dry_run",
                fill=fill,
            )
            return
        if spec.reference != "fill_price":
            self._write_order_event(
                "protective_stop_dropped",
                strategy_id=strategy_id,
                reason="unsupported_reference",
                reference=spec.reference,
                fill=fill,
            )
            log.warning(
                "Protective stop for %s dropped: unsupported reference=%s",
                strategy_id,
                spec.reference,
            )
            return
        if spec.pct <= 0 or fill.quantity <= 0 or fill.price <= 0:
            self._write_order_event(
                "protective_stop_dropped",
                strategy_id=strategy_id,
                reason="invalid_stop_inputs",
                spec=spec,
                fill=fill,
            )
            return

        is_buy_fill = fill.side.upper() in _BUY_FILL_SIDES
        trade_id = self._order_trade_id.get(fill.broker_order_id)
        stop_side = "short" if is_buy_fill else "long"
        if is_buy_fill:
            raw_stop_price = fill.price * (1.0 - spec.pct)
        else:
            raw_stop_price = fill.price * (1.0 + spec.pct)
        stop_price = _round_stop_price(fill.instrument, raw_stop_price)
        stop_key = (
            f"{strategy_id}-{fill.instrument.symbol}-protective-stop-"
            f"{fill.broker_order_id}"
        )
        if trade_id:
            stop_key = f"{stop_key}-{trade_id}"
        order = OrderRequest(
            instrument=fill.instrument,
            side=stop_side,
            quantity=fill.quantity,
            order_type="stop",
            stop_price=stop_price,
            strategy_id=strategy_id,
            idempotency_key=stop_key,
        )
        if not self._validate_order(order, is_entry=False):
            self._write_order_event(
                "protective_stop_dropped",
                strategy_id=strategy_id,
                reason="validation_failed",
                order=order,
                fill=fill,
            )
            return

        self._write_order_event(
            "protective_stop_intent",
            strategy_id=strategy_id,
            fill=fill,
            order=order,
            pct=spec.pct,
            reference=spec.reference,
        )
        status = await self._broker.submit_order(order)
        self._order_owner[status.broker_order_id] = strategy_id
        self._order_role[status.broker_order_id] = "protective_stop"
        self._order_trade_id[status.broker_order_id] = trade_id
        self._write_order_event(
            "protective_stop_submitted",
            strategy_id=strategy_id,
            fill=fill,
            order=order,
            status=status,
        )
        log.info(
            "Submitted protective stop for %s: %s %s %.0f stop=%.4f → order_id=%s status=%s",
            strategy_id,
            stop_side,
            fill.instrument.symbol,
            fill.quantity,
            stop_price,
            status.broker_order_id,
            status.status,
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
            log.warning(
                "Broker %s does not support short entries; dropping order",
                self._broker.name,
            )
            return False
        if not caps.supports_fractional and not float(order.quantity).is_integer():
            log.warning(
                "Broker %s does not support fractional quantity %.4f; dropping order",
                self._broker.name,
                order.quantity,
            )
            return False
        return True

    def _is_dry_run_strategy(self, strategy_id: str) -> bool:
        return self._strategy_modes.get(strategy_id) == STRATEGY_MODE_DRY_RUN

    def _resolve_trade_id(self, signal: Signal, strategy_id: str) -> str | None:
        policy = self._position_policies.get(strategy_id)
        if policy is None or policy.position_mode != POSITION_MODE_MULTI:
            return signal.trade_id
        if signal.trade_id:
            return signal.trade_id
        return f"{signal.instrument.symbol.lower()}_{next(self._trade_counter)}"

    def _dry_run_status(self, strategy_id: str) -> OrderStatus:
        return OrderStatus(
            broker_order_id=f"dry-run-{strategy_id}-{next(self._dry_run_counter)}",
            status="dry_run",
            filled_qty=0.0,
        )

    def _write_dry_run_order_event(
        self,
        event: str,
        *,
        strategy_id: str,
        order: OrderRequest,
        **fields,
    ) -> OrderStatus:
        status = self._dry_run_status(strategy_id)
        self._write_order_event(
            event,
            strategy_id=strategy_id,
            order=order,
            status=status,
            **fields,
        )
        return status

    def _write_order_event(self, event: str, **fields) -> None:
        if self._audit is not None:
            self._audit.order({"event": event, **fields})

    def _write_fill_event(
        self,
        fill,
        strategy_id: str | None,
        role: str,
        trade_id: str | None,
    ) -> None:
        if self._audit is not None:
            self._audit.fill({
                "event": "fill",
                "strategy_id": strategy_id,
                "role": role,
                "trade_id": trade_id,
                "fill": fill,
            })


def _round_stop_price(instrument, price: float) -> float:
    if instrument.asset_class in {"equity", "option"}:
        return round(price, 2)
    return round(price, 4)

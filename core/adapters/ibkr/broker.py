"""IBKRBroker — BrokerAdapter backed by IBKRClient.

Ported from:
  legacy/broker/ibkr_app.py  (order placement, callbacks)
  legacy/services/execution/order_service.py (thin wrapper folded in here)

Order type normalization:
  MARKET → MKT, LIMIT → LMT, STOP LIMIT → STP LMT

All IBKR-side I/O goes through the shared IBKRClient instance.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, AsyncIterator

from ...types import (
    AccountSnapshot,
    BrokerCapabilities,
    Fill,
    Instrument,
    OpenOrder,
    OrderRequest,
    OrderStatus,
    Position,
    QuantityRules,
)
from .contracts import instrument_to_contract

if TYPE_CHECKING:
    from .client import IBKRClient

try:
    from ibapi.order import Order as IBOrder
    _IBAPI_AVAILABLE = True
except ImportError:
    _IBAPI_AVAILABLE = False
    IBOrder = object  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)

_ORDER_TYPE_MAP = {
    "market": "MKT",
    "limit": "LMT",
    "stop": "STP",
    "stop_limit": "STP LMT",
}


class IBKRBroker:
    """BrokerAdapter implementation for Interactive Brokers."""

    name = "ibkr"
    capabilities = BrokerCapabilities(
        asset_classes=frozenset({"equity", "future", "option", "fx"}),
        order_types=frozenset({"market", "limit", "stop", "stop_limit"}),
        quantity_rules={
            "equity": QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "future": QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "option": QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0),
            "fx":     QuantityRules(min_quantity=25_000.0, quantity_step=1.0, quantity_precision=0),
        },
    )

    def __init__(
        self,
        client: "IBKRClient",
        account: str = "",
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ) -> None:
        self._client = client
        self._account = account
        self._host = host
        self._port = port
        self._client_id = client_id

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        if not self._client.is_ready():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                self._client.connect_and_run,
                self._host,
                self._port,
                self._client_id,
                loop,
            )

    async def disconnect(self) -> None:
        self._client.disconnect()

    def is_connected(self) -> bool:
        try:
            return bool(self._client.is_ready() and self._client.isConnected())
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Account / positions
    # ------------------------------------------------------------------

    async def get_account(self) -> AccountSnapshot:
        req_id = self._client.get_next_order_id()
        tags = "NetLiquidation,BuyingPower,AvailableFunds,ExcessLiquidity"
        self._client.reqAccountSummary(req_id, "All", tags)
        data: dict[str, float] = {}
        account_id = self._account
        seen_accounts: set[str] = set()
        matched_configured_account = False
        try:
            while True:
                item = await asyncio.wait_for(self._client.account_queue.get(), timeout=15)
                if item.get("req_id") != req_id:
                    await self._client.account_queue.put(item)
                    continue
                if item.get("done"):
                    break
                item_account = str(item.get("account", ""))
                if item_account:
                    seen_accounts.add(item_account)
                if self._account and item_account != self._account:
                    continue
                if self._account and item_account == self._account:
                    matched_configured_account = True
                data[item["tag"]] = item["value"]
                if not account_id:
                    account_id = item_account
        except asyncio.TimeoutError:
            log.warning("account summary timed out")
        finally:
            try:
                self._client.cancelAccountSummary(req_id)
            except Exception:
                pass
        if self._account and not matched_configured_account:
            raise RuntimeError(
                "configured IBKR account was not returned by account summary: "
                f"{self._account}"
            )
        if not self._account and len(seen_accounts) > 1:
            raise RuntimeError(
                "IBKR returned multiple accounts; configure execution.account explicitly"
            )
        return AccountSnapshot(
            account_id=account_id,
            net_liquidation=data.get("NetLiquidation", 0.0),
            buying_power=data.get("BuyingPower", 0.0),
            available_funds=data.get("AvailableFunds", 0.0),
        )

    async def get_positions(self) -> list[Position]:
        self._client.reqPositions()
        positions: list[Position] = []
        try:
            while True:
                item = await asyncio.wait_for(self._client.position_queue.get(), timeout=15)
                if item.get("done"):
                    break
                if self._account and item.get("account") != self._account:
                    continue
                instr = _instrument_from_ibkr_item(item)
                positions.append(Position(
                    instrument=instr,
                    quantity=item["position"],
                    avg_cost=item["avg_cost"],
                    metadata=_broker_metadata(item),
                ))
        except asyncio.TimeoutError:
            log.warning("positions request timed out")
        finally:
            self._client.cancelPositions()
        return [p for p in positions if not p.is_flat]

    async def get_open_orders(self) -> list[OpenOrder]:
        self._client.reqOpenOrders()
        orders: list[OpenOrder] = []
        try:
            while True:
                item = await asyncio.wait_for(self._client.open_order_queue.get(), timeout=15)
                if item.get("done"):
                    break
                if self._account and item.get("account") not in ("", self._account):
                    continue
                orders.append(_open_order_from_ibkr_item(item))
        except asyncio.TimeoutError:
            log.warning("open orders request timed out")
        return orders

    # ------------------------------------------------------------------
    # Order submission
    # ------------------------------------------------------------------

    async def reserve_order_id(self) -> str:
        return str(self._client.get_next_order_id())

    async def submit_order(self, order: OrderRequest) -> OrderStatus:
        if not _IBAPI_AVAILABLE:
            raise ImportError("ibapi required")
        contract = instrument_to_contract(order.instrument)
        ib_order = self._build_ib_order(order)

        order_id = int(order.client_order_id or self._client.get_next_order_id())
        self._client.placeOrder(order_id, contract, ib_order)
        log.info(
            "Placed order %d: %s %s %.0f @ %s",
            order_id, ib_order.action, order.instrument.symbol,
            order.quantity, order.order_type,
        )
        status = await self._initial_order_status(str(order_id))
        if status is not None:
            return status
        return OrderStatus(
            broker_order_id=str(order_id),
            status="pending",
            filled_qty=0.0,
        )

    async def modify_order(self, broker_order_id: str, order: OrderRequest) -> OrderStatus:
        if not _IBAPI_AVAILABLE:
            raise ImportError("ibapi required")
        order_id = int(broker_order_id)
        contract = instrument_to_contract(order.instrument)
        ib_order = self._build_ib_order(order)
        self._client.placeOrder(order_id, contract, ib_order)
        log.info(
            "Modified order %d: %s %s %.0f @ %s",
            order_id, ib_order.action, order.instrument.symbol,
            order.quantity, order.order_type,
        )
        status = await self._initial_order_status(str(order_id))
        if status is not None:
            return status
        return OrderStatus(
            broker_order_id=str(order_id),
            status="pending",
            filled_qty=0.0,
        )

    async def cancel_order(self, broker_order_id: str) -> None:
        try:
            self._client.cancelOrder(int(broker_order_id), "")
        except Exception as e:
            log.warning("cancel_order failed: %s", e)

    def _build_ib_order(self, order: OrderRequest):
        ib_order = IBOrder()
        ib_order.action = "BUY" if order.side == "long" else "SELL"
        ib_order.totalQuantity = order.quantity
        ib_order.orderType = _ORDER_TYPE_MAP.get(order.order_type, "MKT")
        if order.limit_price is not None:
            ib_order.lmtPrice = order.limit_price
        if order.stop_price is not None:
            ib_order.auxPrice = order.stop_price
        if self._account:
            ib_order.account = self._account
        if order.idempotency_key:
            ib_order.orderRef = order.idempotency_key
        # ibapi defaults these legacy SMART-routing flags to True. Current
        # TWS paper rejects them for simple stock orders with error 10268.
        if hasattr(ib_order, "eTradeOnly"):
            ib_order.eTradeOnly = False
        if hasattr(ib_order, "firmQuoteOnly"):
            ib_order.firmQuoteOnly = False
        ib_order.tif = str(order.tif or "DAY").upper()
        if order.outside_rth is not None:
            ib_order.outsideRth = bool(order.outside_rth)
        ib_order.transmit = True
        return ib_order

    async def _initial_order_status(self, order_id: str) -> OrderStatus | None:
        """Return an immediate reject/accepted state without stealing updates."""
        error_queue = getattr(self._client, "error_queue", None)
        order_update_queue = getattr(self._client, "order_update_queue", None)
        if error_queue is None or order_update_queue is None:
            return None
        deadline = asyncio.get_running_loop().time() + 1.5
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            error_task = asyncio.create_task(error_queue.get())
            update_task = asyncio.create_task(order_update_queue.get())
            done, pending = await asyncio.wait(
                {error_task, update_task},
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if not done:
                return None
            matched_update: dict | None = None
            for task in done:
                item = task.result()
                if task is error_task:
                    if str(item.get("req_id")) == order_id:
                        return OrderStatus(
                            broker_order_id=order_id,
                            status="rejected",
                            filled_qty=0.0,
                            error_code=item.get("error_code"),
                            message=item.get("message"),
                        )
                    await error_queue.put(item)
                    continue
                await order_update_queue.put(item)
                if str(item.get("order_id")) == order_id:
                    matched_update = item
            if matched_update is None:
                continue
            raw = str(matched_update.get("status", "")).lower()
            if raw in {"submitted", "presubmitted"}:
                return OrderStatus(
                    broker_order_id=order_id,
                    status="open" if raw == "submitted" else "pending",
                    filled_qty=float(matched_update.get("filled", 0.0) or 0.0),
                    avg_fill_price=matched_update.get("avg_fill_price"),
                    remaining_qty=float(matched_update.get("remaining", 0.0) or 0.0),
                    permanent_id=str(matched_update.get("perm_id", "") or "") or None,
                )
            if raw in {"inactive", "apicancelled", "cancelled"}:
                return OrderStatus(
                    broker_order_id=order_id,
                    status="cancelled",
                    filled_qty=float(matched_update.get("filled", 0.0) or 0.0),
                    avg_fill_price=matched_update.get("avg_fill_price"),
                    remaining_qty=float(matched_update.get("remaining", 0.0) or 0.0),
                    permanent_id=str(matched_update.get("perm_id", "") or "") or None,
                )

    # ------------------------------------------------------------------
    # Streaming updates
    # ------------------------------------------------------------------

    async def order_updates(self) -> AsyncIterator[OrderStatus]:
        _STATUS_MAP = {
            "submitted": "open",
            "presubmitted": "pending",
            "filled": "filled",
            "cancelled": "cancelled",
            "inactive": "cancelled",
            "apicancelled": "cancelled",
        }
        while True:
            item = await self._client.order_update_queue.get()
            raw = item.get("status", "").lower()
            status = _STATUS_MAP.get(raw, raw)
            yield OrderStatus(
                broker_order_id=item["order_id"],
                status=status,  # type: ignore[arg-type]
                filled_qty=item.get("filled", 0.0),
                avg_fill_price=item.get("avg_fill_price"),
                remaining_qty=item.get("remaining", 0.0),
                permanent_id=str(item.get("perm_id", "") or "") or None,
            )

    async def fills(self) -> AsyncIterator[Fill]:
        while True:
            item = await self._client.fill_queue.get()
            side = "long" if item.get("side", "").upper() in ("BOT", "B") else "short"
            instr = _instrument_from_ibkr_item(item)
            yield Fill(
                broker_order_id=item["order_id"],
                instrument=instr,
                side=side,
                quantity=abs(item["shares"]),
                price=item["price"],
                timestamp=item.get("timestamp", datetime.now(tz=timezone.utc)),
            )


def _sec_type_to_asset_class(sec_type: str) -> str:
    mapping = {
        "STK": "equity",
        "FUT": "future",
        "OPT": "option",
        "CASH": "fx",
        "IND": "index",
        "CRYPTO": "crypto_spot",
    }
    return mapping.get(sec_type.upper(), "equity")


def _instrument_from_ibkr_item(item: dict) -> Instrument:
    asset_class = _sec_type_to_asset_class(str(item.get("sec_type", "STK")))
    return Instrument(
        asset_class=asset_class,
        symbol=str(item["symbol"]),
        exchange=_non_empty(item.get("exchange")),
        currency=_non_empty(item.get("currency")),
        expiry=_parse_ibkr_expiry(item.get("last_trade_date")),
        strike=_parse_optional_float(item.get("strike")),
        right=_non_empty(item.get("right")),  # type: ignore[arg-type]
        multiplier=_parse_multiplier(item.get("multiplier")),
    )


def _open_order_from_ibkr_item(item: dict) -> OpenOrder:
    order_type = _normalize_ibkr_order_type(str(item.get("order_type", "")))
    return OpenOrder(
        broker_order_id=str(item["order_id"]),
        instrument=_instrument_from_ibkr_item(item),
        side=_side_from_ibkr_action(str(item.get("action", ""))),
        quantity=float(item.get("quantity", 0.0) or 0.0),
        order_type=order_type,
        status=str(item.get("status", "") or ""),
        limit_price=_parse_optional_float(item.get("limit_price")),
        stop_price=_parse_optional_float(item.get("stop_price")),
        tif=_non_empty(item.get("tif")),
        account=_non_empty(item.get("account")),
        permanent_id=_non_empty(item.get("perm_id")),
        parent_id=_non_empty(item.get("parent_id")),
        oca_group=_non_empty(item.get("oca_group")),
        order_ref=_non_empty(item.get("order_ref")),
        metadata=_broker_metadata(item),
    )


def _side_from_ibkr_action(action: str) -> str:
    return "long" if action.upper() == "BUY" else "short"


def _normalize_ibkr_order_type(order_type: str) -> str:
    normalized = order_type.strip().upper()
    if normalized == "MKT":
        return "market"
    if normalized == "LMT":
        return "limit"
    if normalized == "STP":
        return "stop"
    if normalized == "STP LMT":
        return "stop_limit"
    return normalized.lower()


def _broker_metadata(item: dict) -> dict[str, str]:
    metadata: dict[str, str] = {}
    con_id = _non_empty(item.get("con_id"))
    local_symbol = _non_empty(item.get("local_symbol"))
    if con_id:
        metadata["broker_con_id"] = con_id
    if local_symbol:
        metadata["local_symbol"] = local_symbol
    return metadata


def _parse_ibkr_expiry(value) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        if len(text) == 6 and text.isdigit():
            return datetime.strptime(text + "01", "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError:
        log.warning("Ignoring unparsable IBKR contract expiry %r", value)
        return None


def _parse_optional_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed != 0.0 else None


def _parse_multiplier(value) -> float:
    if value is None or value == "":
        return 1.0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 1.0
    return parsed if parsed > 0 else 1.0


def _non_empty(value) -> str | None:
    text = str(value or "").strip()
    return text or None

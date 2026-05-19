"""IBKRClient — EWrapper/EClient with reader-thread → asyncio queue bridge.

This is the ONLY file in the tradeframe package that imports ibapi.
All other IBKR adapter files (broker.py, data.py, contracts.py) depend on
this client and never import ibapi directly.

Thread model:
  - EClient.run() executes in a dedicated daemon reader thread.
  - EWrapper callbacks fire in that reader thread.
  - Callbacks push items into asyncio.Queue instances via loop.call_soon_threadsafe.
  - The asyncio event loop drains the queues in broker.py and data.py.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from typing import Any

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import BarData
    from ibapi.contract import Contract, ContractDetails
    from ibapi.execution import Execution
    from ibapi.order import Order
    from ibapi.order_state import OrderState
    _IBAPI_AVAILABLE = True
except ImportError:
    _IBAPI_AVAILABLE = False
    EWrapper = object  # type: ignore[assignment,misc]
    EClient = object   # type: ignore[assignment,misc]

log = logging.getLogger(__name__)

# Info-only error codes that are not actionable
_IGNORE_CODES = {2104, 2106, 2107, 2108, 2119, 2158}


class IBKRClient(EWrapper, EClient):  # type: ignore[misc]
    """EWrapper/EClient with asyncio queue bridge.

    Queues are populated by EWrapper callbacks (reader thread) and drained by
    IBKRBroker and IBKRDataProvider (asyncio event loop).
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if not _IBAPI_AVAILABLE:
            raise ImportError(
                "ibapi is required for IBKR adapter. "
                "Install with: pip install ibapi  (or: pip install tradeframe[ibkr])"
            )
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._loop = loop
        self._ready_event = threading.Event()
        self._next_order_id: int = 0
        self._id_lock = threading.Lock()

        # Queues drained by data.py and broker.py
        self.bar_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.hist_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.fill_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.order_update_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.position_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.account_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.contract_details_queue: asyncio.Queue[dict] = asyncio.Queue()

        # Map reqId → asyncio.Event for blocking requests
        self._req_events: dict[int, threading.Event] = {}
        self._req_results: dict[int, list] = {}

        self._reader_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind callback delivery to the engine's running asyncio loop."""
        self._loop = loop

    def connect_and_run(
        self,
        host: str,
        port: int,
        client_id: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Connect to TWS/Gateway and start reader thread. Blocks until ready."""
        if loop is not None:
            self.bind_loop(loop)
        if self._loop is None:
            raise RuntimeError("IBKRClient requires an asyncio loop before connecting")
        if self.isConnected() and self.is_ready():
            return
        self.connect(host, port, client_id)
        self._reader_thread = threading.Thread(
            target=self.run, name="ibkr-reader", daemon=True
        )
        self._reader_thread.start()
        if not self._ready_event.wait(timeout=20):
            raise TimeoutError("IBKR connection timed out waiting for nextValidId")
        log.info("IBKRClient ready (client_id=%d)", client_id)

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def get_next_order_id(self) -> int:
        with self._id_lock:
            oid = self._next_order_id
            self._next_order_id += 1
            return oid

    # ------------------------------------------------------------------
    # EWrapper: connection callbacks
    # ------------------------------------------------------------------

    def nextValidId(self, orderId: int) -> None:
        with self._id_lock:
            self._next_order_id = orderId
        self._ready_event.set()

    def error(self, reqId: int, *args: Any) -> None:
        # ibapi changed the signature between versions; handle both
        if len(args) >= 2:
            error_code = args[0]
            error_msg = args[1]
        elif len(args) == 1:
            error_code = 0
            error_msg = str(args[0])
        else:
            return

        if error_code in _IGNORE_CODES:
            log.debug("IBKR info [%d]: %s", error_code, error_msg)
            return
        log.warning("IBKR error reqId=%d code=%d: %s", reqId, error_code, error_msg)

    def connectionClosed(self) -> None:
        log.warning("IBKR connection closed")
        self._ready_event.clear()

    # ------------------------------------------------------------------
    # EWrapper: real-time bars (5s) → bar_queue
    # ------------------------------------------------------------------

    def realtimeBar(
        self,
        reqId: int,
        time_: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Any,
        wap: float,
        count: int,
    ) -> None:
        ts = datetime.fromtimestamp(time_, tz=timezone.utc)
        self._push(self.bar_queue, {
            "req_id": reqId,
            "timestamp": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": float(volume),
        })

    # ------------------------------------------------------------------
    # EWrapper: historical bars → hist_queue
    # ------------------------------------------------------------------

    def historicalData(self, reqId: int, bar: "BarData") -> None:
        self._push(self.hist_queue, {
            "req_id": reqId,
            "date": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": float(bar.volume),
            "done": False,
        })

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        self._push(self.hist_queue, {"req_id": reqId, "done": True})

    # ------------------------------------------------------------------
    # EWrapper: order fills → fill_queue
    # ------------------------------------------------------------------

    def execDetails(self, reqId: int, contract: "Contract", execution: "Execution") -> None:
        self._push(self.fill_queue, {
            "order_id": str(execution.orderId),
            "symbol": contract.symbol,
            "sec_type": contract.secType,
            "side": execution.side,  # "BOT" or "SLD"
            "shares": float(execution.shares),
            "price": float(execution.price),
            "timestamp": datetime.now(tz=timezone.utc),
        })

    # ------------------------------------------------------------------
    # EWrapper: order status → order_update_queue
    # ------------------------------------------------------------------

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        self._push(self.order_update_queue, {
            "order_id": str(orderId),
            "status": status.lower(),
            "filled": filled,
            "remaining": remaining,
            "avg_fill_price": avgFillPrice,
        })

    def openOrder(
        self,
        orderId: int,
        contract: "Contract",
        order: "Order",
        orderState: "OrderState",
    ) -> None:
        pass  # Handled via orderStatus

    # ------------------------------------------------------------------
    # EWrapper: positions → position_queue
    # ------------------------------------------------------------------

    def position(
        self, account: str, contract: "Contract", position: float, avgCost: float
    ) -> None:
        self._push(self.position_queue, {
            "account": account,
            "symbol": contract.symbol,
            "sec_type": contract.secType,
            "con_id": contract.conId,
            "local_symbol": contract.localSymbol,
            "position": float(position),
            "avg_cost": float(avgCost),
        })

    def positionEnd(self) -> None:
        self._push(self.position_queue, {"done": True})

    # ------------------------------------------------------------------
    # EWrapper: account summary → account_queue
    # ------------------------------------------------------------------

    def accountSummary(
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ) -> None:
        try:
            fval = float(value)
        except ValueError:
            return
        self._push(self.account_queue, {
            "req_id": reqId,
            "account": account,
            "tag": tag,
            "value": fval,
            "done": False,
        })

    def accountSummaryEnd(self, reqId: int) -> None:
        self._push(self.account_queue, {"req_id": reqId, "done": True})

    # ------------------------------------------------------------------
    # EWrapper: contract details → contract_details_queue
    # ------------------------------------------------------------------

    def contractDetails(self, reqId: int, contractDetails: "ContractDetails") -> None:
        c = contractDetails.contract
        self._push(self.contract_details_queue, {
            "req_id": reqId,
            "con_id": c.conId,
            "symbol": c.symbol,
            "sec_type": c.secType,
            "exchange": c.exchange,
            "currency": c.currency,
            "local_symbol": c.localSymbol,
            "last_trade_date": getattr(c, "lastTradeDateOrContractMonth", ""),
            "multiplier": c.multiplier,
            "done": False,
            "raw": contractDetails,
        })

    def contractDetailsEnd(self, reqId: int) -> None:
        self._push(self.contract_details_queue, {"req_id": reqId, "done": True})

    # ------------------------------------------------------------------
    # Internal bridge helper
    # ------------------------------------------------------------------

    def _push(self, queue: asyncio.Queue, item: dict) -> None:
        """Thread-safe push from reader thread into asyncio queue."""
        if self._loop is None:
            log.warning("Dropping IBKR callback before loop is bound: %s", item)
            return
        self._loop.call_soon_threadsafe(queue.put_nowait, item)

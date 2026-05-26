"""IBKR contract resolution — Instrument ↔ ibapi.Contract.

This file and client.py are the only files that touch ibapi types.
All logic for building contracts and resolving front-month futures lives here.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

from ...types import Instrument

if TYPE_CHECKING:
    from .client import IBKRClient

log = logging.getLogger(__name__)

try:
    from ibapi.contract import Contract as _IBContract
    _IBAPI_AVAILABLE = True
except ImportError:
    _IBAPI_AVAILABLE = False
    _IBContract = object  # type: ignore[assignment,misc]


class IBKRInstrumentResolver:
    """Resolve IBKR-traded instruments that need broker contract discovery."""

    def __init__(
        self,
        client: "IBKRClient",
        *,
        min_days_to_expiry: int = 7,
        lookahead_contracts: int = 2,
    ) -> None:
        self._client = client
        self._min_days_to_expiry = int(min_days_to_expiry)
        self._lookahead_contracts = int(lookahead_contracts)

    async def resolve(self, instrument: Instrument) -> Instrument:
        if instrument.asset_class != "future" or instrument.expiry is not None:
            return instrument
        return await resolve_front_month_future(
            self._client,
            instrument,
            min_days_to_expiry=self._min_days_to_expiry,
            lookahead_contracts=self._lookahead_contracts,
        )


def instrument_to_contract(instrument: Instrument) -> "_IBContract":
    """Build an ibapi Contract from an Instrument.

    Only this file and client.py may import ibapi types.
    """
    if not _IBAPI_AVAILABLE:
        raise ImportError("ibapi required for IBKR adapter")

    c = _IBContract()
    c.symbol = instrument.symbol
    c.currency = instrument.currency or "USD"
    c.exchange = instrument.exchange or "SMART"

    ac = instrument.asset_class
    if ac == "equity":
        c.secType = "STK"
        c.primaryExch = instrument.exchange or "NASDAQ"
        c.exchange = "SMART"
    elif ac == "future":
        c.secType = "FUT"
        c.exchange = instrument.exchange or "CME"
        multiplier = _future_multiplier_field(instrument.multiplier)
        if multiplier:
            c.multiplier = multiplier
        if instrument.expiry:
            c.lastTradeDateOrContractMonth = instrument.expiry.strftime("%Y%m")
    elif ac == "option":
        c.secType = "OPT"
        c.exchange = instrument.exchange or "SMART"
        if instrument.expiry:
            c.lastTradeDateOrContractMonth = instrument.expiry.strftime("%Y%m%d")
        if instrument.strike:
            c.strike = instrument.strike
        if instrument.right:
            c.right = instrument.right
    elif ac == "fx":
        c.secType = "CASH"
        c.exchange = "IDEALPRO"
    elif ac == "index":
        c.secType = "IND"
        c.exchange = instrument.exchange or "CBOE"
    elif ac in ("crypto_spot", "crypto_perp"):
        c.secType = "CRYPTO"
        c.exchange = instrument.exchange or "PAXOS"
    else:
        c.secType = "STK"

    return c


async def resolve_front_month_future(
    client: "IBKRClient",
    instrument: Instrument,
    min_days_to_expiry: int = 7,
    lookahead_contracts: int = 2,
) -> Instrument:
    """Query IBKR for FUT contracts and return the highest-volume front month.

    1. Request contract details for FUT (and CONTFUT fallback).
    2. Filter out contracts expiring within min_days_to_expiry.
    3. Fetch 3-day historical volume for the top lookahead_contracts.
    4. Return the highest-volume candidate as a new Instrument with expiry set.
    """
    if not _IBAPI_AVAILABLE:
        raise ImportError("ibapi required for IBKR adapter")

    # Build a generic FUT contract for the query
    c = _IBContract()
    c.symbol = instrument.symbol
    c.secType = "FUT"
    c.exchange = instrument.exchange or "CME"
    c.currency = instrument.currency or "USD"
    multiplier = _future_multiplier_field(instrument.multiplier)
    if multiplier:
        c.multiplier = multiplier

    req_id = client.get_next_order_id()
    client.reqContractDetails(req_id, c)

    candidates: list[dict] = []
    try:
        while True:
            item = await asyncio.wait_for(client.contract_details_queue.get(), timeout=15)
            if item.get("req_id") != req_id:
                continue
            if item.get("done"):
                break
            candidates.append(item)
    except asyncio.TimeoutError:
        log.warning("contract details request timed out for %s", instrument.symbol)

    if not candidates:
        log.warning("No FUT contracts found for %s, returning instrument as-is", instrument.symbol)
        return instrument

    # Filter by min_days_to_expiry
    today = date.today()
    valid: list[dict] = []
    for c_info in candidates:
        raw = c_info.get("last_trade_date", "")
        try:
            if len(raw) == 8:
                exp = datetime.strptime(raw, "%Y%m%d").date()
            elif len(raw) == 6:
                exp = datetime.strptime(raw + "01", "%Y%m%d").date()
            else:
                continue
            if (exp - today).days >= min_days_to_expiry:
                valid.append({**c_info, "_expiry": exp})
        except ValueError:
            continue

    if not valid:
        log.warning("All %s FUT contracts expire within %d days", instrument.symbol, min_days_to_expiry)
        return instrument

    valid.sort(key=lambda x: x["_expiry"])
    top = valid[:lookahead_contracts]

    # Fetch volume for each candidate (3-day lookback)
    best: dict | None = None
    best_vol = -1.0

    for cand in top:
        hist_contract = _IBContract()
        hist_contract.conId = cand["con_id"]
        hist_contract.exchange = cand["exchange"]

        h_req = client.get_next_order_id()
        client.reqHistoricalData(
            h_req, hist_contract, "", "3 D", "1 day",
            "TRADES", 1, 1, False, []
        )

        total_vol = 0.0
        try:
            while True:
                item = await asyncio.wait_for(client.hist_queue.get(), timeout=15)
                if item.get("req_id") != h_req:
                    await client.hist_queue.put(item)
                    continue
                if item.get("done"):
                    break
                total_vol += item.get("volume", 0.0)
        except asyncio.TimeoutError:
            pass

        if total_vol > best_vol:
            best_vol = total_vol
            best = cand

    if best is None:
        best = top[0]

    expiry = best["_expiry"]
    log.info(
        "Resolved %s front-month: local_symbol=%s expiry=%s vol=%.0f",
        instrument.symbol, best.get("local_symbol"), expiry, best_vol,
    )
    return Instrument(
        asset_class=instrument.asset_class,
        symbol=instrument.symbol,
        exchange=best.get("exchange") or instrument.exchange,
        currency=best.get("currency") or instrument.currency,
        expiry=expiry,
        multiplier=_parse_multiplier(best.get("multiplier"), instrument.multiplier),
    )


def _future_multiplier_field(multiplier: float) -> str:
    if not multiplier or float(multiplier) == 1.0:
        return ""
    if float(multiplier).is_integer():
        return str(int(multiplier))
    return str(multiplier)


def _parse_multiplier(value, fallback: float) -> float:
    if value is None or value == "":
        return fallback
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback

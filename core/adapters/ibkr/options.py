"""IBKR option-chain and quote snapshots."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from ...types import Instrument, OptionChainSnapshot, OptionQuote
from .contracts import instrument_to_contract

if TYPE_CHECKING:
    from .client import IBKRClient

log = logging.getLogger(__name__)

_BID_PRICE = 1
_ASK_PRICE = 2
_BID_SIZE = 0
_ASK_SIZE = 3


class IBKROptionDataProvider:
    """Option data snapshots backed by the shared IBKRClient."""

    def __init__(
        self,
        client: "IBKRClient",
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._client = client
        self._host = host
        self._port = port
        self._client_id = client_id
        self._timeout = float(timeout_seconds)

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
        return None

    async def option_chain(self, underlying: Instrument) -> OptionChainSnapshot:
        con_id = await self._underlying_con_id(underlying)
        req_id = self._client.get_next_order_id()
        self._client.reqSecDefOptParams(
            req_id,
            underlying.symbol,
            "",
            "STK" if underlying.asset_class == "equity" else underlying.asset_class.upper(),
            con_id,
        )

        expirations: set[date] = set()
        strikes: set[float] = set()
        trading_classes: set[str] = set()
        multiplier = 100.0
        try:
            while True:
                item = await asyncio.wait_for(
                    self._client.option_params_queue.get(),
                    timeout=self._timeout,
                )
                if item.get("req_id") != req_id:
                    await self._client.option_params_queue.put(item)
                    continue
                if item.get("done"):
                    break
                for raw in item.get("expirations", ()):
                    parsed = _parse_expiration(raw)
                    if parsed is not None:
                        expirations.add(parsed)
                for raw in item.get("strikes", ()):
                    parsed_strike = _optional_float(raw)
                    if parsed_strike is not None and parsed_strike > 0:
                        strikes.add(parsed_strike)
                trading_class = str(item.get("trading_class") or "").strip()
                if trading_class:
                    trading_classes.add(trading_class)
                parsed_multiplier = _optional_float(item.get("multiplier"))
                if parsed_multiplier and parsed_multiplier > 0:
                    multiplier = parsed_multiplier
        except asyncio.TimeoutError:
            log.warning("option chain request timed out for %s", underlying.symbol)

        return OptionChainSnapshot(
            underlying=underlying,
            expirations=tuple(sorted(expirations)),
            strikes=tuple(sorted(strikes)),
            trading_classes=tuple(sorted(trading_classes)),
            multiplier=multiplier,
            timestamp=datetime.now(tz=timezone.utc),
            source="ibkr",
        )

    async def option_quote(self, option: Instrument) -> OptionQuote:
        req_id = self._client.get_next_order_id()
        contract = instrument_to_contract(option)
        self._client.reqMktData(req_id, contract, "106", True, False, [])
        values: dict[str, float | None] = {
            "bid": None,
            "ask": None,
            "bid_size": None,
            "ask_size": None,
            "model_delta": None,
            "model_price": None,
            "underlying_price": None,
        }
        try:
            while True:
                item = await asyncio.wait_for(
                    self._client.market_data_queue.get(),
                    timeout=self._timeout,
                )
                if item.get("req_id") != req_id:
                    await self._client.market_data_queue.put(item)
                    continue
                if item.get("done"):
                    break
                tick_type = item.get("tick_type")
                if item.get("kind") == "price":
                    if tick_type == _BID_PRICE:
                        values["bid"] = _positive_or_none(item.get("price"))
                    elif tick_type == _ASK_PRICE:
                        values["ask"] = _positive_or_none(item.get("price"))
                elif item.get("kind") == "size":
                    if tick_type == _BID_SIZE:
                        values["bid_size"] = _positive_or_none(item.get("size"))
                    elif tick_type == _ASK_SIZE:
                        values["ask_size"] = _positive_or_none(item.get("size"))
                elif item.get("kind") == "option_computation":
                    delta = _optional_float(item.get("delta"))
                    if delta is not None:
                        values["model_delta"] = delta
                    model_price = _positive_or_none(item.get("option_price"))
                    if model_price is not None:
                        values["model_price"] = model_price
                    underlying_price = _positive_or_none(item.get("underlying_price"))
                    if underlying_price is not None:
                        values["underlying_price"] = underlying_price
        except asyncio.TimeoutError:
            log.warning("option quote request timed out for %s", option.symbol)
        finally:
            try:
                self._client.cancelMktData(req_id)
            except Exception:
                pass

        return OptionQuote(
            instrument=option,
            bid=values["bid"],
            ask=values["ask"],
            bid_size=values["bid_size"],
            ask_size=values["ask_size"],
            model_delta=values["model_delta"],
            model_price=values["model_price"],
            underlying_price=values["underlying_price"],
            timestamp=datetime.now(tz=timezone.utc),
            source="ibkr",
        )

    async def _underlying_con_id(self, underlying: Instrument) -> int:
        req_id = self._client.get_next_order_id()
        self._client.reqContractDetails(req_id, instrument_to_contract(underlying))
        con_id = 0
        try:
            while True:
                item = await asyncio.wait_for(
                    self._client.contract_details_queue.get(),
                    timeout=self._timeout,
                )
                if item.get("req_id") != req_id:
                    await self._client.contract_details_queue.put(item)
                    continue
                if item.get("done"):
                    break
                if item.get("con_id"):
                    con_id = int(item["con_id"])
        except asyncio.TimeoutError:
            log.warning("underlying contract lookup timed out for %s", underlying.symbol)
        return con_id


def _parse_expiration(value) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _optional_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_or_none(value) -> float | None:
    parsed = _optional_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed

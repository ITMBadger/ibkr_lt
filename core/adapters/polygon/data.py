"""Polygon.io historical and live aggregate market data."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.parse import urlencode
from urllib.request import urlopen

from ...engine.timeframes import TF_1M, Timeframe
from ...types import Bar, Instrument, StreamCapabilities

log = logging.getLogger(__name__)


class PolygonDataProvider:
    """Market-data-only provider backed by Polygon.io aggregates."""

    capabilities = StreamCapabilities(
        native_timeframes=frozenset({TF_1M}),
        supports_intrabar=False,
        market_timezone="America/New_York",
        trading_sessions=frozenset({"pre", "regular", "post"}),
    )

    def __init__(
        self,
        api_key: str,
        websocket_url: str = "wss://socket.polygon.io/stocks",
        rest_url: str = "https://api.polygon.io",
        adjusted: bool = False,
    ) -> None:
        self._api_key = api_key
        self._websocket_url = websocket_url
        self._rest_url = rest_url.rstrip("/")
        self._adjusted = adjusted
        self._subscribed: set[Instrument] = set()
        self._queue: asyncio.Queue[Bar] = asyncio.Queue()
        self._ws_task: asyncio.Task | None = None
        self._running = False

    async def connect(self) -> None:
        self._running = True

    async def disconnect(self) -> None:
        self._running = False
        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None

    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None:
        if timeframe != TF_1M:
            log.info("Polygon live provider emits 1m aggregates; requested %s", timeframe.label)
        self._subscribed.add(instrument)
        if self._ws_task is None:
            self._ws_task = asyncio.create_task(self._run_websocket())

    async def unsubscribe(self, instrument: Instrument) -> None:
        self._subscribed.discard(instrument)

    async def bars(self) -> AsyncIterator[Bar]:
        while True:
            yield await self._queue.get()

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]:
        if timeframe != TF_1M:
            raise ValueError("PolygonDataProvider fetch currently supports 1m bars only")

        ticker = _polygon_ticker(instrument)
        start_s = start.astimezone(timezone.utc).date().isoformat()
        end_s = end.astimezone(timezone.utc).date().isoformat()
        query = urlencode({
            "adjusted": str(self._adjusted).lower(),
            "sort": "asc",
            "limit": 50000,
            "apiKey": self._api_key,
        })
        url = (
            f"{self._rest_url}/v2/aggs/ticker/{ticker}/range/1/minute/"
            f"{start_s}/{end_s}?{query}"
        )

        def _request() -> dict:
            with urlopen(url, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))

        payload = await asyncio.to_thread(_request)
        results = payload.get("results") or []
        bars: list[Bar] = []
        start_utc = start.astimezone(timezone.utc)
        end_utc = end.astimezone(timezone.utc)
        for row in results:
            ts = datetime.fromtimestamp(row["t"] / 1000.0, tz=timezone.utc)
            if ts < start_utc or ts > end_utc:
                continue
            bars.append(_bar_from_polygon(instrument, ts, row))
        return bars

    async def _run_websocket(self) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise ImportError(
                "websockets is required for Polygon live data. Install requirements.txt"
            ) from exc

        while self._running:
            try:
                async with websockets.connect(self._websocket_url) as ws:
                    await ws.send(json.dumps({"action": "auth", "params": self._api_key}))
                    await self._send_subscriptions(ws)
                    async for raw in ws:
                        for item in json.loads(raw):
                            if item.get("ev") != "AM":
                                continue
                            instrument = self._instrument_for_ticker(item.get("sym", ""))
                            if instrument is None:
                                continue
                            ts = datetime.fromtimestamp(item["s"] / 1000.0, tz=timezone.utc)
                            await self._queue.put(_bar_from_polygon(instrument, ts, item))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Polygon websocket error: %s", exc)
                await asyncio.sleep(5)

    async def _send_subscriptions(self, ws) -> None:
        if not self._subscribed:
            return
        params = ",".join(f"AM.{_polygon_ticker(instr)}" for instr in self._subscribed)
        await ws.send(json.dumps({"action": "subscribe", "params": params}))

    def _instrument_for_ticker(self, ticker: str) -> Instrument | None:
        for instrument in self._subscribed:
            if _polygon_ticker(instrument) == ticker:
                return instrument
        return None


def _polygon_ticker(instrument: Instrument) -> str:
    if instrument.asset_class == "index" and not instrument.symbol.startswith("I:"):
        return f"I:{instrument.symbol}"
    return instrument.symbol


def _bar_from_polygon(instrument: Instrument, ts: datetime, row: dict) -> Bar:
    return Bar(
        instrument=instrument,
        timeframe=TF_1M,
        timestamp=ts,
        open=float(row.get("o", 0.0)),
        high=float(row.get("h", 0.0)),
        low=float(row.get("l", 0.0)),
        close=float(row.get("c", 0.0)),
        volume=float(row.get("v", 0.0)),
        is_closed=True,
        source="polygon",
    )

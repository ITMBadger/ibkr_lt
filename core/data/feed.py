"""DataFeed — composes historical and live market-data providers.

The engine speaks to this single object while deployments can choose separate
providers for backfill and live bars, such as CSV historical plus Polygon live.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator

from ..engine.timeframes import Timeframe
from ..interfaces.data import HistoricalDataProvider, StreamingDataProvider
from ..types import Bar, Instrument, StreamCapabilities


class DataFeed:
    """Delegates historical fetches and live subscriptions to configured providers."""

    def __init__(
        self,
        historical: HistoricalDataProvider | None,
        live: StreamingDataProvider,
    ) -> None:
        self._historical = historical
        self._live = live

    @property
    def capabilities(self) -> StreamCapabilities:
        return self._live.capabilities

    async def connect(self) -> None:
        if self._historical is not None:
            await self._historical.connect()
        if self._historical is not self._live:
            await self._live.connect()

    async def disconnect(self) -> None:
        if self._historical is not self._live:
            await self._live.disconnect()
        if self._historical is not None:
            await self._historical.disconnect()

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]:
        if self._historical is None:
            return []
        return await self._historical.fetch(instrument, timeframe, start, end)

    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None:
        await self._live.subscribe(instrument, timeframe)

    async def unsubscribe(self, instrument: Instrument) -> None:
        await self._live.unsubscribe(instrument)

    async def bars(self) -> AsyncIterator[Bar]:
        async for bar in self._live.bars():
            yield bar

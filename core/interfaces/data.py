"""Data provider ports — historical backfill and real-time streaming."""

from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator, Protocol, runtime_checkable

from ..types import Bar, Instrument, StreamCapabilities
from ..engine.timeframes import Timeframe


@runtime_checkable
class HistoricalDataProvider(Protocol):
    """Fetch a slice of historical bars for an instrument."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]: ...


@runtime_checkable
class StreamingDataProvider(Protocol):
    """Subscribe to real-time bar events for one or more instruments."""

    capabilities: StreamCapabilities

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None: ...
    async def unsubscribe(self, instrument: Instrument) -> None: ...
    async def bars(self) -> AsyncIterator[Bar]: ...

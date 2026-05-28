"""DataFeed — composes historical and live market-data providers.

The engine speaks to this single object while deployments can choose separate
providers for backfill and live bars, such as CSV historical plus Polygon live.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

    def is_connected(self) -> bool:
        connected = getattr(self._live, "is_connected", None)
        if callable(connected):
            return bool(connected())
        return True

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        *,
        live_session_start: datetime | None = None,
    ) -> list[Bar]:
        if self._historical is None:
            return []
        bars = await self._historical.fetch(instrument, timeframe, start, end)
        if live_session_start is not None and self._historical is not self._live:
            supplemental = await self._fetch_live_session(
                instrument,
                timeframe,
                start,
                end,
                live_session_start,
            )
            if not supplemental:
                return bars
            session_start = _ensure_aware(live_session_start)
            historical_prior = [
                bar for bar in bars
                if _ensure_aware(bar.timestamp) < session_start
            ]
            return sorted(
                [*historical_prior, *supplemental],
                key=lambda bar: bar.timestamp,
            )
        supplemental = await self._fetch_live_gap(instrument, timeframe, start, end, bars)
        if not supplemental:
            return bars
        return sorted([*bars, *supplemental], key=lambda bar: bar.timestamp)

    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None:
        await self._live.subscribe(instrument, timeframe)

    async def unsubscribe(self, instrument: Instrument) -> None:
        await self._live.unsubscribe(instrument)

    async def resubscribe_all(self) -> None:
        resubscribe = getattr(self._live, "resubscribe_all", None)
        if callable(resubscribe):
            await resubscribe()

    async def bars(self) -> AsyncIterator[Bar]:
        async for bar in self._live.bars():
            yield bar

    async def _fetch_live_gap(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        existing: list[Bar],
    ) -> list[Bar]:
        """Use a live provider's historical API to supplement stale offline data."""
        if self._historical is self._live:
            return []
        if not isinstance(self._live, HistoricalDataProvider):
            return []
        gap_start = start
        if existing:
            latest = max(_ensure_aware(bar.timestamp) for bar in existing)
            gap_start = latest + timedelta(seconds=timeframe.seconds)
        if gap_start > end:
            return []
        return await self._live.fetch(instrument, timeframe, gap_start, end)

    async def _fetch_live_session(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        live_session_start: datetime,
    ) -> list[Bar]:
        """Fetch current-session history from the live provider when available."""
        if not isinstance(self._live, HistoricalDataProvider):
            return []
        session_start = max(_ensure_aware(start), _ensure_aware(live_session_start))
        end = _ensure_aware(end)
        if session_start > end:
            return []
        return await self._live.fetch(instrument, timeframe, session_start, end)


def _ensure_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts

"""ReplayDataProvider — drives backtests by replaying pre-recorded bars.

Pairs with SimulatedClock: the Engine calls clock.advance_to(bar.timestamp)
before dispatching each bar, so all time-based strategy logic sees the bar's
timestamp rather than wall time.

Bars are emitted in chronological order. With max_workers=1 in the backtest
thread pool, two runs over the same data produce byte-identical signal logs.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator

from ...types import Bar, Instrument, StreamCapabilities
from ...engine.timeframes import Timeframe, TF_1M


class ReplayDataProvider:
    """Emits pre-loaded bars in chronological order.

    Usage:
        provider = ReplayDataProvider(bars)
        async for bar in provider.bars():
            ...
    """

    capabilities = StreamCapabilities(
        native_timeframes=frozenset({TF_1M}),
        supports_intrabar=False,
    )

    def __init__(self, bars: list[Bar]) -> None:
        self._bars = sorted(bars, key=lambda b: b.timestamp)
        self._subscribed: set[Instrument] = set()

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def subscribe(self, instrument: Instrument, timeframe: Timeframe) -> None:
        self._subscribed.add(instrument)

    async def unsubscribe(self, instrument: Instrument) -> None:
        self._subscribed.discard(instrument)

    async def bars(self) -> AsyncIterator[Bar]:
        for bar in self._bars:
            if not self._subscribed or bar.instrument in self._subscribed:
                yield bar

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]:
        """Historical fetch from the replay corpus (same bars, filtered by range)."""
        return [
            b for b in self._bars
            if b.instrument == instrument
            and start <= b.timestamp <= end
        ]

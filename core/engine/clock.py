"""Clock port and implementations.

The engine is parameterised by a Clock so backtest and live use the same
_run() coroutine. Any call to "what time is it now?" inside framework code
must go through the injected clock — never datetime.now() directly.

WallClock: live trading. Returns real UTC wall time.
SimulatedClock: backtesting. Time only advances when advance_to() is called,
  which the Engine does before dispatching each replayed bar. This makes
  all strategy time checks deterministic and reproducible.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone


class Clock(ABC):
    @abstractmethod
    def now(self) -> datetime:
        """Return current time as tz-aware UTC datetime."""
        ...


class WallClock(Clock):
    """Real wall time. Used for live trading."""

    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


class SimulatedClock(Clock):
    """Deterministic replay clock. Used for backtesting.

    Starts at datetime.min (UTC). The Engine calls advance_to(bar.timestamp)
    before dispatching each bar, so strategy code that calls clock.now()
    gets the bar's timestamp rather than wall time.
    """

    def __init__(self) -> None:
        self._now: datetime = datetime.min.replace(tzinfo=timezone.utc)

    def advance_to(self, ts: datetime) -> None:
        """Advance clock to ts. ts must be tz-aware."""
        if ts.tzinfo is None:
            raise ValueError("SimulatedClock.advance_to requires tz-aware datetime")
        self._now = ts

    def now(self) -> datetime:
        return self._now

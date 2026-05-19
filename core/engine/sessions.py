"""Vendor-neutral session boundary definitions.

Used by session-anchored indicators (session_vwap, session_open_values, etc.)
to know when a new session starts. NOT a guardrail calendar — no holiday
logic, no entry-window enforcement.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

import pytz


@dataclass(frozen=True)
class SessionBoundary:
    """Open and close time of a trading session, with timezone."""

    open: time
    close: time
    tz: str  # IANA timezone string, e.g. "America/New_York"

    def session_open_ts(self, d: date) -> datetime:
        """Return tz-aware UTC datetime for session open on the given date."""
        tz = pytz.timezone(self.tz)
        local_dt = tz.localize(datetime.combine(d, self.open))
        return local_dt.astimezone(pytz.utc)

    def session_close_ts(self, d: date) -> datetime:
        """Return tz-aware UTC datetime for session close on the given date."""
        tz = pytz.timezone(self.tz)
        local_dt = tz.localize(datetime.combine(d, self.close))
        return local_dt.astimezone(pytz.utc)

    def is_in_session(self, ts: datetime) -> bool:
        """Return True if tz-aware `ts` falls within [open, close) on its local date."""
        tz = pytz.timezone(self.tz)
        local = ts.astimezone(tz)
        return self.open <= local.time() < self.close


# Standard session boundaries
US_EQUITY_RTH = SessionBoundary(
    open=time(9, 30),
    close=time(16, 0),
    tz="America/New_York",
)

FUTURES_RTH = SessionBoundary(
    open=time(9, 30),
    close=time(16, 15),
    tz="America/New_York",
)

CRYPTO_24X7 = SessionBoundary(
    open=time(0, 0),
    close=time(23, 59),
    tz="UTC",
)

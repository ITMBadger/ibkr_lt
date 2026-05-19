"""Timeframe value object — canonical bar size representation.

String comparison is wrong: "1h" < "5m" lexicographically.
Resampling needs integer seconds: computed once here and cached.
Source selection ("coarsest native ≤ requested base") needs reliable ordering.
Public API accepts strings ("1m", "5s", "1h", "1d"); normalised to Timeframe at the boundary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import FrozenSet

_UNIT_SECONDS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}

_PARSE_RE = re.compile(r"^(\d+)([smhdw])$")


@dataclass(frozen=True, order=True)
class Timeframe:
    """Immutable bar-size value object. Ordered by seconds (smallest first)."""

    seconds: int
    label: str

    @classmethod
    def parse(cls, s: str) -> "Timeframe":
        """Parse a human string like "1m", "5s", "30m", "1h", "1d" into a Timeframe."""
        s = s.strip().lower()
        m = _PARSE_RE.match(s)
        if not m:
            raise ValueError(f"Cannot parse timeframe: {s!r}")
        quantity = int(m.group(1))
        unit = m.group(2)
        return cls(seconds=quantity * _UNIT_SECONDS[unit], label=s)

    def __str__(self) -> str:
        return self.label


# Standard constants
TF_1S  = Timeframe(1,     "1s")
TF_5S  = Timeframe(5,     "5s")
TF_1M  = Timeframe(60,    "1m")
TF_3M  = Timeframe(180,   "3m")
TF_5M  = Timeframe(300,   "5m")
TF_15M = Timeframe(900,   "15m")
TF_30M = Timeframe(1800,  "30m")
TF_1H  = Timeframe(3600,  "1h")
TF_1D  = Timeframe(86400, "1d")


def coarsest_native_le(
    requested: Timeframe,
    available: FrozenSet[Timeframe],
) -> Timeframe:
    """Return the coarsest timeframe in `available` that is ≤ `requested`.

    Used by DataManager to pick the best native streaming subscription.
    Raises ValueError if no available timeframe satisfies the constraint.
    """
    candidates = [tf for tf in available if tf.seconds <= requested.seconds]
    if not candidates:
        raise ValueError(
            f"No native timeframe ≤ {requested} in available set {available}"
        )
    return max(candidates)  # largest seconds that is still ≤ requested

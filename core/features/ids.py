"""Helpers for legacy compact feature ids."""

from __future__ import annotations

import re

_ID_RE = re.compile(r"^([a-z0-9_]+)@([A-Z0-9]+)\.([a-z0-9]+)$")


def parse_indicator_id(indicator_id: str) -> tuple[str, str, str]:
    """Parse ``ema_20@QQQ.3m`` into ``("ema_20", "QQQ", "3m")``."""
    match = _ID_RE.match(indicator_id)
    if not match:
        return indicator_id, "", ""
    return match.group(1), match.group(2), match.group(3)


__all__ = ["parse_indicator_id"]

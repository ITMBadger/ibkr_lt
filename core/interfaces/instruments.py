"""Instrument resolution ports."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..types import Instrument


@runtime_checkable
class InstrumentResolver(Protocol):
    """Resolve abstract instruments to broker-tradable contracts."""

    async def resolve(self, instrument: Instrument) -> Instrument: ...

"""Option market-data provider port."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..types import Instrument, OptionChainSnapshot, OptionQuote


@runtime_checkable
class OptionDataProvider(Protocol):
    """Async option-chain and quote interface owned by framework/adapters."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def option_chain(self, underlying: Instrument) -> OptionChainSnapshot: ...
    async def option_quote(self, option: Instrument) -> OptionQuote: ...

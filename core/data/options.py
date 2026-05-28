"""In-memory option data cache exposed to strategies through MarketContext."""

from __future__ import annotations

import threading

from ..types import Instrument, OptionChainSnapshot, OptionQuote


class OptionDataCache:
    """Thread-safe cache of framework-refreshed option chains and quotes."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._chains: dict[Instrument, OptionChainSnapshot] = {}
        self._quotes: dict[Instrument, OptionQuote] = {}

    def update_chain(self, snapshot: OptionChainSnapshot) -> None:
        with self._lock:
            self._chains[snapshot.underlying] = snapshot

    def update_quote(self, quote: OptionQuote) -> None:
        with self._lock:
            self._quotes[quote.instrument] = quote

    def chain(self, underlying: Instrument) -> OptionChainSnapshot | None:
        with self._lock:
            return self._chains.get(underlying)

    def quote(self, option: Instrument) -> OptionQuote | None:
        with self._lock:
            return self._quotes.get(option)

    def chains(self) -> list[OptionChainSnapshot]:
        with self._lock:
            return list(self._chains.values())

    def quotes(self) -> list[OptionQuote]:
        with self._lock:
            return list(self._quotes.values())

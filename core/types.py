"""Core public types for tradeframe.

All types are frozen dataclasses — immutable value objects safe to pass
across thread boundaries between the asyncio event loop and the strategy
thread pool.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal, Mapping

import pandas as pd

from .engine.timeframes import Timeframe  # noqa: F401 — re-exported


@dataclass(frozen=True)
class Instrument:
    """Canonical symbol descriptor. Adapters map this to vendor-native contracts."""

    asset_class: Literal[
        "equity", "future", "option", "fx",
        "crypto_spot", "crypto_perp", "index",
    ]
    symbol: str
    exchange: str | None = None
    currency: str | None = None
    expiry: date | None = None
    strike: float | None = None
    right: Literal["C", "P"] | None = None
    multiplier: float = 1.0

    def __str__(self) -> str:
        return self.symbol


@dataclass(frozen=True)
class Bar:
    """Single OHLCV bar. Timestamps are tz-aware UTC, bar-open aligned."""

    instrument: Instrument
    timeframe: Timeframe
    timestamp: datetime   # tz-aware UTC, bar-open
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool
    source: str  # "ibkr" | "csv" | "replay" | "paper"


@dataclass(frozen=True)
class MarketContext:
    """The only thing a strategy receives.

    bars: ctx.bars[instrument]["3m"] → pd.DataFrame of completed bars.
    indicators: ctx.indicators["ema_20@QQQ.3m"] → legacy pre-computed Series.
    features: ctx.features.get("ema", SPY, "1d", period=20) → shared common indicator.

    primary is the instrument whose bar-close triggered this evaluation.
    reference_instruments are also backfilled+streamed per SPEC declaration.
    """

    primary: Instrument
    timestamp: datetime  # tz-aware UTC, matches triggering bar
    bars: Mapping[Instrument, Mapping[str, pd.DataFrame]]
    indicators: Mapping[str, Any]  # pd.Series or pd.DataFrame depending on indicator
    features: Any | None = None  # FeatureRegistry-style accessor for shared indicators


@dataclass(frozen=True)
class Signal:
    """Intent returned by a strategy. Never contains orders or prices."""

    instrument: Instrument  # execution instrument
    side: Literal["long", "short", "flat"]
    trade_id: str | None = None  # optional logical lot id for multi-position use


@dataclass(frozen=True)
class OrderRequest:
    """Validated order intent sent from OrderManager to BrokerAdapter."""

    instrument: Instrument
    side: Literal["long", "short"]
    quantity: float
    order_type: Literal["market", "limit", "stop", "stop_limit"]
    limit_price: float | None = None
    stop_price: float | None = None
    strategy_id: str = ""
    idempotency_key: str = ""


@dataclass(frozen=True)
class OrderStatus:
    """Broker-side order state snapshot."""

    broker_order_id: str
    status: Literal["pending", "open", "filled", "cancelled", "rejected", "dry_run"]
    filled_qty: float = 0.0
    avg_fill_price: float | None = None


@dataclass(frozen=True)
class Fill:
    """A single execution event from the broker."""

    broker_order_id: str
    instrument: Instrument
    side: str
    quantity: float
    price: float
    timestamp: datetime


@dataclass(frozen=True)
class Position:
    """Current broker position for one instrument.

    quantity is signed: positive = long, negative = short, zero = flat.
    """

    instrument: Instrument
    quantity: float
    avg_cost: float
    trade_id: str | None = None

    @property
    def side(self) -> str:
        if self.quantity > 0:
            return "long"
        if self.quantity < 0:
            return "short"
        return "flat"

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0


@dataclass(frozen=True)
class PositionAdoption:
    """Operator-approved mapping for adopting an existing broker position lot."""

    strategy_id: str
    quantity: float
    entry_ts: datetime | None
    trade_id: str | None
    source_position_id: str


@dataclass(frozen=True)
class AccountSnapshot:
    """Point-in-time account summary from the broker."""

    account_id: str
    net_liquidation: float
    buying_power: float
    available_funds: float


@dataclass(frozen=True)
class QuantityRules:
    """Per-asset-class sizing constraints.

    These are correctness rules, not guardrails. Crypto perp step sizes (0.001 BTC)
    cause hard venue rejects. Futures must be integer contracts.
    """

    min_quantity: float      # minimum order size
    quantity_step: float     # order size increment
    quantity_precision: int  # decimal places for serialization

    def round(self, qty: float) -> float:
        """Round qty to the nearest valid step, clamped to min_quantity."""
        if self.quantity_step == 0:
            return qty
        rounded = round(round(qty / self.quantity_step) * self.quantity_step, self.quantity_precision)
        return max(self.min_quantity, rounded)


@dataclass(frozen=True)
class BrokerCapabilities:
    """What a broker adapter can do. OrderManager consults this before submitting."""

    asset_classes: frozenset[str]
    order_types: frozenset[str]
    quantity_rules: Mapping[str, QuantityRules]  # asset_class → rules
    supports_short: bool = True
    supports_fractional: bool = False
    supports_brackets: bool = False
    market_timezone: str = "America/New_York"
    trading_sessions: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class StreamCapabilities:
    """What timeframes a streaming provider natively emits.

    DataManager uses native_timeframes to select the coarsest native ≤ requested
    base timeframe, avoiding unnecessary BarBuilder steps.
    """

    native_timeframes: frozenset[Timeframe]
    supports_intrabar: bool = False
    market_timezone: str = "America/New_York"
    trading_sessions: frozenset[str] = field(default_factory=frozenset)

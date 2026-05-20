"""tradeframe — public facade.

Import from here for all stable public types. Internal sub-modules are
reachable by full path but not guaranteed stable across versions.
"""

from .types import (
    Bar,
    Instrument,
    Timeframe,
    MarketContext,
    Signal,
    OrderRequest,
    OrderStatus,
    Fill,
    Position,
    AccountSnapshot,
    QuantityRules,
    BrokerCapabilities,
    StreamCapabilities,
)
from .interfaces.strategy import ProtectiveStopSpec, StrategyKernel, StrategySpec
from .engine.loader import register_strategy, load_strategies
from .engine.runner import Engine
from .engine.clock import WallClock, SimulatedClock
from .data.feed import DataFeed
from .features.registry import FeatureRegistry
from .audit import AuditLogger, DecisionTrace, record_decision

__all__ = [
    "Bar",
    "Instrument",
    "Timeframe",
    "MarketContext",
    "Signal",
    "OrderRequest",
    "OrderStatus",
    "Fill",
    "Position",
    "AccountSnapshot",
    "QuantityRules",
    "BrokerCapabilities",
    "StreamCapabilities",
    "ProtectiveStopSpec",
    "StrategyKernel",
    "StrategySpec",
    "register_strategy",
    "load_strategies",
    "Engine",
    "WallClock",
    "SimulatedClock",
    "DataFeed",
    "FeatureRegistry",
    "AuditLogger",
    "DecisionTrace",
    "record_decision",
]

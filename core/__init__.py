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
    StrategyIntent,
    OptionDataRequest,
    OptionChainSnapshot,
    OptionQuote,
    PendingApproval,
    OrderRequest,
    OrderStatus,
    Fill,
    Position,
    PositionAdoption,
    AccountSnapshot,
    QuantityRules,
    BrokerCapabilities,
    StreamCapabilities,
)
from .interfaces.strategy import (
    ENTRY_FREQUENCY_ONE_PER_DAY,
    ENTRY_FREQUENCY_ONE_PER_SESSION,
    ENTRY_FREQUENCY_UNLIMITED,
    POSITION_MODE_MULTI,
    POSITION_MODE_SINGLE,
    PositionPolicy,
    ProtectiveStopSpec,
    ProtectiveStopUpdate,
    StrategyKernel,
    StrategySpec,
)
from .interfaces.instruments import InstrumentResolver
from .interfaces.options import OptionDataProvider
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
    "StrategyIntent",
    "OptionDataRequest",
    "OptionChainSnapshot",
    "OptionQuote",
    "PendingApproval",
    "OrderRequest",
    "OrderStatus",
    "Fill",
    "Position",
    "PositionAdoption",
    "AccountSnapshot",
    "QuantityRules",
    "BrokerCapabilities",
    "StreamCapabilities",
    "ENTRY_FREQUENCY_ONE_PER_DAY",
    "ENTRY_FREQUENCY_ONE_PER_SESSION",
    "ENTRY_FREQUENCY_UNLIMITED",
    "POSITION_MODE_MULTI",
    "POSITION_MODE_SINGLE",
    "PositionPolicy",
    "ProtectiveStopSpec",
    "ProtectiveStopUpdate",
    "StrategyKernel",
    "StrategySpec",
    "InstrumentResolver",
    "OptionDataProvider",
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

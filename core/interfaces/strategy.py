"""StrategyKernel base class and StrategySpec.

StrategyKernel is a concrete base class, NOT a Protocol. Reasons:
- on_exit / on_start are genuinely optional (no-op defaults); Protocol methods
  are structurally required, which makes "optional" a lie.
- isinstance(obj, StrategyKernel) is the loader's real check at registration.
- PyArmor/Cython compiles cleanly off a class hierarchy; Protocol structural
  typing interacts badly with frame inspection in compiled code.

Strategies stay plain sync def — CPU-bound NumPy/pandas gains nothing from
async, and sync compiles cleanly with Cython.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Mapping

from ..types import Instrument, MarketContext, Position, Signal

POSITION_MODE_SINGLE = "single_position"
POSITION_MODE_MULTI = "multi_position"

ENTRY_FREQUENCY_ONE_PER_DAY = "one_per_day"
ENTRY_FREQUENCY_ONE_PER_SESSION = "one_per_session"
ENTRY_FREQUENCY_UNLIMITED = "unlimited"

PositionMode = Literal["single_position", "multi_position"]
EntryFrequency = Literal["one_per_day", "one_per_session", "unlimited"]


@dataclass(frozen=True)
class ProtectiveStopSpec:
    """Broker-side protective stop requested by a strategy."""

    pct: float
    reference: str = "fill_price"


@dataclass(frozen=True)
class PositionPolicy:
    """Declares how a strategy may hold positions and emit entry signals."""

    position_mode: PositionMode = POSITION_MODE_SINGLE
    entry_frequency: EntryFrequency = ENTRY_FREQUENCY_UNLIMITED
    max_concurrent_positions: int | None = 1

    def __post_init__(self) -> None:
        valid_modes = {POSITION_MODE_SINGLE, POSITION_MODE_MULTI}
        if self.position_mode not in valid_modes:
            raise ValueError(f"Unsupported position_mode: {self.position_mode!r}")

        valid_frequencies = {
            ENTRY_FREQUENCY_ONE_PER_DAY,
            ENTRY_FREQUENCY_ONE_PER_SESSION,
            ENTRY_FREQUENCY_UNLIMITED,
        }
        if self.entry_frequency not in valid_frequencies:
            raise ValueError(f"Unsupported entry_frequency: {self.entry_frequency!r}")

        if (
            self.max_concurrent_positions is not None
            and self.max_concurrent_positions < 1
        ):
            raise ValueError("max_concurrent_positions must be >= 1 or None")


@dataclass(frozen=True)
class StrategySpec:
    """Declares what data, timeframes, and indicators a strategy needs.

    The engine uses SPEC to:
    - subscribe and backfill primary_instrument and all reference_instruments
    - enforce warmup_bars before calling generate()
    - enforce position and entry-frequency policy before accepting new entries

    indicators is retained for clear/dev and legacy strategies. Protected
    strategies should prefer ctx.features.get(...) at runtime so public metadata
    does not list every feature dependency.
    """

    id: str
    primary_instrument: Instrument
    execution_instrument: Instrument
    reference_instruments: tuple[Instrument, ...] = ()
    timeframes: tuple[str, ...] = ("1m",)
    warmup_bars: Mapping[str, int] = field(default_factory=dict)
    indicators: tuple[str, ...] = ()
    protective_stop: ProtectiveStopSpec | None = None
    position_policy: PositionPolicy = field(default_factory=PositionPolicy)


class StrategyKernel:
    """Base class for all strategies.

    Subclasses must:
    - Set a class-level SPEC: StrategySpec
    - Override generate()

    Subclasses may override on_exit() and on_start() for lifecycle hooks.
    """

    SPEC: ClassVar[StrategySpec]

    def __init__(self, params: Mapping[str, Any] | None = None) -> None:
        self.params: Mapping[str, Any] = params or {}

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        """Required. Called on every bar close for primary_instrument.

        Must be pure: no I/O, no broker calls, no threading primitives.
        Return a Signal to express intent or None to pass.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement generate()"
        )

    def on_exit(
        self,
        ctx: MarketContext,
        position: Position,
        state: dict,
    ) -> str | None:
        """Optional. Called per bar while a position is open.

        Return an exit reason string to close the position, or None to hold.
        Default: always hold (return None).
        """
        return None

    def on_start(self, state: dict) -> None:
        """Optional. Called once when the engine starts.

        Use to initialise state keys before the first generate() call.
        """
        return None

"""Startup position gate controller and adoption orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, Any

from ..audit.serialize import to_jsonable
from ..types import Instrument, Position, PositionAdoption
from .position_gate import (
    PHASE_AWAITING_MAPPING,
    PHASE_BLOCKED,
    PHASE_CLEAR,
    PHASE_INACTIVE,
    PHASE_MAPPED,
    PHASE_RELEASED,
    build_startup_gate_status,
    parse_optional_timestamp,
    position_gate_item,
    position_id,
    validate_startup_allocations,
)

if TYPE_CHECKING:
    from ..interfaces.strategy import StrategyKernel
    from ..portfolio.state import PortfolioState
    from ..risk.policy import RiskPolicy
    from .ownership import PositionOwnershipLedger

log = logging.getLogger(__name__)

WriteStartupEvent = Callable[..., None]


@dataclass(frozen=True)
class StartupGateResult:
    allocations: list[dict[str, Any]]
    positions: list[Position]


class StartupPositionGateController:
    """Thread-safe startup ownership gate used by the engine and control API."""

    def __init__(
        self,
        *,
        enabled: bool,
        mapping_enabled: bool,
        default_risk: "RiskPolicy",
        strategy_risk: Mapping[str, "RiskPolicy"] | None = None,
        strategy_modes: Mapping[str, str] | None = None,
        configured_allocations: Sequence[Mapping[str, Any]] | None = None,
        ownership_ledger: "PositionOwnershipLedger | None" = None,
        write_event: WriteStartupEvent | None = None,
    ) -> None:
        self._enabled = bool(enabled)
        self._mapping_enabled = bool(mapping_enabled)
        self._default_risk = default_risk
        self._strategy_risk = dict(strategy_risk or {})
        self._strategy_modes = dict(strategy_modes or {})
        self._configured_allocations = [
            dict(item)
            for item in (configured_allocations or [])
        ]
        self._ownership_ledger = ownership_ledger
        self._write_event = write_event
        self._lock = RLock()
        self._status: dict[str, Any] = {
            "enabled": self._enabled,
            "phase": PHASE_INACTIVE,
            "message": "",
            "positions": [],
            "allocations": [],
            "unmanaged": [],
            "last_error": None,
        }
        self._event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._action: str | None = None
        self._submitted_allocations: list[dict[str, Any]] | None = None

    def status(self) -> dict[str, Any]:
        with self._lock:
            return to_jsonable(dict(self._status))

    def set_mapping_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._mapping_enabled = bool(enabled)

    def submit_mappings(
        self,
        allocations: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        with self._lock:
            try:
                normalized = validate_startup_allocations(self._status, allocations)
            except ValueError as exc:
                self._status["last_error"] = str(exc)
                self._write_startup_event(
                    "startup_position_mapping_rejected",
                    reason=str(exc),
                    allocations=list(allocations),
                )
                raise
            self._submitted_allocations = normalized
            self._action = "continue"
            self._status["phase"] = PHASE_MAPPED
            self._status["message"] = "Startup position mappings accepted."
            self._status["allocations"] = normalized
            self._status["last_error"] = None
            event = self._event
            loop = self._loop
        self._wake(event, loop)
        return self.status()

    def request_refresh(self) -> dict[str, Any]:
        with self._lock:
            self._action = "refresh"
            self._status["message"] = "Startup position refresh requested."
            event = self._event
            loop = self._loop
        self._wake(event, loop)
        return self.status()

    def mark_clear(self, message: str) -> None:
        self._set_status({
            "enabled": self._enabled,
            "phase": PHASE_CLEAR,
            "message": message,
            "positions": [],
            "allocations": [],
            "unmanaged": [],
            "last_error": None,
        })

    async def run(
        self,
        positions: Sequence[Position],
        strategy_entries: Sequence[tuple["StrategyKernel", dict]],
        *,
        refresh_positions: Callable[[], Awaitable[Sequence[Position]]],
        on_awaiting_mapping: Callable[[], None] | None = None,
        on_released: Callable[[], None] | None = None,
    ) -> StartupGateResult:
        self._loop = asyncio.get_running_loop()
        self._event = asyncio.Event()
        current_positions = list(positions)
        while True:
            status = self.build_status(current_positions, strategy_entries)
            self._set_status(status)
            self._log_status(status)
            if status["phase"] == PHASE_CLEAR:
                return StartupGateResult(
                    allocations=list(status.get("allocations", [])),
                    positions=list(current_positions),
                )
            if status["phase"] == PHASE_BLOCKED:
                detail = (
                    status.get("last_error")
                    or status.get("message")
                    or "startup position gate blocked"
                )
                raise RuntimeError(str(detail))
            if not self._mapping_enabled:
                raise RuntimeError(
                    "Startup broker positions require ownership mapping, but no mapping "
                    "interface is enabled. Enable the control API/dashboard or provide "
                    "adopted_positions/ownership ledger mappings before live startup."
                )

            if on_awaiting_mapping is not None:
                on_awaiting_mapping()
            log.warning(
                "Startup paused: %s",
                status.get("message", "broker positions require mapping"),
            )
            while True:
                await self._event.wait()
                self._event.clear()
                with self._lock:
                    action = self._action
                    self._action = None
                    allocations = list(self._submitted_allocations or [])
                    if action == "continue":
                        self._submitted_allocations = None
                if action == "refresh":
                    current_positions = list(await refresh_positions())
                    break
                if action == "continue":
                    self._write_startup_event(
                        "startup_gate_released",
                        allocations=allocations,
                    )
                    self._set_status({
                        **self.status(),
                        "phase": PHASE_RELEASED,
                        "message": "Startup position mappings released the engine.",
                        "allocations": allocations,
                        "last_error": None,
                    })
                    if on_released is not None:
                        on_released()
                    return StartupGateResult(
                        allocations=allocations,
                        positions=list(current_positions),
                    )

    def build_status(
        self,
        positions: Sequence[Position],
        strategy_entries: Sequence[tuple["StrategyKernel", dict]],
    ) -> dict[str, Any]:
        ledger_allocations = (
            self._ownership_ledger.open_allocations()
            if self._ownership_ledger is not None
            else []
        )
        return build_startup_gate_status(
            positions,
            strategy_entries,
            default_risk=self._default_risk,
            strategy_risk=self._strategy_risk,
            strategy_modes=self._strategy_modes,
            configured_allocations=self._configured_allocations,
            ledger_allocations=ledger_allocations,
        )

    def _set_status(self, status: Mapping[str, Any]) -> None:
        with self._lock:
            self._status = to_jsonable(dict(status))

    def _log_status(self, status: Mapping[str, Any]) -> None:
        for item in status.get("unmanaged", []) or []:
            self._write_startup_event("startup_position_unmanaged", position=item)
            log.warning(
                "Startup broker position unmanaged: %s %.4f reason=%s",
                item.get("symbol"),
                item.get("quantity"),
                item.get("reason"),
            )
        for item in status.get("positions", []) or []:
            event = (
                "startup_position_mapping_blocked"
                if status.get("phase") == PHASE_BLOCKED
                else "startup_position_mapping_required"
            )
            self._write_startup_event(event, position=item)
            log.warning(
                "Startup broker position requires mapping: %s %.4f candidates=%s",
                item.get("symbol"),
                item.get("quantity"),
                [c.get("strategy_id") for c in item.get("candidates", [])],
            )

    def _write_startup_event(self, event: str, **fields: Any) -> None:
        if self._write_event is not None:
            self._write_event(event, **fields)

    @staticmethod
    def _wake(
        event: asyncio.Event | None,
        loop: asyncio.AbstractEventLoop | None,
    ) -> None:
        if event is not None and loop is not None:
            loop.call_soon_threadsafe(event.set)


def resolve_adopted_position_map(
    positions: Sequence[Position],
    strategy_entries: Sequence[tuple["StrategyKernel", dict]],
    configured_map: Mapping[Instrument, str] | None = None,
) -> dict[Instrument, str]:
    configured_map = dict(configured_map or {})
    resolved: dict[Instrument, str] = {}
    by_execution: dict[Instrument, list[str]] = {}
    for kernel, _ in strategy_entries:
        by_execution.setdefault(kernel.SPEC.execution_instrument, []).append(kernel.SPEC.id)

    for position in positions:
        configured = configured_map.get(position.instrument)
        if configured:
            resolved[position.instrument] = configured
            continue
        matches = by_execution.get(position.instrument, [])
        if len(matches) == 1:
            resolved[position.instrument] = matches[0]
        elif len(matches) > 1:
            log.warning(
                "Position %s is ambiguous across strategies %s; add adopted_position_map",
                position.instrument.symbol,
                matches,
            )
    return resolved


def apply_startup_position_allocations(
    allocations: Sequence[Mapping[str, Any]],
    broker_positions: Sequence[Position],
    strategy_entries: Sequence[tuple["StrategyKernel", dict]],
    portfolio: "PortfolioState",
    *,
    ownership_ledger: "PositionOwnershipLedger | None" = None,
    write_event: WriteStartupEvent | None = None,
) -> None:
    if not allocations:
        return
    positions_by_id = {
        position_id(position): position
        for position in broker_positions
    }
    strategy_by_id = {
        kernel.SPEC.id: (kernel, state)
        for kernel, state in strategy_entries
    }
    allocated_by_position: dict[str, float] = defaultdict(float)
    for allocation in allocations:
        position_id_key = str(allocation["position_id"])
        strategy_id = str(allocation["strategy_id"])
        broker_position = positions_by_id[position_id_key]
        kernel, state = strategy_by_id[strategy_id]
        quantity = float(allocation["quantity"])
        signed_quantity = quantity if broker_position.quantity > 0 else -quantity
        entry_ts = parse_optional_timestamp(allocation.get("entry_ts"))
        adoption = PositionAdoption(
            strategy_id=strategy_id,
            quantity=quantity,
            entry_ts=entry_ts,
            trade_id=allocation.get("trade_id"),
            source_position_id=position_id_key,
        )
        candidate_position = Position(
            instrument=broker_position.instrument,
            quantity=signed_quantity,
            avg_cost=broker_position.avg_cost,
            trade_id=adoption.trade_id,
        )
        adopted_position = kernel.on_adopt_position(candidate_position, adoption, state)
        if adopted_position is None:
            raise RuntimeError(f"strategy {strategy_id!r} rejected adopted position")
        portfolio.adopt_strategy_position(strategy_id, adopted_position)
        if ownership_ledger is not None:
            try:
                ownership_ledger.record_adoption(
                    broker_position,
                    adoption,
                    source=str(allocation.get("source") or "operator"),
                )
            except Exception as exc:
                log.warning("Position ownership ledger adoption write failed: %s", exc)
        allocated_by_position[position_id_key] += quantity
        if write_event is not None:
            write_event(
                "startup_position_mapped",
                strategy_id=strategy_id,
                position=adopted_position,
                adoption=adoption,
            )
        log.warning(
            "Startup mapped broker position: %s %.4f to %s trade_id=%s",
            adopted_position.instrument.symbol,
            adopted_position.quantity,
            strategy_id,
            adopted_position.trade_id,
        )

    for current_position_id, position in positions_by_id.items():
        remaining = (
            abs(position.quantity)
            - allocated_by_position.get(current_position_id, 0.0)
        )
        if remaining > 1e-9:
            if write_event is not None:
                write_event(
                    "startup_position_unmanaged",
                    position=position_gate_item(position),
                    unmanaged_quantity=remaining,
                    reason="unallocated_remainder",
                )
            log.warning(
                "Startup broker position remainder unmanaged: %s %.4f",
                position.instrument.symbol,
                remaining,
            )

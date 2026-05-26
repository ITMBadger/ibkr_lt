"""Safe operator-facing facade over Engine public methods."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


class OperatorService:
    """Read/control surface shared by HTTP API routes and dashboard plugins."""

    def __init__(self, engine, *, metadata: Mapping[str, Any] | None = None) -> None:
        self.engine = engine
        self.metadata: dict[str, Any] = dict(metadata or {})

    def snapshot_state(self) -> dict[str, Any]:
        return dict(self.engine.snapshot_state())

    def runtime_snapshot(self) -> dict[str, Any]:
        state = self.snapshot_state()
        state["metadata"] = dict(self.metadata)
        return state

    def strategies(self) -> list[dict[str, Any]]:
        return list(self.snapshot_state().get("strategies", []))

    def positions(self) -> dict[str, Any]:
        return dict(self.snapshot_state().get("positions", {}))

    def events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        recent = list(self.snapshot_state().get("recent_events", []))
        return recent[-int(limit):]

    def startup_gate_status(self) -> dict[str, Any]:
        return dict(self.engine.startup_gate_status())

    def submit_startup_mappings(
        self,
        allocations: Sequence[Mapping[str, Any]],
        *,
        ack_unmanaged_remainders: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return dict(
            self.engine.submit_startup_mappings(
                allocations,
                ack_unmanaged_remainders=ack_unmanaged_remainders,
            )
        )

    def request_startup_gate_refresh(self) -> dict[str, Any]:
        return dict(self.engine.request_startup_gate_refresh())

    def set_metadata(self, **fields: Any) -> None:
        self.metadata.update(fields)

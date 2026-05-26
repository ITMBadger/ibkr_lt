"""Thread-safe approval store for strategy-generated trade intents."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

from ..types import PendingApproval, StrategyIntent


class ApprovalStore:
    """Owns approval lifecycle for intents that require operator consent."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: dict[str, PendingApproval] = {}

    def request(self, strategy_id: str, intent: StrategyIntent) -> PendingApproval:
        approval_id = _approval_id(strategy_id, intent)
        now = datetime.now(tz=timezone.utc)
        with self._lock:
            current = self._items.get(approval_id)
            if current is not None:
                return current
            item = PendingApproval(
                approval_id=approval_id,
                strategy_id=strategy_id,
                intent=intent,
                status="pending",
                created_at=now,
                updated_at=now,
                reason=intent.approval_reason,
            )
            self._items[approval_id] = item
            return item

    def approve(self, approval_id: str, *, operator_note: str | None = None) -> PendingApproval:
        return self._set_status(approval_id, "approved", operator_note=operator_note)

    def reject(self, approval_id: str, *, operator_note: str | None = None) -> PendingApproval:
        return self._set_status(approval_id, "rejected", operator_note=operator_note)

    def mark_submitted(self, approval_id: str) -> PendingApproval:
        return self._set_status(approval_id, "submitted")

    def get(self, approval_id: str) -> PendingApproval | None:
        with self._lock:
            return self._items.get(approval_id)

    def list(self, *, include_terminal: bool = False) -> list[PendingApproval]:
        with self._lock:
            items = list(self._items.values())
        if include_terminal:
            return sorted(items, key=lambda item: item.created_at)
        return sorted(
            [
                item for item in items
                if item.status in {"pending", "approved"}
            ],
            key=lambda item: item.created_at,
        )

    def _set_status(
        self,
        approval_id: str,
        status: str,
        *,
        operator_note: str | None = None,
    ) -> PendingApproval:
        if status not in {"approved", "rejected", "submitted"}:
            raise ValueError(f"unsupported approval status: {status!r}")
        with self._lock:
            current = self._items.get(approval_id)
            if current is None:
                raise ValueError(f"unknown approval_id: {approval_id!r}")
            updated = PendingApproval(
                approval_id=current.approval_id,
                strategy_id=current.strategy_id,
                intent=current.intent,
                status=status,  # type: ignore[arg-type]
                created_at=current.created_at,
                updated_at=datetime.now(tz=timezone.utc),
                reason=current.reason,
                operator_note=operator_note if operator_note is not None else current.operator_note,
            )
            self._items[approval_id] = updated
            return updated


def _approval_id(strategy_id: str, intent: StrategyIntent) -> str:
    key = intent.idempotency_key
    if not key:
        parts = [
            strategy_id,
            intent.instrument.asset_class,
            intent.instrument.symbol,
            intent.instrument.expiry.isoformat() if intent.instrument.expiry else "",
            str(intent.instrument.strike or ""),
            str(intent.instrument.right or ""),
            intent.side,
            str(intent.quantity),
            intent.role,
            intent.trade_id or "",
        ]
        key = ":".join(parts)
    return f"{strategy_id}:{key}"

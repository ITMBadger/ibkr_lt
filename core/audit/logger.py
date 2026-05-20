"""Persistent audit log writer."""

from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .serialize import to_jsonable
from .trace import DecisionTrace


class AuditLogger:
    """Synchronous JSONL audit writer with a small thread lock."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        log_dir: str | Path = "logs",
        profile: str = "owner",
        strategy_decisions: str = "full",
        decision_scope: str = "every_eval",
        decision_interval_minutes: int = 30,
        run_id: str | None = None,
    ) -> None:
        self.enabled = enabled
        self.profile = profile
        self.strategy_decisions = strategy_decisions
        self.decision_scope = decision_scope
        self.decision_interval_minutes = max(1, int(decision_interval_minutes))
        self.run_id = run_id or uuid.uuid4().hex
        self.log_dir = Path(log_dir)
        self._lock = threading.RLock()
        self._last_decision_interval: dict[tuple[str, str], datetime] = {}
        if self.enabled:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "AuditLogger | None":
        cfg = dict(config.get("logging") or {})
        if not cfg:
            return None
        enabled = bool(cfg.get("enabled", True))
        return cls(
            enabled=enabled,
            log_dir=cfg.get("log_dir", "logs"),
            profile=str(cfg.get("profile", "owner")),
            strategy_decisions=str(cfg.get("strategy_decisions", "full")),
            decision_scope=str(cfg.get("decision_scope", "every_eval")),
            decision_interval_minutes=int(cfg.get("decision_interval_minutes", 30)),
        )

    def write(self, filename: str, event: dict[str, Any]) -> None:
        if not self.enabled:
            return
        payload = {
            "run_id": self.run_id,
            "logged_at": datetime.now(tz=timezone.utc).isoformat(),
            **to_jsonable(event),
        }
        line = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        with self._lock:
            with (self.log_dir / filename).open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    def overwrite(self, filename: str, event: dict[str, Any]) -> None:
        if not self.enabled:
            return
        payload = {
            "run_id": self.run_id,
            "logged_at": datetime.now(tz=timezone.utc).isoformat(),
            **to_jsonable(event),
        }
        text = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        with self._lock:
            (self.log_dir / filename).write_text(text + "\n", encoding="utf-8")

    def decision(self, trace: DecisionTrace) -> None:
        if self.profile != "owner" or self.strategy_decisions in {"off", "none", "false"}:
            return
        event = trace.to_event()
        scope = self.decision_scope.lower()
        if scope in {"every_eval", "all", "full"}:
            self.write("strategy_decisions.jsonl", event)
            return
        if scope in {"trigger_and_interval", "trigger_and_30m", "signal_and_interval"}:
            if _is_entry_signal(event):
                self.write("strategy_trigger_decisions.jsonl", event)
            if self._should_write_interval_decision(event):
                strategy_id = _safe_filename_part(str(event.get("strategy_id", "unknown")))
                self.overwrite(f"strategy_30m_latest_{strategy_id}.json", event)
            return
        log = logging.getLogger(__name__)
        log.warning("Unknown decision_scope=%r; writing every evaluation", self.decision_scope)
        self.write("strategy_decisions.jsonl", event)

    def signal(self, event: dict[str, Any]) -> None:
        self.write("signals.jsonl", event)

    def order(self, event: dict[str, Any]) -> None:
        self.write("orders.jsonl", event)

    def fill(self, event: dict[str, Any]) -> None:
        self.write("fills.jsonl", event)

    def _should_write_interval_decision(self, event: dict[str, Any]) -> bool:
        if not _has_full_decision_detail(event):
            return False
        ts = _parse_event_timestamp(event.get("timestamp"))
        if ts is None:
            return False
        bucket = _floor_to_interval(ts, self.decision_interval_minutes)
        key = (
            str(event.get("strategy_id", "unknown")),
            str(event.get("phase", "unknown")),
        )
        if self._last_decision_interval.get(key) == bucket:
            return False
        self._last_decision_interval[key] = bucket
        return True


def configure_runtime_logging(
    *,
    log_dir: str | Path = "logs",
    level: str = "INFO",
    enabled: bool = True,
) -> None:
    """Attach a runtime.log file handler to the root logger."""
    if not enabled:
        return
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    runtime_path = path / "runtime.log"
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    for handler in root.handlers:
        if getattr(handler, "_tradeframe_runtime_log", None) == str(runtime_path):
            return

    handler = logging.FileHandler(runtime_path, encoding="utf-8")
    handler._tradeframe_runtime_log = str(runtime_path)  # type: ignore[attr-defined]
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)


def _is_entry_signal(event: dict[str, Any]) -> bool:
    return event.get("phase") == "entry" and event.get("decision") == "signal"


def _has_full_decision_detail(event: dict[str, Any]) -> bool:
    return (
        bool(event.get("bars"))
        and bool(event.get("conditions"))
        and bool(event.get("indicators"))
    )


def _parse_event_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _floor_to_interval(ts: datetime, minutes: int) -> datetime:
    minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute, second=0, microsecond=0)


def _safe_filename_part(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")
    cleaned = "".join(ch if ch in allowed else "_" for ch in value)
    return cleaned or "unknown"

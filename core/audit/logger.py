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
        run_id: str | None = None,
    ) -> None:
        self.enabled = enabled
        self.profile = profile
        self.strategy_decisions = strategy_decisions
        self.run_id = run_id or uuid.uuid4().hex
        self.log_dir = Path(log_dir)
        self._lock = threading.RLock()
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

    def decision(self, trace: DecisionTrace) -> None:
        if self.profile != "owner" or self.strategy_decisions in {"off", "none", "false"}:
            return
        self.write("strategy_decisions.jsonl", trace.to_event())

    def signal(self, event: dict[str, Any]) -> None:
        self.write("signals.jsonl", event)

    def order(self, event: dict[str, Any]) -> None:
        self.write("orders.jsonl", event)

    def fill(self, event: dict[str, Any]) -> None:
        self.write("fills.jsonl", event)


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

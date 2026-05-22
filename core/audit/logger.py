"""Persistent audit log writer."""

from __future__ import annotations

import csv
import json
import logging
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .decision_csv import csv_fieldnames, decision_table_csvs, flatten_decision_event
from .serialize import to_jsonable
from .trace import DecisionTrace

_FILENAME_TZ = ZoneInfo("America/New_York")


class AuditLogger:
    """Synchronous audit writer with a small thread lock."""

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
        run_subdir: bool = False,
        run_started_at: datetime | None = None,
    ) -> None:
        self.enabled = enabled
        self.profile = profile
        self.strategy_decisions = strategy_decisions
        self.decision_scope = decision_scope
        self.decision_interval_minutes = max(1, int(decision_interval_minutes))
        self.run_id = run_id or uuid.uuid4().hex
        self.base_log_dir = Path(log_dir)
        self.run_subdir = run_subdir
        self.log_dir = self.base_log_dir
        self._lock = threading.RLock()
        self._last_decision_interval: dict[tuple[str, str], datetime] = {}
        if self.enabled:
            if self.run_subdir:
                self.log_dir = allocate_run_log_dir(self.base_log_dir, run_started_at)
            else:
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
            run_subdir=bool(cfg.get("run_subdir", True)),
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

    def write_decision_trace(self, stem: str, event: dict[str, Any]) -> None:
        if not self.enabled:
            return
        detail = {
            **to_jsonable(flatten_decision_event(event)),
            "logged_at": datetime.now(tz=timezone.utc).isoformat(),
            "run_id": self.run_id,
        }
        tables = decision_table_csvs(event)
        with self._lock:
            trace_dir = self._unique_dir(stem)
            tmp_dir = self._unique_dir(f".{stem}.tmp")
            tmp_dir.mkdir()
            try:
                self._write_csv_rows(tmp_dir / "decision.csv", [detail], fieldnames=list(detail))
                for filename, rows in tables:
                    self._write_csv_rows(tmp_dir / filename, rows)
                tmp_dir.rename(trace_dir)
            except Exception:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                raise

    def _unique_dir(self, stem: str) -> Path:
        path = self.log_dir / stem
        if not path.exists():
            return path
        for index in range(2, 10_000):
            candidate = self.log_dir / f"{stem}_{index}"
            if not candidate.exists():
                return candidate
        raise RuntimeError(f"Could not allocate unique audit directory for {stem!r}")

    @staticmethod
    def _write_csv_rows(
        path: Path,
        rows: list[dict[str, Any]],
        *,
        fieldnames: list[str] | None = None,
    ) -> None:
        if not rows:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = fieldnames or csv_fieldnames(rows)
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def decision(self, trace: DecisionTrace) -> None:
        if self.profile != "owner" or self.strategy_decisions in {"off", "none", "false"}:
            return
        event = trace.to_event()
        scope = self.decision_scope.lower()
        strategy_id = _safe_filename_part(str(event.get("strategy_id", "unknown")))
        if scope in {"every_eval", "all", "full"}:
            eval_stem = _decision_file_stem("strategy_eval", strategy_id, event)
            self.write_decision_trace(eval_stem, event)
            return
        if scope in {"trigger_and_interval", "trigger_and_30m", "signal_and_interval"}:
            if _is_entry_signal(event):
                trigger_stem = _decision_file_stem("strategy_trigger", strategy_id, event)
                self.write_decision_trace(trigger_stem, event)
            if self._should_write_interval_decision(event):
                interval_stem = _decision_file_stem(
                    f"strategy_{self.decision_interval_minutes}m",
                    strategy_id,
                    event,
                    interval_minutes=self.decision_interval_minutes,
                )
                self.write_decision_trace(interval_stem, event)
            return
        log = logging.getLogger(__name__)
        log.warning("Unknown decision_scope=%r; writing every evaluation to CSV", self.decision_scope)
        eval_stem = _decision_file_stem("strategy_eval", strategy_id, event)
        self.write_decision_trace(eval_stem, event)

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
    has_market_detail = bool(event.get("bars")) or bool(event.get("tables"))
    has_decision_detail = bool(event.get("conditions")) or bool(event.get("indicators"))
    return has_market_detail and has_decision_detail


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
        return ts.replace(tzinfo=_FILENAME_TZ).astimezone(timezone.utc)
    return ts.astimezone(timezone.utc)


def _floor_to_interval(ts: datetime, minutes: int) -> datetime:
    minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute, second=0, microsecond=0)


def _decision_file_stem(
    prefix: str,
    strategy_id: str,
    event: dict[str, Any],
    *,
    interval_minutes: int | None = None,
) -> str:
    ts = _parse_event_timestamp(event.get("timestamp")) or datetime.now(tz=timezone.utc)
    if interval_minutes is not None:
        ts = _floor_to_interval(ts, interval_minutes)
    ts_et = ts.astimezone(_FILENAME_TZ)
    return f"{prefix}_{strategy_id}_{ts_et.strftime('%Y%m%d_%H%M')}_et"


def allocate_run_log_dir(base_log_dir: Path, started_at: datetime | None = None) -> Path:
    base_log_dir.mkdir(parents=True, exist_ok=True)
    stem = _run_dir_stem(started_at or datetime.now(tz=timezone.utc))
    for index in range(1, 10_000):
        suffix = "" if index == 1 else f"_{index}"
        candidate = base_log_dir / f"{stem}{suffix}"
        try:
            candidate.mkdir()
        except FileExistsError:
            continue
        return candidate
    raise RuntimeError(f"Could not allocate unique run log directory under {base_log_dir}")


_allocate_run_log_dir = allocate_run_log_dir


def _run_dir_stem(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_et = ts.astimezone(_FILENAME_TZ)
    return ts_et.strftime("%Y%m%d_%H%M_et")


def _safe_filename_part(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")
    cleaned = "".join(ch if ch in allowed else "_" for ch in value)
    return cleaned or "unknown"

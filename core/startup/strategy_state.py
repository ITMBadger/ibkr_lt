"""Durable strategy state store for restart recovery."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from ..audit.serialize import to_jsonable
from ..types import Instrument, Position
from .position_gate import instrument_identity_key

log = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_TRANSIENT_STATE_KEYS = {"_last_decision_trace"}


class StrategyStateStore:
    """Single JSON source of truth for strategy restart state."""

    def __init__(
        self,
        path: str | Path = "runs/state/strategy_state.json",
        *,
        enabled: bool = True,
        run_snapshot_dir: str | Path | None = None,
        run_snapshot: bool = True,
        run_id: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.enabled = bool(enabled)
        self.run_snapshot_dir = Path(run_snapshot_dir) if run_snapshot_dir else None
        self.run_snapshot = bool(run_snapshot)
        self.run_id = run_id
        self._lock = threading.RLock()
        self._last_payload_json: str | None = None

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        audit_logger: Any | None = None,
    ) -> "StrategyStateStore | None":
        cfg = dict(config.get("strategy_state") or {})
        enabled = bool(cfg.get("enabled", True))
        if not enabled:
            return None
        run_snapshot_dir = getattr(audit_logger, "log_dir", None) if audit_logger else None
        run_id = getattr(audit_logger, "run_id", None) if audit_logger else None
        path = cfg.get("path") or _default_state_path(config)
        return cls(
            path,
            enabled=enabled,
            run_snapshot_dir=run_snapshot_dir,
            run_snapshot=bool(cfg.get("run_snapshot", True)),
            run_id=run_id,
        )

    def load_state(self, strategy_id: str) -> dict[str, Any]:
        if not self.enabled:
            return {}
        payload = self._load_payload()
        record = _strategy_record(payload, strategy_id)
        state = record.get("state", {})
        return _hydrate_value(state) if isinstance(state, dict) else {}

    def open_allocations(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        payload = self._load_payload()
        allocations: list[dict[str, Any]] = []
        strategies = payload.get("strategies", {})
        if not isinstance(strategies, dict):
            return []
        for strategy_id, strategy_record in strategies.items():
            if not isinstance(strategy_record, dict):
                continue
            positions = strategy_record.get("positions", {})
            if not isinstance(positions, dict):
                continue
            for record in positions.values():
                if not isinstance(record, dict):
                    continue
                quantity = abs(float(record.get("quantity", 0.0) or 0.0))
                if quantity <= 0:
                    continue
                allocation = {
                    "strategy_id": str(strategy_id),
                    "quantity": quantity,
                    "entry_ts": record.get("entry_ts"),
                    "trade_id": record.get("trade_id"),
                    "source": "strategy_state",
                    "side": record.get("side"),
                    "instrument": record.get("instrument"),
                }
                allocations.append(allocation)
        return allocations

    def save_strategy(
        self,
        strategy_id: str,
        state: Mapping[str, Any],
        positions: Sequence[Position],
        *,
        run_id: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        with self._lock:
            payload = self._load_payload()
            now = datetime.now(tz=timezone.utc).isoformat()
            strategies = payload.setdefault("strategies", {})
            if not isinstance(strategies, dict):
                strategies = {}
                payload["strategies"] = strategies
            strategies[str(strategy_id)] = _strategy_payload_record(
                strategy_id,
                state,
                positions,
                updated_at=now,
                run_id=run_id or self.run_id,
            )
            payload["version"] = _SCHEMA_VERSION
            payload["updated_at"] = now
            self._save_payload(payload)

    def save_all(
        self,
        strategy_entries: Sequence[tuple[Any, dict]],
        positions_by_strategy: Mapping[str, Sequence[Position]],
        *,
        run_id: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        with self._lock:
            payload = self._load_payload()
            now = datetime.now(tz=timezone.utc).isoformat()
            strategies = payload.setdefault("strategies", {})
            if not isinstance(strategies, dict):
                strategies = {}
                payload["strategies"] = strategies
            for kernel, state in strategy_entries:
                strategy_id = kernel.SPEC.id
                strategies[str(strategy_id)] = _strategy_payload_record(
                    strategy_id,
                    state,
                    positions_by_strategy.get(strategy_id, ()),
                    updated_at=now,
                    run_id=run_id or self.run_id,
                )
            payload["version"] = _SCHEMA_VERSION
            payload["updated_at"] = now
            self._save_payload(payload)

    def summary(self) -> dict[str, Any]:
        payload = self._load_payload() if self.enabled else {}
        strategies = payload.get("strategies", {}) if isinstance(payload, dict) else {}
        if not isinstance(strategies, dict):
            strategies = {}
        position_count = 0
        for record in strategies.values():
            if isinstance(record, dict) and isinstance(record.get("positions"), dict):
                position_count += len(record["positions"])
        return {
            "enabled": self.enabled,
            "path": str(self.path),
            "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
            "strategies": len(strategies),
            "positions": position_count,
        }

    def _load_payload(self) -> dict[str, Any]:
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return {"version": _SCHEMA_VERSION, "strategies": {}}
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read strategy state store %s: %s", self.path, exc)
            return {"version": _SCHEMA_VERSION, "strategies": {}}
        if not isinstance(payload, dict):
            return {"version": _SCHEMA_VERSION, "strategies": {}}
        payload.setdefault("version", _SCHEMA_VERSION)
        payload.setdefault("strategies", {})
        return payload

    def _save_payload(self, payload: Mapping[str, Any]) -> None:
        text = json.dumps(
            to_jsonable(dict(payload)),
            ensure_ascii=True,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        if text == self._last_payload_json:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            fh.write(text)
            fh.write("\n")
        tmp_path.replace(self.path)
        self._last_payload_json = text
        if self.run_snapshot and self.run_snapshot_dir is not None:
            self.run_snapshot_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = self.run_snapshot_dir / "strategy_state_snapshot.json"
            with snapshot_path.open("w", encoding="utf-8") as fh:
                fh.write(text)
                fh.write("\n")


def _strategy_record(payload: Mapping[str, Any], strategy_id: str) -> dict[str, Any]:
    strategies = payload.get("strategies", {})
    if not isinstance(strategies, dict):
        return {}
    record = strategies.get(strategy_id)
    return dict(record) if isinstance(record, dict) else {}


def _default_state_path(config: Mapping[str, Any]) -> str:
    mode = str(config.get("mode") or "").strip().lower()
    if mode == "paper":
        return "runs/state/strategy_paper_state.json"
    return "runs/state/strategy_state.json"


def _json_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): to_jsonable(value)
        for key, value in state.items()
        if str(key) not in _TRANSIENT_STATE_KEYS
    }


def _strategy_payload_record(
    strategy_id: str,
    state: Mapping[str, Any],
    positions: Sequence[Position],
    *,
    updated_at: str,
    run_id: str | None,
) -> dict[str, Any]:
    return {
        "updated_at": updated_at,
        "run_id": run_id,
        "state": _json_state(state),
        "positions": {
            _position_record_key(position): _position_record(
                strategy_id,
                position,
                state,
            )
            for position in positions
            if not position.is_flat
        },
    }


def _position_record(
    strategy_id: str,
    position: Position,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    entry_ts = _entry_ts_for_position(position, state)
    return {
        "strategy_id": strategy_id,
        "quantity": position.quantity,
        "side": position.side,
        "avg_cost": position.avg_cost,
        "entry_ts": to_jsonable(entry_ts),
        "trade_id": position.trade_id,
        "instrument": _instrument_record(position.instrument),
    }


def _position_record_key(position: Position) -> str:
    identity = "|".join(str(part) for part in instrument_identity_key(position.instrument) if part)
    return f"{identity}|{position.trade_id or ''}"


def _entry_ts_for_position(position: Position, state: Mapping[str, Any]) -> Any:
    trade_id = position.trade_id
    lot_states = state.get("_lot_exit_state")
    if trade_id and isinstance(lot_states, Mapping):
        lot_state = lot_states.get(trade_id)
        if isinstance(lot_state, Mapping):
            for key in ("entry_signal_ts", "entry_ts"):
                value = lot_state.get(key)
                if value:
                    return value
    for key in ("entry_signal_ts", "entry_ts"):
        value = state.get(key)
        if value:
            return value
    return None


def _instrument_record(instrument: Instrument) -> dict[str, Any]:
    return {
        "asset_class": instrument.asset_class,
        "symbol": instrument.symbol,
        "exchange": instrument.exchange,
        "currency": instrument.currency,
        "expiry": instrument.expiry.isoformat() if instrument.expiry else None,
        "strike": instrument.strike,
        "right": instrument.right,
        "multiplier": instrument.multiplier,
    }


def _hydrate_value(value: Any, *, key: str = "") -> Any:
    if isinstance(value, dict):
        return {str(k): _hydrate_value(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_hydrate_value(item) for item in value]
    if not isinstance(value, str):
        return value
    if key.endswith("_date"):
        parsed_date = _parse_date(value)
        if parsed_date is not None:
            return parsed_date
    if key.endswith("_ts") or key in {"timestamp", "updated_at"}:
        parsed_ts = _parse_datetime(value)
        if parsed_ts is not None:
            return parsed_ts
    return value


def _parse_datetime(value: str) -> datetime | None:
    try:
        ts = datetime.fromisoformat(value)
    except ValueError:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None

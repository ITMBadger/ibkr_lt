"""Persistent strategy ownership ledger for live restarts."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

from ..types import Fill, Instrument, Position, PositionAdoption
from .position_gate import instrument_identity_key, position_id

log = logging.getLogger(__name__)


class PositionOwnershipLedger:
    """Small JSON ledger mapping live positions back to strategy ownership.

    The broker remains the source of truth for open positions. This ledger only
    answers: "when a matching broker position exists at startup, which strategy
    owned it before the process restarted?"
    """

    def __init__(self, path: str | Path = "runs/state/position_ownership.json") -> None:
        self.path = Path(path)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "PositionOwnershipLedger":
        startup_cfg = dict(config.get("startup_position_gate") or {})
        path = (
            config.get("ownership_ledger_path")
            or startup_cfg.get("ownership_ledger_path")
            or "runs/state/position_ownership.json"
        )
        return cls(path)

    def open_allocations(self) -> list[dict[str, Any]]:
        records = self._load_records()
        allocations: list[dict[str, Any]] = []
        for record in records.values():
            quantity = abs(float(record.get("quantity", 0.0)))
            if quantity <= 0:
                continue
            allocation = {
                "strategy_id": record["strategy_id"],
                "quantity": quantity,
                "entry_ts": record.get("entry_ts"),
                "trade_id": record.get("trade_id"),
                "source": "ownership_ledger",
                "side": record.get("side"),
                "instrument": record.get("instrument"),
            }
            allocations.append(allocation)
        return allocations

    def record_adoption(
        self,
        position: Position,
        adoption: PositionAdoption,
        *,
        source: str = "operator",
    ) -> None:
        signed_quantity = adoption.quantity if position.quantity > 0 else -adoption.quantity
        record = {
            "strategy_id": adoption.strategy_id,
            "quantity": signed_quantity,
            "side": "long" if signed_quantity > 0 else "short",
            "avg_cost": position.avg_cost,
            "entry_ts": adoption.entry_ts.isoformat() if adoption.entry_ts else None,
            "trade_id": adoption.trade_id,
            "source": source,
            "source_position_id": adoption.source_position_id or position_id(position),
            "instrument": _instrument_record(position.instrument),
        }
        self._upsert_record(record)

    def apply_fill(
        self,
        fill: Fill,
        *,
        strategy_id: str | None,
        role: str = "unknown",
        trade_id: str | None = None,
    ) -> None:
        if not strategy_id:
            return
        signed_quantity = _signed_fill_quantity(fill)
        records = self._load_records()
        key = _record_key(strategy_id, fill.instrument, trade_id)
        current = dict(records.get(key) or {})
        current_qty = float(current.get("quantity", 0.0))
        new_qty = current_qty + signed_quantity
        if abs(new_qty) < 1e-9:
            if key in records:
                records.pop(key, None)
                self._save_records(records)
            return

        entry_ts = current.get("entry_ts")
        if not entry_ts and role == "entry":
            entry_ts = fill.timestamp.isoformat()
        avg_cost = current.get("avg_cost", fill.price)
        if current_qty == 0 or (current_qty > 0) == (signed_quantity > 0):
            gross_qty = abs(current_qty) + abs(signed_quantity)
            if gross_qty > 0:
                avg_cost = (
                    abs(current_qty) * float(avg_cost)
                    + abs(signed_quantity) * float(fill.price)
                ) / gross_qty

        records[key] = {
            "strategy_id": strategy_id,
            "quantity": new_qty,
            "side": "long" if new_qty > 0 else "short",
            "avg_cost": avg_cost,
            "entry_ts": entry_ts,
            "trade_id": trade_id,
            "source": "fill",
            "instrument": _instrument_record(fill.instrument),
        }
        self._save_records(records)

    def _upsert_record(self, record: Mapping[str, Any]) -> None:
        strategy_id = str(record.get("strategy_id") or "")
        instrument = _instrument_from_record(record.get("instrument") or {})
        trade_id = str(record.get("trade_id") or "") or None
        if not strategy_id or instrument is None:
            return
        records = self._load_records()
        records[_record_key(strategy_id, instrument, trade_id)] = dict(record)
        self._save_records(records)

    def _load_records(self) -> dict[str, dict[str, Any]]:
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read position ownership ledger %s: %s", self.path, exc)
            return {}
        positions = payload.get("positions", {}) if isinstance(payload, dict) else {}
        if not isinstance(positions, dict):
            return {}
        return {
            str(key): dict(value)
            for key, value in positions.items()
            if isinstance(value, dict)
        }

    def _save_records(self, records: Mapping[str, Mapping[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "positions": {str(key): dict(value) for key, value in records.items()},
        }
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True, allow_nan=False, indent=2, sort_keys=True)
            fh.write("\n")
        tmp_path.replace(self.path)


def _record_key(strategy_id: str, instrument: Instrument, trade_id: str | None) -> str:
    identity = "|".join(str(part) for part in instrument_identity_key(instrument) if part)
    return f"{strategy_id}|{identity}|{trade_id or ''}"


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


def _instrument_from_record(value: Any) -> Instrument | None:
    if not isinstance(value, Mapping):
        return None
    symbol = value.get("symbol")
    if not symbol:
        return None
    from .position_gate import allocation_instrument

    return allocation_instrument({"instrument": value})


def _signed_fill_quantity(fill: Fill) -> float:
    if fill.side.upper() in {"BOT", "BUY", "B", "LONG"}:
        return float(fill.quantity)
    return -float(fill.quantity)

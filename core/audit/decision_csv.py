"""Flatten decision events into one-row CSV records."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .serialize import to_jsonable

_TOKEN_PATTERN = re.compile(r"[^a-zA-Z0-9]+")
_CSV_TZ = ZoneInfo("America/New_York")


def flatten_decision_event(event: dict[str, Any]) -> dict[str, Any]:
    """Convert one decision event payload into a flat CSV row.

    Output is one row per evaluation, ordered for spreadsheet scanning:
    evaluation datetime, OHLCV bars, indicator values, metrics, conditions.
    """
    row: dict[str, Any] = {}

    _set(row, "eval_datetime_et", _format_datetime_et(event.get("timestamp")))
    _set(row, "strategy_id", event.get("strategy_id"))
    _set(row, "phase", event.get("phase"))
    _set(row, "decision", event.get("decision"))
    _set(row, "reason", event.get("reason"))
    if event.get("exit_reason"):
        _set(row, "exit_reason", event.get("exit_reason"))

    signal = event.get("signal")
    if isinstance(signal, dict):
        instrument = signal.get("instrument")
        if isinstance(instrument, dict):
            _set(row, "signal_symbol", instrument.get("symbol"))
            _set(row, "signal_asset_class", instrument.get("asset_class"))
        _set(row, "signal_side", signal.get("side"))
    elif signal:
        _set(row, "signal", signal)

    bars = event.get("bars")
    if isinstance(bars, dict):
        for label, bar in bars.items():
            if not isinstance(bar, dict):
                _set(row, f"bar_{_token(label)}", bar)
                continue
            prefix = _token(label)
            ohlcv = bar.get("ohlcv")
            if isinstance(ohlcv, dict):
                _set(row, f"{prefix}_datetime_et", _format_datetime_et(ohlcv.get("timestamp")))
                for key in ("open", "high", "low", "close", "volume"):
                    if key in ohlcv:
                        _set(row, f"{prefix}_{key}", ohlcv.get(key))
            else:
                _set(row, f"{prefix}_ohlcv", ohlcv)

    indicators = event.get("indicators")
    if isinstance(indicators, dict):
        for label, item in indicators.items():
            prefix = _token(label)
            if isinstance(item, dict):
                _set(row, prefix, item.get("value"))
            else:
                _set(row, prefix, item)

    metrics = event.get("metrics")
    if isinstance(metrics, dict):
        for key in metrics:
            _set(row, f"metric_{_token(key)}", metrics.get(key))

    conditions = event.get("conditions")
    if isinstance(conditions, list):
        seen_names: dict[str, int] = {}
        for idx, condition in enumerate(conditions):
            if not isinstance(condition, dict):
                _set(row, f"condition_{idx}", condition)
                continue
            raw_name = str(condition.get("name") or f"idx_{idx}")
            norm = _token(raw_name)
            count = seen_names.get(norm, 0) + 1
            seen_names[norm] = count
            suffix = "" if count == 1 else f"_{count}"
            _set(row, f"condition_{norm}{suffix}", condition.get("passed"))

    return row


def _set(row: dict[str, Any], key: str, value: Any) -> None:
    row[key] = _csv_cell(value)


def _csv_cell(value: Any) -> Any:
    if _looks_like_datetime(value):
        return _format_datetime_et(value)
    serializable = to_jsonable(value)
    if _looks_like_datetime(serializable):
        return _format_datetime_et(serializable)
    if isinstance(serializable, float):
        return round(serializable, 4)
    if isinstance(serializable, dict | list):
        return json.dumps(serializable, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return serializable


def _format_datetime_et(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return value
    elif isinstance(value, date):
        return value.isoformat()
    else:
        return value
    if dt.tzinfo is None:
        return dt.isoformat()
    return dt.astimezone(_CSV_TZ).isoformat()


def _looks_like_datetime(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str) or "T" not in value:
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _token(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return "na"
    cleaned = _TOKEN_PATTERN.sub("_", text).strip("_").lower()
    return cleaned or "na"

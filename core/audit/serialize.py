"""JSON-safe serializers for audit events."""

from __future__ import annotations

import dataclasses
import math
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from ..types import Instrument


def to_jsonable(value: Any) -> Any:
    """Convert common trading/runtime values to strict JSON-safe objects."""
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.generic):
        return to_jsonable(value.item())
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Instrument):
        return {
            "asset_class": value.asset_class,
            "symbol": value.symbol,
            "exchange": value.exchange,
            "currency": value.currency,
            "expiry": to_jsonable(value.expiry),
            "strike": value.strike,
            "right": value.right,
            "multiplier": value.multiplier,
        }
    if dataclasses.is_dataclass(value):
        return {
            key: to_jsonable(val)
            for key, val in dataclasses.asdict(value).items()
        }
    if isinstance(value, pd.Series):
        return series_to_record(value)
    if isinstance(value, pd.DataFrame):
        return [series_to_record(row) for _, row in value.iterrows()]
    if isinstance(value, dict):
        return {
            str(to_jsonable(key)): to_jsonable(val)
            for key, val in value.items()
        }
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(item) for item in value]
    return str(value)


def series_to_record(row: pd.Series) -> dict[str, Any]:
    """Serialize a pandas row, preserving its index timestamp when present."""
    data = {str(key): to_jsonable(val) for key, val in row.to_dict().items()}
    name = getattr(row, "name", None)
    if name is not None:
        data.setdefault("timestamp", to_jsonable(name))
    return data

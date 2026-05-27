"""Strategy decision trace builder.

Strategies own decision detail because each strategy has different bars,
conditions, thresholds, and score components. Core owns serialization and
writing.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime
from typing import Any

import pandas as pd

from ..types import Instrument, MarketContext, Position, Signal
from .serialize import series_to_record, to_jsonable

_STATE_KEY = "_last_decision_trace"


class DecisionTrace:
    """Structured condition log assembled by a strategy evaluation."""

    def __init__(
        self,
        *,
        phase: str,
        strategy_id: str,
        timestamp: datetime,
    ) -> None:
        self._event: dict[str, Any] = {
            "phase": phase,
            "strategy_id": strategy_id,
            "timestamp": timestamp,
            "bars": {},
            "tables": {},
            "indicators": {},
            "conditions": [],
            "metrics": {},
            "operator_summary": {},
            "decision": None,
            "reason": None,
        }

    @classmethod
    def entry(cls, ctx: MarketContext, strategy_id: str) -> "DecisionTrace":
        return cls(phase="entry", strategy_id=strategy_id, timestamp=ctx.timestamp)

    @classmethod
    def exit(
        cls,
        ctx: MarketContext,
        strategy_id: str,
        position: Position | None = None,
    ) -> "DecisionTrace":
        trace = cls(phase="exit", strategy_id=strategy_id, timestamp=ctx.timestamp)
        if position is not None:
            trace._event["position"] = to_jsonable(position)
        return trace

    def add_bar(
        self,
        label: str,
        instrument: Instrument,
        timeframe: str,
        row: pd.Series | dict[str, Any] | None,
    ) -> None:
        if row is None:
            data = None
        elif isinstance(row, pd.Series):
            data = series_to_record(row)
        else:
            data = to_jsonable(row)
        self._event["bars"][label] = {
            "instrument": to_jsonable(instrument),
            "timeframe": timeframe,
            "ohlcv": data,
        }

    def add_table(
        self,
        label: str,
        instrument: Instrument,
        timeframe: str,
        frame: pd.DataFrame | list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> None:
        if isinstance(frame, pd.DataFrame):
            rows = [series_to_record(row) for _, row in frame.iterrows()]
        else:
            rows = [to_jsonable(row) for row in frame]
        self._event["tables"][label] = {
            "instrument": to_jsonable(instrument),
            "timeframe": timeframe,
            "rows": rows,
        }

    def add_indicator(
        self,
        label: str,
        value: Any,
        *,
        instrument: Instrument | None = None,
        timeframe: str | None = None,
    ) -> None:
        item = {"value": to_jsonable(value)}
        if instrument is not None:
            item["instrument"] = to_jsonable(instrument)
        if timeframe is not None:
            item["timeframe"] = timeframe
        self._event["indicators"][label] = item

    def add_condition(
        self,
        name: str,
        passed: bool,
        *,
        lhs: Any = None,
        op: str | None = None,
        rhs: Any = None,
        value: Any = None,
        threshold: Any = None,
        refs: list[str] | None = None,
    ) -> None:
        self._event["conditions"].append({
            "name": name,
            "passed": bool(passed),
            "lhs": to_jsonable(lhs),
            "op": op,
            "rhs": to_jsonable(rhs),
            "value": to_jsonable(value),
            "threshold": to_jsonable(threshold),
            "refs": refs or [],
        })

    def add_metric(self, label: str, value: Any) -> None:
        self._event["metrics"][label] = to_jsonable(value)

    def set_entry_readiness(
        self,
        checks: Iterable[bool] | None = None,
        *,
        pct: float | None = None,
        label: str | None = None,
    ) -> None:
        """Attach sanitized operator-facing entry readiness.

        This intentionally stores only a percent and generic label. Raw
        condition names and values stay in the audit trace only.
        """
        if checks is not None:
            items = [bool(item) for item in checks]
            pct = (sum(items) / len(items)) * 100.0 if items else None
        summary = self._event.setdefault("operator_summary", {})
        if pct is None:
            summary["entry_readiness_pct"] = None
        else:
            try:
                value = float(pct)
            except (TypeError, ValueError):
                value = math.nan
            summary["entry_readiness_pct"] = (
                round(max(0.0, min(100.0, value)), 2)
                if math.isfinite(value)
                else None
            )
        if label is not None:
            summary["entry_readiness_label"] = str(label)

    def set_trigger_times(self, times: Iterable[Any]) -> None:
        """Attach sanitized operator-facing trigger action timestamps."""
        summary = self._event.setdefault("operator_summary", {})
        summary["trigger_times"] = to_jsonable(list(times))

    def set_decision(
        self,
        decision: str,
        *,
        reason: str | None = None,
        signal: Signal | None = None,
        exit_reason: str | None = None,
    ) -> None:
        if self._event.get("phase") == "entry":
            if signal is not None or decision == "signal":
                self.set_entry_readiness(pct=100.0, label="Signal ready")
            elif "entry_readiness_pct" not in self._event.get("operator_summary", {}):
                self.set_entry_readiness(pct=None, label="Waiting")
        self._event["decision"] = decision
        self._event["reason"] = reason
        if signal is not None:
            self._event["signal"] = to_jsonable(signal)
        if exit_reason is not None:
            self._event["exit_reason"] = exit_reason

    def to_event(self) -> dict[str, Any]:
        return to_jsonable(self._event)

    def first_failed_condition(self, default: str = "conditions_failed") -> str:
        for condition in self._event["conditions"]:
            if not condition.get("passed", False):
                return str(condition.get("name") or default)
        return default


def record_decision(state: dict, trace: DecisionTrace) -> None:
    state[_STATE_KEY] = trace


def pop_decision(state: dict) -> DecisionTrace | None:
    trace = state.pop(_STATE_KEY, None)
    return trace if isinstance(trace, DecisionTrace) else None

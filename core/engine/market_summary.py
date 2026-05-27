"""Compact market summaries for operator-facing strategy tiles."""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from ..types import Instrument

DEFAULT_MARKET_SUMMARY_POINTS = 120


def build_market_summary(
    instrument: Instrument,
    bars_1m: pd.DataFrame,
    *,
    session_tz: str = "America/New_York",
    max_points: int = DEFAULT_MARKET_SUMMARY_POINTS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a JSON-safe two-session close-price summary.

    The input is the existing DataManager 1m store. This helper does not fetch
    market data; it only reduces already-ingested bars to a small UI payload.
    """
    base = _base_summary(instrument, max_points=max_points)
    frame = _normalized_close_frame(bars_1m)
    frame = _drop_future_rows(frame, now=now)
    if frame.empty:
        base["status"] = "waiting_for_market_data"
        return base

    window, selected_sessions, previous_close = _summary_window(
        instrument,
        frame,
        session_tz,
    )
    if window.empty:
        base["status"] = "waiting_for_market_data"
        return base

    latest_close = _json_float(window["close"].iloc[-1])
    change_abs = None
    change_pct = None
    if previous_close is not None and previous_close != 0 and latest_close is not None:
        change_abs = latest_close - previous_close
        change_pct = (change_abs / previous_close) * 100.0

    return {
        **base,
        "status": "ok" if previous_close is not None else "previous_close_unavailable",
        "latest_close": latest_close,
        "previous_close": previous_close,
        "change_abs": _json_float(change_abs),
        "change_pct": _json_float(change_pct),
        "latest_timestamp": _timestamp_iso(window.index[-1]),
        "window_sessions": [item.isoformat() for item in selected_sessions],
        "raw_points": int(len(window)),
        "points": _close_points(window, max_points=max_points),
    }


def unavailable_market_summary(
    instrument: Instrument,
    *,
    status: str = "data_manager_unavailable",
    max_points: int = DEFAULT_MARKET_SUMMARY_POINTS,
) -> dict[str, Any]:
    payload = _base_summary(instrument, max_points=max_points)
    payload["status"] = status
    return payload


def _base_summary(instrument: Instrument, *, max_points: int) -> dict[str, Any]:
    return {
        "status": "not_loaded",
        "instrument": _instrument_payload(instrument),
        "symbol": instrument.symbol,
        "asset_class": instrument.asset_class,
        "source_timeframe": "1m",
        "max_points": int(max(2, max_points)),
        "latest_close": None,
        "previous_close": None,
        "change_abs": None,
        "change_pct": None,
        "latest_timestamp": None,
        "window_sessions": [],
        "raw_points": 0,
        "points": [],
    }


def _instrument_payload(instrument: Instrument) -> dict[str, Any]:
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


def _normalized_close_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "close" not in frame.columns:
        return pd.DataFrame(
            columns=["close"],
            index=pd.DatetimeIndex([], tz="UTC", name="timestamp"),
        )
    result = frame[["close"]].copy().sort_index()
    if not isinstance(result.index, pd.DatetimeIndex):
        return pd.DataFrame(
            columns=["close"],
            index=pd.DatetimeIndex([], tz="UTC", name="timestamp"),
        )
    if result.index.tz is None:
        result.index = result.index.tz_localize(timezone.utc)
    else:
        result.index = result.index.tz_convert(timezone.utc)
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna(subset=["close"])
    return result


def _drop_future_rows(
    frame: pd.DataFrame,
    *,
    now: datetime | None,
    tolerance: timedelta = timedelta(minutes=2),
) -> pd.DataFrame:
    if frame.empty:
        return frame
    current = now or datetime.now(tz=timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    cutoff = pd.Timestamp(current.astimezone(timezone.utc) + tolerance)
    return frame.loc[frame.index <= cutoff]


def _summary_window(
    instrument: Instrument,
    frame: pd.DataFrame,
    session_tz: str,
) -> tuple[pd.DataFrame, list[date], float | None]:
    if instrument.asset_class == "equity":
        return _equity_summary_window(frame, session_tz)
    local_dates = _local_dates(frame.index, session_tz)
    session_dates = _unique_ordered_dates(local_dates)
    selected_sessions = session_dates[-2:]
    selected_set = set(selected_sessions)
    window = frame.loc[[item in selected_set for item in local_dates]]
    return (
        window,
        selected_sessions,
        _previous_session_close(window, local_dates, selected_sessions),
    )


def _equity_summary_window(
    frame: pd.DataFrame,
    session_tz: str,
) -> tuple[pd.DataFrame, list[date], float | None]:
    local_dates = _local_dates(frame.index, session_tz)
    rth_mask = _equity_rth_mask(frame.index, session_tz)
    rth_dates = _unique_ordered_dates([
        item for item, is_rth in zip(local_dates, rth_mask, strict=False) if is_rth
    ])
    current_date = local_dates[-1]
    previous_candidates = [item for item in rth_dates if item < current_date]
    previous_date = previous_candidates[-1] if previous_candidates else None
    if previous_date is None and current_date not in rth_dates and rth_dates:
        previous_date = rth_dates[-1]

    if previous_date is None:
        window = frame.loc[[item == current_date for item in local_dates]]
        return window, [current_date], None

    window_mask = [
        (item == previous_date and is_rth) or item == current_date
        for item, is_rth in zip(local_dates, rth_mask, strict=False)
    ]
    window = frame.loc[window_mask]
    previous_rows = frame.loc[
        [item == previous_date and is_rth for item, is_rth in zip(local_dates, rth_mask, strict=False)]
    ]
    previous_close = (
        _json_float(previous_rows["close"].iloc[-1])
        if not previous_rows.empty
        else None
    )
    return window, [previous_date, current_date], previous_close


def _equity_rth_mask(index: pd.DatetimeIndex, session_tz: str) -> list[bool]:
    try:
        tz = ZoneInfo(str(session_tz))
    except Exception:
        tz = ZoneInfo("America/New_York")
    market_open = time(9, 30)
    market_close = time(16, 0)
    result: list[bool] = []
    for ts in index:
        local = ts.to_pydatetime().astimezone(tz)
        result.append(market_open <= local.time() < market_close)
    return result


def _local_dates(index: pd.DatetimeIndex, session_tz: str) -> list[date]:
    try:
        tz = ZoneInfo(str(session_tz))
    except Exception:
        tz = ZoneInfo("America/New_York")
    return [ts.to_pydatetime().astimezone(tz).date() for ts in index]


def _unique_ordered_dates(items: list[date]) -> list[date]:
    result: list[date] = []
    seen: set[date] = set()
    for item in items:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result


def _previous_session_close(
    window: pd.DataFrame,
    local_dates: list[date],
    selected_sessions: list[date],
) -> float | None:
    if len(selected_sessions) < 2:
        return None
    previous = selected_sessions[-2]
    window_dates = local_dates[-len(window):]
    previous_rows = window.loc[[item == previous for item in window_dates]]
    if previous_rows.empty:
        return None
    return _json_float(previous_rows["close"].iloc[-1])


def _close_points(frame: pd.DataFrame, *, max_points: int) -> list[dict[str, Any]]:
    max_points = int(max(2, max_points))
    if len(frame) > max_points:
        total = len(frame)
        indexes = sorted({
            round(index * (total - 1) / (max_points - 1))
            for index in range(max_points)
        })
        reduced = frame.iloc[indexes]
    else:
        reduced = frame
    return [
        {
            "timestamp": _timestamp_iso(timestamp),
            "close": _json_float(row.close),
        }
        for timestamp, row in reduced.iterrows()
        if _json_float(row.close) is not None
    ]


def _timestamp_iso(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _json_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


__all__ = [
    "DEFAULT_MARKET_SUMMARY_POINTS",
    "build_market_summary",
    "unavailable_market_summary",
]

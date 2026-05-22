"""CSVDataProvider — historical-only data provider backed by 1-min OHLCV CSV files."""

from __future__ import annotations

from datetime import datetime, time
from pathlib import Path

import pandas as pd
import pytz

from ...path_utils import normalize_local_path
from ...types import Bar, Instrument
from ...engine.timeframes import Timeframe, TF_1M

_OHLCV = ["open", "high", "low", "close", "volume"]


class CSVDataProvider:
    """Load historical 1-min bars from a CSV file or per-symbol CSV directory.

    CSV format expected: datetime index (column 0), then open, high, low, close, volume.
    Timestamps are normalised to tz-aware UTC on load.
    """

    def __init__(
        self,
        csv_path: str | Path,
        session_tz: str = "America/New_York",
        *,
        rth_only: bool = True,
        market_open: str = "09:30",
        market_close: str = "16:00",
    ) -> None:
        self._path = normalize_local_path(csv_path)
        self._session_tz = session_tz
        self._rth_only = rth_only
        self._market_open = _parse_hhmm(market_open)
        self._market_close = _parse_hhmm(market_close)

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def fetch(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[Bar]:
        """Return bars in [start, end] range. timeframe must be 1m for raw CSV data."""
        path = _resolve_csv_path(self._path, instrument)
        if path is None or not path.exists():
            return []

        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.columns = [c.lower() for c in df.columns]
        df = df[[c for c in _OHLCV if c in df.columns]]
        if df.empty:
            return []

        df.index = _normalize_index(df.index, self._session_tz)
        df = df.sort_index()
        if self._rth_only:
            df = _filter_rth(df, self._session_tz, self._market_open, self._market_close)

        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        df = df[(df.index >= start_utc) & (df.index <= end_utc)]

        bars: list[Bar] = []
        for ts, row in df.iterrows():
            bars.append(
                Bar(
                    instrument=instrument,
                    timeframe=TF_1M,
                    timestamp=ts.to_pydatetime(),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume", 0.0)),
                    is_closed=True,
                    source="csv",
                )
            )
        return bars


def _normalize_index(index: pd.Index, tz: str) -> pd.DatetimeIndex:
    try:
        dti = pd.DatetimeIndex(index)
    except ValueError as exc:
        if "Mixed timezones" not in str(exc):
            raise
        return pd.DatetimeIndex(pd.to_datetime(index, utc=True))
    if dti.tz is None:
        local_tz = pytz.timezone(tz)
        dti = dti.tz_localize(local_tz, ambiguous="infer", nonexistent="shift_forward")
    return dti.tz_convert("UTC")


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.utc)
    return dt.astimezone(pytz.utc)


def _resolve_csv_path(path: Path, instrument: Instrument) -> Path | None:
    if path.is_file():
        return path
    if not path.is_dir():
        return None

    symbol = instrument.symbol.upper()
    candidates = [
        path / f"{symbol}.csv",
        path / f"{symbol}.csv.gz",
        path / f"BATS_{symbol}, 1.csv",
        path / f"BATS_{symbol}, 1.csv.gz",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    matches = sorted(path.glob(f"*_{symbol}, 1.csv*"))
    if matches:
        return matches[0]
    matches = sorted(path.glob(f"*{symbol}*.csv*"))
    return matches[0] if matches else None


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", maxsplit=1)
    return time(int(hour), int(minute))


def _filter_rth(
    df: pd.DataFrame,
    session_tz: str,
    market_open: time,
    market_close: time,
) -> pd.DataFrame:
    if df.empty:
        return df
    local_index = df.index.tz_convert(pytz.timezone(session_tz))
    times = local_index.time
    mask = [(market_open <= ts < market_close) for ts in times]
    return df[mask]

"""CSVDataProvider — historical-only data provider backed by a 1-min OHLCV CSV file."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from ...types import Bar, Instrument
from ...engine.timeframes import Timeframe, TF_1M

_OHLCV = ["open", "high", "low", "close", "volume"]


class CSVDataProvider:
    """Load historical 1-min bars from a CSV file.

    CSV format expected: datetime index (column 0), then open, high, low, close, volume.
    Timestamps are normalised to tz-aware UTC on load.
    """

    def __init__(self, csv_path: str | Path, session_tz: str = "America/New_York") -> None:
        self._path = Path(csv_path)
        self._session_tz = session_tz

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
        if not self._path.exists():
            return []

        df = pd.read_csv(self._path, index_col=0, parse_dates=True)
        df.columns = [c.lower() for c in df.columns]
        df = df[[c for c in _OHLCV if c in df.columns]]
        if df.empty:
            return []

        df.index = _normalize_index(df.index, self._session_tz)
        df = df.sort_index()

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
    dti = pd.DatetimeIndex(index)
    if dti.tz is None:
        local_tz = pytz.timezone(tz)
        dti = dti.tz_localize(local_tz, ambiguous="infer", nonexistent="shift_forward")
    return dti.tz_convert("UTC")


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.utc)
    return dt.astimezone(pytz.utc)

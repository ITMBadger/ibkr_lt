"""Resampler — downsample a 1-min DataFrame to any coarser timeframe.

Produces only fully-completed bars (drops the current, still-open bar).
Cached by DataManager so the resample only runs when the revision changes.
"""

from __future__ import annotations

import re

import pandas as pd

from ..engine.timeframes import Timeframe


# Map label suffixes to pandas offset aliases
_PANDAS_ALIAS: dict[str, str] = {
    "s": "s",
    "m": "min",
    "h": "h",
    "d": "D",
    "w": "W",
}

_LABEL_RE = re.compile(r"^(\d+)([smhdw])$")


def _to_pandas_offset(tf: Timeframe) -> str:
    m = _LABEL_RE.match(tf.label)
    if not m:
        raise ValueError(f"Cannot convert timeframe label {tf.label!r} to pandas offset")
    qty, unit = m.group(1), m.group(2)
    return f"{qty}{_PANDAS_ALIAS[unit]}"


class Resampler:
    """Stateless resampler. Each call to resample() is independent."""

    def resample(
        self,
        bars_1m: pd.DataFrame,
        target_tf: Timeframe,
        lookback_bars: int = 0,
    ) -> pd.DataFrame:
        """Resample `bars_1m` (1-min OHLCV DataFrame) to `target_tf`.

        Returns only fully-completed bars. The current (potentially incomplete)
        bar is always dropped.

        Args:
            bars_1m: DataFrame with DatetimeIndex (tz-aware) and columns
                     open, high, low, close, volume.
            target_tf: Target bar size.
            lookback_bars: If > 0, return only the last N completed bars.
        """
        if bars_1m.empty:
            return bars_1m.copy()

        offset = _to_pandas_offset(target_tf)
        resampled = (
            bars_1m.resample(offset, label="left", closed="left")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna(subset=["open"])
        )

        # Drop the last (current, potentially incomplete) bar
        if len(resampled) > 0:
            resampled = resampled.iloc[:-1]

        if lookback_bars > 0:
            resampled = resampled.iloc[-lookback_bars:]

        return resampled

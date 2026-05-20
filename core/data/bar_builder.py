"""BarBuilder — aggregate finer-grained bars into coarser completed bars.

Used when the streaming provider emits bars finer than the requested base
timeframe (e.g. IBKR emits 5s bars; we need 1m bars).
Only instantiated by DataManager when native_stream < base_tf.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ..types import Bar, Instrument
from ..engine.timeframes import Timeframe


class BarBuilder:
    """Stateful aggregator: accumulates source bars and emits a target bar on boundary.

    Example: source_tf=5s, target_tf=1m — emits one completed Bar every minute.
    """

    def __init__(self, instrument: Instrument, source_tf: Timeframe, target_tf: Timeframe) -> None:
        if source_tf.seconds >= target_tf.seconds:
            raise ValueError(
                f"source_tf ({source_tf}) must be finer than target_tf ({target_tf})"
            )
        self._instrument = instrument
        self._source_tf = source_tf
        self._target_tf = target_tf

        self._bar_open: datetime | None = None
        self._open = self._high = self._low = self._close = self._volume = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> Bar | None:
        """Feed one source bar. Returns a completed target Bar when the boundary
        rolls over, otherwise None.

        Timestamps are aligned to bar-open of the target timeframe (floor division).
        """
        ts = bar.timestamp
        target_open = self._floor_to_target(ts)

        if self._bar_open is None:
            # First bar ever
            self._start_new(target_open, bar)
            return None

        if target_open == self._bar_open:
            # Same target bar — accumulate
            self._high = max(self._high, bar.high)
            self._low = min(self._low, bar.low)
            self._close = bar.close
            self._volume += bar.volume
            return None

        # Boundary crossed — emit the completed bar
        completed = Bar(
            instrument=self._instrument,
            timeframe=self._target_tf,
            timestamp=self._bar_open,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            is_closed=True,
            source=bar.source,
        )
        self._start_new(target_open, bar)
        return completed

    def flush(self, source: str = "builder") -> Bar | None:
        """Emit the current incomplete bar (e.g. at session end). Returns None if empty."""
        if self._bar_open is None:
            return None
        bar = Bar(
            instrument=self._instrument,
            timeframe=self._target_tf,
            timestamp=self._bar_open,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            is_closed=True,
            source=source,
        )
        self._bar_open = None
        return bar

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _floor_to_target(self, ts: datetime) -> datetime:
        """Floor ts to the nearest target_tf boundary (epoch-relative)."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elapsed = int((ts - epoch).total_seconds())
        floored = (elapsed // self._target_tf.seconds) * self._target_tf.seconds
        return epoch.replace(tzinfo=timezone.utc) + timedelta(seconds=floored)

    def _start_new(self, target_open: datetime, bar: Bar) -> None:
        self._bar_open = target_open
        self._open = bar.open
        self._high = bar.high
        self._low = bar.low
        self._close = bar.close
        self._volume = bar.volume

"""Phase 1 unit tests: Timeframe, QuantityRules, sessions, SimulatedClock."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, date

from core.engine.timeframes import (
    Timeframe, TF_1S, TF_5S, TF_1M, TF_3M, TF_5M, TF_15M, TF_30M, TF_1H, TF_1D,
    coarsest_native_le,
)
from core.engine.sessions import (
    US_EQUITY_RTH,
)
from core.types import QuantityRules
from core.engine.clock import WallClock, SimulatedClock


# ---------------------------------------------------------------------------
# Timeframe
# ---------------------------------------------------------------------------

class TestTimeframeParse:
    def test_seconds(self):
        tf = Timeframe.parse("5s")
        assert tf.seconds == 5
        assert tf.label == "5s"

    def test_minutes(self):
        tf = Timeframe.parse("3m")
        assert tf.seconds == 180
        assert tf.label == "3m"

    def test_hours(self):
        tf = Timeframe.parse("1h")
        assert tf.seconds == 3600

    def test_days(self):
        tf = Timeframe.parse("1d")
        assert tf.seconds == 86400

    def test_invalid(self):
        with pytest.raises(ValueError):
            Timeframe.parse("badval")

    def test_str(self):
        assert str(TF_1M) == "1m"


class TestTimeframeOrdering:
    def test_order_corrects_string_comparison(self):
        # "1h" < "5m" lexicographically — Timeframe ordering is by seconds
        assert TF_1H > TF_5M
        assert TF_1D > TF_1H > TF_30M > TF_15M > TF_5M > TF_3M > TF_1M > TF_5S > TF_1S

    def test_equality(self):
        assert Timeframe.parse("1m") == TF_1M
        assert Timeframe.parse("1m") == Timeframe(60, "1m")


class TestCoarsestNativeLe:
    def test_exact_match(self):
        available = frozenset({TF_5S, TF_1M, TF_5M})
        assert coarsest_native_le(TF_1M, available) == TF_1M

    def test_picks_coarsest_le(self):
        # Requested 30m, available: 5s and 1m → picks 1m (coarsest ≤ 30m)
        available = frozenset({TF_5S, TF_1M})
        assert coarsest_native_le(TF_30M, available) == TF_1M

    def test_no_candidate_raises(self):
        available = frozenset({TF_1H})
        with pytest.raises(ValueError):
            coarsest_native_le(TF_5M, available)


# ---------------------------------------------------------------------------
# QuantityRules rounding
# ---------------------------------------------------------------------------

class TestQuantityRules:
    def test_equity_integer(self):
        rules = QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0)
        assert rules.round(3.7) == 4.0
        assert rules.round(1.2) == 1.0

    def test_future_integer(self):
        rules = QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0)
        assert rules.round(0.8) == 1.0  # clamped to min

    def test_crypto_step(self):
        rules = QuantityRules(min_quantity=0.001, quantity_step=0.001, quantity_precision=3)
        assert rules.round(0.0015) == 0.002
        assert rules.round(0.0004) == 0.001  # clamped to min

    def test_clamp_to_min(self):
        rules = QuantityRules(min_quantity=1.0, quantity_step=1.0, quantity_precision=0)
        assert rules.round(0.1) == 1.0


# ---------------------------------------------------------------------------
# Session boundaries
# ---------------------------------------------------------------------------

class TestSessionBoundary:
    def test_us_equity_rth_in_session(self):
        import pytz
        ny = pytz.timezone("America/New_York")
        ts = ny.localize(datetime(2026, 5, 1, 10, 30))
        assert US_EQUITY_RTH.is_in_session(ts)

    def test_us_equity_rth_before_open(self):
        import pytz
        ny = pytz.timezone("America/New_York")
        ts = ny.localize(datetime(2026, 5, 1, 9, 0))
        assert not US_EQUITY_RTH.is_in_session(ts)

    def test_us_equity_rth_after_close(self):
        import pytz
        ny = pytz.timezone("America/New_York")
        ts = ny.localize(datetime(2026, 5, 1, 16, 5))
        assert not US_EQUITY_RTH.is_in_session(ts)

    def test_session_open_ts_returns_utc(self):
        ts = US_EQUITY_RTH.session_open_ts(date(2026, 5, 1))
        assert ts.tzinfo is not None
        # US/Eastern 09:30 = UTC 13:30 (EDT, UTC-4)
        assert ts.hour == 13
        assert ts.minute == 30


# ---------------------------------------------------------------------------
# Clock
# ---------------------------------------------------------------------------

class TestSimulatedClock:
    def test_starts_at_min(self):
        c = SimulatedClock()
        assert c.now().tzinfo is not None

    def test_advance_to(self):
        c = SimulatedClock()
        ts = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
        c.advance_to(ts)
        assert c.now() == ts

    def test_advance_requires_tz_aware(self):
        c = SimulatedClock()
        with pytest.raises(ValueError):
            c.advance_to(datetime(2026, 5, 1, 10, 0))  # naive

    def test_wall_clock_is_tz_aware(self):
        c = WallClock()
        assert c.now().tzinfo is not None

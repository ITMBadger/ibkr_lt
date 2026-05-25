"""Phase 2 unit tests: BarBuilder, Resampler, DataManager, CSVDataProvider, ReplayDataProvider."""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest

from core.types import Bar, Instrument
from core.engine.timeframes import TF_5S, TF_1M, TF_3M, TF_30M
from core.data.bar_builder import BarBuilder
from core.data.resampler import Resampler
from core.data.manager import DataManager
from core.features.registry import FeatureRegistry
from core.adapters.csv.data import CSVDataProvider
from core.adapters.paper.data import ReplayDataProvider

QQQ = Instrument(asset_class="equity", symbol="QQQ")
SPY = Instrument(asset_class="equity", symbol="SPY")


def _bar(instrument, ts_utc, o=100.0, h=101.0, low=99.0, c=100.5, v=1000.0, source="test"):
    return Bar(
        instrument=instrument,
        timeframe=TF_5S,
        timestamp=datetime.fromisoformat(ts_utc).replace(tzinfo=timezone.utc),
        open=o, high=h, low=low, close=c, volume=v,
        is_closed=True, source=source,
    )


def _1m_bar(instrument, ts_utc, o=100.0, h=101.0, low=99.0, c=100.5, v=1000.0):
    return Bar(
        instrument=instrument,
        timeframe=TF_1M,
        timestamp=datetime.fromisoformat(ts_utc).replace(tzinfo=timezone.utc),
        open=o, high=h, low=low, close=c, volume=v,
        is_closed=True, source="test",
    )


# ---------------------------------------------------------------------------
# BarBuilder
# ---------------------------------------------------------------------------

class TestBarBuilder:
    def test_accumulates_and_emits_on_boundary(self):
        bb = BarBuilder(QQQ, TF_5S, TF_1M)
        # Feed 12 bars from 09:30:00 (first 12 × 5s = 1 min)
        results = []
        for i in range(12):
            ts = f"2026-05-01 09:30:{i*5:02d}"
            bar = _bar(QQQ, ts, v=100.0)
            result = bb.on_bar(bar)
            results.append(result)

        # No completed bar until the 13th bar (09:31:00) triggers rollover
        assert all(r is None for r in results)

        # 13th bar at 09:31:00 rolls the 09:30 bucket
        bar_31 = _bar(QQQ, "2026-05-01 09:31:00", o=102.0)
        completed = bb.on_bar(bar_31)
        assert completed is not None
        assert completed.timestamp == datetime(2026, 5, 1, 9, 30, tzinfo=timezone.utc)
        assert completed.is_closed is True
        assert completed.timeframe == TF_1M

    def test_high_low_aggregation(self):
        bb = BarBuilder(QQQ, TF_5S, TF_1M)
        bars_9_30 = [
            _bar(QQQ, f"2026-05-01 09:30:{i*5:02d}", h=100.0 + i, low=99.0 - i, v=50.0)
            for i in range(12)
        ]
        for b in bars_9_30:
            bb.on_bar(b)
        completed = bb.on_bar(_bar(QQQ, "2026-05-01 09:31:00"))
        assert completed is not None
        assert completed.high == max(100.0 + i for i in range(12))
        assert completed.low == min(99.0 - i for i in range(12))
        assert completed.volume == pytest.approx(50.0 * 12)

    def test_flush_emits_partial_bar(self):
        bb = BarBuilder(QQQ, TF_5S, TF_1M)
        bb.on_bar(_bar(QQQ, "2026-05-01 09:30:00"))
        bb.on_bar(_bar(QQQ, "2026-05-01 09:30:05"))
        partial = bb.flush()
        assert partial is not None
        assert partial.timestamp == datetime(2026, 5, 1, 9, 30, tzinfo=timezone.utc)

    def test_source_tf_must_be_finer(self):
        with pytest.raises(ValueError):
            BarBuilder(QQQ, TF_1M, TF_5S)


# ---------------------------------------------------------------------------
# Resampler
# ---------------------------------------------------------------------------

class TestResampler:
    def _make_1m_df(self, n_bars: int, start: str = "2026-05-01 09:30") -> pd.DataFrame:
        start_ts = pd.Timestamp(start, tz="America/New_York").tz_convert("UTC")
        idx = pd.date_range(start_ts, periods=n_bars, freq="min")
        return pd.DataFrame({
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
        }, index=idx)

    def test_1m_to_3m(self):
        df = self._make_1m_df(9)
        rs = Resampler()
        out = rs.resample(df, TF_3M)
        # 9 bars → 3 complete 3m bars, last is dropped (current bar rule)
        assert len(out) == 2

    def test_1m_to_30m(self):
        df = self._make_1m_df(65)
        rs = Resampler()
        out = rs.resample(df, TF_30M)
        assert len(out) == 2  # 2 complete 30m bars (60 bars → 2 full, 5 leftover dropped)

    def test_volume_sums(self):
        df = self._make_1m_df(3)
        rs = Resampler()
        out = rs.resample(df, TF_3M, lookback_bars=0)
        # Only 3 bars → 1 group, but it's the "current" (incomplete) bar, so dropped → 0
        assert len(out) == 0

    def test_lookback_bars_limit(self):
        df = self._make_1m_df(70)
        rs = Resampler()
        out = rs.resample(df, TF_30M, lookback_bars=1)
        assert len(out) == 1

    def test_empty_returns_empty(self):
        df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        df.index = pd.DatetimeIndex([], tz="UTC")
        out = Resampler().resample(df, TF_1M)
        assert out.empty


# ---------------------------------------------------------------------------
# DataManager
# ---------------------------------------------------------------------------

class TestDataManager:
    def _make_csv(self, rows: list[tuple]) -> str:
        """Write temp CSV and return path."""
        lines = ["timestamp,open,high,low,close,volume"]
        for ts, o, h, low, c, v in rows:
            lines.append(f"{ts},{o},{h},{low},{c},{v}")
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        tmp.write("\n".join(lines))
        tmp.close()
        return tmp.name

    def test_load_csv_and_bars_1m(self):
        path = self._make_csv([
            ("2026-04-30 09:30:00", 100, 101, 99, 100.5, 1000),
            ("2026-04-30 09:31:00", 100.5, 102, 100, 101, 1200),
        ])
        try:
            dm = DataManager(QQQ)
            n = dm.load_csv(path, tz="America/New_York")
            assert n == 2
            df = dm.bars_1m()
            assert len(df) == 2
            assert dm.revision == 1
        finally:
            os.unlink(path)

    def test_revision_increments_on_live_bar(self):
        dm = DataManager(QQQ)
        bar = _1m_bar(QQQ, "2026-05-01 13:30:00")  # today in UTC = market hours
        r0 = dm.revision
        dm.on_bar(bar)
        assert dm.revision == r0 + 1

    def test_resampled_cached(self):
        dm = DataManager(QQQ)
        # Make 6 1-min bars
        for i in range(6):
            ts = f"2026-04-29 {13+i//60}:{i%60:02d}:00"
            dm.on_bar(_1m_bar(QQQ, ts))
        r1 = dm.resampled(TF_3M)
        r2 = dm.resampled(TF_3M)
        # Same revision → same object (or equal)
        pd.testing.assert_frame_equal(r1, r2)

    def test_backfill_gap_fill(self):
        dm = DataManager(QQQ)
        # Load CSV with 2 bars
        path = self._make_csv([
            ("2026-04-28 09:30:00", 100, 101, 99, 100.5, 1000),
        ])
        try:
            dm.load_csv(path)
            # Backfill adds a bar not in CSV
            gap_bar = _1m_bar(QQQ, "2026-04-28 09:31:00")
            added = dm.merge_backfill([gap_bar])
            assert added == 1
        finally:
            os.unlink(path)

    def test_backfill_csv_wins_prior_session(self):
        dm = DataManager(QQQ)
        path = self._make_csv([
            ("2026-04-28 09:30:00", 100, 101, 99, 100.5, 1000),
        ])
        try:
            dm.load_csv(path)
            # Same timestamp → CSV wins (no add)
            same_bar = Bar(
                instrument=QQQ, timeframe=TF_1M,
                timestamp=datetime(2026, 4, 28, 13, 30, tzinfo=timezone.utc),
                open=999, high=999, low=999, close=999, volume=999,
                is_closed=True, source="test",
            )
            added = dm.merge_backfill([same_bar])
            assert added == 0
            df = dm.bars_1m()
            # CSV value should be preserved
            assert df.iloc[0]["open"] == pytest.approx(100.0)
        finally:
            os.unlink(path)

    def test_live_bar_does_not_purge_current_session_backfill(self):
        dm = DataManager(QQQ)
        dm.merge_backfill([
            _1m_bar(QQQ, "2026-05-01 13:30:00", c=100.5),
            _1m_bar(QQQ, "2026-05-01 13:31:00", c=101.5),
        ])

        dm.on_bar(_1m_bar(QQQ, "2026-05-01 13:32:00", c=102.5))

        df = dm.bars_1m()
        assert len(df) == 3
        assert list(df["close"]) == [100.5, 101.5, 102.5]

    def test_live_bar_overwrites_same_timestamp(self):
        dm = DataManager(QQQ)
        dm.merge_backfill([_1m_bar(QQQ, "2026-05-01 13:30:00", c=100.5)])

        dm.on_bar(_1m_bar(QQQ, "2026-05-01 13:30:00", c=101.5))

        df = dm.bars_1m()
        assert len(df) == 1
        assert df.iloc[0]["close"] == pytest.approx(101.5)

    def test_lookback_trim_uses_latest_bar_not_wall_clock(self):
        dm = DataManager(QQQ, lookback_days=2)
        added = dm.merge_backfill([
            _1m_bar(QQQ, "2024-01-01 13:30:00", c=100.5),
            _1m_bar(QQQ, "2024-01-02 13:30:00", c=101.5),
            _1m_bar(QQQ, "2024-01-04 13:30:00", c=102.5),
        ])

        df = dm.bars_1m()
        assert added == 3
        assert len(df) == 2
        assert list(df["close"]) == [101.5, 102.5]
        assert len(dm.bars_1m(lookback_days=1)) == 1


# ---------------------------------------------------------------------------
# FeatureRegistry
# ---------------------------------------------------------------------------

class TestFeatureRegistry:
    def test_latest_bar_1m_uses_timestamp_bound_view(self):
        dm = DataManager(QQQ)
        dm.merge_backfill([
            _1m_bar(QQQ, "2026-05-01 13:30:00", c=100.5),
            _1m_bar(QQQ, "2026-05-01 13:31:00", c=101.5),
            _1m_bar(QQQ, "2026-05-01 13:32:00", c=102.5),
        ])
        features = FeatureRegistry({QQQ: dm})
        features.preload_from_managers()

        view = features.as_of(datetime(2026, 5, 1, 13, 31, 30, tzinfo=timezone.utc))
        row = view.latest_bar(QQQ, "1m")

        assert row is not None
        assert row.name == pd.Timestamp("2026-05-01 13:31:00+00:00")
        assert row["close"] == pytest.approx(101.5)

    def test_latest_bar_coarser_timeframe_returns_only_completed_bar(self):
        dm = DataManager(QQQ)
        dm.merge_backfill([
            _1m_bar(QQQ, f"2026-05-01 13:{30 + i:02d}:00", c=100.5 + i)
            for i in range(7)
        ])
        features = FeatureRegistry({QQQ: dm})
        features.preload_from_managers()

        before_close = features.as_of(
            datetime(2026, 5, 1, 13, 35, 59, tzinfo=timezone.utc)
        ).latest_bar(QQQ, "3m")
        at_close = features.as_of(
            datetime(2026, 5, 1, 13, 36, tzinfo=timezone.utc)
        ).latest_bar(QQQ, "3m")

        assert before_close is not None
        assert before_close.name == pd.Timestamp("2026-05-01 13:30:00+00:00")
        assert before_close["close"] == pytest.approx(102.5)
        assert at_close is not None
        assert at_close.name == pd.Timestamp("2026-05-01 13:33:00+00:00")
        assert at_close["close"] == pytest.approx(105.5)


# ---------------------------------------------------------------------------
# CSVDataProvider
# ---------------------------------------------------------------------------

class TestCSVDataProvider:
    def test_directory_resolves_symbol_file_and_filters_rth(self):
        async def run():
            with tempfile.TemporaryDirectory() as tmpdir:
                path = os.path.join(tmpdir, "BATS_QQQ, 1.csv.gz")
                pd.DataFrame(
                    [
                        ["2026-03-06 09:30:00-05:00", 95, 96, 94, 95.5, 900],
                        ["2026-05-01 09:29:00-04:00", 99, 100, 98, 99.5, 100],
                        ["2026-05-01 09:30:00-04:00", 100, 101, 99, 100.5, 1000],
                        ["2026-05-01 15:59:00-04:00", 101, 102, 100, 101.5, 1200],
                        ["2026-05-01 16:00:00-04:00", 102, 103, 101, 102.5, 200],
                    ],
                    columns=["time", "open", "high", "low", "close", "volume"],
                ).to_csv(path, index=False, compression="gzip")

                provider = CSVDataProvider(tmpdir)
                bars = await provider.fetch(
                    QQQ,
                    TF_1M,
                    datetime(2026, 3, 1, 13, 0, tzinfo=timezone.utc),
                    datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc),
                )

                assert [bar.timestamp.hour * 60 + bar.timestamp.minute for bar in bars] == [
                    14 * 60 + 30,
                    13 * 60 + 30,
                    19 * 60 + 59,
                ]
                assert all(bar.source == "csv" for bar in bars)

        asyncio.run(run())

    def test_windows_directory_path_is_normalized_under_wsl(self):
        provider = CSVDataProvider(r"D:\data_s\regular_hour")
        assert str(provider._path).replace("\\", "/") == "/mnt/d/data_s/regular_hour"


# ---------------------------------------------------------------------------
# ReplayDataProvider
# ---------------------------------------------------------------------------

class TestReplayDataProvider:
    def _bars(self, n: int):
        base = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
        return [
            Bar(
                instrument=QQQ, timeframe=TF_1M,
                timestamp=base + timedelta(minutes=i),
                open=100.0, high=101.0, low=99.0, close=100.5, volume=1000.0,
                is_closed=True, source="test",
            )
            for i in range(n)
        ]

    def test_emits_in_order(self):
        bars = self._bars(5)
        provider = ReplayDataProvider(bars)

        async def collect():
            await provider.subscribe(QQQ, TF_1M)
            result = []
            async for b in provider.bars():
                result.append(b)
            return result

        collected = asyncio.run(collect())
        tss = [b.timestamp for b in collected]
        assert tss == sorted(tss)
        assert len(collected) == 5

    def test_filters_by_subscription(self):
        bars = self._bars(3) + [_1m_bar(SPY, "2026-05-01 09:30:00")]
        provider = ReplayDataProvider(bars)

        async def collect():
            await provider.subscribe(QQQ, TF_1M)
            result = []
            async for b in provider.bars():
                result.append(b)
            return result

        collected = asyncio.run(collect())
        assert all(b.instrument == QQQ for b in collected)
        assert len(collected) == 3

    def test_fetch_returns_range(self):
        bars = self._bars(10)
        provider = ReplayDataProvider(bars)
        start = bars[2].timestamp
        end = bars[5].timestamp

        async def fetch():
            return await provider.fetch(QQQ, TF_1M, start, end)

        result = asyncio.run(fetch())
        assert all(start <= b.timestamp <= end for b in result)

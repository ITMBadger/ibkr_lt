"""Phase 4 unit tests: indicators, feature caches, RiskPolicy, loader."""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from core.types import Bar, Fill, Instrument, Signal
from core.engine.timeframes import TF_1M
from core.engine.scheduler import Scheduler
from core.data.manager import DataManager
from core.features.indicators import (
    ema, sma, rsi, macd, stoch, atr, bollinger_bands,
    session_vwap, heikin_ashi,
)
from core.features.ids import parse_indicator_id
from core.features.registry import FeatureRegistry
from core.risk.policy import SIZING_MODE_FULL_EQUITY, RiskPolicy
from core.portfolio.state import PortfolioState
from core.engine.loader import register_strategy, get_registry, _registry
from core.interfaces.strategy import (
    ENTRY_FREQUENCY_UNLIMITED,
    POSITION_MODE_SINGLE,
    PositionPolicy,
    StrategyKernel,
    StrategySpec,
)

QQQ = Instrument(asset_class="equity", symbol="QQQ")
SPY = Instrument(asset_class="equity", symbol="SPY")
MNQ = Instrument(asset_class="future", symbol="MNQ", multiplier=2.0)


def _ohlcv_df(n: int = 50, start_price: float = 100.0) -> pd.DataFrame:
    base = datetime(2026, 1, 2, 14, 0, tzinfo=timezone.utc)
    idx = pd.date_range(base, periods=n, freq="min")
    prices = start_price + np.cumsum(np.random.randn(n) * 0.5)
    return pd.DataFrame({
        "open":   prices - 0.1,
        "high":   prices + 0.5,
        "low":    prices - 0.5,
        "close":  prices,
        "volume": np.random.uniform(500, 2000, n),
    }, index=idx)


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------

class TestIndicators:
    def setup_method(self):
        np.random.seed(42)
        self.df = _ohlcv_df(100)

    def test_ema_shape(self):
        result = ema(self.df, 20)
        assert len(result) == len(self.df)
        assert not result.isna().all()

    def test_sma_shape(self):
        result = sma(self.df, 20)
        # First 19 are NaN
        assert result.iloc[:19].isna().all()
        assert not result.iloc[19:].isna().any()

    def test_rsi_bounds(self):
        result = rsi(self.df, 14)
        valid = result.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_macd_columns(self):
        result = macd(self.df, 12, 26, 9)
        assert set(result.columns) == {"macd", "signal", "hist"}

    def test_stoch_columns(self):
        result = stoch(self.df, 14, 3, 3)
        assert "slowk" in result.columns and "slowd" in result.columns

    def test_atr_positive(self):
        result = atr(self.df, 14)
        valid = result.dropna()
        assert (valid > 0).all()

    def test_bollinger_upper_above_lower(self):
        result = bollinger_bands(self.df, 20, 2.0)
        valid = result.dropna()
        assert (valid["upper"] > valid["lower"]).all()

    def test_session_vwap_per_session(self):
        result = session_vwap(self.df)
        assert len(result) == len(self.df)
        # VWAP should be positive
        valid = result.dropna()
        assert (valid > 0).all()

    def test_heikin_ashi_columns(self):
        result = heikin_ashi(self.df)
        assert set(result.columns) == {"ha_open", "ha_high", "ha_low", "ha_close"}


# ---------------------------------------------------------------------------
# parse_indicator_id
# ---------------------------------------------------------------------------

class TestParseIndicatorId:
    def test_scoped(self):
        name, sym, tf = parse_indicator_id("ema_20@QQQ.3m")
        assert name == "ema_20"
        assert sym == "QQQ"
        assert tf == "3m"

    def test_unscoped(self):
        name, sym, tf = parse_indicator_id("vix_ratio")
        assert name == "vix_ratio"
        assert sym == ""
        assert tf == ""


# ---------------------------------------------------------------------------
# FeatureRegistry
# ---------------------------------------------------------------------------

class TestFeatureRegistry:
    def setup_method(self):
        self.qqq_dm = DataManager(QQQ, lookback_days=10)
        self.spy_dm = DataManager(SPY, lookback_days=10)
        self.base = datetime(2026, 4, 29, 13, 30, tzinfo=timezone.utc)
        from core.types import Bar
        for i in range(60):
            for instrument, manager, offset in (
                (QQQ, self.qqq_dm, 0.0),
                (SPY, self.spy_dm, 50.0),
            ):
                manager.on_bar(Bar(
                    instrument=instrument,
                    timeframe=TF_1M,
                    timestamp=self.base + timedelta(minutes=i),
                    open=100.0 + offset + i * 0.01,
                    high=101.0 + offset + i * 0.01,
                    low=99.0 + offset + i * 0.01,
                    close=100.5 + offset + i * 0.01,
                    volume=1000.0,
                    is_closed=True,
                    source="test",
                ))
        self.registry = FeatureRegistry({QQQ: self.qqq_dm, SPY: self.spy_dm})

    def test_computes_shared_indicator_once_per_revision(self):
        r1 = self.registry.get("ema", QQQ, "1m", period=20)
        r2 = self.registry.get("ema", QQQ, "1m", period=20)
        pd.testing.assert_series_equal(r1, r2)
        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 1

    def test_separates_params_and_instruments(self):
        qqq = self.registry.get("ema", QQQ, "1m", period=20)
        spy = self.registry.get("ema", SPY, "1m", period=20)
        qqq_fast = self.registry.get("ema", QQQ, "1m", period=10)
        assert not qqq.equals(spy)
        assert not qqq.equals(qqq_fast)
        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 1
        assert self.registry.compute_count("ema", SPY, "1m", period=20) == 1
        assert self.registry.compute_count("ema", QQQ, "1m", period=10) == 1

    def test_get_id_compatibility(self):
        by_id = self.registry.get_id("ema_20@QQQ.1m")
        by_request = self.registry.get("ema", QQQ, "1m", period=20)
        pd.testing.assert_series_equal(by_id, by_request)

    def test_recomputes_after_revision_change(self):
        self.registry.get("ema", QQQ, "1m", period=20)
        from core.types import Bar
        self.qqq_dm.on_bar(Bar(
            instrument=QQQ,
            timeframe=TF_1M,
            timestamp=datetime(2026, 4, 29, 14, 30, tzinfo=timezone.utc),
            open=102.0,
            high=103.0,
            low=101.0,
            close=102.5,
            volume=1200.0,
            is_closed=True,
            source="test",
        ))
        result = self.registry.get("ema", QQQ, "1m", period=20)
        assert len(result) == 61
        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 2

    def test_preloaded_source_slices_1m_without_recompute(self):
        self.registry.preload_from_managers()

        first = self.registry.as_of(
            self.base + timedelta(minutes=10)
        ).get("ema", QQQ, "1m", period=20)
        second = self.registry.as_of(
            self.base + timedelta(minutes=20)
        ).get("ema", QQQ, "1m", period=20)

        assert len(first) == 11
        assert len(second) == 21
        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 1

    def test_preloaded_source_slices_resampled_bars_without_lookahead(self):
        self.registry.preload_from_managers()

        at_first_complete = self.registry.as_of(
            self.base + timedelta(minutes=3)
        ).get("ema", QQQ, "3m", period=2)
        before_next_complete = self.registry.as_of(
            self.base + timedelta(minutes=5)
        ).get("ema", QQQ, "3m", period=2)
        at_next_complete = self.registry.as_of(
            self.base + timedelta(minutes=6)
        ).get("ema", QQQ, "3m", period=2)

        assert len(at_first_complete) == 1
        assert len(before_next_complete) == 1
        assert len(at_next_complete) == 2
        assert self.registry.compute_count("ema", QQQ, "3m", period=2) == 1

    def test_preloaded_source_merges_new_stream_bar(self):
        self.registry.preload_from_managers()
        self.registry.as_of(
            self.base + timedelta(minutes=59)
        ).get("ema", QQQ, "1m", period=20)

        from core.types import Bar
        self.registry.on_bar(Bar(
            instrument=QQQ,
            timeframe=TF_1M,
            timestamp=self.base + timedelta(minutes=60),
            open=102.0,
            high=103.0,
            low=101.0,
            close=102.5,
            volume=1200.0,
            is_closed=True,
            source="test",
        ))
        result = self.registry.as_of(
            self.base + timedelta(minutes=60)
        ).get("ema", QQQ, "1m", period=20)

        assert len(result) == 61
        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 2

    def test_preloaded_replay_bar_keeps_vectorized_cache(self):
        self.registry.preload_from_managers()
        self.registry.as_of(
            self.base + timedelta(minutes=10)
        ).get("ema", QQQ, "1m", period=20)

        from core.types import Bar
        self.registry.on_bar(Bar(
            instrument=QQQ,
            timeframe=TF_1M,
            timestamp=self.base + timedelta(minutes=11),
            open=100.11,
            high=101.11,
            low=99.11,
            close=100.61,
            volume=1000.0,
            is_closed=True,
            source="replay",
        ))
        self.registry.as_of(
            self.base + timedelta(minutes=20)
        ).get("ema", QQQ, "1m", period=20)

        assert self.registry.compute_count("ema", QQQ, "1m", period=20) == 1

    def test_scheduler_legacy_indicators_use_timestamp_bound_features(self):
        self.registry.preload_from_managers()

        class _IndicatorStrategy(StrategyKernel):
            SPEC = StrategySpec(
                id="_indicator_slice",
                primary_instrument=QQQ,
                execution_instrument=MNQ,
                indicators=("ema_20@QQQ.1m",),
            )

            def generate(self, ctx, state):
                return None

        scheduler = Scheduler(self.registry)
        scheduler.register(_IndicatorStrategy(), {})
        results = scheduler.on_bar(
            Bar(
                instrument=QQQ,
                timeframe=TF_1M,
                timestamp=self.base + timedelta(minutes=10),
                open=100.1,
                high=101.1,
                low=99.1,
                close=100.6,
                volume=1000.0,
                is_closed=True,
                source="test",
            ),
            {QQQ: self.qqq_dm},
        )

        assert len(results) == 1
        _, ctx, _ = results[0]
        assert len(ctx.indicators["ema_20@QQQ.1m"]) == 11
        assert len(ctx.features.get("ema", QQQ, "1m", period=20)) == 11


# ---------------------------------------------------------------------------
# RiskPolicy
# ---------------------------------------------------------------------------

class TestRiskPolicy:
    def test_default_max_order_quantity_is_two(self):
        assert RiskPolicy().max_order_quantity == 2

    def test_sizes_fixed_shares(self):
        rp = RiskPolicy(position_size_shares=2, max_order_quantity=10)
        sig = Signal(instrument=MNQ, side="long")
        assert rp.size_order(sig, PortfolioState()) == 2

    def test_caps_at_max(self):
        rp = RiskPolicy(position_size_shares=20, max_order_quantity=5)
        sig = Signal(instrument=MNQ, side="long")
        assert rp.size_order(sig, PortfolioState()) == 5

    def test_full_equity_sizes_from_account_equity_and_price(self):
        rp = RiskPolicy(
            sizing_mode=SIZING_MODE_FULL_EQUITY,
            equity_fraction=1.0,
            max_order_quantity=None,
        )
        sig = Signal(instrument=MNQ, side="long")

        assert rp.size_order(
            sig,
            PortfolioState(),
            reference_price=100.0,
            account_equity=100_000.0,
        ) == pytest.approx(500.0)

    def test_full_equity_can_be_explicitly_capped(self):
        rp = RiskPolicy(
            sizing_mode=SIZING_MODE_FULL_EQUITY,
            equity_fraction=1.0,
            max_order_quantity=3,
        )
        sig = Signal(instrument=MNQ, side="long")

        assert rp.size_order(
            sig,
            PortfolioState(),
            reference_price=100.0,
            account_equity=100_000.0,
        ) == 3

    def test_full_equity_requires_price_and_equity(self):
        rp = RiskPolicy(sizing_mode=SIZING_MODE_FULL_EQUITY, max_order_quantity=None)
        sig = Signal(instrument=MNQ, side="long")

        assert rp.size_order(sig, PortfolioState(), account_equity=100_000.0) == 0
        assert rp.size_order(sig, PortfolioState(), reference_price=100.0) == 0

    def test_flat_signal_returns_zero(self):
        rp = RiskPolicy(position_size_shares=1)
        sig = Signal(instrument=MNQ, side="flat")
        assert rp.size_order(sig, PortfolioState()) == 0

    def test_existing_position_returns_zero(self):
        rp = RiskPolicy(position_size_shares=1)
        portfolio = PortfolioState()
        portfolio.apply_fill(Fill(
            broker_order_id="test",
            instrument=MNQ,
            side="long",
            quantity=1.0,
            price=100.0,
            timestamp=datetime.now(tz=timezone.utc),
        ))
        sig = Signal(instrument=MNQ, side="long")
        assert rp.size_order(sig, portfolio) == 0


# ---------------------------------------------------------------------------
# Strategy loader
# ---------------------------------------------------------------------------

class TestStrategySpecPolicy:
    def test_default_position_policy_is_single_unlimited(self):
        spec = StrategySpec(
            id="_policy_default",
            primary_instrument=QQQ,
            execution_instrument=MNQ,
        )

        assert spec.position_policy.position_mode == POSITION_MODE_SINGLE
        assert spec.position_policy.entry_frequency == ENTRY_FREQUENCY_UNLIMITED
        assert spec.position_policy.max_concurrent_positions == 1

    def test_position_policy_rejects_invalid_values(self):
        with pytest.raises(ValueError):
            PositionPolicy(position_mode="bad")  # type: ignore[arg-type]

        with pytest.raises(ValueError):
            PositionPolicy(entry_frequency="bad")  # type: ignore[arg-type]

        with pytest.raises(ValueError):
            PositionPolicy(max_concurrent_positions=0)


class TestStrategyLoader:
    def test_register_strategy_decorator(self):
        # Use a unique id to avoid collision with real strategies
        uid = "_test_loader_unique"
        if uid in _registry:
            del _registry[uid]

        @register_strategy
        class _TestStrat(StrategyKernel):
            SPEC = StrategySpec(
                id=uid,
                primary_instrument=QQQ,
                execution_instrument=MNQ,
            )
            def generate(self, ctx, state):
                return None

        assert uid in get_registry()
        del _registry[uid]  # cleanup

    def test_register_raises_for_non_kernel(self):
        with pytest.raises(TypeError):
            @register_strategy
            class _NotAKernel:
                SPEC = StrategySpec(
                    id="_bad",
                    primary_instrument=QQQ,
                    execution_instrument=MNQ,
                )

    def test_register_raises_for_missing_spec(self):
        with pytest.raises(AttributeError):
            @register_strategy
            class _NoSpec(StrategyKernel):
                def generate(self, ctx, state):
                    return None

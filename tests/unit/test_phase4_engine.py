"""Phase 4 unit tests: indicators, feature caches, RiskPolicy, loader."""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from core.types import Fill, Instrument, Signal
from core.engine.timeframes import TF_1M
from core.data.manager import DataManager
from core.features.indicators import (
    ema, sma, rsi, macd, stoch, atr, bollinger_bands,
    session_vwap, heikin_ashi,
)
from core.features.ids import parse_indicator_id
from core.features.registry import FeatureRegistry
from core.risk.policy import RiskPolicy
from core.portfolio.state import PortfolioState
from core.engine.loader import register_strategy, get_registry, _registry
from core.interfaces.strategy import StrategyKernel, StrategySpec

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
        base = datetime(2026, 4, 29, 13, 30, tzinfo=timezone.utc)
        from core.types import Bar
        for i in range(60):
            for instrument, manager, offset in (
                (QQQ, self.qqq_dm, 0.0),
                (SPY, self.spy_dm, 50.0),
            ):
                manager.on_bar(Bar(
                    instrument=instrument,
                    timeframe=TF_1M,
                    timestamp=base + timedelta(minutes=i),
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


# ---------------------------------------------------------------------------
# RiskPolicy
# ---------------------------------------------------------------------------

class TestRiskPolicy:
    def test_sizes_fixed_shares(self):
        rp = RiskPolicy(position_size_shares=2, max_order_quantity=10)
        sig = Signal(instrument=MNQ, side="long")
        assert rp.size_order(sig, PortfolioState()) == 2

    def test_caps_at_max(self):
        rp = RiskPolicy(position_size_shares=20, max_order_quantity=5)
        sig = Signal(instrument=MNQ, side="long")
        assert rp.size_order(sig, PortfolioState()) == 5

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

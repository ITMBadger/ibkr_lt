"""Stateless indicator functions.

All functions take a pandas DataFrame with columns open/high/low/close/volume
(and a tz-aware DatetimeIndex) and return a Series or DataFrame.

Scoped indicator IDs: "ema_20@QQQ.3m"
  → name="ema_20", symbol="QQQ", tf_label="3m"
  → FeatureRegistry parses these and routes to the right function.

Uses pandas/numpy throughout (no talib dependency).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Moving averages
# ---------------------------------------------------------------------------

def ema(bars: pd.DataFrame, period: int, col: str = "close") -> pd.Series:
    """Exponential moving average."""
    return bars[col].ewm(span=period, adjust=False).mean()


def sma(bars: pd.DataFrame, period: int, col: str = "close") -> pd.Series:
    """Simple moving average."""
    return bars[col].rolling(period).mean()


def stddev(bars: pd.DataFrame, period: int, col: str = "close") -> pd.Series:
    """Rolling standard deviation (population, ddof=1 for n-1)."""
    return bars[col].rolling(period).std(ddof=1)


# ---------------------------------------------------------------------------
# Momentum / oscillators
# ---------------------------------------------------------------------------

def rsi(bars: pd.DataFrame, period: int = 14, col: str = "close") -> pd.Series:
    """RSI using Wilder's smoothing (EMA with α = 1/period)."""
    delta = bars[col].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def macd(
    bars: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    col: str = "close",
) -> pd.DataFrame:
    """MACD line, signal line, and histogram."""
    fast_ema = bars[col].ewm(span=fast, adjust=False).mean()
    slow_ema = bars[col].ewm(span=slow, adjust=False).mean()
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return pd.DataFrame({
        "macd": macd_line,
        "signal": signal_line,
        "hist": macd_line - signal_line,
    }, index=bars.index)


def stoch(
    bars: pd.DataFrame,
    fastk_period: int = 14,
    slowk_period: int = 3,
    slowd_period: int = 3,
) -> pd.DataFrame:
    """Stochastic oscillator (slow %K and %D)."""
    low_min = bars["low"].rolling(fastk_period).min()
    high_max = bars["high"].rolling(fastk_period).max()
    fastk = 100.0 * (bars["close"] - low_min) / (high_max - low_min + 1e-12)
    slowk = fastk.rolling(slowk_period).mean()
    slowd = slowk.rolling(slowd_period).mean()
    return pd.DataFrame({"slowk": slowk, "slowd": slowd}, index=bars.index)


def atr(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    prev_close = bars["close"].shift(1)
    tr = pd.concat([
        bars["high"] - bars["low"],
        (bars["high"] - prev_close).abs(),
        (bars["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def adx(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average Directional Index."""
    high = bars["high"]
    low = bars["low"]
    close = bars["close"]
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    dm_plus = (high - prev_high).clip(lower=0.0)
    dm_minus = (prev_low - low).clip(lower=0.0)
    # Zero out where DM- > DM+
    dm_plus_masked = dm_plus.where(dm_plus > dm_minus, 0.0)
    dm_minus_masked = dm_minus.where(dm_minus >= dm_plus, 0.0)

    atr_s = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    di_plus = 100.0 * dm_plus_masked.ewm(alpha=1.0 / period, adjust=False).mean() / (atr_s + 1e-12)
    di_minus = 100.0 * dm_minus_masked.ewm(alpha=1.0 / period, adjust=False).mean() / (atr_s + 1e-12)
    dx = 100.0 * (di_plus - di_minus).abs() / (di_plus + di_minus + 1e-12)
    return dx.ewm(alpha=1.0 / period, adjust=False).mean()


def bollinger_bands(
    bars: pd.DataFrame,
    period: int = 20,
    nbdev: float = 2.0,
    col: str = "close",
) -> pd.DataFrame:
    """Bollinger Bands: upper, middle (SMA), lower."""
    middle = bars[col].rolling(period).mean()
    std = bars[col].rolling(period).std(ddof=1)
    return pd.DataFrame({
        "upper": middle + nbdev * std,
        "middle": middle,
        "lower": middle - nbdev * std,
    }, index=bars.index)


# ---------------------------------------------------------------------------
# Session-anchored indicators
# ---------------------------------------------------------------------------

def session_vwap(bars: pd.DataFrame) -> pd.Series:
    """Session VWAP — cumulative per calendar date of the index.

    The index must be tz-aware. Bars are grouped by their local date
    (derived from converting to the index timezone).
    """
    # Cast to float to guard against object dtype after repeated pd.concat
    high = bars["high"].astype(float)
    low = bars["low"].astype(float)
    close = bars["close"].astype(float)
    volume = bars["volume"].astype(float)
    typical = (high + low + close) / 3.0
    dollar_vol = typical * volume
    # Group by day using integer day offset (avoids pandas groupby dtype issues)
    day_key = bars.index.floor("D")
    result = pd.Series(np.nan, index=bars.index)
    for day in day_key.unique():
        mask = day_key == day
        dv = dollar_vol[mask].cumsum()
        v = volume[mask].cumsum()
        result[mask] = dv / v.replace(0.0, np.nan)
    return result


def session_open_values(bars: pd.DataFrame) -> pd.Series:
    """Broadcast the session open price to every bar in that session."""
    dates = bars.index.normalize()
    first_open = bars["open"].groupby(dates).transform("first")
    return first_open


def daily_ohlcv(bars: pd.DataFrame) -> pd.DataFrame:
    """Aggregate intraday 1-min bars to daily OHLCV.

    Returns a DataFrame indexed by the session date (tz-aware midnight).
    """
    dates = bars.index.normalize()
    return bars.groupby(dates).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )


def heikin_ashi(bars: pd.DataFrame) -> pd.DataFrame:
    """Heikin-Ashi OHLC bars."""
    ha_close = (bars["open"] + bars["high"] + bars["low"] + bars["close"]) / 4.0
    ha_open = ha_close.copy()
    # Iterative HA open
    opens = bars["open"].values
    closes = bars["close"].values
    ha_opens = np.empty(len(bars))
    ha_opens[0] = (opens[0] + closes[0]) / 2.0
    for i in range(1, len(bars)):
        ha_opens[i] = (ha_opens[i - 1] + ha_close.iloc[i - 1]) / 2.0
    ha_open = pd.Series(ha_opens, index=bars.index)
    ha_high = pd.concat([bars["high"], ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([bars["low"], ha_open, ha_close], axis=1).min(axis=1)
    return pd.DataFrame({
        "ha_open": ha_open,
        "ha_high": ha_high,
        "ha_low": ha_low,
        "ha_close": ha_close,
    }, index=bars.index)


def range_ratio_by_session(
    bars: pd.DataFrame,
    window: int = 20,
    min_periods: int = 5,
    shift: int = 1,
) -> pd.Series:
    """Current session range / rolling average of prior session ranges.

    Returns a Series aligned with bars. shift=1 means use prior-day average (default).
    """
    dates = bars.index.normalize()
    session_range = bars.groupby(dates).agg(rng=("high", "max"))["rng"] - \
                    bars.groupby(dates).agg(low_min=("low", "min"))["low_min"]
    rolling_avg = session_range.rolling(window, min_periods=min_periods).mean().shift(shift)
    # Align back to bar index
    date_to_ratio = {}
    for d, rng in session_range.items():
        avg = rolling_avg.get(d, np.nan)
        date_to_ratio[d] = rng / avg if avg and avg > 0 else np.nan
    return dates.map(date_to_ratio)


def vix_ema_ratio(
    vix_path: str,
    tz: str = "America/New_York",
    span: int = 120,
    shift: int = 1,
) -> pd.Series:
    """Load VIX daily CSV and compute VIX / EMA(VIX, span).

    Returns a Series indexed by date (tz-aware). Used by strategies as an
    entry filter: ratio > threshold → elevated VIX, skip long entries.
    """
    try:
        df = pd.read_csv(vix_path, index_col=0, parse_dates=True)
        df.columns = [c.lower() for c in df.columns]
        close_col = "close" if "close" in df.columns else df.columns[0]
        series = df[close_col].sort_index()
        import pytz
        if series.index.tz is None:
            series.index = series.index.tz_localize(pytz.timezone(tz))
        ema_series = series.ewm(span=span, adjust=False).mean().shift(shift)
        return series / ema_series
    except Exception:
        return pd.Series(dtype=float)

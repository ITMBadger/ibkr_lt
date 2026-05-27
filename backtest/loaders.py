"""Strategy and market-data loading for backtests."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from core.adapters.csv.data import CSVDataProvider
from core.engine.timeframes import TF_1D, TF_1M, Timeframe
from core.exceptions import ConfigError
from core.interfaces.strategy import StrategyKernel
from core.types import Bar, Instrument


def instantiate_strategies(
    registry: dict[str, type[StrategyKernel]],
    strategy_ids: Iterable[str],
    strategy_params: dict[str, object] | None = None,
) -> list[tuple[StrategyKernel, dict]]:
    strategies: list[tuple[StrategyKernel, dict]] = []
    params_by_strategy = strategy_params or {}
    for strategy_id in strategy_ids:
        cls = registry[str(strategy_id)]
        params = params_by_strategy.get(str(strategy_id), {})
        if not isinstance(params, dict):
            raise ConfigError(f"strategy_params.{strategy_id} must be a mapping")
        strategies.append((cls(params), {}))
    return strategies


def required_instruments(
    strategies: Iterable[tuple[StrategyKernel, dict]],
) -> list[Instrument]:
    instruments: dict[Instrument, None] = {}
    for kernel, _ in strategies:
        spec = kernel.SPEC
        instruments.setdefault(spec.primary_instrument, None)
        instruments.setdefault(spec.execution_instrument, None)
        for ref in spec.reference_instruments:
            instruments.setdefault(ref, None)
    return sorted(instruments, key=lambda item: (item.symbol, item.asset_class))


def resolve_evaluation_timeframes(
    strategies: Iterable[tuple[StrategyKernel, dict]],
    override: str | None = None,
) -> dict[str, str]:
    """Pick the bar interval used to throttle fast-event entry evaluations."""
    if override:
        parsed = Timeframe.parse(str(override))
        return {
            kernel.SPEC.id: parsed.label
            for kernel, _ in strategies
        }

    resolved: dict[str, str] = {}
    for kernel, _ in strategies:
        spec = kernel.SPEC
        strategy_bar_size = getattr(kernel, "_BAR_SIZE", None)
        strategy_timeframe = None
        if isinstance(strategy_bar_size, str):
            try:
                strategy_timeframe = Timeframe.parse(strategy_bar_size)
            except ValueError:
                strategy_timeframe = None
        if (
            strategy_timeframe is not None
            and strategy_bar_size in spec.timeframes
            and strategy_timeframe.seconds > TF_1M.seconds
        ):
            resolved[spec.id] = strategy_timeframe.label
            continue

        candidates: list[tuple[int, str]] = []
        for label in spec.timeframes:
            timeframe = Timeframe.parse(label)
            if TF_1M.seconds < timeframe.seconds < TF_1D.seconds:
                candidates.append((timeframe.seconds, timeframe.label))
        if candidates:
            resolved[spec.id] = min(candidates)[1]
    return resolved


def validate_csv_path(csv_path: Path, instruments: Iterable[Instrument]) -> None:
    instruments = list(instruments)
    if not csv_path.exists():
        raise ConfigError(f"CSV path does not exist: {csv_path}")
    if csv_path.is_file() and len(instruments) > 1:
        symbols = sorted({instrument.symbol for instrument in instruments})
        raise ConfigError(
            "A single CSV file can only backtest one required instrument. "
            f"Selected strategies require {symbols}; use a CSV directory instead."
        )


def build_csv_provider(
    *,
    csv_path: Path,
    session_tz: str,
    rth_only: bool,
    market_open: str,
    market_close: str,
) -> CSVDataProvider:
    return CSVDataProvider(
        csv_path,
        session_tz=session_tz,
        rth_only=rth_only,
        market_open=market_open,
        market_close=market_close,
    )


async def load_replay_bars(
    provider: CSVDataProvider,
    instruments: Iterable[Instrument],
    *,
    start,
    end,
) -> list[Bar]:
    bars: list[Bar] = []
    missing: list[str] = []
    for instrument in instruments:
        loaded = await provider.fetch(instrument, TF_1M, start, end)
        if not loaded:
            missing.append(instrument.symbol)
        bars.extend(loaded)
    if missing:
        raise ConfigError(
            "No replay bars loaded for required instrument(s): "
            f"{sorted(set(missing))}. Check CSV filenames and date range."
        )
    return sorted(bars, key=lambda bar: (bar.timestamp, bar.instrument.symbol))

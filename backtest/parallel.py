"""Parallel candidate-entry generation for accelerated backtests."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

from core import load_strategies
from core.data.manager import DataManager
from core.engine.loader import get_registry
from core.engine.scheduler import Scheduler
from core.engine.timeframes import TF_1M, Timeframe
from core.exceptions import ConfigError
from core.features.registry import FeatureRegistry
from core.interfaces.strategy import StrategyKernel
from core.types import Bar, Instrument, Signal

from .loaders import instantiate_strategies


@dataclass(frozen=True)
class EntryCandidate:
    strategy_id: str
    timestamp: datetime
    signal: Signal


@dataclass(frozen=True)
class _ChunkPayload:
    index: int
    start: datetime
    end: datetime
    warmup_start: datetime
    bars: list[Bar]
    strategy_packages: list[str]
    strategy_ids: list[str]
    evaluation_timeframes: dict[str, str]
    lookback_days: int
    session_tz: str


@dataclass(frozen=True)
class _ChunkResult:
    index: int
    candidates: list[EntryCandidate]
    active_bars: int
    warmup_bars: int


def validate_parallel_strategies(
    strategies: Iterable[tuple[StrategyKernel, dict]],
) -> None:
    unsafe = [
        kernel.SPEC.id
        for kernel, _ in strategies
        if not bool(getattr(kernel, "_PARALLEL_BACKTEST_SAFE", False))
    ]
    if unsafe:
        raise ConfigError(
            "Parallel backtest mode requires each strategy to opt in with "
            f"_PARALLEL_BACKTEST_SAFE = True. Missing: {unsafe}"
        )


def generate_parallel_entry_candidates(
    *,
    settings,
    bars: Sequence[Bar],
    strategies: Iterable[tuple[StrategyKernel, dict]],
    evaluation_timeframes: Mapping[str, str],
) -> tuple[list[EntryCandidate], dict]:
    strategies = list(strategies)
    workers = _worker_count(settings.max_parallel_workers, bars)
    chunks = _build_chunks(
        bars=bars,
        start=settings.start,
        end=settings.end,
        lookback_days=settings.lookback_days,
        session_tz=settings.session_tz,
        workers=workers,
        strategy_ids=list(settings.strategy_ids),
        strategy_packages=list(settings.strategy_packages),
        evaluation_timeframes=dict(evaluation_timeframes),
    )
    if not chunks:
        return [], {
            "enabled": True,
            "workers": 0,
            "chunks": 0,
            "candidate_count": 0,
            "duplicates_dropped": 0,
        }

    if len(chunks) == 1:
        results = [_generate_chunk_candidates(chunks[0])]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, len(chunks))) as pool:
            results = list(pool.map(_generate_chunk_candidates, chunks))

    ordered_results = sorted(results, key=lambda item: item.index)
    candidates, duplicates = _dedupe_candidates(
        candidate
        for result in ordered_results
        for candidate in result.candidates
    )
    candidates.sort(
        key=lambda item: (
            _ensure_utc(item.timestamp),
            item.strategy_id,
            item.signal.instrument.symbol,
            item.signal.side,
            item.signal.trade_id or "",
        )
    )
    return candidates, {
        "enabled": True,
        "workers": min(workers, len(chunks)),
        "chunks": len(chunks),
        "candidate_count": len(candidates),
        "duplicates_dropped": duplicates,
        "chunk_stats": [
            {
                "index": result.index,
                "active_bars": result.active_bars,
                "warmup_bars": result.warmup_bars,
                "candidates": len(result.candidates),
            }
            for result in ordered_results
        ],
    }


def candidates_to_engine_map(
    candidates: Iterable[EntryCandidate],
) -> dict[str, list[tuple[datetime, Signal]]]:
    result: dict[str, list[tuple[datetime, Signal]]] = defaultdict(list)
    for candidate in candidates:
        result[candidate.strategy_id].append(
            (_ensure_utc(candidate.timestamp), candidate.signal)
        )
    return dict(result)


def _generate_chunk_candidates(payload: _ChunkPayload) -> _ChunkResult:
    load_strategies(payload.strategy_packages)
    registry = get_registry()
    strategies = instantiate_strategies(registry, payload.strategy_ids)
    data_instruments = _strategy_data_instruments(strategies)
    managers = {
        instrument: DataManager(
            instrument,
            payload.lookback_days,
            payload.session_tz,
        )
        for instrument in data_instruments
    }

    warmup_by_instrument: dict[Instrument, list[Bar]] = defaultdict(list)
    active_bars: list[Bar] = []
    for bar in payload.bars:
        ts = _ensure_utc(bar.timestamp)
        if bar.instrument not in managers:
            continue
        if payload.warmup_start <= ts < payload.start:
            warmup_by_instrument[bar.instrument].append(bar)
        elif payload.start <= ts <= payload.end:
            active_bars.append(bar)

    for instrument, warmup in warmup_by_instrument.items():
        managers[instrument].merge_backfill(warmup)

    features = FeatureRegistry(managers)
    features.preload_from_managers()
    features.preload_bars(active_bars)
    scheduler = Scheduler(features)
    for kernel, initial_state in strategies:
        state = dict(initial_state)
        scheduler.register(kernel, state)
        kernel.on_start(state)

    candidates: list[EntryCandidate] = []
    last_evaluation_bars: dict[str, datetime] = {}
    evaluation_timeframes = {
        strategy_id: Timeframe.parse(label)
        for strategy_id, label in payload.evaluation_timeframes.items()
    }

    active_bars.sort(key=lambda item: (item.timestamp, item.instrument.symbol))
    for bar in active_bars:
        manager = managers.get(bar.instrument)
        if manager is None:
            continue
        manager.on_bar(bar)

        def include_strategy(kernel: StrategyKernel, _state: dict) -> bool:
            return _should_generate_entry(
                kernel,
                managers,
                evaluation_timeframes,
                last_evaluation_bars,
            )

        for kernel, ctx, state in scheduler.on_bar(
            bar,
            managers,
            include=include_strategy,
        ):
            signal = kernel.generate(ctx, state)
            if signal is not None:
                candidates.append(
                    EntryCandidate(
                        strategy_id=kernel.SPEC.id,
                        timestamp=_ensure_utc(ctx.timestamp),
                        signal=signal,
                    )
                )

    return _ChunkResult(
        index=payload.index,
        candidates=candidates,
        active_bars=len(active_bars),
        warmup_bars=sum(len(items) for items in warmup_by_instrument.values()),
    )


def _build_chunks(
    *,
    bars: Sequence[Bar],
    start: datetime,
    end: datetime,
    lookback_days: int,
    session_tz: str,
    workers: int,
    strategy_packages: list[str],
    strategy_ids: list[str],
    evaluation_timeframes: dict[str, str],
) -> list[_ChunkPayload]:
    tz = ZoneInfo(session_tz)
    dates = _session_dates(bars, start, end, tz)
    if not dates:
        return []
    worker_count = min(max(1, workers), len(dates))
    date_chunks = _split_dates(dates, worker_count)
    payloads: list[_ChunkPayload] = []
    for index, chunk_dates in enumerate(date_chunks):
        chunk_start, chunk_end = _date_chunk_bounds(chunk_dates, tz)
        warmup_start = chunk_start - timedelta(days=max(0, int(lookback_days)))
        chunk_bars = [
            bar
            for bar in bars
            if warmup_start <= _ensure_utc(bar.timestamp) <= chunk_end
        ]
        payloads.append(
            _ChunkPayload(
                index=index,
                start=chunk_start,
                end=chunk_end,
                warmup_start=warmup_start,
                bars=chunk_bars,
                strategy_packages=list(strategy_packages),
                strategy_ids=strategy_ids,
                evaluation_timeframes=evaluation_timeframes,
                lookback_days=lookback_days,
                session_tz=session_tz,
            )
        )
    return payloads


def _worker_count(max_workers: int, bars: Sequence[Bar]) -> int:
    if not bars:
        return 1
    cpu_count = os.cpu_count() or 1
    configured = max(1, int(max_workers or 4))
    return min(cpu_count, 4, configured)


def _strategy_data_instruments(
    strategies: Iterable[tuple[StrategyKernel, dict]],
) -> set[Instrument]:
    instruments: set[Instrument] = set()
    for kernel, _ in strategies:
        instruments.add(kernel.SPEC.primary_instrument)
        instruments.update(kernel.SPEC.reference_instruments)
    return instruments


def _should_generate_entry(
    kernel: StrategyKernel,
    managers: Mapping[Instrument, DataManager],
    evaluation_timeframes: Mapping[str, Timeframe],
    last_evaluation_bars: dict[str, datetime],
) -> bool:
    timeframe = evaluation_timeframes.get(kernel.SPEC.id)
    if timeframe is None or timeframe.seconds <= TF_1M.seconds:
        return True

    manager = managers.get(kernel.SPEC.primary_instrument)
    if manager is None:
        return True

    latest_ts = manager.latest_timestamp()
    if latest_ts is None:
        return False

    latest_bar = _completed_timeframe_bar_start(latest_ts, timeframe)
    if latest_bar == last_evaluation_bars.get(kernel.SPEC.id):
        return False

    last_evaluation_bars[kernel.SPEC.id] = latest_bar
    return True


def _completed_timeframe_bar_start(latest_ts: datetime, timeframe: Timeframe) -> datetime:
    latest = _ensure_utc(latest_ts)
    candidate = latest - timedelta(seconds=timeframe.seconds)
    anchor = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((candidate - anchor).total_seconds())
    bucket = (elapsed // timeframe.seconds) * timeframe.seconds
    return anchor + timedelta(seconds=bucket)


def _session_dates(
    bars: Sequence[Bar],
    start: datetime,
    end: datetime,
    tz: ZoneInfo,
) -> list[date]:
    start = _ensure_utc(start)
    end = _ensure_utc(end)
    dates = {
        _ensure_utc(bar.timestamp).astimezone(tz).date()
        for bar in bars
        if start <= _ensure_utc(bar.timestamp) <= end
    }
    return sorted(dates)


def _split_dates(dates: Sequence[date], workers: int) -> list[list[date]]:
    chunks: list[list[date]] = []
    total = len(dates)
    start = 0
    for index in range(workers):
        remaining_dates = total - start
        remaining_chunks = workers - index
        size = (remaining_dates + remaining_chunks - 1) // remaining_chunks
        chunks.append(list(dates[start:start + size]))
        start += size
    return [chunk for chunk in chunks if chunk]


def _date_chunk_bounds(dates: Sequence[date], tz: ZoneInfo) -> tuple[datetime, datetime]:
    start_local = datetime.combine(dates[0], dt_time.min, tzinfo=tz)
    end_local = datetime.combine(dates[-1], dt_time.max, tzinfo=tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _dedupe_candidates(
    candidates: Iterable[EntryCandidate],
) -> tuple[list[EntryCandidate], int]:
    seen: set[tuple] = set()
    result: list[EntryCandidate] = []
    duplicates = 0
    for candidate in candidates:
        signal = candidate.signal
        key = (
            candidate.strategy_id,
            _ensure_utc(candidate.timestamp),
            signal.instrument,
            signal.side,
            signal.trade_id,
        )
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        result.append(candidate)
    return result, duplicates


def _ensure_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)

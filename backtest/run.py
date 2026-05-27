"""Run event-based backtests through the production Engine.

Usage:
    python -m backtest.run --start 2025-01-01 --end 2025-03-31
    python -m backtest.run --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
    python -m backtest.run --mode fast-event \
        --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from core import DataFeed, Engine, SimulatedClock, load_strategies
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.audit import AuditLogger, configure_runtime_logging
from core.audit.logger import allocate_run_log_dir
from core.engine.loader import get_registry
from core.engine.timeframes import Timeframe
from core.exceptions import ConfigError
from core.privacy import build_strategy_aliases, redact_payload
from core.risk.policy import SIZING_MODE_FULL_EQUITY, RiskPolicy

from .config import (
    BacktestSettings,
    load_yaml_config,
    resolve_settings,
    resolve_strategy_packages,
)
from .loaders import (
    build_csv_provider,
    instantiate_strategies,
    load_replay_bars,
    required_instruments,
    resolve_evaluation_timeframes,
    validate_csv_path,
)
from .parallel import (
    candidates_to_engine_map,
    generate_parallel_entry_candidates,
    validate_parallel_strategies,
)
from .report_html import write_html_report, write_report_error
from .reporting import audit_file_stats, write_summary

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestResult:
    run_dir: Path
    summary_path: Path
    report_path: Path | None
    processed_bars: int
    strategy_ids: list[str]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Event-based CSV backtest using the production strategy engine"
    )
    parser.add_argument("--config", default="config.yaml", help="Runtime config YAML")
    parser.add_argument("--csv", default=None, help="CSV file or per-symbol CSV directory")
    parser.add_argument(
        "--strategy",
        action="append",
        help=(
            "Strategy id to run. Repeat or comma-separate for multiple. "
            "Defaults to config strategies."
        ),
    )
    parser.add_argument("--start", required=True, help="Start date/time, e.g. 2025-01-01")
    parser.add_argument("--end", required=True, help="End date/time, e.g. 2025-03-31")
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--output-dir", default=None, help="Backtest run output directory")
    parser.add_argument(
        "--mode",
        choices=("event", "fast-event", "parallel"),
        default=None,
        help=(
            "Backtest dispatch mode. event evaluates each primary 1-minute bar; "
            "fast-event evaluates flat entries only when the evaluation bar changes; "
            "parallel precomputes entry candidates in worker processes, then replays "
            "fills and exits chronologically."
        ),
    )
    parser.add_argument(
        "--eval-timeframe",
        default=None,
        help=(
            "Evaluation timeframe for --mode fast-event, e.g. 3m. "
            "Defaults to each strategy's bar size when detectable."
        ),
    )
    parser.add_argument(
        "--all-live",
        action="store_true",
        help="Simulate all selected strategies as live, ignoring dry_run strategy modes.",
    )
    parser.add_argument(
        "--thread-pool-workers",
        type=int,
        default=None,
        help="Backtest strategy worker count. Default is 1 for deterministic replay.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable periodic backtest progress and timing output.",
    )
    parser.add_argument(
        "--progress-interval-bars",
        type=int,
        default=None,
        help="Emit progress after this many processed replay bars.",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=None,
        help="Emit progress after this many wall-clock seconds.",
    )
    parser.add_argument(
        "--max-parallel-workers",
        type=int,
        default=None,
        help="Worker cap for --mode parallel. Default is min(os.cpu_count(), 4).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        args = parse_args(argv)
        config = load_yaml_config(args.config)
        load_strategies(resolve_strategy_packages(config))
        registry = get_registry()
        settings = resolve_settings(
            args=args,
            config=config,
            known_strategy_ids=sorted(registry),
        )
        strategies = instantiate_strategies(
            registry,
            settings.strategy_ids,
            settings.strategy_params,
        )
        result = run_backtest(settings, strategies)
    except (ConfigError, ValueError) as exc:
        print(f"Backtest config error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    print("Backtest complete")
    print(f"Run dir: {result.run_dir}")
    print(f"Summary: {result.summary_path}")
    if result.report_path is not None:
        print(f"Report: {result.report_path}")
    print(f"Processed bars: {result.processed_bars}")
    print(f"Strategies: {result.strategy_ids}")


def run_backtest(
    settings: BacktestSettings,
    strategies,
) -> BacktestResult:
    run_started = time.perf_counter()
    strategies = list(strategies)
    _validate_warmup_lookback(settings, strategies)
    instruments = required_instruments(strategies)
    validate_csv_path(settings.csv_path, instruments)
    evaluation_timeframes = (
        resolve_evaluation_timeframes(strategies, settings.evaluation_timeframe)
        if settings.mode in {"fast-event", "parallel"}
        else {}
    )
    if settings.mode == "parallel":
        validate_parallel_strategies(strategies)
    csv_provider = build_csv_provider(
        csv_path=settings.csv_path,
        session_tz=settings.session_tz,
        rth_only=settings.rth_only,
        market_open=settings.market_open,
        market_close=settings.market_close,
    )
    load_started = time.perf_counter()
    load_start = (
        settings.start - timedelta(days=max(0, settings.lookback_days))
        if settings.mode == "parallel"
        else settings.start
    )
    log.info(
        "Loading replay bars instruments=%s start=%s end=%s csv_path=%s",
        [instrument.symbol for instrument in instruments],
        load_start.isoformat(),
        settings.end.isoformat(),
        settings.csv_path,
    )
    loaded_bars = asyncio.run(
        load_replay_bars(
            csv_provider,
            instruments,
            start=load_start,
            end=settings.end,
        )
    )
    replay_bars = [
        bar
        for bar in loaded_bars
        if settings.start <= _ensure_utc(bar.timestamp) <= settings.end
    ]
    if not replay_bars:
        raise ConfigError(
            "No replay bars loaded inside requested test window. "
            f"start={settings.start.isoformat()} end={settings.end.isoformat()}"
        )
    replay_load_seconds = time.perf_counter() - load_started
    log.info(
        "Loaded bars count=%d replay_bars=%d seconds=%.2f",
        len(loaded_bars),
        len(replay_bars),
        replay_load_seconds,
    )

    candidate_generation_seconds = 0.0
    parallel_stats = None
    precomputed_entry_signals = None
    if settings.mode == "parallel":
        candidate_started = time.perf_counter()
        candidates, parallel_stats = generate_parallel_entry_candidates(
            settings=settings,
            bars=loaded_bars,
            strategies=strategies,
            evaluation_timeframes=evaluation_timeframes,
        )
        candidate_generation_seconds = time.perf_counter() - candidate_started
        precomputed_entry_signals = candidates_to_engine_map(candidates)
        log.info(
            "Generated parallel entry candidates count=%d seconds=%.2f workers=%s",
            len(candidates),
            candidate_generation_seconds,
            parallel_stats.get("workers") if parallel_stats else None,
        )

    profile = str(dict(settings.logging or {}).get("profile", "owner"))
    strategy_aliases = build_strategy_aliases(
        settings.strategy_ids,
        dict(settings.logging or {}).get("strategy_aliases")
        if isinstance(dict(settings.logging or {}).get("strategy_aliases"), dict)
        else {},
    )
    audit_logger, run_dir = _build_audit_logger(
        settings,
        strategy_aliases=strategy_aliases,
    )
    logging_cfg = dict(settings.logging or {})
    configure_runtime_logging(
        log_dir=run_dir,
        level=str(logging_cfg.get("runtime_level", "INFO")),
        enabled=settings.audit_enabled,
        profile=profile,
        strategy_aliases=strategy_aliases,
    )

    clock = SimulatedClock()
    clock.advance_to(settings.start)
    broker = PaperBroker()
    initial_account = asyncio.run(broker.get_account())
    engine = Engine(
        broker=broker,
        data_feed=DataFeed(csv_provider, ReplayDataProvider(replay_bars)),
        clock=clock,
        strategies=strategies,
        risk=RiskPolicy(
            position_size_shares=settings.position_size_shares,
            max_order_quantity=(
                settings.sizing_max_order_quantity
                if settings.sizing_mode == SIZING_MODE_FULL_EQUITY
                else settings.max_order_quantity
            ),
            sizing_mode=settings.sizing_mode,
            equity_fraction=settings.sizing_equity_fraction,
        ),
        thread_pool_workers=settings.thread_pool_workers,
        lookback_days=settings.lookback_days,
        session_tz=settings.session_tz,
        audit_logger=audit_logger,
        strategy_modes=settings.strategy_modes,
        dispatch_mode=settings.mode,
        evaluation_timeframes=evaluation_timeframes,
        precomputed_entry_signals=precomputed_entry_signals,
        feature_preload_bars=replay_bars,
        progress_enabled=settings.progress_enabled,
        progress_total_bars=len(replay_bars),
        progress_interval_bars=settings.progress_interval_bars,
        progress_interval_seconds=settings.progress_interval_seconds,
        metadata_profile=profile,
        strategy_aliases=strategy_aliases,
    )

    log.info(
        "Running backtest mode=%s strategies=%s instruments=%s bars=%d start=%s end=%s",
        settings.mode,
        settings.strategy_ids,
        [instrument.symbol for instrument in instruments],
        len(replay_bars),
        settings.start.isoformat(),
        settings.end.isoformat(),
    )
    engine_started = time.perf_counter()
    engine.run_backtest()
    engine_run_seconds = time.perf_counter() - engine_started
    total_seconds = time.perf_counter() - run_started
    log.info(
        "Backtest engine completed engine_seconds=%.2f total_seconds=%.2f",
        engine_run_seconds,
        total_seconds,
    )
    snapshot = engine.snapshot_state()
    summary = {
        "run_type": "event_backtest",
        "created_at": datetime.now(tz=timezone.utc),
        "config_path": settings.config_path,
        "csv_path": settings.csv_path,
        "start": settings.start,
        "end": settings.end,
        "lookback_days": settings.lookback_days,
        "session_timezone": settings.session_tz,
        "strategies": settings.strategy_ids,
        "strategy_modes": settings.strategy_modes,
        "sizing": {
            "mode": settings.sizing_mode,
            "equity_fraction": settings.sizing_equity_fraction,
            "max_order_quantity": (
                settings.sizing_max_order_quantity
                if settings.sizing_mode == SIZING_MODE_FULL_EQUITY
                else settings.max_order_quantity
            ),
            "position_size_shares": settings.position_size_shares,
        },
        "mode": settings.mode,
        "evaluation_timeframes": evaluation_timeframes,
        "instruments": instruments,
        "replay_bars": len(replay_bars),
        "timings": {
            "replay_load_seconds": replay_load_seconds,
            "candidate_generation_seconds": candidate_generation_seconds,
            "engine_run_seconds": engine_run_seconds,
            "total_seconds": total_seconds,
        },
        "parallel": parallel_stats,
        "initial_account": initial_account,
        "progress": {
            "enabled": settings.progress_enabled,
            "interval_bars": settings.progress_interval_bars,
            "interval_seconds": settings.progress_interval_seconds,
        },
        "audit_enabled": settings.audit_enabled,
        "audit_stats": audit_file_stats(run_dir),
        "engine_snapshot": snapshot,
        "execution_assumptions": {
            "clock": "SimulatedClock advanced to each replay bar timestamp",
            "market_orders": "PaperBroker fills at the next replay bar open",
            "stop_orders": (
                "PaperBroker fills stop orders at stop_price when crossed "
                "by a replay bar"
            ),
            "warmup": (
                "Engine backfills from CSV before the replay start; "
                "strategy logic runs only on replay bars"
            ),
        },
    }
    summary_to_write = redact_payload(
        summary,
        profile=profile,
        aliases=strategy_aliases,
    )
    summary_path = write_summary(run_dir, summary_to_write)
    try:
        report = write_html_report(
            run_dir=run_dir,
            summary=summary_to_write,
            replay_bars=replay_bars,
            initial_equity=float(initial_account.net_liquidation),
        )
    except Exception as exc:
        error_path = write_report_error(run_dir, exc)
        log.exception("Backtest report generation failed; details written to %s", error_path)
        raise RuntimeError(
            f"Backtest report generation failed; details written to {error_path}"
        ) from exc
    summary["report"] = {
        "path": report.report_path,
        "metrics": report.metrics,
        "warnings": report.warnings,
    }
    summary_to_write = redact_payload(
        summary,
        profile=profile,
        aliases=strategy_aliases,
    )
    summary_path = write_summary(run_dir, summary_to_write)
    return BacktestResult(
        run_dir=run_dir,
        summary_path=summary_path,
        report_path=report.report_path,
        processed_bars=len(replay_bars),
        strategy_ids=list(settings.strategy_ids),
    )


def _build_audit_logger(
    settings: BacktestSettings,
    *,
    strategy_aliases: dict[str, str] | None = None,
) -> tuple[AuditLogger | None, Path]:
    if not settings.audit_enabled:
        return None, allocate_run_log_dir(settings.output_dir)
    cfg = dict(settings.logging or {})
    audit_logger = AuditLogger(
        enabled=True,
        log_dir=settings.output_dir,
        profile=str(cfg.get("profile", "owner")),
        strategy_decisions=str(cfg.get("strategy_decisions", "full")),
        decision_scope=str(cfg.get("decision_scope", "trigger_and_interval")),
        decision_interval_minutes=int(cfg.get("decision_interval_minutes", 30)),
        run_subdir=True,
        strategy_aliases=strategy_aliases,
    )
    return audit_logger, audit_logger.log_dir


def _validate_warmup_lookback(settings: BacktestSettings, strategies) -> None:
    problems: list[str] = []
    for kernel, _ in strategies:
        spec = kernel.SPEC
        for timeframe_label, warmup_bars in spec.warmup_bars.items():
            required = _minimum_calendar_lookback_days(
                timeframe_label=str(timeframe_label),
                warmup_bars=int(warmup_bars),
                settings=settings,
            )
            if required is not None and settings.lookback_days < required:
                problems.append(
                    f"{spec.id} warmup_bars[{timeframe_label}]={warmup_bars} "
                    f"needs about {required} calendar lookback days; "
                    f"configured lookback_days={settings.lookback_days}"
                )
    if problems:
        detail = "; ".join(problems)
        raise ConfigError(
            "Configured lookback_days is too short for the selected strategy warmup. "
            f"{detail}. Use --lookback-days or the correct --config file."
        )


def _minimum_calendar_lookback_days(
    *,
    timeframe_label: str,
    warmup_bars: int,
    settings: BacktestSettings,
) -> int | None:
    if warmup_bars <= 0:
        return None
    try:
        timeframe = Timeframe.parse(timeframe_label)
    except ValueError:
        return None

    if timeframe.seconds < 86400:
        session_seconds = _session_seconds(settings.market_open, settings.market_close)
        bars_per_session = max(1, session_seconds // timeframe.seconds)
        required_trading_days = _ceil_div(warmup_bars, bars_per_session)
    else:
        required_trading_days = _ceil_div(warmup_bars, max(1, timeframe.seconds // 86400))

    return _trading_days_to_calendar_days(required_trading_days)


def _session_seconds(market_open: str, market_close: str) -> int:
    open_hour, open_minute = _parse_hhmm(market_open)
    close_hour, close_minute = _parse_hhmm(market_close)
    seconds = ((close_hour * 60 + close_minute) - (open_hour * 60 + open_minute)) * 60
    if seconds <= 0:
        raise ConfigError(
            f"market_close must be after market_open: {market_open!r} to {market_close!r}"
        )
    return seconds


def _parse_hhmm(value: str) -> tuple[int, int]:
    try:
        hour_text, minute_text = value.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError as exc:
        raise ConfigError(f"Invalid market time {value!r}; expected HH:MM") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ConfigError(f"Invalid market time {value!r}; expected HH:MM")
    return hour, minute


def _trading_days_to_calendar_days(required_trading_days: int) -> int:
    # Convert weekday-only sessions to calendar days and leave a small holiday buffer.
    return _ceil_div(required_trading_days * 7, 5) + 10


def _ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


def _ensure_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


if __name__ == "__main__":
    main()

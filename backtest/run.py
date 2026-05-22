"""Run event-based backtests through the production Engine.

Usage:
    python -m backtest.run --start 2025-01-01 --end 2025-03-31
    python -m backtest.run --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from core import DataFeed, Engine, SimulatedClock, load_strategies
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.audit import AuditLogger, configure_runtime_logging
from core.audit.logger import allocate_run_log_dir
from core.engine.loader import get_registry
from core.exceptions import ConfigError
from core.risk.policy import RiskPolicy

from .config import BacktestSettings, load_yaml_config, resolve_settings
from .loaders import (
    build_csv_provider,
    instantiate_strategies,
    load_replay_bars,
    required_instruments,
    validate_csv_path,
)
from .reporting import audit_file_stats, write_summary

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestResult:
    run_dir: Path
    summary_path: Path
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        args = parse_args(argv)
        config = load_yaml_config(args.config)
        load_strategies()
        registry = get_registry()
        settings = resolve_settings(
            args=args,
            config=config,
            known_strategy_ids=sorted(registry),
        )
        strategies = instantiate_strategies(registry, settings.strategy_ids)
        result = run_backtest(settings, strategies)
    except (ConfigError, ValueError) as exc:
        print(f"Backtest config error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    print("Backtest complete")
    print(f"Run dir: {result.run_dir}")
    print(f"Summary: {result.summary_path}")
    print(f"Processed bars: {result.processed_bars}")
    print(f"Strategies: {result.strategy_ids}")


def run_backtest(
    settings: BacktestSettings,
    strategies,
) -> BacktestResult:
    instruments = required_instruments(strategies)
    validate_csv_path(settings.csv_path, instruments)
    csv_provider = build_csv_provider(
        csv_path=settings.csv_path,
        session_tz=settings.session_tz,
        rth_only=settings.rth_only,
        market_open=settings.market_open,
        market_close=settings.market_close,
    )
    replay_bars = asyncio.run(
        load_replay_bars(
            csv_provider,
            instruments,
            start=settings.start,
            end=settings.end,
        )
    )

    audit_logger, run_dir = _build_audit_logger(settings)
    logging_cfg = dict(settings.logging or {})
    configure_runtime_logging(
        log_dir=run_dir,
        level=str(logging_cfg.get("runtime_level", "INFO")),
        enabled=settings.audit_enabled,
    )

    clock = SimulatedClock()
    clock.advance_to(settings.start)
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(csv_provider, ReplayDataProvider(replay_bars)),
        clock=clock,
        strategies=strategies,
        risk=RiskPolicy(
            position_size_shares=settings.position_size_shares,
            max_order_quantity=settings.max_order_quantity,
        ),
        thread_pool_workers=settings.thread_pool_workers,
        lookback_days=settings.lookback_days,
        session_tz=settings.session_tz,
        audit_logger=audit_logger,
        strategy_modes=settings.strategy_modes,
    )

    log.info(
        "Running backtest strategies=%s instruments=%s bars=%d start=%s end=%s",
        settings.strategy_ids,
        [instrument.symbol for instrument in instruments],
        len(replay_bars),
        settings.start.isoformat(),
        settings.end.isoformat(),
    )
    engine.run_backtest()
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
        "instruments": instruments,
        "replay_bars": len(replay_bars),
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
    summary_path = write_summary(run_dir, summary)
    return BacktestResult(
        run_dir=run_dir,
        summary_path=summary_path,
        processed_bars=len(replay_bars),
        strategy_ids=list(settings.strategy_ids),
    )


def _build_audit_logger(settings: BacktestSettings) -> tuple[AuditLogger | None, Path]:
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
    )
    return audit_logger, audit_logger.log_dir


if __name__ == "__main__":
    main()

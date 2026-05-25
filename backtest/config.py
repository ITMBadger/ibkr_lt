"""Configuration helpers for event-based backtests."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import yaml

from core.engine.timeframes import Timeframe
from core.exceptions import ConfigError
from core.path_utils import normalize_local_path
from core.orders.strategy_modes import (
    STRATEGY_MODE_LIVE,
    strategy_mode_map,
    validate_strategy_modes,
)


@dataclass(frozen=True)
class BacktestSettings:
    config_path: Path
    csv_path: Path
    start: datetime
    end: datetime
    lookback_days: int
    session_tz: str
    strategy_ids: list[str]
    strategy_modes: dict[str, str]
    position_size_shares: int
    max_order_quantity: int
    thread_pool_workers: int
    output_dir: Path
    audit_enabled: bool
    logging: dict[str, Any]
    rth_only: bool
    market_open: str
    market_close: str
    mode: str = "event"
    evaluation_timeframe: str | None = None
    progress_enabled: bool = True
    progress_interval_bars: int = 1000
    progress_interval_seconds: float = 30.0
    max_parallel_workers: int = 4
    sizing_mode: str = "fixed_shares"
    sizing_equity_fraction: float = 1.0
    sizing_max_order_quantity: float | None = None


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    config_path = normalize_local_path(path)
    with config_path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    if not isinstance(config, dict):
        raise ConfigError(f"Config must be a YAML mapping: {config_path}")
    if "dry_run" in config:
        raise ConfigError(
            "YAML config must not define dry_run; use strategy_modes.<strategy_id>: dry_run"
        )
    return config


def resolve_settings(
    *,
    args: Any,
    config: dict[str, Any],
    known_strategy_ids: Sequence[str],
) -> BacktestSettings:
    session_tz = str(config.get("session_timezone", "America/New_York"))
    start = parse_boundary(args.start, session_tz, is_end=False)
    end = parse_boundary(args.end, session_tz, is_end=True)
    if end < start:
        raise ConfigError(f"--end must be on or after --start: {args.end!r} < {args.start!r}")

    strategy_ids = resolve_strategy_ids(
        override_values=getattr(args, "strategy", None),
        configured=config.get("strategies"),
        known_strategy_ids=known_strategy_ids,
    )
    validate_strategy_modes(config.get("strategy_modes"), known_strategy_ids)
    if getattr(args, "all_live", False):
        strategy_modes = {strategy_id: STRATEGY_MODE_LIVE for strategy_id in strategy_ids}
    else:
        strategy_modes = strategy_mode_map(config.get("strategy_modes"), strategy_ids)

    data_cfg = dict(config.get("data") or {})
    historical_cfg = dict(data_cfg.get("historical") or {})
    csv_path = getattr(args, "csv", None) or historical_cfg.get("path")
    if not csv_path:
        raise ConfigError(
            "Backtest requires --csv or data.historical.path in config.yaml"
        )
    provider = str(historical_cfg.get("provider", "csv")).lower()
    if provider != "csv" and getattr(args, "csv", None) is None:
        raise ConfigError(
            "Backtest currently replays CSV data; pass --csv or set "
            "data.historical.provider: csv"
        )

    backtest_cfg = dict(config.get("backtest") or {})
    progress_cfg = dict(backtest_cfg.get("progress") or {})
    sizing_cfg = dict(backtest_cfg.get("sizing") or {})
    mode = normalize_backtest_mode(
        getattr(args, "mode", None)
        or backtest_cfg.get("mode", "event")
    )
    evaluation_timeframe = (
        getattr(args, "eval_timeframe", None)
        or backtest_cfg.get("evaluation_timeframe")
    )
    if evaluation_timeframe:
        try:
            evaluation_timeframe = Timeframe.parse(str(evaluation_timeframe)).label
        except ValueError as exc:
            raise ConfigError(
                f"Invalid backtest evaluation timeframe: {evaluation_timeframe!r}"
            ) from exc
    logging_cfg = dict(config.get("logging") or {})
    audit_enabled = bool(logging_cfg.get("enabled", True))
    output_dir = normalize_local_path(
        getattr(args, "output_dir", None)
        or backtest_cfg.get("output_dir")
        or "runs/backtests"
    )
    sizing_mode = normalize_sizing_mode(sizing_cfg.get("mode", "fixed_shares"))
    sizing_equity_fraction = float(sizing_cfg.get("equity_fraction", 1.0))
    if sizing_equity_fraction <= 0:
        raise ConfigError("backtest.sizing.equity_fraction must be > 0")
    sizing_max_order_quantity = sizing_cfg.get("max_order_quantity")
    if sizing_max_order_quantity is not None:
        sizing_max_order_quantity = float(sizing_max_order_quantity)
        if sizing_max_order_quantity <= 0:
            raise ConfigError("backtest.sizing.max_order_quantity must be > 0")

    return BacktestSettings(
        config_path=normalize_local_path(getattr(args, "config", "config.yaml")),
        csv_path=normalize_local_path(csv_path),
        start=start,
        end=end,
        lookback_days=int(
            getattr(args, "lookback_days", None)
            if getattr(args, "lookback_days", None) is not None
            else config.get("lookback_days", 500)
        ),
        session_tz=session_tz,
        strategy_ids=strategy_ids,
        strategy_modes=strategy_modes,
        position_size_shares=int(config.get("position_size_shares", 1)),
        max_order_quantity=int(config.get("max_order_quantity", 2)),
        thread_pool_workers=int(
            getattr(args, "thread_pool_workers", None)
            if getattr(args, "thread_pool_workers", None) is not None
            else backtest_cfg.get("thread_pool_workers", 1)
        ),
        output_dir=output_dir,
        audit_enabled=audit_enabled,
        logging=logging_cfg,
        rth_only=bool(historical_cfg.get("rth_only", True)),
        market_open=str(historical_cfg.get("market_open", "09:30")),
        market_close=str(historical_cfg.get("market_close", "16:00")),
        mode=mode,
        evaluation_timeframe=evaluation_timeframe,
        progress_enabled=(
            not bool(getattr(args, "no_progress", False))
            and bool(progress_cfg.get("enabled", True))
        ),
        progress_interval_bars=int(
            getattr(args, "progress_interval_bars", None)
            if getattr(args, "progress_interval_bars", None) is not None
            else progress_cfg.get("interval_bars", 1000)
        ),
        progress_interval_seconds=float(
            getattr(args, "progress_interval_seconds", None)
            if getattr(args, "progress_interval_seconds", None) is not None
            else progress_cfg.get("interval_seconds", 30.0)
        ),
        max_parallel_workers=int(
            getattr(args, "max_parallel_workers", None)
            if getattr(args, "max_parallel_workers", None) is not None
            else backtest_cfg.get("max_parallel_workers", 4)
        ),
        sizing_mode=sizing_mode,
        sizing_equity_fraction=sizing_equity_fraction,
        sizing_max_order_quantity=sizing_max_order_quantity,
    )


def normalize_backtest_mode(value: str) -> str:
    normalized = str(value or "event").strip().lower().replace("_", "-")
    if normalized not in {"event", "fast-event", "parallel"}:
        raise ConfigError(
            "Backtest mode must be 'event', 'fast-event', or 'parallel'; "
            f"got {value!r}"
        )
    return normalized


def normalize_sizing_mode(value: str) -> str:
    normalized = str(value or "fixed_shares").strip().lower().replace("-", "_")
    if normalized in {"fixed", "fixed_share", "fixed_shares"}:
        return "fixed_shares"
    if normalized in {"full_equity", "full", "full_account", "equity"}:
        return "full_equity"
    raise ConfigError(
        "backtest.sizing.mode must be 'fixed_shares' or 'full_equity'; "
        f"got {value!r}"
    )


def resolve_strategy_ids(
    *,
    override_values: Sequence[str] | None,
    configured: Any,
    known_strategy_ids: Sequence[str],
) -> list[str]:
    known = [str(strategy_id) for strategy_id in known_strategy_ids]
    if override_values:
        raw_ids = list(override_values)
    elif configured:
        raw_ids = [configured] if isinstance(configured, str) else list(configured)
    else:
        raw_ids = known

    strategy_ids: list[str] = []
    for value in raw_ids:
        for item in str(value).split(","):
            strategy_id = item.strip()
            if strategy_id:
                strategy_ids.append(strategy_id)

    unknown = [strategy_id for strategy_id in strategy_ids if strategy_id not in known]
    if unknown:
        raise ConfigError(
            f"Unknown strategy id(s): {unknown}. Available: {sorted(known)}"
        )
    return strategy_ids


def parse_boundary(value: str, session_tz: str, *, is_end: bool) -> datetime:
    raw = str(value).strip()
    tz = ZoneInfo(session_tz)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        boundary_time = time(23, 59, 59, 999999) if is_end else time(0, 0)
        local_ts = datetime.combine(
            datetime.fromisoformat(raw).date(),
            boundary_time,
            tzinfo=tz,
        )
        return local_ts.astimezone(timezone.utc)

    ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=tz)
    return ts.astimezone(timezone.utc)

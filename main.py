"""Runtime entry point for live/paper deployments."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Sequence

from core import DataFeed, Engine, Instrument, WallClock, load_strategies
from core.adapters.ibkr.broker import IBKRBroker
from core.adapters.ibkr.client import IBKRClient
from core.adapters.ibkr.data import IBKRDataProvider
from core.adapters.polygon.data import PolygonDataProvider
from core.adapters.csv.data import CSVDataProvider
from core.audit import AuditLogger, configure_runtime_logging
from core.exceptions import ConfigError
from core.privacy import build_strategy_aliases, is_customer_profile
from core.risk.policy import RiskPolicy
from core.engine.loader import get_registry
from core.orders.strategy_modes import strategy_mode_map, validate_strategy_modes
from core.startup import PositionOwnershipLedger
from api.server import start_control_api_thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DEFAULT_CONFIG_PATH = "config.yaml"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trading runtime")
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"YAML deployment config (default: {DEFAULT_CONFIG_PATH})",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--paper", action="store_true", help="IBKR paper account")
    mode.add_argument("--live", action="store_true", help="IBKR live account")
    parser.add_argument("--gateway", action="store_true", help="IB Gateway instead of TWS")
    parser.add_argument("--host", default=None, help="Override TWS/Gateway host")
    parser.add_argument("--client-id", type=int, default=None, help="Override IBKR API client ID")
    parser.add_argument("--account", default=None, help="Override IBKR account ID")
    parser.add_argument("--strategy", default=None, help="Run one strategy by id")
    parser.add_argument("--lookback-days", type=int, default=None)
    api_mode = parser.add_mutually_exclusive_group()
    api_mode.add_argument(
        "--api",
        action="store_true",
        help="Enable read-only control API (default)",
    )
    api_mode.add_argument(
        "--no-api",
        action="store_true",
        help="Disable read-only control API",
    )
    parser.add_argument("--api-host", default=None, help="Control API host override")
    parser.add_argument("--api-port", type=int, default=None, help="Control API port override")
    parser.add_argument("--api-token-env", default=None, help="Bearer token environment variable")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = _config_from_args(args)
    strategy_packages = _strategy_packages(config)
    load_strategies(strategy_packages)
    registry = get_registry()
    strategy_ids = config.get("strategies") or list(registry.keys())
    if isinstance(strategy_ids, str):
        strategy_ids = [strategy_ids]
    strategy_ids = [str(strategy_id) for strategy_id in strategy_ids]
    validate_strategy_modes(config.get("strategy_modes"), registry.keys())
    strategy_modes = strategy_mode_map(config.get("strategy_modes"), strategy_ids)
    metadata_profile = _metadata_profile(config)
    strategy_aliases = build_strategy_aliases(
        strategy_ids,
        config.get("strategy_aliases") if isinstance(config.get("strategy_aliases"), dict) else {},
    )
    audit_logger = AuditLogger.from_config(config, strategy_aliases=strategy_aliases)
    logging_cfg = dict(config.get("logging") or {})
    if audit_logger is not None:
        configure_runtime_logging(
            log_dir=audit_logger.log_dir,
            level=str(logging_cfg.get("runtime_level", "INFO")),
            enabled=audit_logger.enabled,
            profile=metadata_profile,
            strategy_aliases=strategy_aliases,
        )

    strategies = []
    for sid in strategy_ids:
        if sid not in registry:
            print(f"Unknown strategy '{sid}'. Available: {list(registry.keys())}")
            sys.exit(1)
        strategies.append((registry[sid](), {}))

    broker, shared = _build_broker(config)
    data_feed = _build_data_feed(config, shared)

    print(f"Execution: {broker.name}")
    print(f"IBKR environment: {config.get('mode', '')}")
    print(f"Data hist/live: {_provider_name(config, 'historical')} / {_provider_name(config, 'live')}")
    if is_customer_profile(metadata_profile):
        safe_ids = [strategy_aliases.get(strategy_id, "strategy") for strategy_id in strategy_ids]
        safe_modes = {
            strategy_aliases.get(strategy_id, "strategy"): mode
            for strategy_id, mode in strategy_modes.items()
        }
        print(f"Strategies: {safe_ids}")
        print(f"Strategy modes: {safe_modes}")
        print(f"Runtime profile: {metadata_profile}")
    else:
        print(f"Strategies: {strategy_ids}")
        print(f"Strategy modes: {strategy_modes}")

    default_risk = RiskPolicy(
        position_size_shares=int(config.get("position_size_shares", 1)),
        max_order_quantity=_optional_int(config.get("max_order_quantity", 2)),
    )
    engine = Engine(
        broker=broker,
        data_feed=data_feed,
        clock=WallClock(),
        strategies=strategies,
        risk=default_risk,
        strategy_risk=_strategy_risk(config, strategy_ids, default_risk),
        thread_pool_workers=int(config.get("thread_pool_workers", 4)),
        lookback_days=int(config.get("lookback_days", 500)),
        session_tz=str(config.get("session_timezone", "America/New_York")),
        adopted_position_map=_adopted_position_map(config),
        startup_position_allocations=_adopted_position_allocations(config),
        startup_position_mapping_enabled=_startup_mapping_enabled(config),
        ownership_ledger=PositionOwnershipLedger.from_config(config),
        audit_logger=audit_logger,
        strategy_modes=strategy_modes,
        metadata_profile=metadata_profile,
        strategy_aliases=strategy_aliases,
        startup_position_gate_enabled=str(config.get("mode", "")).lower() == "live",
    )
    api_server = _start_control_api(
        config,
        engine,
        strategy_ids,
        strategy_modes,
        metadata_profile=metadata_profile,
        strategy_aliases=strategy_aliases,
    )

    print("Running. Press Ctrl+C to stop.")
    try:
        engine.run_live()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        if api_server is not None:
            api_server.stop()


def _legacy_cli_config(args: argparse.Namespace) -> dict[str, Any]:
    if args.paper:
        port = 4002 if args.gateway else 7497
        mode = "paper"
    else:
        port = 4001 if args.gateway else 7496
        mode = "live"
    strategy_ids = [args.strategy] if args.strategy else None
    return {
        "mode": mode,
        "lookback_days": args.lookback_days or 500,
        "strategies": strategy_ids,
        "execution": {
            "provider": "ibkr",
            "host": args.host or "127.0.0.1",
            "port": port,
            "client_id": args.client_id or 1,
            "account": args.account or "",
        },
        "data": {
            "historical": {"provider": "ibkr"},
            "live": {"provider": "ibkr"},
        },
        "api": {
            "enabled": not bool(getattr(args, "no_api", False)),
            "host": "127.0.0.1",
            "port": 8550,
            "token_env": "IBKR_LT_API_TOKEN",
        },
    }


def _config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    if not args.config:
        return _legacy_cli_config(args)

    config = _load_yaml(args.config)
    if "mode" in config:
        raise ValueError("YAML config must not define mode; use --paper or --live")
    if "dry_run" in config:
        raise ValueError(
            "YAML config must not define dry_run; use strategy_modes.<strategy_id>: dry_run"
        )

    mode = "paper" if args.paper else "live"
    port = _ibkr_port(mode, args.gateway)
    config["mode"] = mode
    if args.lookback_days is not None:
        config["lookback_days"] = args.lookback_days
    else:
        config.setdefault("lookback_days", 500)

    if args.strategy:
        config["strategies"] = [args.strategy]
    api = config.get("api")
    if not isinstance(api, dict):
        api = {}
        config["api"] = api
    api.setdefault("enabled", True)
    if getattr(args, "api", False):
        api["enabled"] = True
    if getattr(args, "no_api", False):
        api["enabled"] = False
    if args.api_host is not None:
        api["host"] = args.api_host
    if args.api_port is not None:
        api["port"] = args.api_port
    if args.api_token_env is not None:
        api["token_env"] = args.api_token_env

    execution = config.setdefault("execution", {})
    if execution.get("provider", "ibkr") == "ibkr":
        if args.host is not None:
            execution["host"] = args.host
        else:
            execution.setdefault("host", "127.0.0.1")
        execution.setdefault("port", port)
        if args.client_id is not None:
            execution["client_id"] = args.client_id
        else:
            execution.setdefault("client_id", 1)
        if args.account is not None:
            execution["account"] = args.account
        else:
            execution.setdefault("account", "")

    data = config.setdefault("data", {})
    for key in ("historical", "live"):
        provider_cfg = data.setdefault(key, {"provider": "ibkr"})
        if provider_cfg.get("provider", "ibkr") == "ibkr":
            if args.host is not None:
                provider_cfg["host"] = args.host
            else:
                provider_cfg.setdefault("host", execution.get("host", "127.0.0.1"))
            provider_cfg.setdefault("port", execution.get("port", port))
            if args.client_id is not None:
                provider_cfg["client_id"] = args.client_id
            else:
                provider_cfg.setdefault("client_id", execution.get("client_id", 1))

    return config


def _ibkr_port(mode: str, gateway: bool) -> int:
    if mode == "paper":
        return 4002 if gateway else 7497
    return 4001 if gateway else 7496


def _load_yaml(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required for --config. Install requirements.txt") from exc
    with Path(path).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _build_broker(config: dict[str, Any]):
    execution = dict(config.get("execution") or {})
    provider = execution.get("provider", "ibkr")
    shared: dict[str, Any] = {}
    if provider == "ibkr":
        account = str(execution.get("account", "")).strip()
        if not account and str(config.get("mode", "")).lower() != "paper":
            raise ConfigError("IBKR live environment requires execution.account or --account")
        client = IBKRClient()
        shared["ibkr_client"] = client
        broker = IBKRBroker(
            client,
            account=account,
            host=str(execution.get("host", "127.0.0.1")),
            port=int(execution.get("port", 7497)),
            client_id=int(execution.get("client_id", 1)),
        )
        return broker, shared
    raise ConfigError("main.py runtime execution provider must be ibkr")


def _build_data_feed(config: dict[str, Any], shared: dict[str, Any]) -> DataFeed:
    data = dict(config.get("data") or {})
    hist_cfg = dict(data.get("historical") or {"provider": "ibkr"})
    live_cfg = dict(data.get("live") or {"provider": hist_cfg.get("provider", "ibkr")})
    historical = _build_data_provider(hist_cfg, shared)
    live = _build_data_provider(live_cfg, shared)
    return DataFeed(historical, live)


def _build_data_provider(cfg: dict[str, Any], shared: dict[str, Any]):
    provider = cfg.get("provider", "ibkr")
    if provider == "ibkr":
        client = shared.get("ibkr_client") or IBKRClient()
        shared["ibkr_client"] = client
        return IBKRDataProvider(
            client,
            host=str(cfg.get("host", "127.0.0.1")),
            port=int(cfg.get("port", 7497)),
            client_id=int(cfg.get("client_id", 1)),
        )
    if provider == "polygon":
        api_key = cfg.get("api_key") or os.getenv(str(cfg.get("api_key_env", "POLYGON_API_KEY")))
        if not api_key:
            raise ValueError("Polygon provider requires api_key or api_key_env")
        return PolygonDataProvider(
            api_key=str(api_key),
            adjusted=bool(cfg.get("adjusted", False)),
        )
    if provider == "csv":
        return CSVDataProvider(
            cfg["path"],
            session_tz=str(cfg.get("timezone", "America/New_York")),
            rth_only=bool(cfg.get("rth_only", True)),
            market_open=str(cfg.get("market_open", "09:30")),
            market_close=str(cfg.get("market_close", "16:00")),
        )
    raise ValueError(f"Unknown data provider: {provider!r}")


def _provider_name(config: dict[str, Any], key: str) -> str:
    data = dict(config.get("data") or {})
    cfg = dict(data.get(key) or {})
    return str(cfg.get("provider", "ibkr"))


def _strategy_packages(config: dict[str, Any]) -> list[str]:
    configured = config.get("strategy_packages") or ["strategies"]
    if isinstance(configured, str):
        return [configured]
    return [str(item) for item in configured]


def _metadata_profile(config: dict[str, Any]) -> str:
    logging_cfg = dict(config.get("logging") or {})
    return str(config.get("runtime_profile") or logging_cfg.get("profile") or "owner")


def _startup_mapping_enabled(config: dict[str, Any]) -> bool:
    api_cfg = dict(config.get("api") or {})
    return bool(api_cfg.get("enabled", True))


def _adopted_position_map(config: dict[str, Any]) -> dict[Instrument, str]:
    result: dict[Instrument, str] = {}
    for item in config.get("adopted_positions", []) or []:
        instrument = Instrument(
            asset_class=item.get("asset_class", "future"),
            symbol=item["symbol"],
            exchange=item.get("exchange"),
            currency=item.get("currency"),
            expiry=_optional_date(item.get("expiry")),
            strike=_optional_float(item.get("strike")),
            right=item.get("right"),
            multiplier=float(item.get("multiplier", 1.0)),
        )
        result[instrument] = item["strategy_id"]
    return result


def _adopted_position_allocations(config: dict[str, Any]) -> list[dict[str, Any]]:
    allocations: list[dict[str, Any]] = []
    for item in config.get("adopted_positions", []) or []:
        allocation = dict(item)
        allocation.setdefault("source", "config")
        allocation["instrument"] = {
            "asset_class": allocation.get("asset_class", "equity"),
            "symbol": allocation.get("symbol"),
            "exchange": allocation.get("exchange"),
            "currency": allocation.get("currency"),
            "expiry": allocation.get("expiry"),
            "strike": allocation.get("strike"),
            "right": allocation.get("right"),
            "multiplier": allocation.get("multiplier", 1.0),
        }
        allocations.append(allocation)
    return allocations


def _strategy_risk(
    config: dict[str, Any],
    strategy_ids: Sequence[str],
    default_risk: RiskPolicy,
) -> dict[str, RiskPolicy]:
    configured = dict(config.get("strategy_risk") or {})
    result: dict[str, RiskPolicy] = {}
    for strategy_id in strategy_ids:
        item = configured.get(strategy_id)
        if not isinstance(item, dict):
            continue
        result[str(strategy_id)] = RiskPolicy(
            position_size_shares=int(
                item.get("position_size_shares", default_risk.position_size_shares)
            ),
            max_order_quantity=_optional_int(
                item.get("max_order_quantity", default_risk.max_order_quantity)
            ),
            sizing_mode=default_risk.sizing_mode,
            equity_fraction=default_risk.equity_fraction,
        )
    return result


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _optional_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    if len(text) == 6 and text.isdigit():
        return datetime.strptime(text + "01", "%Y%m%d").date()
    return date.fromisoformat(text)


def _start_control_api(
    config: dict[str, Any],
    engine: Engine,
    strategy_ids: list[str],
    strategy_modes: Mapping[str, str] | None = None,
    *,
    metadata_profile: str = "owner",
    strategy_aliases: Mapping[str, str] | None = None,
):
    api_cfg = dict(config.get("api") or {})
    if not bool(api_cfg.get("enabled", True)):
        return None
    host = str(api_cfg.get("host", "127.0.0.1"))
    port = int(api_cfg.get("port", 8550))
    server = start_control_api_thread(
        engine,
        host=host,
        port=port,
        token_env=str(api_cfg.get("token_env", "IBKR_LT_API_TOKEN")),
        log_level=str(api_cfg.get("log_level", "warning")),
        metadata=_api_metadata(
            config,
            strategy_ids,
            strategy_modes,
            metadata_profile=metadata_profile,
            strategy_aliases=strategy_aliases,
        ),
    )
    print(f"Control API: http://{host}:{port}")
    _warn_if_heartbeat_monitor_missing()
    return server


def _cmdline_is_heartbeat_monitor(cmdline: Sequence[str]) -> bool:
    normalized = [str(part).replace("\\", "/") for part in cmdline]
    for index, part in enumerate(normalized):
        if part == "-m" and index + 1 < len(normalized):
            if normalized[index + 1] == "tools.heartbeat_monitor":
                return True
        if part == "heartbeat_monitor.py" or part.endswith("/heartbeat_monitor.py"):
            return True
    return False


def _heartbeat_monitor_process_running(
    proc_root: Path = Path("/proc"),
    *,
    current_pid: int | None = None,
) -> bool | None:
    if not proc_root.exists():
        return None
    current_pid = os.getpid() if current_pid is None else int(current_pid)
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return None

    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            pid = int(entry.name)
        except ValueError:
            continue
        if pid == current_pid:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if not raw:
            continue
        cmdline = [
            part.decode("utf-8", errors="ignore")
            for part in raw.split(b"\0")
            if part
        ]
        if _cmdline_is_heartbeat_monitor(cmdline):
            return True
    return False


def _warn_if_heartbeat_monitor_missing(proc_root: Path = Path("/proc")) -> None:
    running = _heartbeat_monitor_process_running(proc_root)
    if running is not False:
        return
    print(
        "Warning: Heartbeat Monitor process is not detected. "
        "Start it in another terminal with: "
        "~/.venv/bin/python tools/heartbeat_monitor.py",
        file=sys.stderr,
    )


def _api_metadata(
    config: dict[str, Any],
    strategy_ids: list[str],
    strategy_modes: Mapping[str, str] | None = None,
    *,
    metadata_profile: str = "owner",
    strategy_aliases: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    data = dict(config.get("data") or {})
    modes = dict(strategy_modes or strategy_mode_map(config.get("strategy_modes"), strategy_ids))
    aliases = dict(strategy_aliases or {})
    if is_customer_profile(metadata_profile):
        safe_ids = [aliases.get(strategy_id, "strategy") for strategy_id in strategy_ids]
        modes = {aliases.get(strategy_id, "strategy"): mode for strategy_id, mode in modes.items()}
    else:
        safe_ids = list(strategy_ids)
    return {
        "mode": str(config.get("mode", "")),
        "runtime_profile": metadata_profile,
        "strategy_modes": modes,
        "strategies": safe_ids,
        "execution_provider": str(dict(config.get("execution") or {}).get("provider", "ibkr")),
        "historical_provider": str(dict(data.get("historical") or {}).get("provider", "ibkr")),
        "live_provider": str(dict(data.get("live") or {}).get("provider", "ibkr")),
    }


if __name__ == "__main__":
    main()

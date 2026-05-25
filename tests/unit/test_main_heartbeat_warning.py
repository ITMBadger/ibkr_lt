from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from core.exceptions import ConfigError
from core.observability import (
    cmdline_is_heartbeat_monitor,
    heartbeat_monitor_process_running,
    warn_if_heartbeat_monitor_missing,
)
from core.orders.strategy_modes import validate_strategy_modes
from main import (
    _api_metadata,
    _build_broker,
    _config_from_args,
    _legacy_cli_config,
    _start_control_api,
    _startup_mapping_enabled,
)


def _write_cmdline(proc_root: Path, pid: int, args: list[str]) -> None:
    proc_dir = proc_root / str(pid)
    proc_dir.mkdir(parents=True)
    proc_dir.joinpath("cmdline").write_bytes(b"\0".join(arg.encode() for arg in args))


def _args(**overrides):
    values = {
        "config": None,
        "paper": True,
        "live": False,
        "gateway": False,
        "host": None,
        "client_id": None,
        "account": None,
        "strategy": None,
        "lookback_days": None,
        "api": False,
        "no_api": False,
        "api_host": None,
        "api_port": None,
        "api_token_env": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_legacy_cli_config_enables_api_by_default():
    assert _legacy_cli_config(_args())["api"]["enabled"] is True


def test_legacy_cli_config_allows_no_api():
    assert _legacy_cli_config(_args(no_api=True))["api"]["enabled"] is False


def test_yaml_config_enables_api_when_api_block_is_missing(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("strategies: []\n", encoding="utf-8")

    config = _config_from_args(_args(config=str(config_path)))

    assert config["api"]["enabled"] is True


def test_yaml_config_respects_no_api_override(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("api:\n  enabled: true\nstrategies: []\n", encoding="utf-8")

    config = _config_from_args(_args(config=str(config_path), no_api=True))

    assert config["api"]["enabled"] is False


def test_yaml_config_api_flag_reenables_explicitly_disabled_api(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("api:\n  enabled: false\nstrategies: []\n", encoding="utf-8")

    config = _config_from_args(_args(config=str(config_path), api=True))

    assert config["api"]["enabled"] is True


def test_startup_mapping_enabled_accepts_api_or_dashboard():
    assert _startup_mapping_enabled({"api": {"enabled": True}}) is True
    assert _startup_mapping_enabled({"api": {"enabled": False}}) is False
    assert (
        _startup_mapping_enabled(
            {"api": {"enabled": False}},
            dashboard_active=True,
        )
        is True
    )


def test_yaml_config_rejects_legacy_global_dry_run(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("dry_run: true\nstrategies: []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="strategy_modes"):
        _config_from_args(_args(config=str(config_path)))


def test_api_metadata_includes_strategy_modes():
    metadata = _api_metadata(
        {"mode": "paper", "strategy_modes": {"a": "dry_run"}},
        ["a", "b"],
    )

    assert metadata["strategy_modes"] == {"a": "dry_run", "b": "live"}


def test_api_metadata_uses_normalized_strategy_modes_argument():
    metadata = _api_metadata(
        {"mode": "paper", "strategy_modes": {"a": "live"}},
        ["a"],
        {"a": "dry_run"},
    )

    assert metadata["strategy_modes"] == {"a": "dry_run"}


def test_strategy_modes_reject_unknown_strategy_id():
    with pytest.raises(ValueError, match="unknown strategy"):
        validate_strategy_modes({"missing": "live"}, ["known"])


def test_strategy_modes_reject_invalid_mode():
    with pytest.raises(ValueError, match="Invalid strategy mode"):
        validate_strategy_modes({"known": "paper"}, ["known"])


def test_ibkr_broker_requires_explicit_account():
    with pytest.raises(ConfigError, match="execution.account"):
        _build_broker({"execution": {"provider": "ibkr", "account": ""}})


def test_no_api_skips_heartbeat_monitor_warning(monkeypatch):
    def fail_if_called():
        raise AssertionError("heartbeat monitor warning should not run")

    monkeypatch.setattr("main.warn_if_heartbeat_monitor_missing", fail_if_called)

    assert _start_control_api({"api": {"enabled": False}}, engine=None, strategy_ids=[]) is None


def test_cmdline_detects_heartbeat_monitor_script_and_module():
    assert cmdline_is_heartbeat_monitor([
        "python",
        "tools/heartbeat_monitor.py",
    ])
    assert cmdline_is_heartbeat_monitor(["python", "-m", "tools.heartbeat_monitor"])
    assert not cmdline_is_heartbeat_monitor([
        "pytest",
        "tests/unit/test_heartbeat_monitor.py",
    ])


def test_process_scan_detects_running_monitor(tmp_path: Path):
    _write_cmdline(tmp_path, 100, ["python", "tools/heartbeat_monitor.py"])

    assert heartbeat_monitor_process_running(tmp_path, current_pid=1) is True


def test_process_scan_ignores_current_process_and_test_file(tmp_path: Path):
    _write_cmdline(tmp_path, 1, ["python", "tools/heartbeat_monitor.py"])
    _write_cmdline(tmp_path, 2, ["pytest", "tests/unit/test_heartbeat_monitor.py"])

    assert heartbeat_monitor_process_running(tmp_path, current_pid=1) is False


def test_process_scan_returns_none_when_unavailable(tmp_path: Path):
    assert heartbeat_monitor_process_running(tmp_path / "missing") is None


def test_missing_monitor_warning_uses_stderr(tmp_path: Path, capsys):
    warn_if_heartbeat_monitor_missing(tmp_path)

    captured = capsys.readouterr()
    assert "Heartbeat Monitor process is not detected" in captured.err
    assert "tools/heartbeat_monitor.py" in captured.err

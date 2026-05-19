from __future__ import annotations

from pathlib import Path

from tools.heartbeat_monitor import (
    AlertEmitter,
    MonitorConfig,
    MonitorState,
    api_url,
    evaluate_health,
    websocket_url,
    write_json_atomic,
)


def test_monitor_url_helpers():
    assert api_url("http://127.0.0.1:8550/", "/api/v1/health") == (
        "http://127.0.0.1:8550/api/v1/health"
    )
    assert websocket_url("http://127.0.0.1:8550", "/ws/events", token="secret") == (
        "ws://127.0.0.1:8550/ws/events?token=secret"
    )
    assert websocket_url("https://example.test/api", "/ws/events") == (
        "wss://example.test/api/ws/events"
    )


def test_evaluate_health_respects_expectations():
    healthy = {"status": "ok", "phase": "running", "running": True, "connected": True}
    assert evaluate_health(healthy, expect_running=True, expect_connected=True) == {}

    stopped = {"status": "ok", "phase": "stopped", "running": False, "connected": False}
    assert evaluate_health(stopped, expect_running=False, expect_connected=True) == {}
    assert "engine_not_running" in evaluate_health(
        stopped,
        expect_running=True,
        expect_connected=False,
    )

    disconnected = {"status": "ok", "phase": "running", "running": True, "connected": False}
    assert "engine_not_connected" in evaluate_health(
        disconnected,
        expect_running=True,
        expect_connected=True,
    )

    errored = {"status": "ok", "phase": "error", "running": False, "connected": False}
    assert "engine_error" in evaluate_health(
        errored,
        expect_running=False,
        expect_connected=False,
    )


def test_alert_emitter_deduplicates_and_resolves(tmp_path: Path, capsys):
    alert_file = tmp_path / "alerts.jsonl"
    state = MonitorState()
    config = MonitorConfig(alert_file=alert_file, status_file=None, json_stdout=True)
    emitter = AlertEmitter(config, state)

    emitter.emit_once(severity="warning", code="api_health_unreachable", message="down")
    emitter.emit_once(severity="warning", code="api_health_unreachable", message="down")
    emitter.resolve("api_health_unreachable", "up")

    lines = alert_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert "api_health_unreachable" not in state.active_alerts
    assert "api_health_unreachable_resolved" in lines[1]

    stdout_lines = capsys.readouterr().out.strip().splitlines()
    assert len(stdout_lines) == 2


def test_write_json_atomic_creates_parent(tmp_path: Path):
    path = tmp_path / "var" / "heartbeat_monitor" / "status.json"

    write_json_atomic(path, {"service": "heartbeat_monitor"})

    assert path.exists()
    assert "heartbeat_monitor" in path.read_text(encoding="utf-8")

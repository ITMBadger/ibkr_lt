"""Unit tests for the read-only control API."""

from __future__ import annotations

from fastapi.testclient import TestClient

from api.app import create_control_api_app


class _SnapshotEngine:
    def __init__(self) -> None:
        self.snapshot = {
            "phase": "running",
            "running": True,
            "connection": {
                "broker_connected": True,
                "data_connected": True,
                "connected": True,
            },
            "positions": {
                "broker": [{"instrument": {"symbol": "MES"}, "quantity": 1.0}],
                "strategy": [],
                "net_liquidation": 100_000.0,
            },
            "strategies": [{"id": "example_strategy"}],
            "recent_events": [
                {"timestamp": "2026-05-19T00:00:00+00:00", "source": "test", "message": "ready"}
            ],
        }

    def snapshot_state(self) -> dict:
        return dict(self.snapshot)


def test_health_and_meta_are_public():
    client = TestClient(
        create_control_api_app(
            _SnapshotEngine(),
            api_token="secret",
            metadata={"mode": "paper", "dry_run": True},
        )
    )

    health = client.get("/api/v1/health")
    meta = client.get("/api/v1/meta")
    capabilities = client.get("/api/v1/meta/capabilities")

    assert health.status_code == 200
    assert health.json()["connected"] is True
    assert health.json()["mode"] == "paper"
    assert health.json()["dry_run"] is True
    assert meta.status_code == 200
    assert meta.json()["service"] == "ibkr_lt_control_api"
    assert capabilities.status_code == 200
    assert capabilities.json()["manual_trade"] is False


def test_runtime_snapshot_requires_bearer_token_when_configured():
    client = TestClient(create_control_api_app(_SnapshotEngine(), api_token="secret"))

    assert client.get("/api/v1/runtime/snapshot").status_code == 401
    assert client.get(
        "/api/v1/runtime/snapshot",
        headers={"Authorization": "Bearer wrong"},
    ).status_code == 403

    response = client.get(
        "/api/v1/runtime/snapshot",
        headers={"Authorization": "Bearer secret"},
    )

    assert response.status_code == 200
    assert response.json()["phase"] == "running"


def test_positions_and_events_use_engine_snapshot():
    client = TestClient(create_control_api_app(_SnapshotEngine()))

    positions = client.get("/api/v1/positions")
    events = client.get("/api/v1/events?limit=1")

    assert positions.status_code == 200
    assert positions.json()["broker"][0]["instrument"]["symbol"] == "MES"
    assert events.status_code == 200
    assert events.json()[0]["message"] == "ready"


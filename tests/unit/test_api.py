"""Unit tests for the read-only control API."""

from __future__ import annotations

import pytest
import httpx

from api.app import create_control_api_app
from api.server import is_local_control_host, resolve_control_api_token
from core.operator import OperatorService
from dashboard.loader import load_dashboard_plugin, mount_dashboard_plugin


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

    def startup_gate_status(self) -> dict:
        return {
            "enabled": True,
            "phase": "awaiting_mapping",
            "positions": [{"position_id": "position:equity:QQQ:long"}],
            "allocations": [],
            "unmanaged": [],
        }

    def submit_startup_mappings(
        self,
        allocations: list[dict],
        *,
        ack_unmanaged_remainders: list[dict] | None = None,
    ) -> dict:
        if not allocations:
            raise ValueError("at least one allocation is required")
        return {
            **self.startup_gate_status(),
            "allocations": allocations,
            "unmanaged_remainder_acknowledgements": list(ack_unmanaged_remainders or []),
        }

    def request_startup_gate_refresh(self) -> dict:
        return {
            **self.startup_gate_status(),
            "message": "Startup position refresh requested.",
        }


def test_operator_service_delegates_runtime_and_startup_controls():
    engine = _SnapshotEngine()
    operator = OperatorService(engine, metadata={"mode": "paper"})

    assert operator.runtime_snapshot()["metadata"] == {"mode": "paper"}
    assert operator.positions()["broker"][0]["instrument"]["symbol"] == "MES"
    assert operator.events(limit=1)[0]["message"] == "ready"
    assert operator.startup_gate_status()["phase"] == "awaiting_mapping"
    assert operator.request_startup_gate_refresh()["message"] == "Startup position refresh requested."
    mapped = operator.submit_startup_mappings([
        {
            "position_id": "position:equity:QQQ:long",
            "strategy_id": "example_live_strategy",
            "quantity": 1,
        }
    ])
    assert mapped["allocations"][0]["quantity"] == 1


def test_dashboard_loader_skips_missing_module():
    result = load_dashboard_plugin({"dashboard": {"module": "_missing_dashboard_for_test"}})

    assert result.plugin is None
    assert result.status.active is False
    assert result.status.reason == "dashboard_module_not_found"


@pytest.mark.anyio
async def test_dashboard_loader_mounts_licensed_plugin(tmp_path, monkeypatch):
    package = tmp_path / "licensed_dashboard"
    package.mkdir()
    package.joinpath("__init__.py").write_text(
        "\n".join([
            "class Plugin:",
            "    def status(self):",
            "        return {'available': True, 'licensed': True, 'reason': ''}",
            "    def mount(self, app, operator_service, *, config, metadata):",
            "        @app.get('/dashboard-test')",
            "        async def dashboard_test():",
            "            return {'phase': operator_service.snapshot_state()['phase']}",
            "def get_dashboard_plugin():",
            "    return Plugin()",
        ]),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    app = create_control_api_app(_SnapshotEngine())

    result = load_dashboard_plugin({"dashboard": {"module": "licensed_dashboard"}})
    status = mount_dashboard_plugin(
        app,
        result,
        app.state.operator_service,
        config={},
        metadata={},
    )

    assert status.active is True
    async for client in _client(app):
        response = await client.get("/dashboard-test")
        assert response.status_code == 200
        assert response.json() == {"phase": "running"}


def test_dashboard_loader_skips_expired_plugin(tmp_path, monkeypatch):
    package = tmp_path / "expired_dashboard"
    package.mkdir()
    package.joinpath("__init__.py").write_text(
        "\n".join([
            "class Plugin:",
            "    def status(self):",
            "        return {'available': True, 'licensed': False, 'reason': 'license_expired'}",
            "    def mount(self, app, operator_service, *, config, metadata):",
            "        raise AssertionError('expired dashboard should not mount')",
            "def get_dashboard_plugin():",
            "    return Plugin()",
        ]),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = load_dashboard_plugin({"dashboard": {"module": "expired_dashboard"}})

    assert result.plugin is None
    assert result.status.licensed is False
    assert result.status.reason == "license_expired"


def test_dashboard_loader_treats_import_error_as_nonfatal(tmp_path, monkeypatch):
    package = tmp_path / "broken_dashboard"
    package.mkdir()
    package.joinpath("__init__.py").write_text(
        "raise RuntimeError('boom')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = load_dashboard_plugin({"dashboard": {"module": "broken_dashboard"}})

    assert result.plugin is None
    assert result.status.active is False
    assert result.status.reason.startswith("import_error:")


async def _client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.mark.anyio
async def test_health_and_meta_are_public():
    app = create_control_api_app(
        _SnapshotEngine(),
        api_token="secret",
        metadata={"mode": "paper", "strategy_modes": {"example_strategy": "dry_run"}},
    )
    async for client in _client(app):
        health = await client.get("/api/v1/health")
        meta = await client.get("/api/v1/meta")
        capabilities = await client.get("/api/v1/meta/capabilities")

        assert health.status_code == 200
        assert health.json()["connected"] is True
        assert health.json()["mode"] == "paper"
        assert health.json()["strategy_modes"] == {"example_strategy": "dry_run"}
        assert meta.status_code == 200
        assert meta.json()["service"] == "ibkr_lt_control_api"
        assert capabilities.status_code == 200
        assert capabilities.json()["manual_trade"] is False


@pytest.mark.anyio
async def test_runtime_snapshot_requires_bearer_token_when_configured():
    app = create_control_api_app(_SnapshotEngine(), api_token="secret")
    async for client in _client(app):
        assert (await client.get("/api/v1/runtime/snapshot")).status_code == 401
        assert (
            await client.get(
                "/api/v1/runtime/snapshot",
                headers={"Authorization": "Bearer wrong"},
            )
        ).status_code == 403

        response = await client.get(
            "/api/v1/runtime/snapshot",
            headers={"Authorization": "Bearer secret"},
        )

        assert response.status_code == 200
        assert response.json()["phase"] == "running"


@pytest.mark.anyio
async def test_positions_and_events_use_engine_snapshot():
    app = create_control_api_app(_SnapshotEngine())
    async for client in _client(app):
        positions = await client.get("/api/v1/positions")
        events = await client.get("/api/v1/events?limit=1")

        assert positions.status_code == 200
        assert positions.json()["broker"][0]["instrument"]["symbol"] == "MES"
        assert events.status_code == 200
        assert events.json()[0]["message"] == "ready"


@pytest.mark.anyio
async def test_startup_gate_endpoints_require_auth_and_use_engine():
    app = create_control_api_app(_SnapshotEngine(), api_token="secret")
    async for client in _client(app):
        assert (await client.get("/api/v1/startup/gate")).status_code == 401

        headers = {"Authorization": "Bearer secret"}
        gate = await client.get("/api/v1/startup/gate", headers=headers)
        assert gate.status_code == 200
        assert gate.json()["phase"] == "awaiting_mapping"

        bad = await client.post(
            "/api/v1/startup/mappings",
            json={"allocations": []},
            headers=headers,
        )
        assert bad.status_code == 400

        mapped = await client.post(
            "/api/v1/startup/mappings",
            json={
                "allocations": [
                    {
                        "position_id": "position:equity:QQQ:long",
                        "strategy_id": "example_live_strategy",
                        "quantity": 1,
                        "entry_ts": "2026-05-25T10:18:00-04:00",
                    }
                ],
                "ack_unmanaged_remainders": [
                    {
                        "position_id": "position:equity:QQQ:long",
                        "quantity": 1,
                        "reason": "operator_acknowledged_unmanaged_remainder",
                    }
                ],
            },
            headers=headers,
        )
        assert mapped.status_code == 200
        assert mapped.json()["allocations"][0]["strategy_id"] == "example_live_strategy"
        assert mapped.json()["unmanaged_remainder_acknowledgements"][0]["quantity"] == 1

        refresh = await client.post("/api/v1/startup/refresh", headers=headers)
        assert refresh.status_code == 200
        assert refresh.json()["message"] == "Startup position refresh requested."


def test_local_control_hosts_can_run_without_token(monkeypatch):
    monkeypatch.delenv("IBKR_LT_API_TOKEN", raising=False)

    assert is_local_control_host("127.0.0.1") is True
    assert is_local_control_host("localhost") is True
    assert is_local_control_host("::1") is True
    assert resolve_control_api_token(
        host="127.0.0.1",
        token_env="IBKR_LT_API_TOKEN",
    ) == ""


def test_non_local_control_host_requires_token(monkeypatch):
    monkeypatch.delenv("IBKR_LT_API_TOKEN", raising=False)

    assert is_local_control_host("0.0.0.0") is False
    with pytest.raises(ValueError, match="non-local host"):
        resolve_control_api_token(host="0.0.0.0", token_env="IBKR_LT_API_TOKEN")


def test_non_local_control_host_accepts_token(monkeypatch):
    monkeypatch.setenv("IBKR_LT_API_TOKEN", "secret")

    assert resolve_control_api_token(
        host="0.0.0.0",
        token_env="IBKR_LT_API_TOKEN",
    ) == "secret"

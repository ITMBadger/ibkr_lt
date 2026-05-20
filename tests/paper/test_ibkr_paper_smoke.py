"""IBKR paper smoke tests for connection, data, engine startup, and API."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from core.engine.timeframes import TF_1M, TF_5S

from .conftest import (
    detect_paper_account,
    read_json_url,
    utc_now,
    wait_for_stdout,
)

pytestmark = pytest.mark.paper


def test_ibkr_paper_account_positions_and_history(
    run_paper_scenario,
    paper_config,
    qqq,
) -> None:
    async def scenario(stack) -> None:
        account_id = await detect_paper_account(stack.broker, paper_config.account)
        positions = await stack.broker.get_positions()
        end = utc_now()
        bars = await stack.data.fetch(qqq, TF_1M, end - timedelta(days=3), end)

        assert account_id.startswith("DU")
        assert isinstance(positions, list)
        assert bars
        assert all(bar.instrument == qqq for bar in bars)
        assert all(bar.timeframe == TF_1M for bar in bars)
        assert all(bar.source == "ibkr" for bar in bars)

    run_paper_scenario(scenario, client_id_offset=0)


def test_ibkr_paper_realtime_subscription_smoke(
    run_paper_scenario,
    paper_config,
    qqq,
) -> None:
    async def scenario(stack) -> None:
        await stack.data.subscribe(qqq, TF_5S)
        stream = stack.data.bars()
        try:
            import asyncio

            bar = await asyncio.wait_for(stream.__anext__(), timeout=paper_config.realtime_timeout)
        except TimeoutError:
            pytest.skip("no realtime IBKR 5-second bar arrived before timeout")

        assert bar.instrument == qqq
        assert bar.timeframe == TF_5S
        assert bar.source == "ibkr"
        assert bar.volume >= 0

    run_paper_scenario(scenario, client_id_offset=1)


def test_main_py_paper_startup_and_control_api(main_py_runner, paper_config) -> None:
    proc = main_py_runner()
    output = wait_for_stdout(proc, "Subscribed realtime bars", timeout=60)
    assert "Execution: ibkr" in output
    assert "Strategies: ['stoch_3m_cross_long']" in output

    health = json.loads(
        read_json_url(f"http://127.0.0.1:{paper_config.api_port}/api/v1/health")
    )
    assert health["status"] == "ok"
    assert health["phase"] == "running"
    assert health["mode"] == "paper"
    assert health["dry_run"] is False

    snapshot = json.loads(
        read_json_url(f"http://127.0.0.1:{paper_config.api_port}/api/v1/runtime/snapshot")
    )
    assert snapshot["connection"]["connected"] is True
    assert snapshot["metadata"]["strategies"] == ["stoch_3m_cross_long"]
    assert snapshot["data"]["instruments"][0]["symbol"] == paper_config.symbol

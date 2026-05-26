"""Shared fixtures for opt-in IBKR paper-account tests.

These tests intentionally talk to a running TWS/IB Gateway paper session.
They are skipped unless IBKR_LT_RUN_PAPER_TESTS=1 is set.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import TypeVar
from zoneinfo import ZoneInfo

import pytest

from core.adapters.ibkr.broker import IBKRBroker
from core.adapters.ibkr.client import IBKRClient
from core.adapters.ibkr.data import IBKRDataProvider
from core.types import Instrument, OrderRequest

T = TypeVar("T")


@dataclass(frozen=True)
class PaperConfig:
    host: str
    port: int
    client_id: int
    account: str
    symbol: str
    api_port: int
    realtime_timeout: float
    order_timeout: float
    market_order_qty: int
    allow_market_orders: bool


@dataclass
class PaperStack:
    client: IBKRClient
    broker: IBKRBroker
    data: IBKRDataProvider
    config: PaperConfig


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "paper: opt-in tests that connect to a live TWS/IB Gateway paper session",
    )


@pytest.fixture(scope="session")
def paper_config() -> PaperConfig:
    if os.getenv("IBKR_LT_RUN_PAPER_TESTS") != "1":
        pytest.skip("set IBKR_LT_RUN_PAPER_TESTS=1 to run TWS paper-account tests")

    host = os.getenv("IBKR_LT_PAPER_HOST", "127.0.0.1")
    port = int(os.getenv("IBKR_LT_PAPER_PORT", "7497"))
    if port in {4001, 7496}:
        pytest.fail(f"refusing to run paper tests against live IBKR port {port}")

    account = os.getenv("IBKR_LT_PAPER_ACCOUNT", "").strip()
    if account and not account.upper().startswith("DU"):
        pytest.fail("IBKR_LT_PAPER_ACCOUNT must be a paper account starting with DU")

    qty = int(os.getenv("IBKR_LT_PAPER_MARKET_QTY", "1"))
    if qty != 1:
        pytest.fail("paper tests currently require IBKR_LT_PAPER_MARKET_QTY=1")

    return PaperConfig(
        host=host,
        port=port,
        client_id=int(os.getenv("IBKR_LT_PAPER_CLIENT_ID", "91")),
        account=account,
        symbol=os.getenv("IBKR_LT_PAPER_SYMBOL", "QQQ").upper(),
        api_port=int(os.getenv("IBKR_LT_PAPER_API_PORT", "8560")),
        realtime_timeout=float(os.getenv("IBKR_LT_PAPER_REALTIME_TIMEOUT", "20")),
        order_timeout=float(os.getenv("IBKR_LT_PAPER_ORDER_TIMEOUT", "45")),
        market_order_qty=qty,
        allow_market_orders=os.getenv("IBKR_LT_ALLOW_PAPER_MARKET_ORDERS") == "1",
    )


@pytest.fixture
def qqq(paper_config: PaperConfig) -> Instrument:
    return Instrument(asset_class="equity", symbol=paper_config.symbol)


@pytest.fixture
def run_paper_scenario(paper_config: PaperConfig):
    def run(
        scenario: Callable[[PaperStack], Awaitable[T]],
        *,
        client_id_offset: int = 0,
    ) -> T:
        async def runner() -> T:
            client = IBKRClient()
            client_id = paper_config.client_id + client_id_offset
            broker = IBKRBroker(
                client,
                account=paper_config.account,
                host=paper_config.host,
                port=paper_config.port,
                client_id=client_id,
            )
            data = IBKRDataProvider(
                client,
                host=paper_config.host,
                port=paper_config.port,
                client_id=client_id,
            )
            stack = PaperStack(client=client, broker=broker, data=data, config=paper_config)
            try:
                await broker.connect()
                return await scenario(stack)
            finally:
                try:
                    await data.disconnect()
                finally:
                    await broker.disconnect()

        return asyncio.run(runner())

    return run


@pytest.fixture
def main_py_runner(paper_config: PaperConfig):
    processes: list[subprocess.Popen[str]] = []

    def start(*extra_args: str) -> subprocess.Popen[str]:
        args = [
            sys.executable,
            "-u",
            "main.py",
            "--paper",
            "--client-id",
            str(paper_config.client_id + 50),
            "--api-port",
            str(paper_config.api_port),
        ]
        if paper_config.account:
            args.extend(["--account", paper_config.account])
        args.extend(extra_args)
        proc = subprocess.Popen(
            args,
            cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(proc)
        return proc

    yield start

    for proc in processes:
        stop_process(proc)


def stop_process(proc: subprocess.Popen[str], timeout: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def paper_market_is_open() -> bool:
    now = datetime.now(tz=ZoneInfo("America/New_York"))
    if now.weekday() >= 5:
        return False
    # Keep a buffer after open and before close so market orders and cleanup
    # have a normal RTH fill path.
    return dtime(9, 35) <= now.time() <= dtime(15, 50)


def require_paper_market_orders(config: PaperConfig) -> None:
    if not config.allow_market_orders:
        pytest.skip("set IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1 to place paper market orders")
    if not paper_market_is_open():
        pytest.skip("paper market order tests require US equity RTH, 09:35-15:50 ET")


async def detect_paper_account(broker: IBKRBroker, expected: str = "") -> str:
    snapshot = await broker.get_account()
    account_id = snapshot.account_id
    if expected:
        assert account_id == expected
    assert account_id.upper().startswith("DU")
    return account_id


async def actual_position_qty(broker: IBKRBroker, instrument: Instrument) -> float:
    positions = await broker.get_positions()
    for position in positions:
        if paper_instruments_match(position.instrument, instrument):
            return position.quantity
    return 0.0


def paper_instruments_match(left: Instrument, right: Instrument) -> bool:
    if left == right:
        return True
    if left.asset_class != right.asset_class or left.symbol != right.symbol:
        return False
    if left.asset_class != "future":
        return False
    if left.currency and right.currency and left.currency != right.currency:
        return False
    if left.expiry and right.expiry:
        left_month = (left.expiry.year, left.expiry.month)
        right_month = (right.expiry.year, right.expiry.month)
        if left_month != right_month:
            return False
    if left.multiplier != 1.0 and right.multiplier != 1.0:
        return left.multiplier == right.multiplier
    return True


async def cancel_quietly(broker: IBKRBroker, order_id: str | None) -> None:
    if not order_id:
        return
    try:
        await broker.cancel_order(order_id)
    except Exception:
        pass


async def flatten_delta(
    broker: IBKRBroker,
    instrument: Instrument,
    initial_qty: float,
    *,
    strategy_id: str,
) -> None:
    current_qty = await actual_position_qty(broker, instrument)
    delta = current_qty - initial_qty
    if abs(delta) < 0.5:
        return
    side = "short" if delta > 0 else "long"
    order = OrderRequest(
        instrument=instrument,
        side=side,
        quantity=abs(delta),
        order_type="market",
        strategy_id=strategy_id,
        idempotency_key=f"{strategy_id}-{instrument.symbol}-cleanup",
    )
    await broker.submit_order(order)
    deadline = asyncio.get_running_loop().time() + 30
    while asyncio.get_running_loop().time() < deadline:
        if abs((await actual_position_qty(broker, instrument)) - initial_qty) < 0.5:
            return
        await asyncio.sleep(1)
    raise AssertionError(
        f"cleanup did not restore {instrument.symbol} position to {initial_qty}"
    )


async def wait_for_position(
    broker: IBKRBroker,
    instrument: Instrument,
    predicate: Callable[[float], bool],
    *,
    timeout: float,
) -> float:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        qty = await actual_position_qty(broker, instrument)
        if predicate(qty):
            return qty
        await asyncio.sleep(1)
    raise AssertionError(f"position condition not reached for {instrument.symbol}")


async def wait_for_manager_role(manager, role: str, *, timeout: float) -> list[str]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        ids = [oid for oid, seen_role in manager._order_role.items() if seen_role == role]
        if ids:
            return ids
        await asyncio.sleep(0.2)
    raise AssertionError(f"OrderManager did not record role={role!r}")


async def wait_for_file(path: Path, needle: str, *, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if path.exists() and needle in path.read_text(encoding="utf-8"):
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"{needle!r} not found in {path}")


def wait_for_stdout(
    proc: subprocess.Popen[str],
    needle: str,
    *,
    timeout: float,
) -> str:
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout
    output: list[str] = []
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if line:
            output.append(line)
            if needle in line:
                return "".join(output)
        elif proc.poll() is not None:
            break
        else:
            time.sleep(0.1)
    raise AssertionError(f"{needle!r} not seen before timeout. Output:\n{''.join(output)}")


def read_json_url(url: str, *, timeout: float = 5.0) -> str:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                return response.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            time.sleep(0.2)
    raise AssertionError(f"could not read {url}: {last_error}")


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)

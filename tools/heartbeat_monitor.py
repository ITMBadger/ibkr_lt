"""Read-only heartbeat monitor process for the Hermes agent.

The monitor is intentionally outside the trading runtime. It watches the
ibkr_lt API, writes local status/alert files for an agent to consume, and never
calls broker adapters, strategies, or order-management code directly.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

import websockets


DEFAULT_API_URL = "http://127.0.0.1:8550"
DEFAULT_STATUS_FILE = Path("runs/heartbeat_monitor/status.json")
DEFAULT_ALERT_FILE = Path("runs/heartbeat_monitor/alerts.jsonl")


@dataclass(frozen=True)
class MonitorConfig:
    api_url: str = DEFAULT_API_URL
    token_env: str = "IBKR_LT_API_TOKEN"
    health_interval: float = 5.0
    failure_threshold: int = 3
    request_timeout: float = 5.0
    ws_stale_seconds: float = 15.0
    ws_reconnect_delay: float = 2.0
    expect_running: bool = True
    expect_connected: bool = False
    status_file: Path | None = DEFAULT_STATUS_FILE
    alert_file: Path | None = DEFAULT_ALERT_FILE
    json_stdout: bool = False
    once: bool = False


@dataclass
class MonitorState:
    health_ok: bool = False
    ws_connected: bool = False
    consecutive_health_failures: int = 0
    consecutive_ws_failures: int = 0
    last_health_at: str | None = None
    last_ws_at: str | None = None
    last_ws_ping_at: str | None = None
    last_health: dict[str, Any] | None = None
    last_event_payload: dict[str, Any] | None = None
    active_alerts: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_alert: dict[str, Any] | None = None


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def load_token(token_env: str) -> str:
    token_env = str(token_env or "").strip()
    return str(os.environ.get(token_env, "") or "") if token_env else ""


def api_url(base_url: str, path: str) -> str:
    return f"{str(base_url).rstrip('/')}/{path.lstrip('/')}"


def websocket_url(base_url: str, path: str, token: str = "") -> str:
    parsed = urlparse(api_url(base_url, path))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    query = parsed.query
    if token:
        token_query = urlencode({"token": token})
        query = f"{query}&{token_query}" if query else token_query
    return urlunparse((scheme, parsed.netloc, parsed.path, "", query, ""))


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def fetch_json(url: str, *, token: str = "", timeout: float = 5.0) -> dict[str, Any]:
    request = Request(url, headers=auth_headers(token))
    try:
        with urlopen(request, timeout=float(timeout)) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} from {url}") from exc
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise RuntimeError(f"failed to reach {url}: {reason}") from exc

    data = json.loads(body)
    if not isinstance(data, dict):
        raise RuntimeError(f"expected JSON object from {url}")
    return data


def evaluate_health(
    health: dict[str, Any],
    *,
    expect_running: bool,
    expect_connected: bool,
) -> dict[str, str]:
    alerts: dict[str, str] = {}
    status = str(health.get("status", "")).lower()
    phase = str(health.get("phase", "")).lower()
    running = bool(health.get("running"))
    connected = bool(health.get("connected"))

    if status and status != "ok":
        alerts["api_status_unhealthy"] = f"API status is {status!r}."
    if phase == "error":
        alerts["engine_error"] = "Engine phase is error."
    if expect_running and not running:
        alerts["engine_not_running"] = "Engine is not running while monitor expects it to run."
    if expect_connected and running and not connected:
        alerts["engine_not_connected"] = (
            "Engine is running but broker/data connection is not complete."
        )
    return alerts


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


class AlertEmitter:
    def __init__(self, config: MonitorConfig, state: MonitorState) -> None:
        self.config = config
        self.state = state

    def emit(
        self,
        *,
        severity: str,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = {
            "timestamp_utc": utc_now(),
            "source": "heartbeat_monitor",
            "severity": severity,
            "code": code,
            "message": message,
            "details": dict(details or {}),
        }
        self.state.latest_alert = event
        if severity != "info":
            self.state.active_alerts[code] = event

        line = json.dumps(event, sort_keys=True)
        if self.config.alert_file is not None:
            self.config.alert_file.parent.mkdir(parents=True, exist_ok=True)
            with self.config.alert_file.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

        if self.config.json_stdout:
            print(line, flush=True)
        else:
            print(
                f"[{event['timestamp_utc']}] {severity.upper()} {code}: {message}",
                flush=True,
            )
        return event

    def emit_once(
        self,
        *,
        severity: str,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        if code in self.state.active_alerts:
            return
        self.emit(severity=severity, code=code, message=message, details=details)

    def resolve(self, code: str, message: str) -> None:
        if code not in self.state.active_alerts:
            return
        self.state.active_alerts.pop(code, None)
        self.emit(severity="info", code=f"{code}_resolved", message=message)


class HeartbeatMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.state = MonitorState()
        self.emitter = AlertEmitter(config, self.state)
        self._stop = asyncio.Event()

    def status_payload(self) -> dict[str, Any]:
        return {
            "timestamp_utc": utc_now(),
            "service": "heartbeat_monitor",
            "api_url": self.config.api_url,
            "token_env": self.config.token_env,
            "health_ok": self.state.health_ok,
            "ws_connected": self.state.ws_connected,
            "consecutive_health_failures": self.state.consecutive_health_failures,
            "consecutive_ws_failures": self.state.consecutive_ws_failures,
            "last_health_at": self.state.last_health_at,
            "last_ws_at": self.state.last_ws_at,
            "last_ws_ping_at": self.state.last_ws_ping_at,
            "last_health": self.state.last_health,
            "last_event_payload": self.state.last_event_payload,
            "active_alerts": self.state.active_alerts,
            "latest_alert": self.state.latest_alert,
        }

    def write_status(self) -> None:
        if self.config.status_file is not None:
            write_json_atomic(self.config.status_file, self.status_payload())

    async def check_health_once(self) -> bool:
        token = load_token(self.config.token_env)
        url = api_url(self.config.api_url, "/api/v1/health")
        try:
            health = await asyncio.to_thread(
                fetch_json,
                url,
                token=token,
                timeout=self.config.request_timeout,
            )
        except Exception as exc:  # pragma: no cover - exact network exception varies by platform
            self.state.health_ok = False
            self.state.consecutive_health_failures += 1
            if self.state.consecutive_health_failures >= self.config.failure_threshold:
                self.emitter.emit_once(
                    severity="critical",
                    code="api_health_unreachable",
                    message="ibkr_lt API health endpoint is unreachable.",
                    details={
                        "url": url,
                        "failure_count": self.state.consecutive_health_failures,
                        "error": str(exc),
                    },
                )
            self.write_status()
            return False

        self.state.health_ok = True
        self.state.consecutive_health_failures = 0
        self.state.last_health_at = utc_now()
        self.state.last_health = health
        self.emitter.resolve("api_health_unreachable", "ibkr_lt API health endpoint recovered.")

        health_alerts = evaluate_health(
            health,
            expect_running=self.config.expect_running,
            expect_connected=self.config.expect_connected,
        )
        active_codes = set(health_alerts)
        for code, message in health_alerts.items():
            self.emitter.emit_once(
                severity="critical" if code == "engine_error" else "warning",
                code=code,
                message=message,
                details={"health": health},
            )
        resolved_codes = (
            "api_status_unhealthy",
            "engine_error",
            "engine_not_running",
            "engine_not_connected",
        )
        for code in resolved_codes:
            if code not in active_codes:
                self.emitter.resolve(code, f"{code} recovered.")

        self.write_status()
        return not active_codes

    async def health_loop(self) -> None:
        while not self._stop.is_set():
            await self.check_health_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.config.health_interval)
            except asyncio.TimeoutError:
                continue

    async def websocket_loop(self) -> None:
        while not self._stop.is_set():
            token = load_token(self.config.token_env)
            url = websocket_url(self.config.api_url, "/ws/events")
            try:
                async with websockets.connect(
                    url,
                    additional_headers=auth_headers(token) or None,
                    open_timeout=self.config.request_timeout,
                    ping_interval=None,
                ) as websocket:
                    self.state.ws_connected = True
                    self.state.consecutive_ws_failures = 0
                    self.state.last_ws_at = utc_now()
                    self.emitter.resolve(
                        "websocket_disconnected",
                        "Runtime event WebSocket recovered.",
                    )
                    self.write_status()

                    while not self._stop.is_set():
                        try:
                            raw = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self.config.ws_stale_seconds,
                            )
                        except asyncio.TimeoutError:
                            pong = await websocket.ping()
                            await asyncio.wait_for(
                                pong,
                                timeout=min(5.0, self.config.ws_stale_seconds),
                            )
                            self.state.last_ws_ping_at = utc_now()
                            self.write_status()
                            continue

                        self.state.last_ws_at = utc_now()
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            payload = {"raw": raw}
                        self.state.last_event_payload = (
                            payload if isinstance(payload, dict) else {"event": payload}
                        )
                        self.write_status()
            except Exception as exc:
                self.state.ws_connected = False
                self.state.consecutive_ws_failures += 1
                if self.state.consecutive_ws_failures >= self.config.failure_threshold:
                    self.emitter.emit_once(
                        severity="warning",
                        code="websocket_disconnected",
                        message="Runtime event WebSocket is disconnected.",
                        details={
                            "url": websocket_url(self.config.api_url, "/ws/events"),
                            "failure_count": self.state.consecutive_ws_failures,
                            "error": str(exc),
                        },
                    )
                self.write_status()
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self.config.ws_reconnect_delay,
                    )
                except asyncio.TimeoutError:
                    continue

    async def run(self) -> int:
        if self.config.once:
            ok = await self.check_health_once()
            return 0 if ok and not self.state.active_alerts else 1

        loop = asyncio.get_running_loop()
        for signame in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, signame, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        await asyncio.gather(self.health_loop(), self.websocket_loop())
        return 0


def parse_args(argv: list[str] | None = None) -> MonitorConfig:
    parser = argparse.ArgumentParser(description="Run the ibkr_lt heartbeat monitor process.")
    parser.add_argument("--api-url", default=os.environ.get("IBKR_LT_API_URL", DEFAULT_API_URL))
    parser.add_argument(
        "--token-env",
        default=os.environ.get("IBKR_LT_API_TOKEN_ENV", "IBKR_LT_API_TOKEN"),
    )
    parser.add_argument("--health-interval", type=float, default=5.0)
    parser.add_argument("--failure-threshold", type=int, default=3)
    parser.add_argument("--request-timeout", type=float, default=5.0)
    parser.add_argument("--ws-stale-seconds", type=float, default=15.0)
    parser.add_argument("--ws-reconnect-delay", type=float, default=2.0)
    parser.add_argument("--allow-stopped", action="store_true")
    parser.add_argument("--expect-connected", action="store_true")
    parser.add_argument("--status-file", default=str(DEFAULT_STATUS_FILE))
    parser.add_argument("--alert-file", default=str(DEFAULT_ALERT_FILE))
    parser.add_argument("--no-files", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_stdout")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    status_file = None if args.no_files else Path(args.status_file)
    alert_file = None if args.no_files else Path(args.alert_file)
    return MonitorConfig(
        api_url=args.api_url,
        token_env=args.token_env,
        health_interval=max(1.0, args.health_interval),
        failure_threshold=max(1, args.failure_threshold),
        request_timeout=max(1.0, args.request_timeout),
        ws_stale_seconds=max(2.0, args.ws_stale_seconds),
        ws_reconnect_delay=max(0.5, args.ws_reconnect_delay),
        expect_running=not args.allow_stopped,
        expect_connected=bool(args.expect_connected),
        status_file=status_file,
        alert_file=alert_file,
        json_stdout=bool(args.json_stdout),
        once=bool(args.once),
    )


async def async_main(argv: list[str] | None = None) -> int:
    monitor = HeartbeatMonitor(parse_args(argv))
    return await monitor.run()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

"""Background uvicorn server for the control API."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from ipaddress import ip_address
from typing import Any

import uvicorn

from .app import create_control_api_app

log = logging.getLogger(__name__)


def is_local_control_host(host: str) -> bool:
    normalized = str(host or "").strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


def resolve_control_api_token(*, host: str, token_env: str) -> str:
    token_env = str(token_env or "").strip()
    token = str(os.environ.get(token_env, "") or "") if token_env else ""
    if token:
        return token

    if is_local_control_host(host):
        if token_env:
            log.info(
                "Control API token env var is empty; allowing tokenless localhost access: %s",
                token_env,
            )
        return ""

    if token_env:
        raise ValueError(
            f"Control API token env var is empty for non-local host {host!r}: {token_env}"
        )
    raise ValueError(f"Control API token is required for non-local host {host!r}")


@dataclass
class ControlApiServer:
    host: str = "127.0.0.1"
    port: int = 8550
    token_env: str = ""
    log_level: str = "warning"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    def start(self, engine) -> threading.Thread:
        if self._thread is not None:
            return self._thread

        token = resolve_control_api_token(host=self.host, token_env=self.token_env)
        app = create_control_api_app(engine, api_token=token, metadata=self.metadata)
        config = uvicorn.Config(
            app,
            host=str(self.host),
            port=int(self.port),
            log_level=self.log_level,
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run,
            name="control-api",
            daemon=True,
        )
        self._thread.start()
        return self._thread

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True


def start_control_api_thread(
    engine,
    *,
    host: str,
    port: int,
    token_env: str = "",
    log_level: str = "warning",
    metadata: dict[str, Any] | None = None,
) -> ControlApiServer:
    server = ControlApiServer(
        host=host,
        port=int(port),
        token_env=token_env,
        log_level=log_level,
        metadata=dict(metadata or {}),
    )
    server.start(engine)
    return server


__all__ = [
    "ControlApiServer",
    "is_local_control_host",
    "resolve_control_api_token",
    "start_control_api_thread",
]

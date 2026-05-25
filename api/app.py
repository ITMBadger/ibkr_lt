"""Control API application factory."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from core.operator import OperatorService

from .errors import install_error_handlers
from .routes import events, health, meta, positions, runtime, startup
from .stream import runtime_events_websocket


def create_control_api_app(
    engine=None,
    *,
    operator_service: OperatorService | None = None,
    api_token: str = "",
    metadata: dict[str, Any] | None = None,
    include_api: bool = True,
    title: str = "ibkr_lt Control API",
) -> FastAPI:
    if operator_service is None:
        if engine is None:
            raise ValueError("create_control_api_app requires engine or operator_service")
        operator_service = OperatorService(engine, metadata=metadata)
    engine = operator_service.engine
    app = FastAPI(title=title, version="1.0")
    app.state.engine = engine
    app.state.operator_service = operator_service
    app.state.api_token = str(api_token or "")
    app.state.metadata = dict(metadata or {})
    app.state.api_enabled = bool(include_api)
    install_error_handlers(app)

    if include_api:
        app.include_router(health.router, prefix="/api/v1")
        app.include_router(meta.router, prefix="/api/v1")
        app.include_router(runtime.router, prefix="/api/v1")
        app.include_router(positions.router, prefix="/api/v1")
        app.include_router(events.router, prefix="/api/v1")
        app.include_router(startup.router, prefix="/api/v1")
        app.websocket("/ws/events")(runtime_events_websocket)
    return app


__all__ = ["create_control_api_app"]

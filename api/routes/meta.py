from __future__ import annotations

from fastapi import APIRouter

from ..schemas import ApiMeta

router = APIRouter(prefix="/meta", tags=["meta"])


@router.get("", response_model=ApiMeta)
def meta() -> dict:
    return {
        "service": "ibkr_lt_control_api",
        "api_version": "1.0",
        "docs": {
            "openapi": "/openapi.json",
            "interactive": "/docs",
            "capabilities": "/api/v1/meta/capabilities",
        },
        "auth": {
            "type": "bearer",
            "health_public": True,
            "meta_public": True,
            "protected_paths": [
                "/api/v1/runtime/*",
                "/api/v1/positions",
                "/api/v1/events",
                "/ws/events",
            ],
        },
        "capabilities": {
            "runtime_snapshot": True,
            "positions": True,
            "event_polling": True,
            "event_stream": True,
            "agent_direct_read": True,
            "heartbeat_monitor": True,
            "manual_trade": False,
            "manual_trade_reason": "manual trading requires a command bus and explicit guardrails",
        },
    }


@router.get("/capabilities")
def capabilities() -> dict:
    return meta()["capabilities"]


__all__ = ["router"]

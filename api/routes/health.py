from __future__ import annotations

from fastapi import APIRouter, Depends

from ..dependencies import engine_from_request, metadata_from_request
from ..schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(
    engine=Depends(engine_from_request),
    metadata: dict = Depends(metadata_from_request),
) -> dict:
    snap = engine.snapshot_state()
    connection = dict(snap.get("connection") or {})
    connected = bool(connection.get("connected"))
    running = bool(snap.get("running"))
    if not running:
        next_endpoint = "/api/v1/health"
        operator_message = "Engine is not running yet. Start the runtime or poll health again."
    elif not connected:
        next_endpoint = "/api/v1/health"
        operator_message = "Engine is running but not fully connected yet."
    else:
        next_endpoint = "/api/v1/runtime/snapshot"
        operator_message = "Engine is running and connected."

    return {
        "status": "ok",
        "phase": str(snap.get("phase", "")),
        "running": running,
        "connected": connected,
        "mode": str(metadata.get("mode", "")),
        "strategy_modes": dict(metadata.get("strategy_modes") or {}),
        "next_endpoint": next_endpoint,
        "operator_message": operator_message,
    }


__all__ = ["router"]

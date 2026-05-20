from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from ..dependencies import AuthDependency, engine_from_request

router = APIRouter(prefix="/events", tags=["events"], dependencies=[AuthDependency])


@router.get("")
async def events(
    limit: int = Query(default=100, ge=1, le=500),
    engine=Depends(engine_from_request),
) -> list[dict]:
    recent = list(engine.snapshot_state().get("recent_events", []))
    return recent[-int(limit):]


__all__ = ["router"]

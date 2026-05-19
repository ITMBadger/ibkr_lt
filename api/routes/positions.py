from __future__ import annotations

from fastapi import APIRouter, Depends

from ..dependencies import AuthDependency, engine_from_request

router = APIRouter(prefix="/positions", tags=["positions"], dependencies=[AuthDependency])


@router.get("")
def positions(engine=Depends(engine_from_request)) -> dict:
    return dict(engine.snapshot_state().get("positions", {}))


__all__ = ["router"]


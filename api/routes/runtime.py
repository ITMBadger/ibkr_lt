from __future__ import annotations

from fastapi import APIRouter, Depends

from ..dependencies import AuthDependency, engine_from_request, metadata_from_request

router = APIRouter(prefix="/runtime", tags=["runtime"], dependencies=[AuthDependency])


@router.get("/snapshot")
def snapshot(
    engine=Depends(engine_from_request),
    metadata: dict = Depends(metadata_from_request),
) -> dict:
    state = engine.snapshot_state()
    state["metadata"] = metadata
    return state


@router.get("/strategies")
def strategies(engine=Depends(engine_from_request)) -> list[dict]:
    return list(engine.snapshot_state().get("strategies", []))


__all__ = ["router"]


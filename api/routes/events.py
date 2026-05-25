from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from ..dependencies import AuthDependency, operator_service_from_request

router = APIRouter(prefix="/events", tags=["events"], dependencies=[AuthDependency])


@router.get("")
async def events(
    limit: int = Query(default=100, ge=1, le=500),
    operator=Depends(operator_service_from_request),
) -> list[dict]:
    return operator.events(limit=int(limit))


__all__ = ["router"]

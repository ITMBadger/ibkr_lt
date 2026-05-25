from __future__ import annotations

from fastapi import APIRouter, Depends

from ..dependencies import AuthDependency, operator_service_from_request

router = APIRouter(prefix="/runtime", tags=["runtime"], dependencies=[AuthDependency])


@router.get("/snapshot")
async def snapshot(
    operator=Depends(operator_service_from_request),
) -> dict:
    return operator.runtime_snapshot()


@router.get("/strategies")
async def strategies(operator=Depends(operator_service_from_request)) -> list[dict]:
    return operator.strategies()


__all__ = ["router"]

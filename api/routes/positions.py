from __future__ import annotations

from fastapi import APIRouter, Depends

from ..dependencies import AuthDependency, operator_service_from_request

router = APIRouter(prefix="/positions", tags=["positions"], dependencies=[AuthDependency])


@router.get("")
async def positions(operator=Depends(operator_service_from_request)) -> dict:
    return operator.positions()


__all__ = ["router"]

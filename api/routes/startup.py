"""Startup broker-state gate endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..dependencies import AuthDependency, operator_service_from_request

router = APIRouter(prefix="/startup", tags=["startup"], dependencies=[AuthDependency])


@router.get("/gate")
async def startup_gate(operator=Depends(operator_service_from_request)) -> dict:
    return operator.startup_gate_status()


@router.post("/mappings")
async def submit_startup_mappings(
    payload: dict,
    operator=Depends(operator_service_from_request),
) -> dict:
    allocations = payload.get("allocations")
    if not isinstance(allocations, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="payload must include allocations list",
        )
    try:
        return operator.submit_startup_mappings(allocations)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post("/refresh")
async def refresh_startup_gate(operator=Depends(operator_service_from_request)) -> dict:
    return operator.request_startup_gate_refresh()

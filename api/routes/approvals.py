"""Operator approval endpoints for strategy-generated actions."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from ..dependencies import AuthDependency, operator_service_from_request

router = APIRouter(prefix="/approvals", tags=["approvals"], dependencies=[AuthDependency])


@router.get("")
async def pending_approvals(operator=Depends(operator_service_from_request)) -> list[dict]:
    return operator.pending_approvals()


@router.post("/{approval_id}/approve")
async def approve_pending_action(
    approval_id: str,
    payload: dict | None = None,
    operator=Depends(operator_service_from_request),
) -> dict:
    try:
        return operator.approve_pending_action(
            approval_id,
            operator_note=(payload or {}).get("note"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post("/{approval_id}/reject")
async def reject_pending_action(
    approval_id: str,
    payload: dict | None = None,
    operator=Depends(operator_service_from_request),
) -> dict:
    try:
        return operator.reject_pending_action(
            approval_id,
            operator_note=(payload or {}).get("note"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

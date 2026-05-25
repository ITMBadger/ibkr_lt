"""FastAPI dependencies for the control API."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


_bearer = HTTPBearer(auto_error=False)


async def engine_from_request(request: Request):
    return request.app.state.engine


async def operator_service_from_request(request: Request):
    return request.app.state.operator_service


async def metadata_from_request(request: Request) -> dict:
    return dict(getattr(request.app.state, "metadata", {}) or {})


async def require_api_auth(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)] = None,
) -> None:
    token = str(getattr(request.app.state, "api_token", "") or "")
    if not token:
        return
    if credentials is None or str(credentials.scheme).lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    if credentials.credentials != token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid bearer token",
        )


AuthDependency = Depends(require_api_auth)


__all__ = [
    "AuthDependency",
    "engine_from_request",
    "metadata_from_request",
    "operator_service_from_request",
    "require_api_auth",
]

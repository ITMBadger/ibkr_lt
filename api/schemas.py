"""Pydantic schemas for public API responses."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(description="API health status.")
    phase: str = Field(description="Engine phase: initialized, starting, running, stopped, or error.")
    running: bool = Field(description="Whether the engine is currently running.")
    connected: bool = Field(description="Whether broker and data feed are connected.")
    mode: str = Field(default="", description="Runtime mode selected at bootstrap.")
    dry_run: bool = Field(default=False, description="Whether native broker order placement is disabled.")
    next_endpoint: str | None = Field(default=None, description="Suggested endpoint to call next.")
    operator_message: str = Field(description="Short human-readable API state summary.")


class ApiMeta(BaseModel):
    service: str
    api_version: str
    docs: dict[str, str]
    auth: dict[str, Any]
    capabilities: dict[str, Any]


__all__ = ["ApiMeta", "HealthResponse"]


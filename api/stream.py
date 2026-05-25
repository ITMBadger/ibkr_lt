"""WebSocket event stream for recent engine events."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect, status


async def runtime_events_websocket(websocket: WebSocket) -> None:
    token = str(getattr(websocket.app.state, "api_token", "") or "")
    if token:
        supplied = str(websocket.query_params.get("token", "") or "")
        header = str(websocket.headers.get("authorization", "") or "")
        if header.lower().startswith("bearer "):
            supplied = header[7:].strip()
        if supplied != token:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

    await websocket.accept()
    operator = websocket.app.state.operator_service
    seen: set[tuple[Any, Any, Any]] = set()
    try:
        while True:
            snap = operator.snapshot_state()
            events = list(snap.get("recent_events", []))
            new_events = []
            for event in events:
                key = (
                    event.get("timestamp"),
                    event.get("source"),
                    event.get("message"),
                )
                if key in seen:
                    continue
                seen.add(key)
                new_events.append(event)
            if new_events:
                await websocket.send_json({
                    "type": "events",
                    "phase": snap.get("phase"),
                    "events": new_events,
                })
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return


__all__ = ["runtime_events_websocket"]

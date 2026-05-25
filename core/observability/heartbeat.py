"""Heartbeat monitor process detection."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path


def cmdline_is_heartbeat_monitor(cmdline: Sequence[str]) -> bool:
    normalized = [str(part).replace("\\", "/") for part in cmdline]
    for index, part in enumerate(normalized):
        if part == "-m" and index + 1 < len(normalized):
            if normalized[index + 1] == "tools.heartbeat_monitor":
                return True
        if part == "heartbeat_monitor.py" or part.endswith("/heartbeat_monitor.py"):
            return True
    return False


def heartbeat_monitor_process_running(
    proc_root: Path = Path("/proc"),
    *,
    current_pid: int | None = None,
) -> bool | None:
    if not proc_root.exists():
        return None
    current_pid = os.getpid() if current_pid is None else int(current_pid)
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return None

    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            pid = int(entry.name)
        except ValueError:
            continue
        if pid == current_pid:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        if not raw:
            continue
        cmdline = [
            part.decode("utf-8", errors="ignore")
            for part in raw.split(b"\0")
            if part
        ]
        if cmdline_is_heartbeat_monitor(cmdline):
            return True
    return False


def warn_if_heartbeat_monitor_missing(proc_root: Path = Path("/proc")) -> None:
    running = heartbeat_monitor_process_running(proc_root)
    if running is not False:
        return
    print(
        "Warning: Heartbeat Monitor process is not detected. "
        "Start it in another terminal with: "
        "~/.venv/bin/python tools/heartbeat_monitor.py",
        file=sys.stderr,
    )

"""Filesystem path helpers shared by adapters and tools."""

from __future__ import annotations

import os
import re
from pathlib import Path


def normalize_local_path(path: str | Path) -> Path:
    """Normalize local paths, including Windows drive paths when running in WSL."""
    raw = str(path)
    if raw.startswith("~"):
        return Path(raw).expanduser()
    if os.name != "nt":
        match = re.match(r"^([A-Za-z]):[\\/](.*)$", raw)
        if match:
            drive = match.group(1).lower()
            rest = match.group(2).replace("\\", "/")
            return Path("/mnt") / drive / rest
    return Path(raw)

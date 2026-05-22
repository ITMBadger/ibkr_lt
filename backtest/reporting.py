"""Backtest run reporting."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from core.audit.serialize import to_jsonable


def audit_file_stats(run_dir: Path) -> dict[str, Any]:
    return {
        "signals": _jsonl_stats(run_dir / "signals.jsonl"),
        "orders": _jsonl_stats(run_dir / "orders.jsonl"),
        "fills": _jsonl_stats(run_dir / "fills.jsonl"),
    }


def write_summary(run_dir: Path, summary: dict[str, Any]) -> Path:
    path = run_dir / "summary.json"
    payload = to_jsonable(summary)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=True, allow_nan=False, indent=2)
        fh.write("\n")
    return path


def _jsonl_stats(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"count": 0, "events": {}}
    events: Counter[str] = Counter()
    count = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            count += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                events["<invalid_json>"] += 1
                continue
            events[str(event.get("event", "<missing>"))] += 1
    return {"count": count, "events": dict(sorted(events.items()))}

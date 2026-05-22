"""Strategy execution-mode helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

STRATEGY_MODE_LIVE = "live"
STRATEGY_MODE_DRY_RUN = "dry_run"
VALID_STRATEGY_MODES = frozenset({STRATEGY_MODE_LIVE, STRATEGY_MODE_DRY_RUN})


def normalize_strategy_mode(value: Any) -> str:
    """Normalize a strategy mode from config/API input."""
    mode = str(value or STRATEGY_MODE_LIVE).strip().lower().replace("-", "_")
    if mode == "dryrun":
        mode = STRATEGY_MODE_DRY_RUN
    if mode not in VALID_STRATEGY_MODES:
        valid = ", ".join(sorted(VALID_STRATEGY_MODES))
        raise ValueError(f"Invalid strategy mode {value!r}; expected one of: {valid}")
    return mode


def strategy_mode_map(
    strategy_modes: Mapping[str, Any] | None,
    strategy_ids: Iterable[str],
) -> dict[str, str]:
    """Return an explicit mode for each active strategy id."""
    raw = _strategy_modes_dict(strategy_modes)
    return {
        str(strategy_id): normalize_strategy_mode(raw.get(str(strategy_id), STRATEGY_MODE_LIVE))
        for strategy_id in strategy_ids
    }


def validate_strategy_modes(
    strategy_modes: Mapping[str, Any] | None,
    known_strategy_ids: Iterable[str],
) -> None:
    """Fail fast on typos in configured strategy ids or modes."""
    raw = _strategy_modes_dict(strategy_modes)
    if not raw:
        return
    known = {str(strategy_id) for strategy_id in known_strategy_ids}
    unknown = sorted(
        str(strategy_id)
        for strategy_id in raw
        if str(strategy_id) not in known
    )
    if unknown:
        raise ValueError(
            "strategy_modes contains unknown strategy id(s): "
            f"{unknown}. Available: {sorted(known)}"
        )
    for mode in raw.values():
        normalize_strategy_mode(mode)


def _strategy_modes_dict(strategy_modes: Mapping[str, Any] | None) -> dict[str, Any]:
    if strategy_modes is None:
        return {}
    if not isinstance(strategy_modes, Mapping):
        raise ValueError("strategy_modes must be a mapping of strategy id to mode")
    return dict(strategy_modes)

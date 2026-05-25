"""Strategy identity redaction helpers for protected/customer runtimes."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any

CUSTOMER_PROFILE = "customer"
OWNER_PROFILE = "owner"


def is_customer_profile(profile: str | None) -> bool:
    return str(profile or OWNER_PROFILE).strip().lower() == CUSTOMER_PROFILE


def build_strategy_aliases(
    strategy_ids: Sequence[str],
    configured: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    configured = dict(configured or {})
    for index, strategy_id in enumerate(strategy_ids, start=1):
        raw_alias = configured.get(strategy_id)
        alias = str(raw_alias).strip() if raw_alias else f"strategy_{index}"
        aliases[str(strategy_id)] = alias
    return aliases


def safe_strategy_id(
    strategy_id: Any,
    *,
    profile: str | None,
    aliases: Mapping[str, str] | None = None,
) -> Any:
    if not is_customer_profile(profile):
        return strategy_id
    key = str(strategy_id)
    return dict(aliases or {}).get(key, "strategy")


def redact_payload(
    value: Any,
    *,
    profile: str | None,
    aliases: Mapping[str, str] | None = None,
) -> Any:
    """Recursively replace strategy IDs in JSON-style payloads.

    The helper is intentionally conservative: it only rewrites explicit
    strategy_id-ish fields and string occurrences of known strategy IDs.
    """
    if not is_customer_profile(profile):
        return value
    alias_map = dict(aliases or {})
    return _redact(value, alias_map)


def redact_text(
    value: str,
    *,
    profile: str | None,
    aliases: Mapping[str, str] | None = None,
) -> str:
    if not is_customer_profile(profile):
        return value
    text = str(value)
    for strategy_id, alias in sorted(
        dict(aliases or {}).items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if strategy_id:
            text = text.replace(strategy_id, alias)
    return text


def _redact(value: Any, aliases: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return _replace_known_ids(value, aliases)
    if isinstance(value, list):
        return [_redact(item, aliases) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item, aliases) for item in value)
    if isinstance(value, set):
        return {_redact(item, aliases) for item in value}
    if isinstance(value, frozenset):
        return frozenset(_redact(item, aliases) for item in value)
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        for key, item in value.items():
            redacted_key = _redact(key, aliases)
            if str(key) in {"strategy_id", "strategy"}:
                result[redacted_key] = aliases.get(str(item), "strategy")
            elif str(key) in {"strategies", "strategy_ids"} and isinstance(item, list):
                if all(not isinstance(entry, dict | list | tuple | set | frozenset) for entry in item):
                    result[redacted_key] = [aliases.get(str(sid), "strategy") for sid in item]
                else:
                    result[redacted_key] = [_redact(entry, aliases) for entry in item]
            elif str(key) == "strategy_modes" and isinstance(item, dict):
                result[redacted_key] = {
                    aliases.get(str(sid), "strategy"): _redact(mode, aliases)
                    for sid, mode in item.items()
                }
            elif str(key) == "strategy_risk" and isinstance(item, dict):
                result[redacted_key] = {
                    aliases.get(str(sid), "strategy"): _redact(risk, aliases)
                    for sid, risk in item.items()
                }
            else:
                result[redacted_key] = _redact(item, aliases)
        return result
    if dataclasses.is_dataclass(value):
        return _redact(dataclasses.asdict(value), aliases)
    return value


def _replace_known_ids(value: str, aliases: Mapping[str, str]) -> str:
    text = value
    for strategy_id, alias in sorted(
        aliases.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if strategy_id:
            text = text.replace(strategy_id, alias)
    return text

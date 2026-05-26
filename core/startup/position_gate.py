"""Startup broker-position classification and adoption validation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from ..interfaces.strategy import POSITION_MODE_MULTI, StrategyKernel
from ..orders.strategy_modes import STRATEGY_MODE_DRY_RUN
from ..risk.policy import RiskPolicy
from ..types import Instrument, Position

if TYPE_CHECKING:
    from ..interfaces.strategy import PositionPolicy

PHASE_INACTIVE = "inactive"
PHASE_CLEAR = "clear"
PHASE_AWAITING_MAPPING = "awaiting_mapping"
PHASE_BLOCKED = "blocked"
PHASE_MAPPED = "mapped"
PHASE_RELEASED = "released"
_QTY_EPSILON = 1e-9

_DERIVATIVE_REQUIRED_FIELDS = {
    "future": ("exchange", "currency", "expiry", "multiplier"),
    "option": ("exchange", "currency", "expiry", "strike", "right", "multiplier"),
}


@dataclass(frozen=True)
class StartupMappingSubmission:
    allocations: list[dict[str, Any]]
    unmanaged_remainder_acknowledgements: list[dict[str, Any]]


def build_startup_gate_status(
    positions: Sequence[Position],
    strategy_entries: Sequence[tuple[StrategyKernel, dict]],
    *,
    default_risk: RiskPolicy,
    strategy_risk: Mapping[str, RiskPolicy] | None = None,
    strategy_modes: Mapping[str, str] | None = None,
    configured_allocations: Sequence[Mapping[str, Any]] | None = None,
    ledger_allocations: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    strategy_risk = dict(strategy_risk or {})
    strategy_modes = dict(strategy_modes or {})
    candidates = [
        _strategy_candidate(
            kernel,
            default_risk=default_risk,
            strategy_risk=strategy_risk,
            strategy_modes=strategy_modes,
        )
        for kernel, _ in strategy_entries
    ]

    required: list[dict[str, Any]] = []
    unmanaged: list[dict[str, Any]] = []
    blocked = False
    for position in positions:
        item = position_gate_item(position)
        matching_underlying = [
            candidate
            for candidate in candidates
            if _same_underlying(position.instrument, candidate["execution_instrument"])
        ]
        if not matching_underlying:
            unmanaged.append({
                **item,
                "reason": "instrument_not_used_by_enabled_strategies",
            })
            continue

        exact_candidates = [
            _candidate_view(candidate)
            for candidate in matching_underlying
            if instruments_match(position.instrument, candidate["execution_instrument"])
        ]
        if not exact_candidates:
            blocked = True
            required.append({
                **item,
                "phase": PHASE_BLOCKED,
                "reason": "instrument_contract_not_exactly_declared",
                "candidates": [_candidate_view(candidate) for candidate in matching_underlying],
            })
            continue

        adoptable = [candidate for candidate in exact_candidates if candidate["adoptable"]]
        if not adoptable:
            blocked = True
            required.append({
                **item,
                "phase": PHASE_BLOCKED,
                "reason": "no_adoptable_strategy_candidate",
                "candidates": exact_candidates,
            })
            continue

        required.append({
            **item,
            "phase": PHASE_AWAITING_MAPPING,
            "candidates": exact_candidates,
        })

    if blocked:
        return {
            "enabled": True,
            "phase": PHASE_BLOCKED,
            "message": "Broker positions match enabled strategies but cannot be adopted safely.",
            "positions": required,
            "allocations": [],
            "unmanaged_remainder_acknowledgements": [],
            "unmanaged": unmanaged,
            "last_error": None,
        }

    if not required:
        return {
            "enabled": True,
            "phase": PHASE_CLEAR,
            "message": "No broker positions match enabled strategy execution instruments.",
            "positions": [],
            "allocations": [],
            "unmanaged_remainder_acknowledgements": [],
            "unmanaged": unmanaged,
            "last_error": None,
        }

    status = {
        "enabled": True,
        "phase": PHASE_AWAITING_MAPPING,
        "message": "Broker positions require ownership mapping before live startup can continue.",
        "positions": required,
        "allocations": [],
        "unmanaged_remainder_acknowledgements": [],
        "unmanaged": unmanaged,
        "last_error": None,
    }
    auto_allocations = [
        *[
            allocation
            for allocation in (ledger_allocations or [])
            if _allocation_matches_any_position(allocation, required)
        ],
        *list(configured_allocations or []),
    ]
    if not auto_allocations:
        return status

    try:
        normalized = validate_startup_allocations(
            status,
            auto_allocations,
            require_awaiting=False,
            require_remainder_ack=False,
        )
    except ValueError as exc:
        status["phase"] = PHASE_BLOCKED
        status["message"] = "Startup ownership mappings are invalid."
        status["last_error"] = str(exc)
        return status

    allocated_by_position: dict[str, float] = defaultdict(float)
    for allocation in normalized:
        allocated_by_position[str(allocation["position_id"])] += float(allocation["quantity"])
    missing = [
        item["position_id"]
        for item in required
        if (
            abs(float(item.get("quantity", 0.0)))
            - allocated_by_position.get(str(item["position_id"]), 0.0)
        ) > _QTY_EPSILON
    ]
    status["allocations"] = normalized
    if not missing:
        status["phase"] = PHASE_CLEAR
        status["message"] = "Stored ownership mappings cover all managed broker positions."
    return status


def validate_startup_allocations(
    status: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
    *,
    ack_unmanaged_remainders: Sequence[Mapping[str, Any]] | None = None,
    require_awaiting: bool = True,
    require_remainder_ack: bool = True,
) -> list[dict[str, Any]]:
    return validate_startup_mapping_submission(
        status,
        allocations,
        ack_unmanaged_remainders=ack_unmanaged_remainders,
        require_awaiting=require_awaiting,
        require_remainder_ack=require_remainder_ack,
    ).allocations


def validate_startup_mapping_submission(
    status: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
    *,
    ack_unmanaged_remainders: Sequence[Mapping[str, Any]] | None = None,
    require_awaiting: bool = True,
    require_remainder_ack: bool = True,
) -> StartupMappingSubmission:
    if require_awaiting and status.get("phase") != PHASE_AWAITING_MAPPING:
        raise ValueError("startup gate is not awaiting position mappings")
    if not allocations:
        raise ValueError("at least one allocation is required")

    positions = {
        item["position_id"]: item
        for item in status.get("positions", [])
    }
    if not positions:
        raise ValueError("startup gate has no managed broker positions")

    allocated_by_position: dict[str, float] = defaultdict(float)
    allocated_by_strategy_position: dict[tuple[str, str], int] = defaultdict(int)
    normalized: list[dict[str, Any]] = []

    for raw in allocations:
        position_id_value = str(raw.get("position_id", "")).strip()
        if position_id_value:
            position_id_key = position_id_value
        else:
            position_id_key = _match_allocation_position_id(raw, positions)
        strategy_id = str(raw.get("strategy_id", "")).strip()
        if position_id_key not in positions:
            raise ValueError(f"unknown startup position_id: {position_id_key!r}")
        if not strategy_id:
            raise ValueError(f"startup allocation for {position_id_key!r} is missing strategy_id")

        position = positions[position_id_key]
        candidates = {
            item["strategy_id"]: item
            for item in position.get("candidates", [])
        }
        candidate = candidates.get(strategy_id)
        if candidate is None:
            raise ValueError(
                f"strategy {strategy_id!r} cannot adopt position {position_id_key!r}"
            )
        if candidate.get("mode") == STRATEGY_MODE_DRY_RUN:
            raise ValueError(f"dry-run strategy {strategy_id!r} cannot adopt live positions")
        if not candidate.get("supports_position_adoption"):
            raise ValueError(f"strategy {strategy_id!r} does not support position adoption")
        if not candidate.get("adoptable", False):
            reason = candidate.get("blocked_reason") or "not_adoptable"
            raise ValueError(f"strategy {strategy_id!r} cannot adopt position: {reason}")

        if "quantity" not in raw:
            raise ValueError(
                f"startup allocation for {position_id_key!r} must include quantity"
            )
        try:
            quantity = float(raw.get("quantity"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"startup allocation for {position_id_key!r} has invalid quantity"
            ) from exc
        if quantity <= 0:
            raise ValueError(
                f"startup allocation for {position_id_key!r} must have quantity > 0"
            )

        required_fields = set(candidate.get("required_fields", []))
        entry_ts = raw.get("entry_ts")
        parsed_entry_ts = parse_optional_timestamp(entry_ts)
        if "entry_ts" in required_fields and parsed_entry_ts is None:
            raise ValueError(f"strategy {strategy_id!r} requires entry_ts for adoption")

        allocated_by_position[position_id_key] += quantity
        available = abs(float(position.get("quantity", 0.0)))
        if allocated_by_position[position_id_key] - available > 1e-9:
            raise ValueError(
                f"allocated quantity {allocated_by_position[position_id_key]} exceeds "
                f"broker quantity {available} for {position_id_key}"
            )

        strategy_position_key = (position_id_key, strategy_id)
        allocated_by_strategy_position[strategy_position_key] += 1
        if (
            allocated_by_strategy_position[strategy_position_key] > 1
            and candidate.get("position_mode") != POSITION_MODE_MULTI
        ):
            raise ValueError(
                f"strategy {strategy_id!r} is not multi-position and cannot adopt multiple lots"
            )

        normalized.append({
            "position_id": position_id_key,
            "strategy_id": strategy_id,
            "quantity": quantity,
            "entry_ts": parsed_entry_ts.isoformat() if parsed_entry_ts else None,
            "trade_id": str(raw.get("trade_id") or "") or None,
            "source": str(raw.get("source") or "operator"),
        })
    remainder_acknowledgements = _validate_unmanaged_remainder_acknowledgements(
        positions,
        allocated_by_position,
        ack_unmanaged_remainders or [],
        require_remainder_ack=require_remainder_ack,
    )
    return StartupMappingSubmission(
        allocations=normalized,
        unmanaged_remainder_acknowledgements=remainder_acknowledgements,
    )


def _validate_unmanaged_remainder_acknowledgements(
    positions: Mapping[str, Mapping[str, Any]],
    allocated_by_position: Mapping[str, float],
    acknowledgements: Sequence[Mapping[str, Any]],
    *,
    require_remainder_ack: bool,
) -> list[dict[str, Any]]:
    remainders: dict[str, float] = {}
    for position_id_key, position in positions.items():
        available = abs(float(position.get("quantity", 0.0)))
        remaining = available - float(allocated_by_position.get(position_id_key, 0.0))
        if remaining > _QTY_EPSILON:
            remainders[position_id_key] = remaining

    if not require_remainder_ack:
        return []
    if not remainders:
        if acknowledgements:
            raise ValueError("unmanaged remainder acknowledgement has no matching remainder")
        return []

    normalized: list[dict[str, Any]] = []
    acknowledged: set[str] = set()
    for raw in acknowledgements:
        position_id_key = str(raw.get("position_id", "")).strip()
        if not position_id_key:
            raise ValueError("unmanaged remainder acknowledgement is missing position_id")
        if position_id_key not in remainders:
            raise ValueError(
                f"unmanaged remainder acknowledgement for {position_id_key!r} "
                "has no matching remainder"
            )
        if position_id_key in acknowledged:
            raise ValueError(
                f"duplicate unmanaged remainder acknowledgement for {position_id_key!r}"
            )
        try:
            quantity = float(raw.get("quantity"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"unmanaged remainder acknowledgement for {position_id_key!r} "
                "has invalid quantity"
            ) from exc
        if quantity <= 0:
            raise ValueError(
                f"unmanaged remainder acknowledgement for {position_id_key!r} "
                "must have quantity > 0"
            )
        expected = remainders[position_id_key]
        if abs(quantity - expected) > _QTY_EPSILON:
            raise ValueError(
                f"unmanaged remainder acknowledgement for {position_id_key!r} "
                f"must cover remaining quantity {expected}"
            )
        reason = str(raw.get("reason") or "").strip()
        if not reason:
            raise ValueError(
                f"unmanaged remainder acknowledgement for {position_id_key!r} "
                "is missing reason"
            )
        acknowledged.add(position_id_key)
        normalized.append({
            "position_id": position_id_key,
            "quantity": quantity,
            "reason": reason,
            "source": str(raw.get("source") or "operator"),
        })

    missing = sorted(set(remainders) - acknowledged)
    if missing:
        raise ValueError(
            "startup allocation leaves unmanaged remainder; acknowledgement "
            f"required for {', '.join(missing)}"
        )
    return normalized


def instruments_match(position_instrument: Instrument, strategy_instrument: Instrument) -> bool:
    if not _same_underlying(position_instrument, strategy_instrument):
        return False
    asset_class = str(position_instrument.asset_class).lower()
    if asset_class not in _DERIVATIVE_REQUIRED_FIELDS:
        return _simple_fields_match(position_instrument, strategy_instrument)
    required_fields = _DERIVATIVE_REQUIRED_FIELDS[asset_class]
    for field in required_fields:
        if asset_class == "future" and field == "expiry":
            if not _future_contract_month_equal(
                position_instrument.expiry,
                strategy_instrument.expiry,
            ):
                return False
            continue
        if not _instrument_field_equal(
            getattr(position_instrument, field),
            getattr(strategy_instrument, field),
        ):
            return False
    return True


def instrument_identity_key(
    instrument: Instrument,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[Any, ...]:
    metadata = dict(metadata or {})
    return (
        str(instrument.asset_class).lower(),
        str(instrument.symbol).upper(),
        _normalize_optional(instrument.exchange, upper=True),
        _normalize_optional(instrument.currency, upper=True),
        _normalize_date(instrument.expiry),
        _normalize_float(instrument.strike),
        _normalize_optional(instrument.right, upper=True),
        _normalize_float(instrument.multiplier),
        _normalize_optional(metadata.get("broker_con_id")),
        _normalize_optional(metadata.get("local_symbol"), upper=True),
    )


def position_id(position: Position) -> str:
    parts = [
        "position",
        *[
            str(part)
            for part in instrument_identity_key(
                position.instrument,
                metadata=getattr(position, "metadata", None),
            )
            if part not in {None, ""}
        ],
        position.side,
    ]
    return ":".join(parts)


def position_gate_item(position: Position) -> dict[str, Any]:
    return {
        "position_id": position_id(position),
        "asset_class": position.instrument.asset_class,
        "symbol": position.instrument.symbol,
        "side": position.side,
        "quantity": position.quantity,
        "avg_cost": position.avg_cost,
        "instrument": position.instrument,
        "instrument_identity": {
            "asset_class": position.instrument.asset_class,
            "symbol": position.instrument.symbol,
            "exchange": position.instrument.exchange,
            "currency": position.instrument.currency,
            "expiry": position.instrument.expiry,
            "strike": position.instrument.strike,
            "right": position.instrument.right,
            "multiplier": position.instrument.multiplier,
            **dict(getattr(position, "metadata", None) or {}),
        },
    }


def parse_optional_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value))
        except ValueError as exc:
            raise ValueError(f"invalid entry_ts: {value!r}") from exc
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def allocation_instrument(raw: Mapping[str, Any]) -> Instrument | None:
    source = raw.get("instrument")
    if isinstance(source, Instrument):
        return source
    if isinstance(source, Mapping):
        item = dict(source)
    else:
        item = dict(raw)
    symbol = item.get("symbol")
    if not symbol:
        return None
    return Instrument(
        asset_class=str(item.get("asset_class", "equity")),
        symbol=str(symbol),
        exchange=_optional_string(item.get("exchange")),
        currency=_optional_string(item.get("currency")),
        expiry=_parse_optional_date(item.get("expiry")),
        strike=_optional_float(item.get("strike")),
        right=_optional_string(item.get("right")),  # type: ignore[arg-type]
        multiplier=float(item.get("multiplier", 1.0) or 1.0),
    )


def _strategy_candidate(
    kernel: StrategyKernel,
    *,
    default_risk: RiskPolicy,
    strategy_risk: Mapping[str, RiskPolicy],
    strategy_modes: Mapping[str, str],
) -> dict[str, Any]:
    strategy_id = kernel.SPEC.id
    risk = strategy_risk.get(strategy_id, default_risk)
    policy = kernel.SPEC.position_policy
    blocked_reason = _candidate_blocked_reason(policy, strategy_modes.get(strategy_id, "live"))
    return {
        "strategy_id": strategy_id,
        "execution_instrument": kernel.SPEC.execution_instrument,
        "position_mode": policy.position_mode,
        "mode": strategy_modes.get(strategy_id, "live"),
        "supports_position_adoption": policy.supports_position_adoption,
        "required_fields": list(kernel.POSITION_ADOPTION_REQUIRED_FIELDS),
        "position_size_shares": float(risk.position_size_shares),
        "max_order_quantity": risk.max_order_quantity,
        "adoptable": blocked_reason is None,
        "blocked_reason": blocked_reason,
    }


def _candidate_view(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": candidate["strategy_id"],
        "position_mode": candidate["position_mode"],
        "mode": candidate["mode"],
        "supports_position_adoption": candidate["supports_position_adoption"],
        "required_fields": list(candidate["required_fields"]),
        "position_size_shares": candidate["position_size_shares"],
        "max_order_quantity": candidate["max_order_quantity"],
        "adoptable": candidate["adoptable"],
        "blocked_reason": candidate["blocked_reason"],
        "execution_instrument": candidate["execution_instrument"],
    }


def _candidate_blocked_reason(policy: "PositionPolicy", mode: str) -> str | None:
    if mode == STRATEGY_MODE_DRY_RUN:
        return "strategy_mode_dry_run"
    if not policy.supports_position_adoption:
        return "strategy_does_not_support_position_adoption"
    return None


def _match_allocation_position_id(
    raw: Mapping[str, Any],
    positions: Mapping[str, Mapping[str, Any]],
) -> str:
    instrument = allocation_instrument(raw)
    if instrument is None:
        raise ValueError("startup allocation must include position_id or instrument fields")
    side = str(raw.get("side") or "").strip().lower()
    matches: list[str] = []
    for position_id_key, item in positions.items():
        position_instrument = _item_instrument(item)
        if position_instrument is None:
            continue
        if side and side != str(item.get("side", "")).lower():
            continue
        if instruments_match(position_instrument, instrument):
            matches.append(position_id_key)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(
            f"startup allocation instrument {instrument.symbol!r} does not match broker positions"
        )
    raise ValueError(
        f"startup allocation instrument {instrument.symbol!r} matches multiple broker positions; "
        "use position_id"
    )


def _allocation_matches_any_position(
    raw: Mapping[str, Any],
    positions: Sequence[Mapping[str, Any]],
) -> bool:
    try:
        _match_allocation_position_id(
            raw,
            {str(item["position_id"]): item for item in positions},
        )
    except ValueError:
        return False
    return True


def _item_instrument(item: Mapping[str, Any]) -> Instrument | None:
    instrument = item.get("instrument")
    if isinstance(instrument, Instrument):
        return instrument
    if isinstance(instrument, Mapping):
        return allocation_instrument({"instrument": instrument})
    return None


def _same_underlying(left: Instrument, right: Instrument) -> bool:
    return (
        str(left.asset_class).lower() == str(right.asset_class).lower()
        and str(left.symbol).upper() == str(right.symbol).upper()
    )


def _simple_fields_match(left: Instrument, right: Instrument) -> bool:
    if left.currency and right.currency and left.currency.upper() != right.currency.upper():
        return False
    left_exchange = (left.exchange or "").upper()
    right_exchange = (right.exchange or "").upper()
    if left_exchange and right_exchange and "SMART" not in {left_exchange, right_exchange}:
        return left_exchange == right_exchange
    return True


def _instrument_field_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    if isinstance(left, date) or isinstance(right, date):
        return _normalize_date(left) == _normalize_date(right)
    if isinstance(left, float | int) or isinstance(right, float | int):
        return _normalize_float(left) == _normalize_float(right)
    return str(left).upper() == str(right).upper()


def _future_contract_month_equal(left: Any, right: Any) -> bool:
    left_date = _parse_optional_date(left)
    right_date = _parse_optional_date(right)
    if left_date is None or right_date is None:
        return False
    return (
        left_date.year == right_date.year
        and left_date.month == right_date.month
    )


def _normalize_optional(value: Any, *, upper: bool = False) -> str | None:
    if value is None or value == "":
        return None
    text = str(value)
    return text.upper() if upper else text


def _normalize_date(value: Any) -> str | None:
    parsed = _parse_optional_date(value)
    return parsed.isoformat() if parsed is not None else None


def _normalize_float(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        return f"{float(value):.8g}"
    except (TypeError, ValueError):
        return str(value)


def _optional_string(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _parse_optional_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        if len(text) == 6 and text.isdigit():
            return datetime.strptime(text + "01", "%Y%m%d").date()
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid instrument expiry: {value!r}") from exc

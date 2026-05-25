"""Auto-discovery for optional proprietary dashboard plugins."""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_DASHBOARD_MODULES = ("protected_dashboard",)
DISABLE_ENV = "IBKR_LT_DASHBOARD_DISABLED"


@dataclass(frozen=True)
class DashboardPluginStatus:
    available: bool = False
    licensed: bool = False
    active: bool = False
    module: str = ""
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "licensed": self.licensed,
            "active": self.active,
            "module": self.module,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class DashboardLoadResult:
    plugin: Any | None
    status: DashboardPluginStatus


def load_dashboard_plugin(config: Mapping[str, Any] | None = None) -> DashboardLoadResult:
    """Load a dashboard plugin if one is present and licensed.

    Missing, disabled, unlicensed, or broken dashboard packages are non-fatal so
    the runtime can continue in API-only mode.
    """
    config = dict(config or {})
    if _dashboard_disabled(config):
        return DashboardLoadResult(
            plugin=None,
            status=DashboardPluginStatus(reason="dashboard_disabled"),
        )

    last_reason = "dashboard_module_not_found"
    for module_name in _dashboard_modules(config):
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name or module_name.startswith(f"{exc.name}."):
                last_reason = "dashboard_module_not_found"
                continue
            log.warning("Dashboard module %s import failed: %s", module_name, exc)
            last_reason = f"import_error: {exc}"
            continue
        except Exception as exc:
            log.warning("Dashboard module %s import failed: %s", module_name, exc)
            last_reason = f"import_error: {exc}"
            continue

        factory = getattr(module, "get_dashboard_plugin", None)
        if not callable(factory):
            last_reason = "plugin_factory_missing"
            log.warning("Dashboard module %s has no get_dashboard_plugin()", module_name)
            continue
        try:
            plugin = factory()
            status = _normalize_plugin_status(plugin, module_name)
        except Exception as exc:
            log.warning("Dashboard plugin %s status check failed: %s", module_name, exc)
            return DashboardLoadResult(
                plugin=None,
                status=DashboardPluginStatus(
                    available=False,
                    licensed=False,
                    active=False,
                    module=module_name,
                    reason=f"status_error: {exc}",
                ),
            )
        if not status.available or not status.licensed:
            log.info(
                "Dashboard skipped module=%s available=%s licensed=%s reason=%s",
                module_name,
                status.available,
                status.licensed,
                status.reason,
            )
            return DashboardLoadResult(plugin=None, status=status)
        return DashboardLoadResult(plugin=plugin, status=status)

    return DashboardLoadResult(
        plugin=None,
        status=DashboardPluginStatus(reason=last_reason),
    )


def mount_dashboard_plugin(
    app,
    result: DashboardLoadResult,
    operator_service,
    *,
    config: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> DashboardPluginStatus:
    if result.plugin is None:
        return result.status
    try:
        result.plugin.mount(
            app,
            operator_service,
            config=dict(config or {}),
            metadata=dict(metadata or {}),
        )
    except Exception as exc:
        module = result.status.module
        log.warning("Dashboard mount failed module=%s: %s", module, exc)
        return replace(result.status, active=False, reason=f"mount_error: {exc}")
    return replace(result.status, active=True, reason="")


def _normalize_plugin_status(plugin: Any, module_name: str) -> DashboardPluginStatus:
    status_fn = getattr(plugin, "status", None)
    raw = status_fn() if callable(status_fn) else {}
    if raw is None:
        raw = {}
    available = _status_value(raw, "available", default=True)
    licensed = _status_value(raw, "licensed", default=True)
    reason = str(_status_value(raw, "reason", default="") or "")
    module = str(_status_value(raw, "module", default=module_name) or module_name)
    active = bool(_status_value(raw, "active", default=False))
    return DashboardPluginStatus(
        available=bool(available),
        licensed=bool(licensed),
        active=active,
        module=module,
        reason=reason,
    )


def _status_value(raw: Any, field: str, *, default: Any) -> Any:
    if isinstance(raw, Mapping):
        return raw.get(field, default)
    return getattr(raw, field, default)


def _dashboard_disabled(config: Mapping[str, Any]) -> bool:
    if _truthy(os.getenv(DISABLE_ENV, "")):
        return True
    dashboard_cfg = dict(config.get("dashboard") or {})
    if bool(dashboard_cfg.get("disabled", False)):
        return True
    if "enabled" in dashboard_cfg and not bool(dashboard_cfg.get("enabled")):
        return True
    return False


def _dashboard_modules(config: Mapping[str, Any]) -> list[str]:
    dashboard_cfg = dict(config.get("dashboard") or {})
    configured = dashboard_cfg.get("modules", dashboard_cfg.get("module"))
    if configured is None:
        return list(DEFAULT_DASHBOARD_MODULES)
    if isinstance(configured, str):
        return [configured]
    return [str(item) for item in configured]


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}

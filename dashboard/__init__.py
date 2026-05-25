"""Optional proprietary dashboard plugin loader."""

from .loader import (
    DashboardLoadResult,
    DashboardPluginStatus,
    load_dashboard_plugin,
    mount_dashboard_plugin,
)

__all__ = [
    "DashboardLoadResult",
    "DashboardPluginStatus",
    "load_dashboard_plugin",
    "mount_dashboard_plugin",
]

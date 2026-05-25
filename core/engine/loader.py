"""Strategy registry and discovery loader.

register_strategy: decorator that validates and registers a StrategyKernel subclass.
load_strategies: walk a package directory and import each strategy module so
  decorators fire and populate the registry.

Implemented in Phase 4. Stub here so the tradeframe facade is importable from Phase 1.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..interfaces.strategy import StrategyKernel

_registry: dict[str, type["StrategyKernel"]] = {}


def register_strategy(cls: type["StrategyKernel"]) -> type["StrategyKernel"]:
    """Decorator. Validates SPEC presence, isinstance check, registers by SPEC.id."""
    from ..interfaces.strategy import StrategyKernel as _SK  # local import avoids circular

    if not (isinstance(cls, type) and issubclass(cls, _SK)):
        raise TypeError(f"{cls} is not a subclass of StrategyKernel")
    if not hasattr(cls, "SPEC"):
        raise AttributeError(f"{cls.__name__} must define a class-level SPEC")
    spec_id = cls.SPEC.id
    if spec_id in _registry:
        raise ValueError(f"Strategy id {spec_id!r} already registered")
    _registry[spec_id] = cls
    return cls


def load_strategies(package: str | Sequence[str] = "strategies") -> None:
    """Import all non-underscore strategy modules from one or more packages.

    Default package is "strategies" — a flat folder at the repo root where
    each file defines one strategy class decorated with @register_strategy.

    Each module's @register_strategy decorator fires on import and populates
    the registry. Package discovery uses importlib/pkgutil so normal Python
    files and protected/compiled package outputs can share the same loading path.
    """
    packages = [package] if isinstance(package, str) else list(package)
    for package_name in packages:
        _load_strategy_package(str(package_name))


def _load_strategy_package(package: str) -> None:
    pkg = importlib.import_module(package)
    package_path = getattr(pkg, "__path__", None)
    if package_path is None:
        return
    for module in sorted(pkgutil.iter_modules(package_path), key=lambda item: item.name):
        if module.name.startswith("_"):
            continue
        importlib.import_module(f"{package}.{module.name}")


def get_registry() -> dict[str, type["StrategyKernel"]]:
    """Return a snapshot of the current strategy registry."""
    return dict(_registry)

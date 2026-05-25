"""Tool-agnostic customer/protected package leak checker."""

from __future__ import annotations

import argparse
import fnmatch
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DEFAULT_EXCLUDED_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    "htmlcov",
    "runs",
}

_RUNTIME_ARTIFACT_SUFFIXES = {
    ".csv",
    ".db",
    ".feather",
    ".h5",
    ".hdf5",
    ".json",
    ".jsonl",
    ".log",
    ".npy",
    ".npz",
    ".parquet",
    ".pkl",
    ".pickle",
    ".sqlite",
    ".sqlite3",
}

_SAFE_PROTECTED_PY_FILES = {"__init__.py"}


@dataclass(frozen=True)
class Finding:
    path: Path
    message: str


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = Path(args.root).resolve()
    findings = check_customer_package(
        root=root,
        forbidden_tokens=list(args.forbidden_token or []),
        config_path=Path(args.config).resolve() if args.config else None,
        allow_patterns=list(args.allow_path or []),
    )
    if findings:
        for finding in findings:
            print(f"{finding.path}: {finding.message}", file=sys.stderr)
        return 1
    print(f"Customer package check passed: {root}")
    return 0


def check_customer_package(
    *,
    root: Path,
    forbidden_tokens: list[str] | None = None,
    config_path: Path | None = None,
    allow_patterns: list[str] | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    forbidden_tokens = [token for token in (forbidden_tokens or []) if token]
    allow_patterns = list(allow_patterns or [])

    if not root.exists():
        return [Finding(root, "package root does not exist")]
    if config_path is not None:
        findings.extend(_check_customer_config(config_path))

    for path in _iter_files(root):
        rel = path.relative_to(root)
        rel_text = rel.as_posix()
        if _is_allowed(rel_text, allow_patterns):
            continue
        findings.extend(_check_file_shape(path, rel))
        findings.extend(_check_forbidden_tokens(path, rel, forbidden_tokens))
    return findings


def _check_customer_config(path: Path) -> list[Finding]:
    if not path.exists():
        return [Finding(path, "customer config does not exist")]
    try:
        config = _load_yaml(path)
    except Exception as exc:
        return [Finding(path, f"could not read customer config: {exc}")]

    findings: list[Finding] = []
    logging_cfg = dict(config.get("logging") or {})
    if str(config.get("runtime_profile") or logging_cfg.get("profile") or "") != "customer":
        findings.append(Finding(path, "customer config must set runtime_profile/logging.profile to customer"))
    if str(logging_cfg.get("strategy_decisions", "off")).lower() not in {"off", "none", "false"}:
        findings.append(Finding(path, "customer config must disable full strategy decision traces"))

    packages = config.get("strategy_packages") or []
    if isinstance(packages, str):
        packages = [packages]
    if "protected_strategies" not in [str(item) for item in packages]:
        findings.append(Finding(path, "customer config should load protected_strategies"))

    strategy_ids = config.get("strategies") or []
    if isinstance(strategy_ids, str):
        strategy_ids = [strategy_ids]
    unsafe_ids = [
        str(strategy_id)
        for strategy_id in strategy_ids
        if not _looks_public_strategy_id(str(strategy_id))
    ]
    if unsafe_ids:
        findings.append(Finding(path, f"customer strategy ids should be neutral/public-safe: {unsafe_ids}"))
    return findings


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError("YAML root must be a mapping")
    return payload


def _check_file_shape(path: Path, rel: Path) -> list[Finding]:
    findings: list[Finding] = []
    rel_parts = rel.parts
    suffix = path.suffix.lower()
    if suffix in _RUNTIME_ARTIFACT_SUFFIXES:
        findings.append(Finding(rel, "runtime/log/data artifact should not be in customer package"))
    if rel_parts and rel_parts[0] == "strategies":
        findings.append(Finding(rel, "raw strategies package should not be in customer package"))
    if (
        rel_parts
        and rel_parts[0] == "protected_strategies"
        and suffix == ".py"
        and path.name not in _SAFE_PROTECTED_PY_FILES
    ):
        findings.append(Finding(rel, "protected strategy Python source should not be distributed"))
    return findings


def _check_forbidden_tokens(
    path: Path,
    rel: Path,
    forbidden_tokens: list[str],
) -> list[Finding]:
    if not forbidden_tokens:
        return []
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []
    except OSError as exc:
        return [Finding(rel, f"could not scan file: {exc}")]
    return [
        Finding(rel, f"forbidden token found: {token}")
        for token in forbidden_tokens
        if token in content or token in rel.as_posix()
    ]


def _iter_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in _DEFAULT_EXCLUDED_DIRS for part in path.relative_to(root).parts):
            continue
        yield path


def _is_allowed(rel_path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel_path, pattern) for pattern in patterns)


def _looks_public_strategy_id(strategy_id: str) -> bool:
    normalized = strategy_id.strip().lower()
    if normalized.startswith("strategy_") and normalized.replace("_", "").isalnum():
        return True
    if normalized.startswith("strategy-") and normalized.replace("-", "").isalnum():
        return True
    return normalized in {"strategy", "demo_strategy", "sample_strategy"}


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Customer package directory to scan")
    parser.add_argument("--config", default=None, help="Customer YAML config to validate")
    parser.add_argument(
        "--forbidden-token",
        action="append",
        default=[],
        help="Private token or strategy ID that must not appear in package files",
    )
    parser.add_argument(
        "--allow-path",
        action="append",
        default=[],
        help="Glob path to exempt from checks, relative to --root",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

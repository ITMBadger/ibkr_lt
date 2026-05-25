from __future__ import annotations

import json
import logging
from pathlib import Path

from core import DataFeed, Engine
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.audit import AuditLogger
from core.audit.logger import configure_runtime_logging
from core.interfaces.strategy import StrategyKernel, StrategySpec
from core.types import Instrument, MarketContext, Signal
from main import _api_metadata
from tools.customer_package_check import check_customer_package

QQQ = Instrument(asset_class="equity", symbol="QQQ")


class _SecretStrategy(StrategyKernel):
    SPEC = StrategySpec(
        id="secret_alpha",
        primary_instrument=QQQ,
        execution_instrument=QQQ,
        timeframes=("1m",),
    )

    def generate(self, ctx: MarketContext, state: dict) -> Signal | None:
        return None


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


def test_api_metadata_customer_profile_redacts_strategy_ids():
    metadata = _api_metadata(
        {"mode": "live", "strategy_modes": {"secret_alpha": "live"}},
        ["secret_alpha"],
        metadata_profile="customer",
        strategy_aliases={"secret_alpha": "strategy_1"},
    )

    text = json.dumps(metadata)
    assert "secret_alpha" not in text
    assert metadata["strategies"] == ["strategy_1"]
    assert metadata["strategy_modes"] == {"strategy_1": "live"}


def test_engine_customer_snapshot_minimizes_strategy_metadata():
    engine = Engine(
        broker=PaperBroker(),
        data_feed=DataFeed(None, ReplayDataProvider([])),
        strategies=[(_SecretStrategy(), {})],
        metadata_profile="customer",
        strategy_aliases={"secret_alpha": "strategy_1"},
    )

    snapshot = engine.snapshot_state()
    text = json.dumps(snapshot)

    assert "secret_alpha" not in text
    assert snapshot["strategies"] == [
        {"id": "strategy_1", "mode": "live", "status": "loaded"}
    ]
    assert "primary_instrument" not in snapshot["strategies"][0]


def test_audit_customer_profile_redacts_payloads_and_skips_decisions(tmp_path: Path):
    audit = AuditLogger(
        log_dir=tmp_path,
        profile="customer",
        strategy_aliases={"secret_alpha": "strategy_1"},
    )
    audit.signal({
        "event": "entry",
        "strategy_id": "secret_alpha",
        "idempotency_key": "secret_alpha-QQQ-long",
    })
    audit.decision(object())  # customer profile returns before reading trace detail

    text = (tmp_path / "signals.jsonl").read_text(encoding="utf-8")
    assert "secret_alpha" not in text
    assert "strategy_1" in text
    assert not list(tmp_path.glob("strategy_*"))


def test_runtime_logging_customer_profile_filters_existing_handlers(tmp_path: Path):
    root = logging.getLogger()
    old_handlers = list(root.handlers)
    capture = _CaptureHandler()
    capture.setFormatter(logging.Formatter("%(message)s"))
    try:
        root.handlers = [capture]
        configure_runtime_logging(
            log_dir=tmp_path,
            profile="customer",
            strategy_aliases={"secret_alpha": "strategy_1"},
        )
        root.warning("strategy secret_alpha emitted a runtime event")
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers = old_handlers

    assert capture.messages
    assert "secret_alpha" not in capture.messages[-1]
    assert "strategy_1" in capture.messages[-1]
    runtime_log = (tmp_path / "runtime.log").read_text(encoding="utf-8")
    assert "secret_alpha" not in runtime_log
    assert "strategy_1" in runtime_log


def test_customer_template_and_package_check_are_public_safe(tmp_path: Path):
    package = tmp_path / "customer_package"
    package.mkdir()
    protected = package / "protected_strategies"
    protected.mkdir()
    (protected / "__init__.py").write_text("", encoding="utf-8")
    template = Path("configs/customer.template.yaml")

    findings = check_customer_package(
        root=package,
        config_path=template,
        forbidden_tokens=["secret_alpha"],
    )

    assert findings == []


def test_customer_package_check_flags_raw_strategy_source(tmp_path: Path):
    package = tmp_path / "customer_package"
    protected = package / "protected_strategies"
    protected.mkdir(parents=True)
    (protected / "__init__.py").write_text("", encoding="utf-8")
    (protected / "secret_alpha.py").write_text("PRIVATE_TOKEN = 'secret_alpha'\n", encoding="utf-8")

    findings = check_customer_package(
        root=package,
        forbidden_tokens=["secret_alpha"],
    )

    messages = "\n".join(finding.message for finding in findings)
    assert "protected strategy Python source" in messages
    assert "forbidden token" in messages


def test_customer_package_check_flags_raw_dashboard_source(tmp_path: Path):
    package = tmp_path / "customer_package"
    protected = package / "protected_dashboard"
    protected.mkdir(parents=True)
    (protected / "__init__.py").write_text("", encoding="utf-8")
    (protected / "app.py").write_text("PRIVATE_DASHBOARD_TOKEN = 'secret_alpha'\n", encoding="utf-8")

    findings = check_customer_package(
        root=package,
        forbidden_tokens=["secret_alpha"],
    )

    messages = "\n".join(finding.message for finding in findings)
    assert "protected dashboard Python source" in messages
    assert "forbidden token" in messages


def test_customer_package_check_flags_top_level_dashboard_source(tmp_path: Path):
    package = tmp_path / "customer_package"
    package.mkdir()
    (package / "protected_dashboard.py").write_text("PRIVATE = 'secret_alpha'\n", encoding="utf-8")

    findings = check_customer_package(
        root=package,
        forbidden_tokens=["secret_alpha"],
    )

    messages = "\n".join(finding.message for finding in findings)
    assert "protected dashboard Python source" in messages
    assert "forbidden token" in messages

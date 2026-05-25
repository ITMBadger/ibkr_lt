# Project Rules

This file captures durable engineering rules for future code changes.

## Modular Design

Design and update the code in a modular way. Prefer small, focused modules with clear ownership boundaries so future upgrades, broker/provider changes, strategy changes, and maintenance stay practical.

- Keep framework behavior in `core/`, API behavior in `api/`, operator tools in `tools/`, and strategy logic in one strategy file per strategy.
- Prefer existing interfaces and adapters over direct cross-module calls.
- Keep strategy code pure inside `generate()` and `on_exit()`: no broker calls, no file or network I/O, and no framework state mutation beyond the provided `state` dict.
- Add shared behavior to framework modules only when it is genuinely reusable and does not expose proprietary strategy logic.
- Keep changes narrowly scoped. Do not mix feature work, refactors, and unrelated cleanup in one change unless the cleanup is required for correctness.

## Broker Test Safety

Tests that connect to a real broker session, including IBKR paper, must stay opt-in and isolated from normal unit and integration tests.

- Put real-session broker tests under `tests/paper/` or another clearly named opt-in folder.
- Gate them behind explicit environment variables so `pytest tests/` does not place orders by accident.
- Refuse live ports/accounts when the test intent is paper-only.
- Use small allowed instruments and quantities.
- Clean up open test orders and flatten only the test-created position delta.
- Do not encode proprietary strategy thresholds, identifiers, or private logic into broker smoke tests.

## Strategy Privacy

All strategy implementations are highly sensitive and proprietary unless explicitly listed as public.

- Public strategy source allowed in git: `strategies/stoch_3m_cross_long.py`.
- Public copy-only strategy scaffold allowed in git: `strategies/_sample_strategy.py`.
- Public package marker allowed in git: `strategies/__init__.py`.
- Public protected package marker allowed in git: `protected_strategies/__init__.py`.
- All other strategy source files, compiled artifacts, configs, docs, tests, formulas, thresholds, identifiers, logs, and derivative materials must remain hidden from git and public documentation.
- Do not move proprietary strategy details into public docs, tests, config examples, comments, or shared framework modules.
- When adding a new public/demo strategy, add an explicit `.gitignore` exception for that file only.
- Customer/protected runtimes should load private modules through `strategy_packages: [protected_strategies]` in a private config and use `logging.profile: customer` with aliases when logs or API output may be shared.
- Start customer configs from `configs/customer.template.yaml` and run `python -m tools.customer_package_check` before sharing a package or config.

## Strategy Authoring Style

New strategies should follow the existing `strategies/stoch_3m_cross_long.py` shape unless there is a specific reason to deviate.

- Put one strategy class per strategy file under `strategies/`; use module-level `Instrument` and timezone constants for shared objects.
- Decorate the class with `@register_strategy`, subclass `StrategyKernel`, and define a class-level `SPEC = StrategySpec(...)`.
- Start from `strategies/_sample_strategy.py` when creating a new strategy, then rename the file, class, and `StrategySpec.id`.
- Treat `StrategySpec.id` as the stable runtime identity used by config, logs, API metadata, and adopted-position ownership. Do not rely on the filename as the strategy id.
- Keep `SPEC` minimal: declare only instruments, required timeframes, warmup bars, position policy, and broker-side protective stops the engine must know before runtime.
- Declare `StrategySpec.position_policy` explicitly. Use `single_position` for one open strategy position per execution instrument; use `multi_position` only when the strategy creates independent logical lots and can manage per-lot state, preferably with deterministic `Signal.trade_id` values.
- Declare the entry-frequency rule in `position_policy`: `one_per_day`, `one_per_session`, or `unlimited`. Do not duplicate date-throttle checks inside each strategy unless a private rule is stricter than the framework policy.
- Leave `supports_position_adoption=False` unless the strategy can safely seed all state needed to manage a broker position that existed before startup.
- When adoption is supported, implement `on_adopt_position()` and require any operator-provided fields the strategy needs through `POSITION_ADOPTION_REQUIRED_FIELDS`.
- Startup adoption mappings must specify explicit quantity. Do not infer adoption quantity from strategy risk sizing.
- For protected/private strategies, prefer `ctx.features.get(...)` inside `generate()` or `on_exit()` for common indicators instead of listing every feature in `StrategySpec.indicators`. Example: `ctx.features.get("ema", QQQ, "3m", period=20)`.
- Keep proprietary formulas, thresholds, scoring, entry/exit conditions, and condition names inside the private strategy file. Do not copy them into shared framework modules, config examples, public docs, or tests.
- Keep `generate()` and `on_exit()` pure: no broker calls, file I/O, network I/O, thread management, or framework mutation outside the provided `state` dict.
- Initialize strategy state in `on_start()` and use the provided `state` dict for runtime memory such as last signal date, last evaluated bar, or dedup keys.
- Use deterministic pandas/numpy operations on `ctx.bars` or `ctx.features`; guard against insufficient bars before reading latest/prior rows.
- Use `DecisionTrace` with a local `finish()` helper when decision logging is needed, and record the trace with `record_decision(state, trace)` before every return path.
- Keep decision traces owner/dev oriented. Include only the bars, metrics, tables, indicators, and conditions needed to debug the strategy, because these logs can reveal behavior if shared.

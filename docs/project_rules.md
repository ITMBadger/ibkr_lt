# Project Rules

This file captures durable engineering rules for future code changes.

## Modular Design

Design and update the code in a modular way. Prefer small, focused modules with clear ownership boundaries so future upgrades, broker/provider changes, strategy changes, and maintenance stay practical.

- Keep framework behavior in `core/`, API behavior in `api/`, operator tools in `tools/`, and strategy logic in one strategy file per strategy.
- Prefer existing interfaces and adapters over direct cross-module calls.
- Keep strategy code pure inside `generate()` and `on_exit()`: no broker calls, no file or network I/O, and no framework state mutation beyond the provided `state` dict.
- Add shared behavior to framework modules only when it is genuinely reusable and does not expose proprietary strategy logic.
- Keep changes narrowly scoped. Do not mix feature work, refactors, and unrelated cleanup in one change unless the cleanup is required for correctness.

## Strategy Privacy

All strategy implementations are highly sensitive and proprietary unless explicitly listed as public.

- Public strategy source allowed in git: `strategies/stoch_3m_cross_long.py`.
- Public package marker allowed in git: `strategies/__init__.py`.
- All other strategy source files, compiled artifacts, configs, docs, tests, formulas, thresholds, identifiers, logs, and derivative materials must remain hidden from git and public documentation.
- Do not move proprietary strategy details into public docs, tests, config examples, comments, or shared framework modules.
- When adding a new public/demo strategy, add an explicit `.gitignore` exception for that file only.

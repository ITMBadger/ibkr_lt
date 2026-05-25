# Guardrails

This document lists the safety controls currently active in the MVP framework.
Deferred controls and future hardening work are tracked in the local roadmap
area under `docs/roadmap/`.

## What Is Active (MVP)

### DataManager Dedup Policy

The most critical safety control at the data layer. It keeps startup backfill and live updates deterministic.

| Scenario | Rule |
|---|---|
| Offline CSV has a timestamp | CSV/offline historical source wins during startup backfill |
| Offline CSV is stale | Live provider historical fetch fills the missing gap up to startup |
| Live stream emits a timestamp that already exists | Live stream overwrites that timestamp |
| Duplicate 1-min timestamp | Latest writer wins (`keep='last'`) |

The shared runtime config uses regular-hours CSV files for offline history and IBKR for both gap backfill and live streaming. CSV, IBKR historical, and IBKR live data are expected to be regular trading hours only (`09:30` to `16:00` America/New_York).

### QuantityRules Validation

`OrderManager` consults `BrokerAdapter.capabilities.quantity_rules` before every submission:

- Rejects orders whose `instrument.asset_class` is not in the broker's `asset_classes`.
- Rejects order types not in `order_types`.
- Rejects short entries when `supports_short=False`.
- Rejects fractional quantities when `supports_fractional=False`.
- Rounds quantity to the nearest valid `quantity_step`, clamped to `min_quantity`.

This prevents hard venue rejects for crypto step sizes and ensures futures are always integer contracts.

### RiskPolicy Cap

`RiskPolicy.max_order_quantity` (default `2`) prevents oversized fixed-share
orders from reaching the broker. It is configured by the runtime bootstrap from
CLI defaults or YAML.

Backtests may opt into `backtest.sizing.mode: full_equity`, which sizes from the
simulated mark-to-market account equity and the latest replay price. In that
mode, `backtest.sizing.max_order_quantity` is optional; when omitted, sizing is
intentionally uncapped for account-based research runs.

### Warmup Guard

`Scheduler` checks `StrategySpec.warmup_bars` before dispatching a strategy. A strategy whose primary timeframe has fewer bars than `warmup_bars` will not receive a `MarketContext` and cannot generate a signal. This prevents signals on thin data at startup.

### Strategy Position Policy

Strategies declare `StrategySpec.position_policy`.

`position_mode="single_position"` permits one open strategy position per execution instrument. While that position is open, `Engine` calls `on_exit()` instead of `generate()`.

`position_mode="multi_position"` permits independent logical lots on the same instrument. Multi-position strategies should use deterministic `Signal.trade_id` values when they need per-lot exit state.

`entry_frequency` is enforced by `Engine` before order submission. Supported values are `one_per_day`, `one_per_session`, and `unlimited`. The current session key is the market date in the broker/session timezone.

### Centralized Order Submission

`OrderManager` owns strategy signal submission, strategy close submission, per-strategy dry-run enforcement, fill application, broker order-status logging, and configured broker-side protective stops.

### Dry Run

Dry-run is configured per strategy with `strategy_modes.<strategy_id>: dry_run`. The runtime still uses the real IBKR broker path for account, position, and market visibility, but `OrderManager` does not call `BrokerAdapter.submit_order()` for dry-run strategies. It writes `order_intent` plus `order_dry_run` or `close_dry_run` audit events with `OrderStatus(status="dry_run")`. Strategies not listed default to `live`.

IBKR paper/live is only an environment and port selection. It is not an execution-mode wrapper: `--paper` selects the IBKR paper port, and `--live` selects the IBKR live port.

### Startup Position Adoption

`Engine` loads broker positions on startup and seeds `PortfolioState`.

In live mode, the startup position gate is enabled. Broker positions that do
not match any enabled strategy execution instrument are logged as unmanaged and
startup continues. Broker positions that match one or more enabled strategy
execution instruments either use a stored ownership-ledger/config allocation or
pause startup at `awaiting_startup_mapping` until an operator submits protected
API allocations. A strategy can adopt a live broker position only when its
`PositionPolicy.supports_position_adoption` is `True` and
`on_adopt_position()` returns the strategy-owned `Position`.

Each allocation must include an explicit `quantity`; startup no longer assumes
the strategy's configured entry size is the adoption size. If multiple enabled
strategies share the same execution instrument, the operator or private config
must choose the owner. If mapping is required and the control API is disabled,
live startup fails fast rather than waiting without a mapping path.

`runs/state/position_ownership.json` records bot-created live ownership from
fills and is used only to remap still-open broker positions on restart. YAML
`adopted_positions` remains available as an explicit startup ownership mapping
when the ledger cannot know the owner.

### Strategy-Owned Exits

If a strategy has an owned open position, `Engine` calls `StrategyKernel.on_exit()` on each matching bar. A returned reason submits an opposite-side market close through `OrderManager`.

### Broker-Side Protective Stops

Strategies may declare `StrategySpec.protective_stop`. When an entry fill arrives, `OrderManager` can submit an opposite-side broker-native stop order using the actual fill price as reference. This is order-management protection, not strategy `on_exit()` logic. Because the stop is based on the actual fill, it is submitted after the fill callback rather than pre-attached atomically before entry fill.

If a strategy-owned `on_exit()` close is accepted before the protective stop
fills, `OrderManager` requests cancellation of the tracked protective stop order.
If the protective stop fills first, `OrderManager` requests cancellation of any
pending strategy close for the same strategy/instrument/trade lot.

### Pending Close Dedupe

`OrderManager.submit_close()` keeps a pending-close index keyed by strategy,
instrument, and logical `trade_id`. While a close is pending, duplicate
`on_exit()` reasons for the same lot are dropped centrally and audited as
`close_already_pending`. The pending marker is cleared when the close fill is
applied, or when the broker reports the close as cancelled/rejected.

### Event Backtests

`python -m backtest.run` uses the production `Engine` with `SimulatedClock`, `ReplayDataProvider`, `PaperBroker`, and real strategy modules. The runner loads warmup data from CSV before the replay start timestamp, then executes strategy logic only on replay bars inside the requested window.

Default `event` mode dispatches selected strategies on every primary 1-minute replay bar, matching the live event cadence. `fast-event` mode keeps every 1-minute data, broker, order, and exit update, but only builds flat-entry strategy context when the selected evaluation timeframe has a new completed bar. Use `--mode fast-event` for faster research passes and `--eval-timeframe <bar_size>` when auto-detection is not enough.

`parallel` mode requires each selected strategy to declare
`_PARALLEL_BACKTEST_SAFE = True`. It loads warmup bars for each worker chunk,
generates flat-entry candidates in worker processes capped by
`min(os.cpu_count(), 4)` or `--max-parallel-workers`, then replays accepted
entries, fills, protective stops, and `on_exit()` chronologically through the
same single-process engine/order path. Treat `parallel` as a research
acceleration mode and validate strategy results against `fast-event`.

Backtests may preload replay bars into the shared feature registry so common
indicators are vectorized once, then sliced to the current
`MarketContext.timestamp`. Resampled features only expose bars completed before
the current replay bar, so preloading must not create future leakage.

Backtest market orders fill at the next replay bar open. Stop orders fill at `stop_price` when crossed by a replay bar. These assumptions are deterministic and close to the live event flow, but they are not a guarantee of identical IBKR live fills, partial fills, slippage, or gap-through stop behavior.

Completed backtests write `summary.json` and `report.html`. The HTML report
reconstructs trades, mark-to-market equity, drawdowns, period returns, and
exposure from replay bars plus `fills.jsonl`; if report generation fails, the
runner writes `report_error.txt` and raises rather than silently publishing an
incomplete report.

### Audit Logs

When `logging.enabled=true`, the runtime creates a per-run folder under the configured run directory, named with minute-level ET time such as `runs/paper/20260522_1047_et/`, and writes runtime, strategy decision, signal, order, and fill logs there. Full decision traces are owner/dev artifacts and include relevant OHLCV, condition thresholds, indicator values, and pass/fail state.

Decision logging is controlled by `logging.decision_scope`:

- `every_eval`: write each strategy decision trace folder as `strategy_eval_<strategy_id>_<YYYYMMDD_HHMM>_et/`.
- `trigger_and_interval`: write per-trigger trace folders named `strategy_trigger_<strategy_id>_<YYYYMMDD_HHMM>_et/` and per-interval trace folders named `strategy_<N>m_<strategy_id>_<YYYYMMDD_HHMM>_et/`.

The shared deployment config uses `trigger_and_interval` with `decision_interval_minutes: 30` to avoid minute-by-minute live log noise while preserving full trigger traces and one historical diagnostic snapshot per 30-minute wall-clock bucket. Duplicate trace folders in the same minute receive a numeric suffix rather than overwriting. Each trace folder contains `decision.csv` plus optional per-timeframe table CSVs, typically current bar plus the previous four bars.

Signal, order, and fill logs remain append-only: `signals.jsonl`, `orders.jsonl`, and `fills.jsonl`.

For protected/customer distribution, use `logging.profile: customer` and
strategy aliases. Customer profile suppresses full decision traces, minimizes
API strategy metadata, and redacts known strategy IDs in audit/runtime log
payloads. Owner profile remains the debugging profile and can reveal strategy
behavior.

Use `configs/customer.template.yaml` for customer-safe defaults and run
`python -m tools.customer_package_check` on any assembled customer folder before
sharing it. The checker is obfuscation-tool agnostic: it rejects runtime/log/data
artifacts, raw `strategies/` packages, protected Python source files, and any
private tokens supplied with `--forbidden-token`.

### IBKR Paper Test Guardrails

Tests under `tests/paper/` are opt-in because they connect to TWS/IB Gateway paper and can place paper orders.

- Skipped unless `IBKR_LT_RUN_PAPER_TESTS=1`.
- Refuse live IBKR ports `7496` and `4001`.
- Require paper account IDs to start with `DU` when `IBKR_LT_PAPER_ACCOUNT` is set.
- Use separate API client IDs from the normal runtime.
- Limit market-order tests to one share and require `IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1`.
- Market-order tests require a guarded US equity RTH window.
- Cleanup cancels test protective stops and flattens only the position delta created by the test.

### Hermes Control API

The FastAPI control API is non-trading in the current framework. It exposes
health, metadata, runtime snapshot, positions, recent events, startup position
gate state, startup position mapping controls, and an event WebSocket for the
Hermes agent/operator.

- Enabled by default; use `--no-api` or `api.enabled=false` only when the API should be disabled.
- Public: `/api/v1/health`, `/api/v1/meta`, `/api/v1/meta/capabilities`.
- Local-only hosts (`127.0.0.1`, `localhost`, `::1`) may run without a token.
- Non-local hosts such as `0.0.0.0` or LAN IPs require `IBKR_LT_API_TOKEN` before startup.
- Protected when a token is set: `/api/v1/runtime/*`, `/api/v1/positions`, `/api/v1/events`, `/api/v1/startup/*`, `/ws/events`.
- API routes read through `Engine.snapshot_state()`.
- API routes must not call broker adapters, `OrderManager`, or strategies directly.
- No manual trade or order-cancel endpoints are active.
- Startup mutation is limited to `/api/v1/startup/mappings` and `/api/v1/startup/refresh`.

### Heartbeat Monitor

`tools/heartbeat_monitor.py` is a separate read-only process for Hermes/operator liveness monitoring. It is not started inside the engine and does not share the engine process.

- Polls `/api/v1/health` every 5 seconds by default.
- Keeps `/ws/events` connected and pings it when no runtime events arrive.
- Writes `runs/heartbeat_monitor/status.json` for current monitor state.
- Appends `runs/heartbeat_monitor/alerts.jsonl` for alert events.
- Alerts when the API is unreachable, the engine enters `error`, the engine is not running when expected, or the WebSocket remains disconnected.
- `main.py` warns at control API startup if no `heartbeat_monitor.py` process is detected.
- The missing-monitor warning is built into API startup and is skipped only when `--no-api` disables the API.
- The missing-monitor warning is non-blocking; the engine still starts and runs when API is enabled.
- Never calls broker adapters, `OrderManager`, strategies, or mutating endpoints.

## Infrastructure Safety

`IBKRBroker.connect()` and `IBKRDataProvider.connect()` call `IBKRClient.connect_and_run()` on the engine's running asyncio loop. The client blocks until `nextValidId` is received (20-second timeout). If the connection fails, the engine raises before the dispatch loop starts.

`BarBuilder.flush()` can be called at session end to emit any partial 1-minute bar. The engine does not currently call this automatically — it is available for the lifecycle management phase.

V1 does not place full broker-native brackets. Configured broker-side protective stops are submitted after entry fills; strategy `on_exit()` exits stop running if the bot is stopped. Check TWS or IBKR Mobile after unplanned shutdown.

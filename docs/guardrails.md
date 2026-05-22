# Guardrails

This document lists the safety controls currently active in the MVP framework, and what is deferred to later phases.

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

`RiskPolicy.max_order_quantity` (default `2`) prevents oversized computed orders from reaching the broker. It is configured by the runtime bootstrap from CLI defaults or YAML.

### Warmup Guard

`Scheduler` checks `StrategySpec.warmup_bars` before dispatching a strategy. A strategy whose primary timeframe has fewer bars than `warmup_bars` will not receive a `MarketContext` and cannot generate a signal. This prevents signals on thin data at startup.

### One Signal Per Day

All live strategies enforce a `state["last_signal_date"]` check inside `generate()`. A second signal on the same calendar day is silently dropped. This is strategy-side enforcement (inside each `.py` file), not a framework gate.

### Centralized Order Submission

`OrderManager` owns strategy signal submission, strategy close submission, per-strategy dry-run enforcement, fill application, broker order-status logging, and configured broker-side protective stops.

### Dry Run

Dry-run is configured per strategy with `strategy_modes.<strategy_id>: dry_run`. The runtime still uses the real IBKR broker path for account, position, and market visibility, but `OrderManager` does not call `BrokerAdapter.submit_order()` for dry-run strategies. It writes `order_intent` plus `order_dry_run` or `close_dry_run` audit events with `OrderStatus(status="dry_run")`. Strategies not listed default to `live`.

IBKR paper/live is only an environment and port selection. It is not an execution-mode wrapper: `--paper` selects the IBKR paper port, and `--live` selects the IBKR live port.

### Startup Position Adoption

`Engine` loads broker positions on startup and seeds `PortfolioState`. If an adopted position maps to exactly one strategy execution instrument, it is assigned to that strategy. If multiple strategies share the instrument, YAML `adopted_positions` mapping is required before strategy exits manage it.

### Strategy-Owned Exits

If a strategy has an owned open position, `Engine` calls `StrategyKernel.on_exit()` on each matching bar. A returned reason submits an opposite-side market close through `OrderManager`.

### Broker-Side Protective Stops

Strategies may declare `StrategySpec.protective_stop`. When an entry fill arrives, `OrderManager` can submit an opposite-side broker-native stop order using the actual fill price as reference. This is order-management protection, not strategy `on_exit()` logic. Because the stop is based on the actual fill, it is submitted after the fill callback rather than pre-attached atomically before entry fill.

### Event Backtests

`python -m backtest.run` uses the production `Engine` with `SimulatedClock`, `ReplayDataProvider`, `PaperBroker`, and real strategy modules. The runner loads warmup data from CSV before the replay start timestamp, then executes strategy logic only on replay bars inside the requested window.

Backtest market orders fill at the next replay bar open. Stop orders fill at `stop_price` when crossed by a replay bar. These assumptions are deterministic and close to the live event flow, but they are not a guarantee of identical IBKR live fills, partial fills, slippage, or gap-through stop behavior.

### Audit Logs

When `logging.enabled=true`, the runtime creates a per-run folder under the configured log directory, named with minute-level ET time such as `logs/20260522_1047_et/`, and writes runtime, strategy decision, signal, order, and fill logs there. Full decision traces are owner/dev artifacts and include relevant OHLCV, condition thresholds, indicator values, and pass/fail state.

Decision logging is controlled by `logging.decision_scope`:

- `every_eval`: write each strategy decision trace folder as `strategy_eval_<strategy_id>_<YYYYMMDD_HHMM>_et/`.
- `trigger_and_interval`: write per-trigger trace folders named `strategy_trigger_<strategy_id>_<YYYYMMDD_HHMM>_et/` and per-interval trace folders named `strategy_<N>m_<strategy_id>_<YYYYMMDD_HHMM>_et/`.

The shared deployment config uses `trigger_and_interval` with `decision_interval_minutes: 30` to avoid minute-by-minute live log noise while preserving full trigger traces and one historical diagnostic snapshot per 30-minute wall-clock bucket. Duplicate trace folders in the same minute receive a numeric suffix rather than overwriting. Each trace folder contains `decision.csv` plus optional per-timeframe table CSVs, typically current bar plus the previous four bars.

Signal, order, and fill logs remain append-only: `signals.jsonl`, `orders.jsonl`, and `fills.jsonl`.

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

The FastAPI control API is read-only in the current framework. It exposes health, metadata, runtime snapshot, positions, recent events, and an event WebSocket for the Hermes agent/operator.

- Enabled by default; use `--no-api` or `api.enabled=false` only when the API should be disabled.
- Public: `/api/v1/health`, `/api/v1/meta`, `/api/v1/meta/capabilities`.
- Local-only hosts (`127.0.0.1`, `localhost`, `::1`) may run without a token.
- Non-local hosts such as `0.0.0.0` or LAN IPs require `IBKR_LT_API_TOKEN` before startup.
- Protected when a token is set: `/api/v1/runtime/*`, `/api/v1/positions`, `/api/v1/events`, `/ws/events`.
- API routes read through `Engine.snapshot_state()`.
- API routes must not call broker adapters, `OrderManager`, or strategies directly.
- No manual trade, order cancel, startup approval, or state mutation endpoints are active.

### Heartbeat Monitor

`tools/heartbeat_monitor.py` is a separate read-only process for Hermes/operator liveness monitoring. It is not started inside the engine and does not share the engine process.

- Polls `/api/v1/health` every 5 seconds by default.
- Keeps `/ws/events` connected and pings it when no runtime events arrive.
- Writes `var/heartbeat_monitor/status.json` for current monitor state.
- Appends `var/heartbeat_monitor/alerts.jsonl` for alert events.
- Alerts when the API is unreachable, the engine enters `error`, the engine is not running when expected, or the WebSocket remains disconnected.
- `main.py` warns at control API startup if no `heartbeat_monitor.py` process is detected.
- The missing-monitor warning is built into API startup and is skipped only when `--no-api` disables the API.
- The missing-monitor warning is non-blocking; the engine still starts and runs when API is enabled.
- Never calls broker adapters, `OrderManager`, strategies, or mutating endpoints.

---

## What Is Deferred (Phase 7+)

These controls existed in the archived legacy system and will be ported back once the framework spine is proven.

| Control | Legacy location | Status |
|---|---|---|
| Centralized entry window gate (`min_entry_time`, `max_entry_time`) | archived runtime policy | Deferred; bundled strategies still apply their own local entry windows |
| Daily drawdown kill switch | archived runtime state + heartbeat | Deferred |
| Priority pairs (long/short conflict prevention) | archived priority-pair policy | Deferred |
| Configurable position modes (`first_only`, `allow_scaling`) | archived trade policy | Deferred |
| Trigger dedup (same `trigger_ts` across bars) | archived dispatcher | Deferred |
| L1 session filters | archived central runtime | Deferred |
| Manual startup adoption review workflow | archived adoption workflow | Deferred |
| Open-order reconciliation | archived open-order workflow | Deferred |
| Broker-native bracket management | archived execution service | Deferred |
| General runtime close-percent protective stops | strategy/runtime policy | Deferred; configured broker-side fill-price stops are active |
| JSON state persistence | archived runtime persistence | Deferred |
| Buying-power check (warning) | archived central runtime | Deferred |
| Mutating Hermes/API command bus | archived `RuntimeCommandBus` | Deferred; current API is read-only |

---

## Infrastructure Safety

`IBKRBroker.connect()` and `IBKRDataProvider.connect()` call `IBKRClient.connect_and_run()` on the engine's running asyncio loop. The client blocks until `nextValidId` is received (20-second timeout). If the connection fails, the engine raises before the dispatch loop starts.

`BarBuilder.flush()` can be called at session end to emit any partial 1-minute bar. The engine does not currently call this automatically — it is available for the lifecycle management phase.

V1 does not place full broker-native brackets. Configured broker-side protective stops are submitted after entry fills; strategy `on_exit()` exits stop running if the bot is stopped. Check TWS or IBKR Mobile after unplanned shutdown.

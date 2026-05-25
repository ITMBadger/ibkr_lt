# ibkr_lt

`ibkr_lt` is a modular Python trading framework built around a ports-and-adapters design.

The core idea is simple: the engine is stable, while broker and market-data providers plug in like cartridges through small interface contracts.

![ibkr_lt project flow](assets/ibkr-lt-project-flow.webp)

## Architecture

- Strategies produce intent only: `Signal` or exit reason.
- Strategies declare position ownership and entry throttling through `StrategySpec.position_policy`.
- The engine owns scheduling, market context construction, risk routing, and execution flow.
- `OrderManager` is the only framework component that submits orders to broker adapters and enforces per-strategy dry-run.
- `DataFeed` composes historical and live data providers.
- `DataManager` owns bar storage, deduplication, revisioning, and resampling.
- `FeatureRegistry` computes shared indicators once per instrument/timeframe/revision.

The shared paper config uses local regular-hours CSV files for offline historical data and IBKR for both supplemental historical gap fill and 5-second live bars. At startup the engine loads CSV history, fills any recent gap from IBKR historical bars, then subscribes to IBKR live streaming.

## Cartridge Boundaries

Broker cartridges implement `core/interfaces/broker.py`.

Market-data cartridges implement `core/interfaces/data.py`.

Strategy modules implement `core/interfaces/strategy.py`.

This keeps broker SDKs, data-provider SDKs, and private strategy logic outside the core engine.

## Public Repo Scope

This repository contains the framework, adapters, tests, and public runtime skeleton.

Proprietary strategy implementations, protected build artifacts, detailed strategy docs, research notebooks, market data, configs, and decision logs are intentionally excluded from Git.

To run the project from a fresh public clone, add your own public/demo strategy module under `strategies/`, or put protected customer/private strategy modules under `protected_strategies/` and add that package to `strategy_packages` in a private config.

## Strategy Authoring

Start from the copy-only scaffold:

```text
strategies/_sample_strategy.py
```

The leading underscore keeps it out of automatic strategy loading. Copy it to a new non-underscore file, rename the class, and give it a stable `StrategySpec.id`.

Every real strategy declares a `PositionPolicy`:

```python
position_policy=PositionPolicy(
    position_mode=POSITION_MODE_SINGLE,
    entry_frequency=ENTRY_FREQUENCY_ONE_PER_DAY,
)
```

`single_position` means one open strategy position per execution instrument. `multi_position` is for independent logical lots and should use deterministic `Signal.trade_id` values when per-lot exit state matters.

Entry frequency is enforced by the engine with `one_per_day`, `one_per_session`, or `unlimited`. Do not duplicate date-throttle checks inside strategies unless a private strategy needs a stricter rule than the declared framework policy.

## Hermes Control API

The project starts a non-trading FastAPI control surface by default for the Hermes agent and operator runtime visibility:

```bash
python main.py --paper
```

Disable it only when needed:

```bash
python main.py --paper --no-api
```

Default URL:

```text
http://127.0.0.1:8550
```

API auth policy:

- Local-only hosts (`127.0.0.1`, `localhost`, `::1`) may run without a token for a local Hermes agent.
- Non-local hosts such as `0.0.0.0` or LAN IPs require `IBKR_LT_API_TOKEN` to be set before startup.
- When a token is set, protected HTTP endpoints require `Authorization: Bearer <token>`.
- `WS /ws/events` accepts the same bearer header or `?token=<token>`.

Public endpoints:

- `GET /api/v1/health`
- `GET /api/v1/meta`
- `GET /api/v1/meta/capabilities`

Protected endpoints:

- `GET /api/v1/runtime/snapshot`
- `GET /api/v1/runtime/strategies`
- `GET /api/v1/positions`
- `GET /api/v1/events`
- `GET /api/v1/startup/gate`
- `POST /api/v1/startup/mappings`
- `POST /api/v1/startup/refresh`
- `WS /ws/events`

Hermes should call `GET /api/v1/health` first, then use `next_endpoint` to decide whether to poll health again, read `/api/v1/runtime/snapshot`, or inspect `/api/v1/startup/gate`.

The API does not submit trades or cancel broker orders. The only active mutation endpoints are the protected startup gate endpoints, which are limited to live-startup position mapping and refresh.

## Audit Logs

When `logging.enabled=true`, runtime output is written under a per-run folder:
`runs/paper/<YYYYMMDD_HHMM>_et/` or `runs/live/<YYYYMMDD_HHMM>_et/`. If two app
runs start in the same minute, the later folder receives a numeric suffix such
as `_2`.

The default shared config uses quieter owner decision logging:

- `strategy_trigger_<strategy_id>_<YYYYMMDD_HHMM>_et/` stores each trigger trace as a folder of CSV files.
- `strategy_30m_<strategy_id>_<YYYYMMDD_HHMM>_et/` stores one diagnostic trace per 30-minute wall-clock bucket.
- `strategy_eval_<strategy_id>_<YYYYMMDD_HHMM>_et/` stores each evaluation when `logging.decision_scope: every_eval`.

Each decision trace folder contains `decision.csv` plus optional per-timeframe
table CSVs such as `qqq_3m.csv`. Strategy table CSVs use one row per bar,
typically current bar plus the previous four bars.

Signal, order, and fill audit files remain append-only:

- `signals.jsonl`
- `orders.jsonl`
- `fills.jsonl`

Customer/protected builds should use `logging.profile: customer`. In that
profile, full decision traces are disabled, API strategy metadata is minimized,
and strategy IDs in audit/runtime log payloads are replaced with configured or
generated aliases.

Use `configs/customer.template.yaml` as the safe starting point for shared
customer configs. Before packaging, run the tool-agnostic leak checker with any
private IDs or tokens you need to block:

```bash
python -m tools.customer_package_check \
  --root dist/customer_package \
  --config configs/customer.template.yaml \
  --forbidden-token <private_strategy_id>
```

## Strategy Modes

Runtime paper/live selection only chooses the IBKR environment and port. Native
order placement is controlled per strategy in `config.yaml`:

```yaml
strategy_modes:
  stoch_3m_cross_long: live
  another_strategy: dry_run
```

`dry_run` strategies still see real account, position, and market data, but
`OrderManager` logs order intent without calling the broker submit API.

## Live Startup Position Gate

Live mode checks broker positions after connecting. Broker positions that do
not match enabled strategy execution instruments are logged as unmanaged and
startup continues. Matching positions pause startup at `awaiting_startup_mapping`
until an operator submits allocations through `POST /api/v1/startup/mappings`.
Only strategies that declare `supports_position_adoption=True` and implement
`on_adopt_position()` can receive an adopted live position.

Allocations must include an explicit `quantity`. Stored ownership is recovered
from `runs/state/position_ownership.json` when the bot previously opened the
position; `adopted_positions` in a private config can provide explicit startup
ownership when needed. If live startup needs operator mapping and the control
API is disabled, startup fails fast instead of waiting indefinitely.

## Event Backtesting

Backtests use the same `Engine`, strategy modules, `DataManager`,
`FeatureRegistry`, `OrderManager`, and `PaperBroker` path as paper/live runtime:

```bash
python -m backtest.run --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
python -m backtest.run --mode fast-event --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
python -m backtest.run --mode parallel --strategy stoch_3m_cross_long --start 2025-01-01 --end 2025-03-31
```

The runner reads CSV data from `data.historical.path` in `config.yaml`, or from
`--csv`. Use a directory when selected strategies require more than one symbol.
It backfills warmup bars before the start timestamp, then replays test-window
bars event by event. Results are written under `runs/backtests/`, including
`summary.json` and a self-contained `report.html` with fill-derived trades,
equity, drawdown, monthly/yearly returns, and exposure metrics.

Use `--mode fast-event` for faster research replays. It still feeds every
1-minute bar through the production data, broker, order, and exit path, but it
only calls flat-entry strategy logic when the strategy evaluation timeframe
changes. Pass `--eval-timeframe 3m` to override the auto-detected bar size.
The backtest runner also preloads replay bars into the shared feature registry
so common indicators are vectorized once and sliced to each replay timestamp.

Use `--mode parallel` for long research passes on strategies that declare
`_PARALLEL_BACKTEST_SAFE = True`. Parallel mode gives each worker its own
lookback warmup, generates entry candidates in worker processes capped by
`min(os.cpu_count(), 4)` or `--max-parallel-workers`, then replays accepted
entries, fills, stops, and `on_exit()` chronologically in one process. Validate
each strategy against `fast-event` before treating parallel results as final.

Backtests default to fixed-share sizing from the normal runtime settings.
Research runs may opt into account-based sizing through YAML:

```yaml
backtest:
  sizing:
    mode: full_equity
    equity_fraction: 1.0
    max_order_quantity: null
```

`full_equity` uses the simulated mark-to-market account equity and latest replay
price at each signal. Leave `max_order_quantity` unset only when the run is
intended to model uncapped account-based sizing.

Configured `strategy_modes` are respected. Use `--all-live` when you want a
simulation fill path for strategies that are marked `dry_run` in live/paper
config.

## Heartbeat Monitor

`tools/heartbeat_monitor.py` is the separate Hermes watchdog process. It is a read-only API client, not part of the trading runtime.

```bash
python tools/heartbeat_monitor.py
```

Process design:

```text
Agent -----------> ibkr_lt API -> Engine snapshot
Heartbeat Monitor -> ibkr_lt API -> Engine snapshot
Heartbeat Monitor -> Agent/operator alert path
```

The monitor polls `/api/v1/health` every 5 seconds, keeps `/ws/events` connected, pings the WebSocket if no events arrive, and writes local files for an agent to watch:

- `runs/heartbeat_monitor/status.json`
- `runs/heartbeat_monitor/alerts.jsonl`

When the control API starts, `main.py` warns if no `heartbeat_monitor.py` process is detected. This is part of API startup and is only skipped when the API is disabled with `--no-api`.

Useful options:

```bash
python tools/heartbeat_monitor.py --json
python tools/heartbeat_monitor.py --once --no-files
python tools/heartbeat_monitor.py --api-url http://127.0.0.1:8550 --expect-connected
```

## Tests

```bash
python -m pytest tests/
```

In this workspace, the test suite is normally run with:

```bash
~/.venv/bin/python -m pytest tests/
```

IBKR paper-account tests are opt-in because they connect to TWS/IB Gateway paper and can place paper orders:

```bash
IBKR_LT_RUN_PAPER_TESTS=1 \
IBKR_LT_PAPER_ACCOUNT=DUM408165 \
IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1 \
~/.venv/bin/python -m pytest tests/paper/ -m paper
```

Without `IBKR_LT_ALLOW_PAPER_MARKET_ORDERS=1`, market-entry tests are skipped. Market-order tests also require the guarded US equity RTH window.

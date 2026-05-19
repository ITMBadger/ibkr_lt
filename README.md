# ibkr_lt

`ibkr_lt` is a modular Python trading framework built around a ports-and-adapters design.

The core idea is simple: the engine is stable, while broker and market-data providers plug in like cartridges through small interface contracts.

![ibkr_lt project flow](assets/ibkr-lt-project-flow.webp)

## Architecture

- Strategies produce intent only: `Signal` or exit reason.
- The engine owns scheduling, market context construction, risk routing, and execution flow.
- `OrderManager` is the only framework component that submits orders to broker adapters.
- `DataFeed` composes historical and live data providers.
- `DataManager` owns bar storage, deduplication, revisioning, and resampling.
- `FeatureRegistry` computes shared indicators once per instrument/timeframe/revision.

## Cartridge Boundaries

Broker cartridges implement `core/interfaces/broker.py`.

Market-data cartridges implement `core/interfaces/data.py`.

Strategy modules implement `core/interfaces/strategy.py`.

This keeps broker SDKs, data-provider SDKs, and private strategy logic outside the core engine.

## Public Repo Scope

This repository contains the framework, adapters, tests, and public runtime skeleton.

Proprietary strategy implementations, detailed strategy docs, research notebooks, market data, and decision logs are intentionally excluded from Git.

To run the project from a fresh public clone, add your own strategy module under `strategies/` or update `config.yaml` to point at an available local strategy.

## Hermes Control API

The project includes a read-only FastAPI control surface for the Hermes agent and operator runtime visibility:

```bash
python main.py --paper --api
```

Default URL:

```text
http://127.0.0.1:8550
```

Public endpoints:

- `GET /api/v1/health`
- `GET /api/v1/meta`
- `GET /api/v1/meta/capabilities`

Protected endpoints require `Authorization: Bearer <token>` when `IBKR_LT_API_TOKEN` is set:

- `GET /api/v1/runtime/snapshot`
- `GET /api/v1/runtime/strategies`
- `GET /api/v1/positions`
- `GET /api/v1/events`
- `WS /ws/events`

Hermes should call `GET /api/v1/health` first, then use `next_endpoint` to decide whether to poll health again or read `/api/v1/runtime/snapshot`.

The API is intentionally read-only. Manual trading, order cancellation, and startup approval commands should be added later through a command bus with explicit guardrails.

## Tests

```bash
python -m pytest tests/
```

In this workspace, the test suite is normally run with:

```bash
~/.venv/bin/python -m pytest tests/
```

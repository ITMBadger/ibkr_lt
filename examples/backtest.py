"""Replay a CSV file through strategies using PaperBroker + SimulatedClock.

Usage:
    python examples/backtest.py --csv /path/to/SPY_1min.csv
    python examples/backtest.py --csv /path/to/SPY_1min.csv --strategy my_strategy
    python examples/backtest.py --csv /path/to/SPY_1min.csv --lookback-days 250
"""

from __future__ import annotations

import argparse
import logging
import sys

from core import Engine, SimulatedClock, load_strategies
from core.adapters.csv.data import CSVDataProvider
from core.adapters.paper.broker import PaperBroker
from core.adapters.paper.data import ReplayDataProvider
from core.risk.policy import RiskPolicy
from core.engine.loader import get_registry
from core.types import Instrument

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

SPY = Instrument(asset_class="equity", symbol="SPY")


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper backtest via CSV replay")
    parser.add_argument("--csv", required=True, help="Path to 1-min OHLCV CSV")
    parser.add_argument("--strategy", default=None, help="Strategy id to run (all if omitted)")
    parser.add_argument("--lookback-days", type=int, default=500)
    parser.add_argument("--timezone", default="America/New_York")
    args = parser.parse_args()

    load_strategies()
    registry = get_registry()

    strategy_ids = [args.strategy] if args.strategy else list(registry.keys())
    strategy_instances = []
    for sid in strategy_ids:
        if sid not in registry:
            print(f"Unknown strategy: {sid}. Available: {list(registry.keys())}")
            sys.exit(1)
        cls = registry[sid]
        strategy_instances.append((cls(), {}))

    print(f"Loading CSV: {args.csv}")
    provider = CSVDataProvider(args.csv, session_tz=args.timezone)

    import asyncio
    from datetime import datetime, timezone, timedelta

    async def load_bars():
        now = datetime.now(tz=timezone.utc)
        start = now - timedelta(days=args.lookback_days)
        return await provider.fetch(SPY, None, start, now)

    bars = asyncio.run(load_bars())
    print(f"Loaded {len(bars)} bars from CSV")

    if not bars:
        print("No bars loaded. Check CSV path and format.")
        sys.exit(1)

    replay = ReplayDataProvider(bars)
    broker = PaperBroker()
    risk = RiskPolicy(position_size_shares=1, max_order_quantity=2)

    engine = Engine(
        broker=broker,
        streaming=replay,
        historical=None,
        clock=SimulatedClock(),
        strategies=strategy_instances,
        risk=risk,
        thread_pool_workers=1,
        lookback_days=args.lookback_days,
        session_tz=args.timezone,
    )

    print(f"Running backtest with strategies: {strategy_ids}")
    engine.run_backtest()

    print("\n=== Backtest complete ===")
    print(f"Processed {len(bars)} bars")


if __name__ == "__main__":
    main()

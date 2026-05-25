# Backtest Notes

## Private Strategy Warmup

Use a private local config when running an ignored proprietary strategy that
needs a longer warmup than the shared sample config:

```bash
~/.venv/bin/python -u -m backtest.run \
  --config config.<private>.local.yaml \
  --strategy <private_strategy_id> \
  --mode parallel \
  --start 2025-01-01 \
  --end 2026-05-25 \
  --output-dir runs/backtests/<private_strategy_id>_parallel_check
```

Do not rely on the default `config.yaml` for strategies with larger warmup
requirements. As of the May 2026 checks, the shared config uses
`lookback_days: 10`; a private strategy may require many more 1-minute RTH bars.
Too short a lookback can produce zero candidates and an empty run.

`backtest.run` now fails fast when the selected strategy warmup requires more
calendar lookback than the configured `lookback_days`.

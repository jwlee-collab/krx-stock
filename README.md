# SQLite Daily Market Pipeline

This repository contains a single coherent Python codebase that implements an end-to-end SQLite pipeline for:

1. `daily_prices` ingestion into SQLite
2. `daily_features` generation from OHLCV data
3. `daily_scores` generation and ranking
4. historical scoring support for all dates
5. SQLite-based backtesting engine
6. SQLite-based paper trading engine
7. end-to-end SQLite pipeline validator
8. scripts to run and validate the full pipeline

## Project structure

- `pipeline/db.py` — SQLite connection + schema creation.
- `pipeline/ingest.py` — CSV ingest to `daily_prices`.
- `pipeline/features.py` — feature engineering from OHLCV into `daily_features`.
- `pipeline/scoring.py` — unified scoring formula + ranking into `daily_scores`.
- `pipeline/backtest.py` — SQLite-native long-only, equal-weight backtest.
- `pipeline/paper_trading.py` — SQLite-native paper trading rebalance cycle.
- `pipeline/validator.py` — end-to-end pipeline validation runner.
- `scripts/generate_sample_prices.py` — synthetic OHLCV sample data generator.
- `scripts/run_pipeline.py` — full pipeline execution script.
- `scripts/validate_pipeline.py` — full pipeline validation script.

## Requirements

- Python 3.10+
- No external dependencies required (standard library only)

## Quickstart

### 1) Generate sample daily prices

```bash
python scripts/generate_sample_prices.py
```

Creates `data/sample_daily_prices.csv`.

### 2) Run full pipeline

```bash
python scripts/run_pipeline.py --db data/market_pipeline.db --prices-csv data/sample_daily_prices.csv --top-n 3
```

This will:
- initialize SQLite schema
- ingest prices
- generate features
- generate historical scores/ranks
- run backtest
- run paper trading cycle

### 3) Validate end-to-end pipeline

```bash
python scripts/validate_pipeline.py --db data/market_pipeline.db --top-n 3
```

Validation checks:
- rows exist for prices/features/scores
- backtest run generated result rows
- paper trading cycle can execute successfully

## Data model (core tables)

- `daily_prices(symbol, date, open, high, low, close, volume)`
- `daily_features(symbol, date, ret_1d, ret_5d, momentum_20d, range_pct, volume_z20)`
- `daily_scores(symbol, date, score, rank)`
- `backtest_runs(run_id, created_at, top_n, start_date, end_date)`
- `backtest_results(run_id, date, equity, daily_return, position_count)`
- `paper_positions(symbol, qty, entry_price, updated_at)`
- `paper_orders(order_id, created_at, symbol, side, qty, price, reason)`

## Unified scoring behavior

A **single scoring formula** is used for both latest-only and historical scoring:

```text
score = 0.20*ret_1d + 0.35*ret_5d + 0.35*momentum_20d + 0.10*volume_z20 - 0.05*range_pct
```

- Latest scoring: `generate_daily_scores(..., include_history=False)`
- Historical scoring: `generate_daily_scores(..., include_history=True)`

Both paths call the same `score_formula()` implementation.

## Notes

- Dates are stored as ISO-8601 strings (`YYYY-MM-DD`) in SQLite.
- Ingest uses upsert semantics (`ON CONFLICT`) so reruns are idempotent.
- Feature and score generation also upsert, enabling incremental reruns.

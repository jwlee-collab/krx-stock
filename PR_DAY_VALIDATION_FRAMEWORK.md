# PR: DAY Validation And KIS EOD Replay Operations Framework

## Summary

This change adds a safety-first DAY trading validation and operations framework for the existing KRX stock project. DAY remains separated under `pipeline/day_trading/` and is designed for SIGNAL_ONLY/PAPER validation, not LIVE trading.

The framework covers:

- DAY configuration, signal generation, risk checks, exits, position separation, logging, validation, and reporting.
- Intraday data loading, quality checks, replay backtesting, promotion gating, and markdown reports.
- Point-in-time SWING candidate usage with `score_date < trade_date` by default.
- Dataset planning, data availability auditing, daily replay preparation, rolling replay, and EOD operations.
- KIS quote-only market data collection for intraday candles.
- ETF market proxy fallback using `069500` (KODEX 200) when KIS index/sector minute endpoints are unavailable.
- KIS zero-volume bar policy comparison:
  - `strict_invalid` (default)
  - `no_trade_context` (diagnostic)
  - `drop_no_trade` (comparison)
- KIS daily ops and end-of-day ops:
  - intraday collection
  - DB upsert
  - availability audit
  - replay
  - zero-volume policy comparison
  - rolling replay
  - status reports
  - D replay before D daily refresh
  - D daily score generation only for D+1 readiness

## Safety Guarantees

- LIVE trading is not implemented.
- Broker execution is not implemented.
- Order, account, balance, and fill endpoints are not used.
- KIS collection is quote-only.
- App keys, app secrets, access tokens, account identifiers, `.env` contents, local DB files, real CSV data, and generated reports are excluded from commit scope.
- DAY and SWING positions, orders, exits, and logs are separated by `strategy_id`.
- DAY logic may only close or mutate `strategy_id="DAY"` positions.
- Missing, stale, partial, or ambiguous data fails closed.
- Same-day post-close `daily_scores` remain forbidden for intraday DAY replay by default.
- FINAL/EOD investor-flow data is not used for intraday DAY decisions.

## Replay And Data Quality

- Replay is point-in-time: only candles and intraday context rows available at or before the replay clock are exposed.
- Incomplete 15-minute confirm bars are not used.
- Partial sessions are excluded from rolling replay by default.
- Promotion gates block readiness on insufficient samples, negative net expectancy, weak profit factor, excessive drawdown, lookahead risk, data quality failure, open positions at session end, and incomplete sessions.
- Performance reports include costs, transaction tax, and slippage assumptions.
- Fixture and smoke results validate pipeline behavior only; they do not prove strategy profitability.

## Operations

Recommended EOD flow:

1. Run `scripts/run_kis_end_of_day_ops.py` after 15:45 KST.
2. Replay trade date D using only `score_date < D`.
3. Refresh D daily prices and regenerate D daily scores only after D replay completes.
4. Use generated score date D only for D+1 readiness.
5. Keep rolling replay limited to complete sessions unless explicitly debugging partial data.

## Not Included

- No LIVE order path.
- No broker executor.
- No account/balance/fill/order API usage.
- No strategy parameter tuning for better backtest returns.
- No SWING scoring formula changes.
- No paid or secret-backed external data dependency.
- No claim of real strategy profitability.

## Commit Exclusions

Do not commit:

- `.env`
- `.venv/`
- `__pycache__/`
- `*.pyc`
- `data/market_pipeline.db`
- `data/*.csv`
- `data/*.json`
- `data/.kis_token_cache*.json`
- `reports/*.md`
- `reports/*.json`
- `scripts/research/*`
- `scripts/local/*`
- local DB files and generated artifacts

## Merge Notes

This branch is merge-ready only if:

- Unit tests pass.
- Compileall passes.
- CLI help checks pass.
- Staged files contain code, docs, tests, and fixtures only.
- No real market data, DB, report, token cache, or secret files are staged.

Post-merge, the next required work is operational data accumulation: run EOD collection after market close for multiple complete sessions, then evaluate rolling 3-5 day smoke windows, 20-day initial analysis, and longer 60-day-plus validation before any LIVE review.

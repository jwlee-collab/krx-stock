# KIS Daily DAY Replay Ops

This directory contains a launchd template for quote-only KIS end-of-day collection, DAY replay validation, and next-day SWING score refresh.

Safety rules:

- The job uses quotation data only.
- Order, account, balance, and fill endpoints are forbidden.
- `.env`, token cache, DB files, collected CSVs, and reports must not be committed.
- `strict_invalid` remains the default zero-volume policy.
- `no_trade_context` and `drop_no_trade` are diagnostics only.
- Rolling replay excludes partial sessions by default.
- Partial sessions are for smoke/debug only and must not be used for profitability assessment.
- Daily and rolling reports are data-quality and replay-readiness checks, not profitability claims.
- PAPER account accounting is internal only: no broker/account endpoint is called. Use `daily_return_pct` for account-equity return and keep it separate from trade-return aggregates.
- The job should run after the close, e.g. after 15:45 KST. The template uses 16:10 KST.
- End-of-day ops runs DAY replay first with `score_date < D`, then refreshes D daily prices/scores for D+1 readiness.
- D post-close scores must never be used for D intraday replay.

Example manual run:

```bash
/Users/jwlee/Projects/krx-stock/.venv/bin/python \
  /Users/jwlee/Projects/krx-stock/scripts/run_kis_end_of_day_ops.py \
  --db /Users/jwlee/Projects/krx-stock/data/market_pipeline.db \
  --auto-trade-date \
  --market-symbol 069500 \
  --top-n 20 \
  --max-symbols 20 \
  --require-full-top-n-coverage \
  --zero-volume-bar-policy strict_invalid \
  --paper-initial-cash-krw 10000000 \
  --paper-notional-per-trade-krw 1000000 \
  --paper-max-total-exposure-krw 3000000 \
  --paper-max-position-value-krw 1000000 \
  --compare-zero-volume-policies \
  --exclude-partial-sessions \
  --max-score-staleness-days 3 \
  --refresh-daily-after-replay \
  --daily-prices-output /Users/jwlee/Projects/krx-stock/data/public_daily_prices_eod.csv \
  --universe-csv /Users/jwlee/Projects/krx-stock/data/krx_source_universe_500.csv \
  --rolling-windows 3,5,20,60 \
  --env-file /Users/jwlee/Projects/krx-stock/.env \
  --token-cache /Users/jwlee/Projects/krx-stock/data/.kis_token_cache.json
```

Interpretation:

- Fewer than 3 complete sessions is only a pipeline smoke state.
- 20 complete sessions is the first analysis candidate threshold.
- 60 complete sessions is more meaningful for validation, still not LIVE approval.
- If `STALE_SCORE_DATE` appears, update daily prices and regenerate SWING daily_scores before rerunning daily ops.
- If daily refresh fails, the next run may be blocked by `STALE_SCORE_DATE`; inspect `day_eod_ops_status.md`.
- If partial sessions are excluded, check `reports/daily_ops/day_ops_status.md` for `excluded_partial_dates` and the next complete-day counts.

launchd setup:

1. Copy `ops/launchd/com.krxstock.kis-daily-ops.plist.template` to `~/Library/LaunchAgents/com.krxstock.kis-daily-ops.plist`.
2. Review paths, Python executable, DB path, and `.env` path.
3. Load or bootstrap the LaunchAgent yourself after review.

Codex must not install or activate this LaunchAgent automatically. macOS scheduler registration is an operational decision.

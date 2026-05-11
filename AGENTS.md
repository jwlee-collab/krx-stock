# krx-stock Agent Safety Rules

This project is a KRX stock research and automated trading workspace. Treat trading changes as safety-critical.

## Trading Safety Defaults

- Real-money trading must remain disabled by default.
- LIVE orders are forbidden unless the user explicitly enables LIVE mode, confirms the reviewed broker/order path, and accepts the operational risk.
- `day_trading.enabled` must default to `false`.
- `day_trading.mode` must default to `SIGNAL_ONLY`.
- DAY and SWING positions, orders, exits, and logs must be separated by `strategy_id`.
- DAY logic must only close or mutate positions with `strategy_id="DAY"`.
- Missing, stale, or ambiguous market/intraday data must fail closed.
- DAY trade date and SWING score date must stay point-in-time safe; same-day `daily_scores` are forbidden unless explicitly reviewed as pre-market scores.

## Strategy And Risk Changes

- Do not pause for routine implementation choices. Use conservative defaults and keep moving unless a human-review blocker is present.
- After implementation, continue through tests, self-review, fixes, and retesting when feasible.
- Routine implementation judgments should be made autonomously with conservative defaults; do not ask for user confirmation unless a human-review blocker applies.
- Continue through implementation, relevant tests, self-review, fixes, and retests before reporting completion.
- Human-review blockers are limited to real-money trading paths, broker executors, account/API secrets, destructive DB changes, external API integrations, large new dependencies, or major strategy judgments that cannot be handled with feature flags and conservative defaults.
- Run relevant tests after changing strategy, risk, order, position, or logging behavior.
- Keep structured logs for candidate collection, rejection reasons, signals, risk skips, entries, exits, and daily summaries.
- Do not report performance without fees, transaction tax, and slippage assumptions.
- Replay backtests must only expose candles and optional intraday context rows with timestamps at or before the replay clock.
- Do not optimize for fixed monthly income claims or guaranteed returns.
- Avoid large new dependencies unless there is no reasonable existing or standard-library alternative.
- Long historical datasets are useful, but replay execution should start with short 3-5 trading day windows and expand gradually.
- Same-day post-close scores, FINAL/EOD/CONFIRMED_CLOSE investor-flow data, and other close-confirmed data must not be used for intraday DAY decisions.
- KIS or broker-adjacent data collectors must remain quote-only unless separately reviewed; order, account, balance, and fill endpoints are forbidden in this workspace.
- App keys, app secrets, access tokens, account identifiers, and `.env` contents must never be printed, committed, or written to reports.

## DAY Strategy Guardrails

- DAY is intraday momentum trading, not HFT, tick scalping, order-book queue trading, or one-tick capture.
- DAY should use SWING candidates by default and must not scan the entire market by default.
- DAY positions must be closed intraday when `no_overnight=true`.
- SIGNAL_ONLY and PAPER validation should precede any LIVE review.
- Fixture and smoke-test results are pipeline checks only; do not interpret them as real strategy profitability.

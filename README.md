# SQLite KRX Daily Market Pipeline

이 저장소는 **국내장(KRX: KOSPI/KOSDAQ)** 기준으로 동작하는 SQLite 기반 주식 파이프라인입니다.
기존 파이프라인 흐름(적재 → 피처 → 스코어/랭킹 → 히스토리컬 스코어 → 백테스트 → 모의매매 → 검증)은 유지하고,
데이터 소스와 종목 코드 체계를 KRX 기준으로 맞췄습니다.

## 파이프라인 구성

1. `daily_prices` 적재
2. `daily_features` 생성
3. `daily_scores` 생성 및 랭킹
   - (신규) 전체 시장 사용 시 유니버스 필터를 먼저 적용
4. historical scoring (`include_history=True`)
5. SQLite 기반 백테스트
6. SQLite 기반 모의매매(paper trading)
7. end-to-end 검증

## 프로젝트 구조

- `pipeline/db.py` — SQLite 연결 + 스키마 생성
- `pipeline/ingest.py` — CSV 적재 + `pykrx` 기반 KRX 적재
- `pipeline/features.py` — OHLCV 기반 피처 생성
- `pipeline/scoring.py` — 스코어 계산 + 랭킹 저장
- `pipeline/universe_filter.py` — 스코어링 전 유니버스(후보군) 필터링
- `pipeline/backtest.py` — 리밸런싱 주기/보유기간/유지규칙을 지원하는 백테스트
- `pipeline/paper_trading.py` — 동일 규칙을 반영한 모의매매 리밸런싱 사이클
- `pipeline/day_trading/` — SWING 후보군 기반 DAY 인트라데이 전략 모듈
- `pipeline/validator.py` — 전체 파이프라인 검증
- `scripts/generate_sample_prices.py` — KRX 6자리 코드 기반 샘플 데이터 생성
- `scripts/run_pipeline.py` — 전체 파이프라인 실행
- `scripts/run_day_trading.py` — DAY 전략 SIGNAL_ONLY/PAPER 실행
- `scripts/validate_pipeline.py` — 전체 파이프라인 검증 실행

## DAY 인트라데이 전략

DAY 전략은 초단타/HFT/호가 큐 전략이 아니라 5분봉/15분봉 기반 장중 모멘텀 전략입니다.
기본 후보군은 기존 `daily_scores`의 SWING 후보군이며, 전체 시장 무제한 스캔은 기본으로 수행하지 않습니다.

안전 기본값:

- `day_trading.enabled = false`
- `day_trading.mode = "SIGNAL_ONLY"`
- `day_trading.strategy_id = "DAY"`
- `day_trading.universe_source = "SWING_CANDIDATES"`
- `day_trading.allow_independent_universe = false`
- `day_trading.no_overnight = true`
- `day_trading.timeframe_primary = "5m"`
- `day_trading.timeframe_confirm = "15m"`
- `day_trading.max_trades_per_day = 3`
- `day_trading.max_trades_per_symbol_per_day = 1`
- `day_trading.max_open_positions = 2`
- `day_trading.force_exit_time = "15:10"`
- `day_trading.trailing_stop_enabled = false`
- `day_trading.require_market_trend_data = true`
- `day_trading.fail_closed_on_missing_data = true`

DAY 전용 테이블:

- `intraday_prices`: 5분봉/15분봉 등 분봉 OHLCV 저장
- `day_trade_logs`: 후보군, 거절 사유, 신호, 리스크 스킵, PAPER 진입/청산, 일일 요약
- `day_paper_positions`: `strategy_id` 기준 DAY 페이퍼 포지션
- `day_paper_orders`: `strategy_id` 기준 DAY 페이퍼 주문 로그
- `intraday_investor_flows`: 선택 장중 외국인/기관 잠정 수급
- `intraday_trade_strength`: 선택 장중 체결강도/매수세
- `intraday_program_flows`: 선택 장중 프로그램 순매수/순매도

`intraday_prices` 데이터 요구사항:

- 필수: `symbol,timestamp,open,high,low,close,volume`
- 선택: `timeframe`, `traded_value` 또는 `amount`, `source`
- `timestamp`는 ISO 형식(`YYYY-MM-DDTHH:MM:SS` 또는 `YYYY-MM-DD HH:MM:SS`)을 사용합니다.
- `timestamp`는 해당 분봉의 시작 시각으로 취급합니다. 5분봉 `09:05`는 `09:05`까지 알고 있는 완료 봉으로 리플레이됩니다.
- 공급 데이터가 bar close time 기준이면 적재 전에 bar start time으로 정규화해야 합니다. 현재 CSV 로더는 close-time 자동 보정 옵션을 제공하지 않으므로, close-time 데이터를 그대로 넣지 마세요.
- 15분 확인봉은 리플레이 시각 기준으로 완료된 봉만 사용합니다. 아직 완성되지 않은 15분봉은 신호 판단에 노출하지 않습니다.
- 시장 프록시는 후보 종목과 같은 `intraday_prices` 테이블에 별도 `symbol`로 적재합니다.
- 장마감 후 확정 데이터는 장중 판단에 사용하지 않습니다.
- 기본 검증은 5분봉/15분봉, OHLC 유효성, 음수 거래량/거래대금, 장중 시간 범위, 시장 프록시 존재 여부를 확인합니다.

DB bootstrap:

```bash
python3 scripts/bootstrap_market_db.py \
  --db data/market_pipeline.db
```

`--dry-run`을 붙이면 DB 파일을 만들거나 스키마를 변경하지 않고 필요한 DAY 테이블 준비 상태만 확인합니다.

`daily_prices` CSV 요구사항:

- 필수: `date,symbol,open,high,low,close,volume`
- 선택: `traded_value` 또는 `amount`, `name`, `market`, `source`, `created_at`
- 현재 DB 스키마는 `daily_prices(symbol,date,open,high,low,close,volume)`만 저장하므로 선택 컬럼은 로더 리포트에 `ignored_columns`로 표시되고 저장하지 않습니다.
- `(symbol,date)`가 primary key입니다. 중복 row는 마지막 row가 적용되며 `duplicate_key_rows`와 경고에 표시됩니다.
- OHLC는 양수, `high >= max(open, close)`, `low <= min(open, close)`, `volume >= 0`이어야 합니다.

`daily_prices.csv` 예시:

```csv
date,symbol,open,high,low,close,volume,traded_value
2026-05-07,005930,80000,81000,79500,80500,1200000,96600000000
```

`daily_prices` CSV 적재:

```bash
python3 scripts/load_daily_prices.py \
  --db data/market_pipeline.db \
  --csv data/daily_prices.csv
```

DB가 없으면 로더는 기본적으로 중단합니다. 먼저 bootstrap을 실행하거나, 정말 의도한 경우에만 `--bootstrap-if-missing`을 명시하세요.
기존 SWING 전체 파이프라인도 `scripts/run_pipeline.py --source csv --prices-csv ...`로 `daily_prices`를 적재할 수 있지만, DAY replay 데이터 준비만 할 때는 위 전용 로더가 더 작은 경로입니다.

Public/no-secret 일봉 수집:

```bash
python3 scripts/fetch_public_daily_prices.py \
  --universe-csv data/krx_source_universe_500.csv \
  --max-symbols 20 \
  --count 90 \
  --output data/public_daily_prices_smoke.csv
```

이 스크립트는 공개 일봉 차트 데이터만 CSV로 저장합니다. broker, 계좌, 주문, credential, secret 경로는 없습니다.
수집한 CSV는 로컬 검증 입력이며 PR/commit에 포함하지 마세요.
public 데이터 제공처가 차단, captcha, 로그인, 유료 계약, API key를 요구하면 즉시 중단하고 human review가 필요합니다.

`daily_scores` CSV 요구사항:

- 필수: `date` 또는 `score_date`, `symbol`, `rank` 또는 `score`
- 선택: `score`, `strategy`, `reason`, `created_at` 등
- 현재 DB 스키마는 `daily_scores(symbol,date,score,rank)`만 저장하므로 추가 컬럼은 로더 리포트에 `ignored_columns`로 표시되고 저장하지 않습니다.
- `daily_scores`는 기존 스키마의 외래키 때문에 같은 `(symbol,date)`의 `daily_prices`가 먼저 있어야 합니다.
- `rank`만 있으면 `score=0.0`으로 저장하고, `score`만 있으면 같은 날짜 안에서 score 내림차순으로 rank를 계산합니다.
- DAY 거래일 `D`에는 기본적으로 `D`보다 이전의 가장 최근 `daily_scores.date`만 후보군으로 사용됩니다.

`daily_scores.csv` 예시:

```csv
date,symbol,rank,score
2026-05-07,005930,1,0.91
```

`daily_scores` CSV 적재:

```bash
python3 scripts/load_daily_scores.py \
  --db data/market_pipeline.db \
  --csv data/daily_scores.csv \
  --trade-start-date 2026-05-08 \
  --trade-end-date 2026-05-31
```

DB가 없으면 로더는 기본적으로 중단합니다. 먼저 bootstrap을 실행하거나, 정말 의도한 경우에만 `--bootstrap-if-missing`을 명시하세요.
same-day score만 있어 기본 룰상 쓸 수 없는 거래일은 `previous_score_date_summary`와 경고에 표시됩니다.

분봉 CSV 적재:

`intraday_prices.csv` 예시:

```csv
symbol,timestamp,open,high,low,close,volume,timeframe,traded_value
005930,2026-05-08T09:05:00,100,101,99,100.5,10000,5m,1005000000
```

```bash
python3 scripts/load_intraday_prices.py \
  --db data/market_pipeline.db \
  --csv data/intraday_5m.csv \
  --default-timeframe 5m \
  --source CSV \
  --validate \
  --market-symbol MARKET_PROXY
```

DB가 없으면 분봉 로더도 기본적으로 중단합니다. 이 동작은 실수로 빈 DB를 만들고 적재 성공처럼 보이는 상황을 막기 위한 것입니다.
시장 프록시 데이터도 같은 명령으로 `--csv data/market_proxy_5m.csv --source MARKET_PROXY_CSV`처럼 적재합니다.
5분봉과 15분봉이 별도 CSV라면 각각 `--default-timeframe 5m`, `--default-timeframe 15m`로 적재하거나 CSV의 `timeframe` 컬럼을 사용하세요.
현재 저장소의 public/no-secret 일봉 수집기는 intraday 5분 OHLCV를 제공하지 않습니다.
일부 공개 minute endpoint는 현재가/누적거래량만 제공하고 OHLC가 비어 있어 DAY replay 검증용 `intraday_prices` 요구사항을 충족하지 못할 수 있습니다.
증권사/HTS/키움/KIS 등에서 분봉 접근이 가능하더라도 로그인, token, 계좌, 주문 API와 분리된 quote-only 검토가 필요하면 Codex는 secret 없이 멈추고 사람 검토를 요청해야 합니다.

KIS quote-only 분봉 수집:

KIS Open API를 이미 신청했고 App Key/App Secret이 준비되어 있다면, DAY replay용 분봉을 시세 조회 전용 collector로 수집할 수 있습니다.
이 저장소의 KIS collector는 token 발급과 quotation endpoint만 사용하며 주문, 계좌, 잔고, 체결 endpoint는 hard-block합니다.
App Key/App Secret, access token은 콘솔, 로그, 리포트에 출력하지 않습니다.

`.env` 예시:

```dotenv
KIS_ENV=paper
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
KIS_BASE_URL=https://openapivts.koreainvestment.com:29443
```

`.env`와 token cache는 git commit 금지 대상입니다. 현재 `.gitignore`는 `.env`와 `.env.*`를 제외합니다.

KIS token troubleshooting:

토큰 발급 403은 전략/분봉 파서 문제가 아니라 인증 설정 문제로 먼저 분리합니다.
token probe가 통과하기 전에는 raw field audit, intraday collection, replay를 추가 호출하지 마세요.

환경 진단:

```bash
python3 scripts/check_kis_env.py --env-file .env
```

safe token probe:

```bash
python3 scripts/probe_kis_token.py \
  --env-file .env \
  --token-cache data/.kis_token_cache.json
```

기본 규칙:

- `KIS_ENV=real`이면 `KIS_BASE_URL=https://openapi.koreainvestment.com:9443`
- `KIS_ENV=paper`이면 `KIS_BASE_URL=https://openapivts.koreainvestment.com:29443`
- 실전/모의 App Key와 URL이 섞이면 token 발급이 실패할 수 있습니다.
- token cache가 유효하면 `tokenP`를 다시 호출하지 않습니다.
- 강제 재발급은 필요한 경우에만 `--force-refresh-token`을 사용합니다.
- token 발급 403은 API 신청/승인 상태, key 종류, 호출 제한, KIS 포털 상태 확인이 필요할 수 있습니다.
- App Key/App Secret/access token 값은 로그, 리포트, commit에 절대 포함하지 않습니다.
- token cache는 `data/.kis_token_cache.json` 같은 ignored 경로에만 둡니다.

단일 종목/시장 프록시 분봉 endpoint probe:

```bash
python3 scripts/probe_kis_intraday.py \
  --env-file .env \
  --env paper \
  --symbol 005930 \
  --market-symbol MARKET_PROXY \
  --market-index-code 0001
```

`scripts/probe_kis_intraday.py`는 다음을 확인합니다.

- access token 발급 가능 여부(token 값은 출력하지 않음)
- 국내주식 주식당일분봉조회 응답 row 수
- 5분봉 집계 가능 여부
- 국내업종/지수 분봉 프록시 조회 가능 여부
- 과거 날짜 조회 가능 여부는 probe 결과의 반환 날짜와 required date를 비교해 판단

KIS raw field audit:

KIS 주식당일분봉 응답의 volume/amount 필드는 endpoint 응답 구조에 맞게 먼저 검증해야 합니다.
봉별 체결량과 누적 거래량/누적 거래대금을 혼동하면 `zero_volume_count` 또는 `zero_volume_positive_amount_count`처럼 데이터 품질 오류가 생길 수 있습니다.
전략 조건을 완화하기 전에 raw field audit과 invalid bar analysis로 원천 필드 매핑을 확인하세요.

```bash
python3 scripts/audit_kis_intraday_fields.py \
  --env-file .env \
  --symbol 005930 \
  --market-symbol 069500 \
  --max-rows 10 \
  --output-md reports/kis_intraday_field_audit.md \
  --output-json reports/kis_intraday_field_audit.json
```

현재 parser 원칙:

- 가격은 `stck_oprc`, `stck_hgpr`, `stck_lwpr`, `stck_prpr`를 우선 사용합니다.
- 거래량은 봉별 후보 필드가 양수이면 우선 사용하고, 봉별 값이 없거나 0인데 `acml_vol` 누적값 차분이 가능하면 누적 차분을 사용합니다.
- 거래대금은 봉별 후보 필드가 있으면 우선 사용하고, 없으면 `acml_tr_pbmn` 누적값 차분을 사용합니다.
- 거래대금이 없고 거래량이 양수인 경우 `close * volume`으로 보수 추정하며, 리포트의 `estimated_traded_value_count`에 표시합니다.
- `zero_volume_positive_amount_count`, `positive_volume_zero_amount_count`, `negative_cumulative_diff_count`가 보이면 먼저 데이터 매핑 문제로 취급합니다.
- 빈 시간대를 억지로 zero-volume bar로 생성하지 않습니다.

required intraday manifest 기반 수집:

```bash
python3 scripts/fetch_kis_intraday_prices.py \
  --env-file .env \
  --required-intraday-csv reports/day_required_intraday_2026-05-08_2026-05-14.csv \
  --output-csv data/intraday_kis_5m.csv \
  --db data/market_pipeline.db \
  --market-symbol MARKET_PROXY \
  --market-index-code 0001 \
  --timeframe 5m \
  --sleep-seconds 0.12
```

출력 CSV는 `symbol,timestamp,open,high,low,close,volume,timeframe,traded_value,source` 형식입니다.
기본적으로 원천 minute row에서 primary `5m`와 confirm `15m` 봉을 함께 생성합니다. confirm 봉을 쓰지 않으려면 `--no-confirm-timeframe`을 명시하세요.
KIS 당일분봉 endpoint가 당일 또는 최근 영업일만 제공하면 긴 과거 replay 데이터는 한 번에 받을 수 없습니다.
이 경우 매일 장중/장후에 quote-only collector를 실행해 `intraday_prices`를 누적하고, `audit_day_data_availability.py`로 coverage를 확인한 뒤 짧은 구간부터 replay하세요.
KIS가 추가 API 신청, 권한, 유료 계약, captcha, 로그인 세션 또는 계좌/주문 API를 요구하면 자동 진행하지 말고 human review blocker로 다룹니다.

KIS 시장 프록시 ETF fallback:

KIS 업종/지수 분봉 endpoint가 사용할 수 없거나 서비스 코드 오류를 반환하면, quote-only 범위 안에서 거래되는 ETF를 시장 프록시로 사용할 수 있습니다.
KOSPI proxy 예시는 `069500` KODEX 200, KOSDAQ proxy 예시는 `229200` KODEX 코스닥150입니다.
ETF는 지수 자체가 아니라 거래 상품이므로 추적오차, 괴리, ETF 자체 유동성 영향이 있을 수 있습니다.
DAY replay에서는 시장 분위기 필터용 proxy로만 사용하고, 실제 성과 검증이나 지수 대체 수단으로 과해석하지 마세요.

```bash
python3 scripts/fetch_kis_intraday_prices.py \
  --env-file .env \
  --required-intraday-csv reports/day_required_intraday_2026-05-08_2026-05-14.csv \
  --output-csv data/intraday_kis_5m_with_etf_proxy.csv \
  --db data/market_pipeline.db \
  --market-proxy-source ETF \
  --market-proxy-symbol 069500 \
  --replace-market-proxy-symbol \
  --timeframe 5m
```

ETF proxy를 실제 종목코드로 저장하는 것이 기본입니다. 따라서 audit/replay에는 같은 ETF 종목코드를 사용합니다.

```bash
python3 scripts/audit_day_data_availability.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-08 \
  --market-symbol 069500

python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-08 \
  --enable-day-trading \
  --market-symbol 069500 \
  --report-md reports/day_replay_kis_etf_proxy_smoke.md
```

KIS daily intraday collection 운영:

KIS 주식당일분봉은 과거 full-session 확보가 제한적일 수 있습니다.
장기 DAY replay를 하려면 매일 quote-only collector를 실행해 `intraday_prices`를 누적해야 합니다.
historical replay는 누적된 DB 범위에서만 신뢰할 수 있고, 일부 시간대만 있는 분봉은 partial session으로 표시해야 합니다.
`069500` ETF proxy는 지수 자체가 아니며 시장 분위기 필터용 fallback입니다. KOSDAQ 후보 비중이 커지면 `229200` KODEX 코스닥150도 별도 검토하세요.
signal이 0개인 날은 곧바로 전략 실패나 파라미터 완화 대상으로 보지 말고, rejection reason, 후보 usable count, market trend coverage, VWAP/breakout/volume/liquidity 실패 사유를 먼저 확인합니다.

하루치 수집, 적재, audit, replay를 묶어 실행:

```bash
python3 scripts/run_kis_daily_intraday_collection.py \
  --db data/market_pipeline.db \
  --trade-date 2026-05-11 \
  --market-symbol 069500 \
  --market-proxy-source ETF \
  --top-n 50 \
  --max-symbols 5 \
  --output-csv data/intraday_kis_2026-05-11.csv \
  --audit-report-md reports/day_data_availability_2026-05-11.md \
  --replay-report-md reports/day_replay_2026-05-11.md
```

운영 기본값:

- trade_date `D`에는 `D`보다 이전의 가장 최근 `daily_scores.date`를 사용합니다.
- same-day score는 기본으로 사용하지 않습니다.
- 후보 종목과 `069500` proxy를 KIS quote-only 주식당일분봉 endpoint로 수집합니다.
- 출력 CSV와 DB 적재는 idempotent upsert입니다.
- 기존 DB row와 새 수집 row의 OHLCV가 다르면 기본적으로 blocked 처리합니다.
- `--force-refresh`를 주면 해당 `date/symbol/timeframe` 범위만 삭제 후 재적재합니다. 전체 DB 삭제는 하지 않습니다.
- `--dry-run`은 DB 변경과 KIS 호출 없이 score_date와 수집 대상만 확인합니다.
- `--max-symbols`가 `--top-n`보다 작으면 partial universe smoke로 표시하고, 기본적으로 수집/보유된 후보만 replay합니다.
- `--require-full-top-n-coverage`를 주면 top-n 전체 후보의 intraday coverage가 없을 때 replay를 blocked 처리합니다.

KIS daily ops 오케스트레이터:

매일 장마감 후에는 개별 collector 대신 `run_kis_daily_ops.py`를 사용할 수 있습니다. 이 스크립트는 quote-only KIS 수집, DB upsert, data availability audit, 당일 replay, zero-volume policy 비교, rolling replay 요약, 상태 파일 생성을 한 번에 묶습니다.

```bash
python3 scripts/run_kis_daily_ops.py \
  --db data/market_pipeline.db \
  --auto-trade-date \
  --market-symbol 069500 \
  --top-n 20 \
  --max-symbols 20 \
  --require-full-top-n-coverage \
  --zero-volume-bar-policy strict_invalid \
  --compare-zero-volume-policies \
  --exclude-partial-sessions \
  --max-score-staleness-days 3 \
  --rolling-windows 3,5,20,60 \
  --output-dir reports/daily_ops \
  --data-output-dir data/intraday \
  --env-file .env \
  --token-cache data/.kis_token_cache.json
```

운영 해석 원칙:

- 기본 market proxy는 `069500` KODEX 200 ETF fallback입니다.
- 기본 zero-volume policy는 `strict_invalid`입니다.
- `--compare-zero-volume-policies`는 `no_trade_context`, `drop_no_trade` 비교 진단용입니다.
- rolling replay는 기본적으로 `session_complete=True`인 날짜만 사용합니다.
- partial session은 `excluded_partial_sessions`로 따로 보고하며, 수익성 판단에 포함하지 않습니다.
- partial data를 디버그 목적으로 포함해야 할 때만 `--include-partial-sessions`를 명시합니다. 이 경우 report는 profitability 판단 불가로 표시되어야 합니다.
- `score_date`는 항상 `trade_date`보다 이전이어야 하며, 기본 `--max-score-staleness-days 3`을 넘으면 `STALE_SCORE_DATE`로 차단합니다.
- 오래된 점수를 진단 목적으로만 허용하려면 `--allow-stale-score`를 명시하지만, 이 결과는 운영 검증으로 보지 않습니다.
- 3~5거래일 rolling 결과는 pipeline smoke입니다.
- 20거래일 이상부터 1차 분석 후보, 60거래일 이상부터 더 의미 있는 검증 후보로 봅니다.
- 충분한 표본 전에는 promotion gate가 LIVE_READY를 허용해서는 안 됩니다.
- 수집 CSV, DB, reports, token cache는 commit하지 않습니다.
- 주문/계좌/잔고/체결 API는 사용하지 않습니다.
- 현재 trade_date 수집은 기본적으로 장마감 이후에만 실행합니다. `run_kis_daily_ops.py`는 KST 기준 `--earliest-run-time` 이전의 current-day non-dry-run 수집을 `BEFORE_POST_CLOSE_COLLECTION_WINDOW`로 차단합니다.

daily_scores 최신성 차단 시 다음 순서로 갱신합니다.

```bash
# 1) 일봉 데이터 갱신 또는 CSV 준비
python3 scripts/run_pipeline.py \
  --db data/market_pipeline.db \
  --source csv \
  --prices-csv data/daily_prices.csv

# 2) 기존 SWING pipeline으로 daily_scores 재생성
python3 scripts/prepare_day_replay_db.py \
  --db data/market_pipeline.db \
  --run-swing-pipeline \
  --prices-csv data/daily_prices.csv \
  --universe-csv data/krx_source_universe_500.csv \
  --pipeline-source csv \
  --start-date 2026-05-08 \
  --end-date 2026-05-11 \
  --market-symbol 069500 \
  --top-n 20 \
  --dry-run

# 3) score_date < trade_date가 확보된 뒤 daily ops 재실행
python3 scripts/run_kis_daily_ops.py \
  --db data/market_pipeline.db \
  --auto-trade-date \
  --market-symbol 069500 \
  --top-n 20 \
  --max-symbols 20 \
  --require-full-top-n-coverage \
  --exclude-partial-sessions
```

일일 상태 파일:

- `reports/daily_ops/day_ops_status.json`
- `reports/daily_ops/day_ops_status.md`

상태 파일에는 마지막 실행 시각, trade_date, score_date, 수집 심볼, DB 적재 결과, replay 결과, zero-volume policy, rolling report 경로, blocked reason이 기록됩니다.
또한 complete/partial replayable day, excluded partial dates, rolling window별 blocked status, 3일 smoke와 20일 분석까지 추가로 필요한 complete session 수를 함께 기록합니다.

KIS end-of-day ops 오케스트레이터:

장마감 후 실제 운영 루틴은 `run_kis_end_of_day_ops.py`를 권장합니다. 이 스크립트는 먼저 trade_date `D`의 DAY intraday 수집/replay를 실행하고, 그 다음에만 `D`의 daily_prices/daily_features/daily_scores를 갱신합니다. 따라서 `D` replay에는 `D` 장마감 후 score를 절대 사용하지 않고, 생성된 `D` score는 `D+1` 준비용으로만 저장됩니다.

```bash
python3 scripts/run_kis_end_of_day_ops.py \
  --db data/market_pipeline.db \
  --auto-trade-date \
  --market-symbol 069500 \
  --top-n 20 \
  --max-symbols 20 \
  --require-full-top-n-coverage \
  --zero-volume-bar-policy strict_invalid \
  --compare-zero-volume-policies \
  --exclude-partial-sessions \
  --max-score-staleness-days 3 \
  --refresh-daily-after-replay \
  --daily-prices-output data/public_daily_prices_eod.csv \
  --universe-csv data/krx_source_universe_500.csv \
  --rolling-windows 3,5,20,60 \
  --output-dir reports/daily_ops \
  --data-output-dir data/intraday \
  --env-file .env \
  --token-cache data/.kis_token_cache.json
```

EOD 순서:

1. env/token masked 진단
2. DB 존재 확인
3. trade_date 결정
4. `score_date < trade_date` 확인
5. KIS quote-only intraday 수집/DB 적재
6. D 당일 replay와 zero-volume policy 비교
7. rolling replay/status 생성
8. replay 완료 후 public/no-secret daily_prices 수집
9. 기존 SWING pipeline으로 daily_features/daily_scores 생성
10. `generated_daily_score_date=D`, `score_ready_for_next_trade_date=True` 확인

refresh가 실패하면 status에 `DAILY_PRICE_FETCH_FAILED`, `DAILY_PRICE_FOR_TRADE_DATE_MISSING`, `DAILY_SCORES_GENERATION_FAILED`, `NEXT_DAY_SCORE_DATE_NOT_GENERATED` 같은 blocked reason이 기록됩니다. 이런 경우 다음 거래일 daily ops는 `STALE_SCORE_DATE`로 막힐 수 있으므로, 최신 daily_prices를 수집하거나 승인된 CSV를 적재한 뒤 SWING scoring을 다시 생성하세요.

EOD 상태 파일:

- `reports/daily_ops/day_eod_ops_status.json`
- `reports/daily_ops/day_eod_ops_status.md`

이 파일에는 `score_date_used_for_replay`, `replay_uses_same_day_score`, `daily_refresh_ran_after_replay`, `refreshed_daily_price_date`, `generated_daily_score_date`, `next_trade_date_candidate`, `score_ready_for_next_trade_date`, daily refresh blocked reason이 기록됩니다.

macOS launchd 준비:

`ops/launchd/com.krxstock.kis-daily-ops.plist.template`와 `ops/README_kis_daily_ops.md`는 매일 16:10 KST에 end-of-day ops를 실행하는 예시를 제공합니다. 템플릿은 자동 설치되지 않으며, `launchctl load` 또는 `launchctl bootstrap`은 사용자가 경로와 운영 리스크를 검토한 뒤 직접 실행해야 합니다. Codex는 launchd 등록을 시스템에 설치하거나 활성화하지 않습니다.

피해야 할 smoke 예시:

```bash
python3 scripts/run_kis_daily_intraday_collection.py \
  --db data/market_pipeline.db \
  --trade-date 2026-05-11 \
  --top-n 50 \
  --max-symbols 5
```

위 명령은 top 50 후보 중 5개만 수집하므로 MISSING_5M_DATA가 대량 발생할 수 있습니다.

pipeline smoke 권장:

```bash
python3 scripts/run_kis_daily_intraday_collection.py \
  --db data/market_pipeline.db \
  --trade-date 2026-05-11 \
  --market-symbol 069500 \
  --top-n 5 \
  --max-symbols 5 \
  --replay-collected-only
```

실제 검증 권장:

```bash
python3 scripts/run_kis_daily_intraday_collection.py \
  --db data/market_pipeline.db \
  --trade-date 2026-05-11 \
  --market-symbol 069500 \
  --top-n 20 \
  --max-symbols 20 \
  --require-full-top-n-coverage
```

장기 운영에서는 `--top-n 50`도 가능하지만, top-n 전체 후보의 intraday 데이터와 market proxy가 함께 누적되어야 합니다.
replay report의 coverage audit에서 `missing_intraday_symbols`, `intraday_coverage_ratio`, `partial_universe`, `replay_collected_only`를 먼저 확인하세요.
`INVALID_5M_BAR`나 `INVALID_15M_BAR`가 보이면 invalid bar analysis의 zero-volume, invalid OHLC, incomplete aggregation 샘플을 확인하고, 전략 조건을 완화하기 전에 데이터 원인을 먼저 분리합니다.

KIS 분봉 aggregation 품질 감사:

KIS 당일분봉을 수집한 뒤 `zero_volume_count`, `INVALID_5M_BAR`, `INVALID_15M_BAR`가 크면 전략 필터를 완화하지 말고 raw row, normalized 1m row, aggregated 5m/15m bar를 먼저 대조합니다.
zero-volume은 raw 단계, normalization 단계, aggregation 단계로 나누어 보고, invalid count는 event count와 unique bar count를 구분해야 합니다. replay는 같은 과거 invalid bar를 매 timestamp마다 반복 평가할 수 있으므로, 큰 rejection count가 항상 같은 수의 고유 불량 bar를 뜻하지는 않습니다.
하루 smoke 결과는 pipeline/data-quality 검증이며 수익성 검증이 아닙니다. 누적 데이터가 쌓이면 3~5거래일, 1개월, 3개월 순서로 확장하세요.

```bash
python3 scripts/audit_kis_intraday_aggregation.py \
  --env-file .env \
  --trade-date 2026-05-11 \
  --symbols 005930,000660,005490,069500 \
  --market-symbol 069500 \
  --output-md reports/kis_intraday_aggregation_audit_2026-05-11.md \
  --output-json reports/kis_intraday_aggregation_audit_2026-05-11.json
```

리포트에서 우선 확인할 항목:

- `raw_zero_volume_count`, `normalized_zero_volume_count`, `aggregated_5m_zero_volume_count`, `aggregated_15m_zero_volume_count`
- `raw_volume_sum`, `normalized_volume_sum`, `aggregated_5m_volume_sum`, `aggregated_15m_volume_sum`
- `zero_volume_cause_counts`
- `duplicate_timestamp_count`, `missing_timestamp_gaps`, `top_invalid_time_buckets`
- replay report의 `invalid_event_count`, `invalid_unique_bar_count`, `invalid_repeated_evaluation_count`

KIS zero-volume bar policy:

KIS 주식당일분봉에는 `cntg_vol=0`이지만 OHLC 가격은 정상인 row가 많이 섞일 수 있습니다. 이 row는 필드 매핑 오류가 아니라 no-trade 또는 quote carry-forward 성격일 수 있으므로, 전략 조건을 완화하기 전에 정책별 replay를 비교합니다.

- `strict_invalid`: 기본값입니다. `volume=0` bar를 `INVALID_BAR`로 처리해 가장 보수적으로 fail-closed 합니다.
- `no_trade_context`: OHLC가 정상인 zero-volume bar를 `NO_TRADE_BAR`로 분리합니다. 가격 continuity 진단에는 남기지만 VWAP, 거래량 증가, 유동성 조건은 positive-volume bar만 사용하고, no-trade bar 자체로 entry/exit를 발생시키지 않습니다.
- `drop_no_trade`: zero-volume row를 replay 입력에서 제외해 비교합니다. positive-volume row가 없는 bucket은 missing/no-trade로 해석합니다.

정책 비교는 데이터 품질 정책 검증이지 수익성 검증이 아닙니다. 어떤 정책을 쓰더라도 충분한 거래일을 누적한 뒤 3~5거래일, 1개월, 3개월 순서로 확장하세요.

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-11 \
  --end-date 2026-05-11 \
  --enable-day-trading \
  --market-symbol 069500 \
  --max-universe-symbols 20 \
  --compare-zero-volume-policies \
  --report-md reports/day_replay_zero_volume_policy_compare_2026-05-11.md
```

DAY PAPER account capital model:

DAY replay의 `gross_return_sum`과 `net_return_sum`은 거래별 수익률 합계입니다. 실제 모의 계좌 기준 일일 수익률은 `Paper Account Summary`의 `daily_return_pct`를 사용하세요. PAPER replay는 주문/계좌 API를 호출하지 않고, 내부 장부에서만 현금, 노출, 수수료, 거래세, 슬리피지, 원화 손익을 계산합니다.

기본값은 보수적인 검증용입니다.

- `paper_initial_cash_krw`: 10,000,000
- `paper_notional_per_trade_krw`: 1,500,000
- `paper_max_position_value_krw`: 1,500,000
- `paper_max_total_exposure_krw`: 4,000,000
- `paper_max_open_positions`: DAY 기본 최대 포지션과 일관되게 제한
- 현금 부족, 총 노출 초과, 일일 손실 한도 초과 시 PAPER 진입을 거절합니다.
- 소수점 주식은 허용하지 않고 정수 수량만 사용합니다.

예시:

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-11 \
  --end-date 2026-05-11 \
  --enable-day-trading \
  --market-symbol 069500 \
  --max-universe-symbols 20 \
  --zero-volume-bar-policy strict_invalid \
  --paper-initial-cash-krw 10000000 \
  --paper-notional-per-trade-krw 1000000 \
  --paper-max-total-exposure-krw 3000000 \
  --paper-max-position-value-krw 1000000 \
  --report-md reports/day_replay_paper_account_2026-05-11.md
```

데이터 품질 검증:

```bash
python3 scripts/validate_intraday_prices.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-08 \
  --market-symbol MARKET_PROXY
```

SIGNAL_ONLY 실행 예시:

```bash
python3 scripts/run_day_trading.py \
  --db data/market_pipeline.db \
  --as-of-date 2026-05-08 \
  --enable-day-trading \
  --mode SIGNAL_ONLY \
  --market-symbol MARKET_PROXY
```

PAPER 실행 예시:

```bash
python3 scripts/run_day_trading.py \
  --db data/market_pipeline.db \
  --as-of-date 2026-05-08 \
  --enable-day-trading \
  --mode PAPER \
  --market-symbol MARKET_PROXY \
  --notional-per-trade 1000000
```

`MARKET_PROXY`는 `intraday_prices`에 저장된 시장 지수 또는 시장 프록시용 심볼로 교체하세요.
시장 프록시 분봉을 제공하지 않으면 기본 설정에서는 fail-closed로 신호가 거절됩니다.
`run_day_trading.py`와 replay 스크립트는 DB를 조용히 새로 만들지 않습니다. 먼저 bootstrap과 데이터 적재를 완료해야 합니다.

SWING 후보군 룩어헤드 방지:

- DAY 거래일 `D`의 기본 후보군은 `D`보다 이전의 가장 최근 `daily_scores.date`를 사용합니다.
- 같은 날짜 점수는 기본 금지입니다.
- pre-market 확정 점수를 쓰려면 `--allow-same-day-scores --score-date-override YYYY-MM-DD`를 명시해야 하며, 리포트에 룩어헤드 위험으로 기록됩니다.

리플레이 백테스트 및 승격 게이트:

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-31 \
  --enable-day-trading \
  --market-symbol MARKET_PROXY \
  --report-md reports/day_validation_2026-05.md
```

리플레이는 각 timestamp에서 그 시점까지 완료된 5분/15분봉과 시장 프록시만 DAY 엔진에 제공합니다.
PAPER 체결은 매수/매도 모두 불리한 슬리피지와 수수료/거래세를 반영합니다.
승격 게이트는 표본 수, 관찰일 수, 비용 차감 기대값, Profit Factor, MDD, 연속 손실, 데이터 품질,
시장 프록시, 룩어헤드 검증을 모두 통과해야 readiness 후보로 표시합니다. LIVE 주문 실행은 여전히 제공하지 않습니다.

`LIVE` 주문 실행기는 의도적으로 구현되어 있지 않습니다. LIVE 전환은 별도 사람 검토, 브로커 주문 경로 검증,
계좌/키/운영 리스크 확인 이후에만 다뤄야 합니다.

실제 데이터 E2E 준비 순서:

1. DB bootstrap

```bash
python3 scripts/bootstrap_market_db.py --db data/market_pipeline.db
```

2. `daily_prices` 적재

```bash
python3 scripts/load_daily_prices.py \
  --db data/market_pipeline.db \
  --csv data/daily_prices.csv
```

3. `daily_scores` 적재

```bash
python3 scripts/load_daily_scores.py \
  --db data/market_pipeline.db \
  --csv data/daily_scores.csv \
  --trade-start-date 2026-05-08 \
  --trade-end-date 2026-05-31
```

4. 후보 종목 5분봉/15분봉 적재

```bash
python3 scripts/load_intraday_prices.py \
  --db data/market_pipeline.db \
  --csv data/intraday_candidates_5m.csv \
  --default-timeframe 5m \
  --source CANDIDATE_5M_CSV \
  --validate \
  --market-symbol MARKET_PROXY
```

5. 시장 프록시 5분봉/15분봉 적재

```bash
python3 scripts/load_intraday_prices.py \
  --db data/market_pipeline.db \
  --csv data/intraday_market_proxy_5m.csv \
  --default-timeframe 5m \
  --source MARKET_PROXY_5M_CSV \
  --validate \
  --market-symbol MARKET_PROXY
```

시장 프록시는 후보 종목과 같은 `intraday_prices` 테이블에 별도 `symbol`로 적재해야 합니다.

6. 데이터 가용성 audit

```bash
python3 scripts/audit_day_data_availability.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --market-symbol MARKET_PROXY \
  --report-md reports/day_data_availability_2026-05-08_2026-05-14.md
```

7. 3~5거래일 replay

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --enable-day-trading \
  --market-symbol MARKET_PROXY \
  --report-md reports/day_replay_2026-05-08_2026-05-14.md
```

8. 1개월 replay

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-01 \
  --end-date 2026-05-31 \
  --enable-day-trading \
  --market-symbol MARKET_PROXY \
  --report-md reports/day_replay_2026-05.md
```

9. markdown report 확인

- `reports/day_data_availability_*.md`: 리플레이 가능한 날짜/불가능한 날짜, previous score_date, 후보/분봉 교집합, 시장 프록시 상태
- `reports/day_replay_*.md`: 날짜별 signal/entry/exit/open position, 비용 차감 gross/net, rejection reason, promotion gate

실제 데이터 준비 체크리스트:

필수:

- trade_date 이전 `daily_scores` 존재
- `daily_scores`와 같은 `(symbol,date)`의 `daily_prices` 존재
- 후보 종목 `intraday_prices` 존재
- 시장 프록시 `intraday_prices` 존재
- primary 5분봉 존재
- timestamp가 bar start 기준인지 확인
- 데이터 gap/중복/OHLCV 오류 없음
- market proxy usable
- candidate usable count > 0

선택:

- 장중 외국인/기관 잠정 수급
- 프로그램 매매
- 체결강도
- 섹터 프록시

장기 DAY replay 데이터 준비 절차:

긴 기간 데이터는 많이 모을수록 좋지만, 실행은 짧은 구간부터 단계적으로 확대해야 합니다.
긴 데이터라도 룩어헤드가 있으면 검증은 무효입니다. 당일 장마감 후 계산된 `daily_scores`나 장마감 후 확정 `FINAL` 수급 데이터를 장중 판단에 쓰면 안 됩니다.

1. 먼저 긴 기간 `daily_prices`를 준비합니다.

```bash
python3 scripts/load_daily_prices.py \
  --db data/market_pipeline.db \
  --csv data/daily_prices_2026.csv
```

2. 같은 기간의 `daily_scores`를 준비합니다. DAY trade_date보다 이전 score_date가 있어야 합니다.

```bash
python3 scripts/load_daily_scores.py \
  --db data/market_pipeline.db \
  --csv data/daily_scores_2026.csv \
  --trade-start-date 2026-01-01 \
  --trade-end-date 2026-12-31
```

3. `plan_day_replay_dataset.py`로 필요한 intraday 후보 목록을 만듭니다.

```bash
python3 scripts/plan_day_replay_dataset.py \
  --db data/market_pipeline.db \
  --start-date 2026-01-01 \
  --end-date 2026-12-31 \
  --market-symbol MARKET_PROXY \
  --top-n 50 \
  --timeframe 5m \
  --required-intraday-csv reports/day_required_intraday_2026.csv \
  --missing-csv reports/day_missing_data_2026.csv \
  --report-md reports/day_dataset_plan_2026.md
```

`reports/day_required_intraday_*.csv` 예시:

```csv
date,symbol,timeframe,source_type,score_date,rank,score,required_reason
2026-05-08,005930,5m,CANDIDATE,2026-05-07,1,0.91,TOP_N_SWING_CANDIDATE
2026-05-08,MARKET_PROXY,5m,MARKET_PROXY,,,,MARKET_CONTEXT
```

`reports/day_missing_data_*.csv` 예시:

```csv
date,symbol,data_type,timeframe,reason
2026-05-08,005930,INTRADAY,5m,MISSING_INTRADAY
2026-05-08,MARKET_PROXY,INTRADAY,5m,MISSING_MARKET_PROXY
2026-05-08,,DAILY_SCORE,,NO_PRIOR_SCORE_DATE
```

4. required intraday CSV를 보고 후보 종목 5분봉과 시장 프록시 5분봉을 수집합니다. 외부 API 연동은 이 저장소의 DAY 검증 경로에 자동으로 붙이지 않습니다.
5. 수집한 intraday CSV를 `load_intraday_prices.py`로 적재합니다.
6. `audit_day_data_availability.py`로 replay 가능 날짜와 불가능 날짜를 확인합니다.
7. 처음에는 3~5거래일만 replay합니다.
8. 정상 동작 확인 후 1개월, 3개월, 6개월, 1년 순서로 확장합니다.
9. 수익률보다 먼저 rejection reason, usable count, cost impact, open position audit을 확인합니다.

장기 replay 전 점검 기준:

- `dates_without_prior_score`가 0에 가까운가
- `missing_intraday_count`와 `missing_market_proxy_count`가 충분히 낮은가
- `candidate_usable_symbol_count`가 0이 아닌가
- `same-day daily_scores`가 기본 경로에서 쓰이지 않았는가
- 비용 차감 전후 성과 차이와 슬리피지 민감도가 리포트에 보이는가
- `open_position_count_at_end`가 full-session replay에서 0인가

실제 데이터로 DAY replay 시작하기:

기존 SWING 파이프라인은 `daily_prices`에서 `daily_features`를 만들고,
기존 `pipeline/scoring.py::generate_daily_scores(...)`로 `daily_scores(symbol,date,score,rank)`를 갱신할 수 있습니다.
DAY replay는 이 점수를 사용하되, trade_date `D`에는 기본적으로 `D`보다 이전 score_date만 사용합니다.
긴 데이터라도 룩어헤드가 있으면 검증은 무효이며, 당일 장마감 후 점수나 장마감 후 확정 `FINAL` 수급을 장중 DAY 판단에 쓰면 안 됩니다.

흐름 A: 기존 SWING pipeline으로 `daily_scores` 생성

`prepare_day_replay_db.py --run-swing-pipeline`은 CSV 기반 daily price 입력을 받아 기존 SWING feature/scoring 모듈을 내부에서 호출합니다.
이 경로는 외부 API를 호출하지 않으며, `--pipeline-source krx`처럼 외부 수집이 필요한 설정은 DAY 준비 경로에서 blocked 처리합니다.
일봉 CSV가 없다면 먼저 `fetch_public_daily_prices.py`로 소량 public/no-secret 일봉을 받아 smoke DB 준비를 할 수 있습니다.

```bash
python3 scripts/prepare_day_replay_db.py \
  --db data/market_pipeline.db \
  --bootstrap \
  --run-swing-pipeline \
  --prices-csv data/daily_prices.csv \
  --universe-csv data/krx_source_universe_500.csv \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --market-symbol MARKET_PROXY \
  --top-n 50 \
  --dataset-plan-md reports/day_dataset_plan_2026-05-08_2026-05-14.md \
  --audit-report-md reports/day_data_availability_2026-05-08_2026-05-14.md
```

입력/출력 테이블:

- 입력: `daily_prices(symbol,date,open,high,low,close,volume)`
- 중간: `daily_features(symbol,date,...)`
- 후보/점수: `daily_scores(symbol,date,score,rank)`
- DAY 준비 리포트: daily score 날짜 범위, 날짜별 symbol 수, trade_date별 previous score_date, same-day-only 차단 여부

이 실행은 intraday CSV가 없어도 required intraday manifest와 availability audit을 생성할 수 있습니다.
다만 replay 가능한 후보/시장 프록시 분봉이 없으면 `NO_INTRADAY_DATA`, `MISSING_MARKET_PROXY`, `DATA_NOT_REPLAYABLE_AFTER_PREPARE` 같은 blocked 사유로 끝나야 정상입니다.

흐름 B: `daily_scores.csv` 직접 제공

```bash
python3 scripts/load_daily_scores.py \
  --db data/market_pipeline.db \
  --csv data/daily_scores.csv \
  --trade-start-date 2026-05-08 \
  --trade-end-date 2026-05-31
```

`daily_scores`는 같은 `(symbol,date)`의 `daily_prices`가 먼저 있어야 합니다.
후보 CSV가 `date,symbol,rank,score` 형태라면 별도 변환 없이 이 로더를 사용하세요.
임의 점수 생성 로직은 만들지 않습니다.

직접 CSV 적재 흐름을 한 번에 묶는 예시:

```bash
python3 scripts/prepare_day_replay_db.py \
  --db data/market_pipeline.db \
  --bootstrap \
  --daily-prices-csv data/daily_prices.csv \
  --daily-scores-csv data/daily_scores.csv \
  --intraday-csv data/intraday_prices.csv \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --market-symbol MARKET_PROXY \
  --top-n 50 \
  --dataset-plan-md reports/day_dataset_plan_2026-05-08_2026-05-14.md \
  --audit-report-md reports/day_data_availability_2026-05-08_2026-05-14.md
```

`--run-swing-pipeline`과 `--daily-scores-csv`를 동시에 주면 기본적으로 충돌로 막힙니다.
둘 중 하나를 선택하고, 정말 직접 score CSV 적재를 건너뛸 때만 `--skip-direct-daily-scores-load`를 사용하세요.

프로젝트 데이터 소스 상태 점검:

```bash
python3 scripts/audit_project_data_sources.py \
  --db data/market_pipeline.db \
  --data-dir data \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --market-symbol MARKET_PROXY \
  --report-md reports/project_data_sources.md
```

DAY replay 단계 확장:

```bash
python3 scripts/run_day_replay_backtest.py \
  --db data/market_pipeline.db \
  --start-date 2026-05-08 \
  --end-date 2026-05-14 \
  --enable-day-trading \
  --market-symbol MARKET_PROXY \
  --max-universe-symbols 50 \
  --report-md reports/day_replay_2026-05-08_2026-05-14.md
```

처음에는 3~5거래일만 replay하고, 문제 없이 point-in-time slicing, rejection reason, usable count, cost impact, open position audit이 보이면 1개월, 3개월, 6개월, 1년 순서로 확대하세요.
데이터는 길게 모으되 실행은 짧은 구간부터 확장합니다.

## Requirements

- Python 3.10+
- SQLite (Python 내장)
- 실데이터 적재 시: `pykrx`

```bash
pip install pykrx
```

## 종목 코드 정책 (KRX 6자리)

- 모든 종목은 **6자리 숫자 코드**로 처리합니다.
  - 예: `005930` (삼성전자), `000660` (SK하이닉스)
- CSV ingest 및 pykrx ingest 모두 코드 정규화(`zfill(6)`)를 수행합니다.

## 실행 방법

### 1) 샘플 CSV 생성 (오프라인/테스트용)

```bash
python scripts/generate_sample_prices.py
```

생성 파일: `data/sample_daily_prices.csv`

### 2) CSV 기반 파이프라인 실행

```bash
python scripts/run_pipeline.py \
  --source csv \
  --db data/market_pipeline.db \
  --prices-csv data/sample_daily_prices.csv \
  --top-n 3 \
  --rebalance-frequency daily \
  --min-holding-days 5 \
  --keep-rank-threshold 5
```


### 2-1) 매매 빈도 제어 옵션 (백테스트/모의매매 공통)

- `--rebalance-frequency`: `daily`(기본) 또는 `weekly`
  - `weekly` 정의: **ISO 주차 기준으로 각 주의 첫 거래일에만 종목 교체**를 수행합니다.
  - 주중 나머지 거래일에는 기존 포지션을 유지하고 수익률만 반영합니다.
- `--min-holding-days` (기본: `5`)
  - 신규 매수 종목은 최소 N거래일 유지합니다.
- `--keep-rank-threshold` (기본: `top_n`)
  - 기존 보유 종목의 순위가 임계치 이내면 즉시 교체하지 않고 계속 보유합니다.

예시(회전율 완화형):

```bash
python scripts/run_pipeline.py \
  --source csv \
  --db data/market_pipeline.db \
  --prices-csv data/sample_daily_prices.csv \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 5
```

### 3) KRX 실데이터(pykrx) 기반 실행

#### (a) 지정 종목

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --symbols 005930,000660,035420,035720 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 3 \
  --rebalance-frequency daily \
  --min-holding-days 5 \
  --keep-rank-threshold 5
```

#### (b) 시장 단위 유니버스 (KOSPI/KOSDAQ/ALL)

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10
```

`--symbols`를 생략하면 `--market` 기준으로 티커를 자동 수집합니다.

#### (c) 후보군 CSV 파일 직접 지정 (`--universe-file`)

`symbol` 컬럼만 있으면 됩니다.

- `data/kospi100_manual.csv`: **KRX 100개 종목 코드 후보군**(기존 파일, 단일 컬럼 유지)
- `data/krx_source_universe_500.csv`: **dynamic universe 실험용 500개 source pool**
  - 컬럼: `symbol,name,market,note`
  - `symbol`은 KRX 6자리 문자열
  - `note`에 표시된 것처럼 **수동 관리 후보군이며 정기 업데이트 필요**

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --universe-file data/krx_source_universe_500.csv \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10
```

우선순위 규칙:
- `--universe-file` 지정 시: CSV의 `symbol` 후보군 사용 (가장 우선)
- `--symbols`만 지정 시: 커맨드라인 종목 사용
- 둘 다 없으면: `--market` 기반 자동 수집

`symbol`은 문자열로 읽고 KRX 6자리 코드로 정규화(`zfill(6)`)합니다.

시장 자동 수집은 아래 순서로 동작합니다.

1. `pykrx.stock.get_market_ticker_list(...)` 시도
2. 실패하거나 0건이면 공개 KIND 다운로드(`https://kind.krx.co.kr/...corpList.do?method=download`)로 fallback

즉, `--market` 경로는 로그인 환경변수(`KRX_ID`, `KRX_PW`)에 의존하지 않도록 구성되어 있습니다.

수집 실패 시에는 뒤 단계(backtest)까지 진행하지 않고, 앞단에서 명확한 오류 메시지로 종료합니다.
또한 유니버스 필터 결과가 0개이면 scoring/backtest/paper trading을 건너뛰고, 원인 요약을 출력합니다.

### 3-1) 전체 시장 스캔 시 유니버스 필터

기본적으로 `run_pipeline.py`는 **스코어링 전에** 다음 필터를 적용합니다.

1. 최근 종가 하한 (`--min-close-price`, 기본: `3000`)
2. 최근 20일 평균 거래대금 하한 (`--min-avg-dollar-volume-20d`, 기본: `1_000_000_000`)
3. 최근 20일 평균 거래량 하한 (`--min-avg-volume-20d`, 기본: `100_000`)
4. 최근 60거래일 데이터 최소 개수 (`--min-data-days-60d`, 기본: `60`)
5. 이상 급등/급락 필터
   - 최근 `N`일(`--shock-lookback-days`, 기본: `20`) 동안
   - 일간 수익률 절대값이 `threshold`(`--shock-abs-return-threshold`, 기본: `0.18`) 이상인 횟수가
   - `--shock-max-hits`(기본: `1`) 초과 시 제외

실행 시 필터 전/후 종목 수와, 이유별 제거 건수를 로그로 출력합니다.

필터를 끄려면:

```bash
python scripts/run_pipeline.py --source krx --market ALL --disable-universe-filter
```

전체 시장 스캔 예시(필터 파라미터 조정):

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market ALL \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 20 \
  --min-close-price 3000 \
  --min-avg-dollar-volume-20d 1000000000 \
  --min-avg-volume-20d 100000 \
  --min-data-days-60d 60 \
  --shock-lookback-days 20 \
  --shock-abs-return-threshold 0.18 \
  --shock-max-hits 1
```

### 3-2) Colab에서 가장 쉬운 실행 예시

아래 3줄이면 후보군 CSV를 바로 사용해 실행할 수 있습니다.

```bash
!python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --universe-file data/kospi100_manual.csv \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10
```

실행 로그에서 아래 두 줄이 보이면 정상입니다.

- `[universe] loaded symbols=100 from file=data/kospi100_manual.csv`
- `[universe] verified symbols=100 for kospi100_manual.csv`

### 3-3) `--symbols` vs `--universe-file` 차이

- `--symbols`: CLI에 직접 콤마 구분으로 입력 (`--symbols 005930,000660,...`)
- `--universe-file`: CSV 파일 경로만 넘기면 `symbol` 컬럼을 읽어 자동 후보군 구성
  - 운영/실험에서 후보군 버전 관리가 쉽고, Colab에서 문자열 조립 작업이 사라집니다.

### 3-3-1) Static Universe vs Dynamic Universe

- **static**(기본): 기존과 동일하게 `--universe-file` 또는 `--symbols`로 고정 후보군을 사용합니다.
- **rolling_liquidity**: 날짜별로 후보군을 다시 구성합니다.
  - 구현 범위(이번 버전): 현재 DB의 `daily_prices` 종목 풀 안에서 계산
  - 공식: `avg_dollar_volume_20d = mean(close * volume)` (최근 20거래일, **t-1까지**)
  - t일 후보군은 `t-1`까지의 정보만으로 상위 N개를 뽑습니다(룩어헤드 바이어스 방지).

CLI 옵션:

- `--universe-mode static|rolling_liquidity` (기본: `static`)
- `--universe-size` (기본: `100`)
- `--universe-lookback-days` (기본: `20`)

`rolling_liquidity` 실행 시 `daily_universe` 테이블이 생성/갱신되며, scoring/backtest는 해당 날짜의 유니버스 종목만 사용합니다.

### 3-3-2) 룩어헤드 바이어스 방지 원칙

- 원칙: **t일 후보군 계산에 t일 데이터는 사용하지 않는다.**
- 구현: 각 symbol/date에 대해 `ROW_NUMBER`를 사용해 직전 `lookback_days` 구간(`rn-lookback` ~ `rn-1`)의 `close*volume` 평균으로 유니버스를 산출합니다.
- 검증: 파이프라인 실행 시 `lookahead_validation` 로그(checked/violations)를 출력합니다.


### Final 후보 비교 리포트 생성 예시

```bash
python scripts/generate_final_candidate_report.py \
  --db data/kospi_495_rolling_3y.db \
  --universe-file data/kospi_valid_universe_495.csv \
  --outdir reports/final_candidate_report/latest \
  --benchmark-mode universe \
  --eval-frequencies monthly,quarterly \
  --horizons 1,3,6,12 \
  --start-date 2024-01-01 \
  --end-date 2025-12-31 \
  --overwrite
```

생성 파일:
- `manifest.json`
- `candidate_summary.csv`
- `window_results.csv`
- `equity_curve.csv`
- `drawdown_curve.csv`
- `monthly_returns.csv`
- `worst_windows.csv`
- `final_candidate_report.md`
- `equity_curve.png`
- `drawdown_curve.png`
- `monthly_returns_plot.png` (또는 heatmap)

추가 옵션:
- `--output-dir` 는 하위 호환 alias(내부적으로 `--outdir`로 매핑)
- `--allow-smoke` 를 지정하면 full-period 단일 백테스트만 수행(robustness window 생략 가능)
- 기본 strict final mode(`--allow-smoke` 미지정)에서는 `window_results.csv`가 비어 있으면 실패

### 3-3-3) Entry Gate + 현금 보유 허용 (무조건 top_n 매수 방지)

왜 필요한가:
- 기존 top_n 전략은 후보군이 전반적으로 약한 날에도 “상대적 1등”을 강제로 매수할 수 있습니다.
- 이 경우 회복장 이전 구간에서 손실/변동성이 커질 수 있습니다.

Entry Gate 개념:
- **신규 진입 후보**에 최소 품질 조건을 적용해, 조건 통과 종목만 매수합니다.
- 기존 보유 종목은 기존 규칙(`min_holding_days`, `keep_rank_threshold`)을 먼저 적용합니다.
- 조건 통과 종목이 부족하면 **남은 슬롯은 비우고 현금으로 유지**합니다.

`run_pipeline.py` 옵션:
- `--enable-entry-gate` (기본 OFF)
- `--min-entry-score` (기본 `0.0`)
- `--require-positive-momentum20` (기본 OFF)
- `--require-positive-momentum60` (기본 OFF)
- `--require-above-sma20` (기본 OFF)
- `--require-above-sma60` (기본 OFF)

진단 지표(백테스트 결과):
- `entry_gate_enabled`
- `entry_gate_rejected_count`
- `entry_gate_cash_days`
- `average_actual_position_count`
- `min_actual_position_count`
- `max_actual_position_count`

예시(기본 trend gate):

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/entry_gate_demo.db \
  --universe-file data/krx_source_universe_500.csv \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 5 \
  --rebalance-frequency weekly \
  --enable-entry-gate \
  --min-entry-score 0.0 \
  --require-positive-momentum20 \
  --require-above-sma20
```

### 3-3-3-a) Optional Overheat Entry Gate (신규 진입 과열 추격 방지)

목적:
- 기존 scoring 수식은 유지하고, **신규 진입 후보에만** 과열 조건을 적용해 급등 추격 매수를 줄입니다.
- 기존 보유 종목은 `min_holding_days` / `keep_rank_threshold` 규칙을 그대로 따릅니다.

`run_pipeline.py` 옵션:
- `--enable-overheat-entry-gate` (기본 OFF)
- `--max-entry-ret-1d` (기본 `0.08`)
- `--max-entry-ret-5d` (기본 `0.15`)
- `--max-entry-range-pct` (기본 `0.10`)
- `--max-entry-volume-z20` (기본 `3.0`)
- `--enable-volume-surge-overheat-rule` (기본 OFF)
- `--volume-surge-threshold` (기본 `3.0`)
- `--volume-surge-ret-5d-threshold` (기본 `0.10`)

예시:

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/overheat_gate_demo.db \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --rebalance-frequency weekly \
  --scoring-version old \
  --top-n 5 \
  --enable-overheat-entry-gate \
  --max-entry-ret-1d 0.08 \
  --max-entry-ret-5d 0.15 \
  --max-entry-range-pct 0.10 \
  --max-entry-volume-z20 3.0 \
  --enable-volume-surge-overheat-rule \
  --volume-surge-threshold 3.0 \
  --volume-surge-ret-5d-threshold 0.10
```

### 3-3-3-b) Optional Entry Quality Gate (range/volatility 기반 신규 진입 품질 게이트)

- 기본 동작: OFF (기존 전략 동작 유지)
- 적용 대상: **신규 진입 후보만**(기존 보유 종목 강제매도 없음)
- 개별 룰 ON/OFF:
  - `--enable-entry-quality-gate`
  - `--enable-entry-range-rule`
  - `--enable-entry-volatility-rule`
  - `--enable-entry-ret5-minus-range-rule`
  - `--enable-entry-range-to-ret5-rule`
  - `--enable-entry-volatility-to-momentum20-rule`
- 임계값:
  - `--max-entry-range-pct` (품질 range 룰 상한)
  - `--max-entry-volatility-20d`
  - `--min-entry-ret5-minus-range`
  - `--max-entry-range-to-ret5`
  - `--max-entry-volatility-to-momentum20`

예시:

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/entry_quality_gate_demo.db \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --rebalance-frequency weekly \
  --scoring-version old \
  --top-n 5 \
  --enable-entry-quality-gate \
  --enable-entry-range-rule \
  --enable-entry-volatility-rule \
  --enable-entry-ret5-minus-range-rule \
  --enable-entry-range-to-ret5-rule \
  --enable-entry-volatility-to-momentum20-rule \
  --max-entry-range-pct 0.16 \
  --max-entry-volatility-20d 0.06 \
  --min-entry-ret5-minus-range 0.00 \
  --max-entry-range-to-ret5 1.5 \
  --max-entry-volatility-to-momentum20 0.5
```

### 3-3-4) KOSPI / KOSDAQ 분리 진단 (market scope)

왜 분리해서 보나:
- source universe 500이 KOSPI/KOSDAQ 혼합이면, 시장 특성 차이(변동성/유동성/테마 민감도)로 성과가 섞여 보입니다.
- 동일 전략이라도 시장별 적합도가 다를 수 있어 분리 진단이 필요합니다.

`run_robustness_experiments.py` 옵션:
- `--market-scopes KOSPI,KOSDAQ,ALL`
  - `KOSPI`: `universe-file`의 `market=KOSPI`만 사용
  - `KOSDAQ`: `market=KOSDAQ`만 사용
  - `ALL`: 전체 혼합

결과에 함께 기록:
- `market_scope`, `source_symbol_count`, `average_daily_universe_count`
- `selected_kospi_count`, `selected_kosdaq_count`
- `kospi_contribution_return`, `kosdaq_contribution_return`

추천 비교 실험:

```bash
python scripts/run_robustness_experiments.py \
  --db data/entry_gate_market_scope.db \
  --universe-file data/krx_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --period-months 3,6,12 \
  --top-n-values 5 \
  --min-holding-days-values 5 \
  --keep-rank-offsets 4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-scopes KOSPI,KOSDAQ,ALL \
  --entry-gate-modes off,on \
  --entry-gate-rule-set basic_trend \
  --min-entry-score-values 0.0,0.1
```

요청한 최소 조합만 돌릴 때:

```bash
python scripts/run_robustness_experiments.py \
  --db data/entry_gate_market_scope_minimal.db \
  --universe-file data/krx_source_universe_500.csv \
  --period-months 3,6,12 \
  --top-n-values 5 \
  --min-holding-days-values 5 \
  --keep-rank-offsets 4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-scopes KOSPI,KOSDAQ,ALL \
  --entry-gate-modes off,on \
  --entry-gate-rule-set basic_trend \
  --min-entry-score-values 0.0
```

### 3-4) 가장 안전한 검증 경로

1. CSV 형식/행 수 점검 (`symbol` 컬럼 + 500개 행):

```bash
python scripts/validate_universe_csv.py --file data/krx_source_universe_500.csv
```

원라인(행 수/고유값/중복/6자리/market 분포) 확인 예시:

```bash
python - <<'PY'
import csv
import re
from collections import Counter

with open("data/krx_source_universe_500.csv", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    rows = list(r)
    cols = r.fieldnames or []

symbols = [str(x.get("symbol", "")).zfill(6) for x in rows]
print("rows:", len(rows))
print("unique_symbols:", len(set(symbols)))
print("columns:", cols)
print("all_6digit:", all(re.fullmatch(r"\\d{6}", s) for s in symbols))
if "market" in cols:
    print("market_distribution:", dict(Counter((x.get("market") or "").upper() for x in rows)))
else:
    print("market_distribution: no market column")
PY
```

2. 파이프라인 실행 (`--universe-file`):

```bash
python scripts/run_pipeline.py --source krx --universe-file data/krx_source_universe_500.csv --start-date 2025-01-01 --end-date 2025-01-31
```

3. 기존 방식과 충돌 없는지 확인 (`--symbols`와 동시 전달):

```bash
python scripts/run_pipeline.py --source krx --symbols 005930 --universe-file data/krx_source_universe_500.csv --start-date 2025-01-01 --end-date 2025-01-31
```

실행 로그에서 `[universe] loaded symbols=... from file=...`가 보이면 `--universe-file` 우선 규칙이 적용된 것입니다.

Dynamic universe 검증(요약/카운트/Top10/Static 비교):

```bash
python scripts/inspect_daily_universe.py \
  --db data/dynamic_universe_3y.db \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --compare-static-universe-file data/kospi100_manual.csv
```

출력에서 아래를 확인합니다.
- `daily_universe_summary.row_count`, `min_date`, `max_date`
- `universe_count_by_date_*` (날짜별 count)
- `top10_for_date` (특정 날짜 상위 10개)
- `size_violations_over_limit` (universe_size 준수 여부)
- `rolling_vs_static_comparison.is_different` (static 대비 후보군 차이)

### 3-4-1) Colab dynamic universe 실행 예시

```bash
!python scripts/run_pipeline.py \
  --source krx \
  --db data/dynamic_universe_3y.db \
  --universe-file data/krx_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old
```

위 예시는 **source universe = 500개**, **rolling universe size = 100개**, **lookback = 20거래일** 설정입니다.

### 3-4-2) static kospi100 vs rolling_liquidity 비교 예시

```bash
# 1) static
python scripts/run_pipeline.py \
  --source krx \
  --db data/static_kospi100.db \
  --universe-file data/kospi100_manual.csv \
  --universe-mode static \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old

# 2) rolling_liquidity
python scripts/run_pipeline.py \
  --source krx \
  --db data/dynamic_universe_3y.db \
  --universe-file data/kospi100_manual.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old
```

### 3-5) 시장 필터(Market Regime Filter) 옵션

목적: **종목을 더 잘 고르는 것보다, 시장이 약할 때 포지션을 줄여 낙폭을 완화**하기 위한 보수적 옵션입니다.

동작 기준(프록시):
- 시장 지표는 `daily_prices` 전체 종목의 일별 평균 종가를 사용한 **KOSPI 방향 프록시**입니다.
- `--enable-market-filter`를 켜면 아래 규칙이 리밸런싱 시점에 적용됩니다.

규칙:
1. 20일선 하회: 목표 보유 수를 축소
   - `--market-filter-ma20-reduce-by` (기본 1)
   - 예: `top_n=3`이고 20일선 하회면 `2종목` 목표
2. 60일선 하회: 위험 회피 모드
   - `--market-filter-ma60-mode block_new_buys` (기본): 신규 매수 금지, 기존 보유만 유지
   - `--market-filter-ma60-mode cash`: 목표 보유 0으로 축소(현금 대기)
   - `--market-filter-ma60-mode none`: 60일선 규칙 비활성

기본 전략(old + weekly + hold=5 + keep=7 + top_n=3)에 필터 ON 적용:

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --scoring-version old \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --enable-market-filter \
  --market-filter-ma20-reduce-by 1 \
  --market-filter-ma60-mode block_new_buys
```

필터 OFF(기존 기준 전략):

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --scoring-version old \
  --top-n 3 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7
```

`run_pipeline.py` 출력 JSON의 `market_filter` 필드에서 ON/OFF, 파라미터, 그리고 진단 요약(`diagnostics`)을 확인할 수 있습니다.

시장 필터가 실제로 작동한 일자를 확인하려면 아래 SQL을 사용하세요.

```sql
SELECT
  date,
  market_proxy_value,
  market_proxy_ma20,
  market_proxy_ma60,
  below_ma20,
  below_ma60,
  original_target_count,
  adjusted_target_count,
  ma60_mode,
  action
FROM backtest_market_filter_events
WHERE run_id = '<backtest_run_id>'
ORDER BY date;
```

`action` 해석:
- `reduce_holdings`: MA20 하회로 목표 보유 수 축소가 적용됨
- `block_new_buys`: MA60 하회 + `ma60_mode=block_new_buys`로 신규 매수 차단이 적용됨
- `cash`: MA60 하회 + `ma60_mode=cash`로 현금 대기(목표 보유 0) 적용됨
- `none`: 트리거는 있었지만(예: MA60 + mode=none) 포지션 제약이 추가로 걸리지 않음

### 3-6) robustness에서 필터 ON/OFF 비교

아래처럼 `--market-filter-modes off,on`을 주면 같은 파라미터 조합을 필터 OFF/ON으로 모두 실행합니다.

```bash
python scripts/run_robustness_experiments.py \
  --db data/market_pipeline.db \
  --output-dir data/reports \
  --period-months 3,6,12 \
  --top-n-values 3 \
  --min-holding-days-values 5 \
  --keep-rank-offsets 4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-filter-modes off,on \
  --market-filter-ma20-reduce-by 1 \
  --market-filter-ma60-mode block_new_buys
```

생성되는 요약 Markdown/CSV에서 `market_filter_enabled`, `market_filter_ma20_reduce_by`, `market_filter_ma60_mode`와 함께
`ma20_trigger_count`, `ma60_trigger_count`, `reduced_target_count_days`, `blocked_new_buy_days`, `cash_mode_days` 컬럼으로 실제 작동 빈도를 비교할 수 있습니다.

`block_new_buys`와 `cash` 결과가 거의 같게 보일 때는 아래를 먼저 확인하세요.
1. `ma60_trigger_count`가 0인지 (애초에 MA60 하회가 거의 없으면 두 모드 차이가 작습니다)
2. `blocked_new_buy_days` 대비 `cash_mode_days` 차이
3. `backtest_market_filter_events.adjusted_target_count`가 이미 낮은지(예: MA20 축소 + 기존 보유 적음)
4. 리밸런싱 빈도(weekly에서는 신호가 있어도 반영 횟수가 줄어듦)

### 4) 검증 실행

```bash
python scripts/validate_pipeline.py --db data/market_pipeline.db --top-n 3
```

검증 항목:
- 가격/피처/스코어 row 존재 여부
- 유니버스 필터 요약 일관성 점검(필터 전/후 count, reason 집계)
- 백테스트 결과 row 생성 여부
- paper trading 사이클 정상 실행 여부


### 5-1) 전략 강건성(robustness) 실험 자동화

`old / hybrid_v4`를 기본 축으로 두고(필요 시 `trend_v2`/`hybrid_v3` 보조 비교), 특정 구간에서만 우연히 좋았는지 확인하기 위해 동일 후보군/데이터에서 여러 파라미터 조합을 반복 실행해 비교 리포트를 생성할 수 있습니다.

```bash
python scripts/run_robustness_experiments.py \
  --db data/market_pipeline.db \
  --output-dir data/reports \
  --period-months 3,6,12 \
  --top-n-values 3,5,10 \
  --min-holding-days-values 5,10 \
  --keep-rank-offsets 2,4 \
  --scoring-versions old,hybrid_v4 \
  --rebalance-frequency daily
```

codex/add-universe-file-option-for-candidate-csv-input
후보군 고정 실험이 필요하면 `--universe-file`(또는 `--symbols`)를 추가로 지정하세요.

```bash
python scripts/run_robustness_experiments.py \
  --db data/market_pipeline.db \
  --output-dir data/reports \
  --period-months 3,6,12 \
  --top-n-values 3,5,10 \
  --min-holding-days-values 5,10 \
  --keep-rank-offsets 2,4 \
  --scoring-versions old,hybrid_v3 \
  --universe-file data/kospi100_manual.csv
```

`--universe-file`이 주어지면 `--symbols`보다 우선합니다.
`data/kospi100_manual.csv`를 전달하면 로그에서 `loaded symbols=100` 및 `verified symbols=100`을 함께 확인할 수 있습니다.

`trend_v2`를 보조 실험군으로 함께 포함하려면:

```bash
python scripts/run_robustness_experiments.py --db data/market_pipeline.db --include-trend-v2
```

main
핵심 동작:
- 조합 축 자동 반복:
  - 기간: 3/6/12개월
  - `top_n`: 3/5/10
  - `min_holding_days`: 5/10
  - `keep_rank_threshold`: `top_n+2`, `top_n+4`
  - `scoring_version` 기본: `old`, `hybrid_v4` (옵션으로 `trend_v2`, `hybrid_v3` 추가 가능)
- 각 조합마다 `daily_scores` 재계산 + `run_backtest(...)` 실행
- 결과 저장:
  - SQLite
    - `robustness_experiment_batches`
    - `robustness_experiment_results`
    - `robustness_experiment_stability`
  - CSV
    - `robustness_experiments_<batch_id>.csv`
    - `robustness_stability_<batch_id>.csv`
  - Markdown 요약
    - `robustness_summary_<batch_id>.md`

최소 비교 지표(요구사항 반영):
- 총 수익률 (`total_return`)
- 최대 낙폭 (`max_drawdown`)
- 샤프비율 (`sharpe`)
- 거래 횟수 (`trade_count`)
- 후보군 평균 대비 초과수익 (`excess_return_vs_universe`)

정렬/해석:
- 개별 실험은 `robustness_score` 기준으로 정렬
- 기간 축을 묶은 안정성 요약은 `stability_score` 기준으로 정렬
- 요약 Markdown에서 “가장 안정적인 설정”을 쉬운 문장으로 확인 가능

### 5-2) 시작일 기준 Rolling-window Robustness 평가 (신규)

단일 종료일 기반 평가(`run_robustness_experiments.py`)는 빠르게 비교할 때 유용하지만, **마지막 날짜 하나만 기준으로 보면 특정 구간에 과적합될 수 있습니다**.  
그래서 `scripts/run_start_window_robustness.py`는 **시작일을 여러 개로 바꿔야 전략의 기간 강건성을 확인할 수 있다**는 원칙으로, 각 시작일마다 1/3/6/12개월 forward 성과를 반복 측정합니다.

핵심 해석 포인트:
- **1개월 수익률은 노이즈가 크고, 3/6/12개월과 함께 봐야 한다**.
- `monthly` 시작일은 창(window)들이 서로 겹치므로 완전 독립 표본은 아니지만,  
  **rolling monthly windows는 서로 겹치므로 완전히 독립 샘플은 아니지만, 단일 종료일 평가보다 훨씬 낫다**.

예시(KOSPI-only, rolling liquidity universe):

```bash
python scripts/run_start_window_robustness.py \
  --db data/kospi_risk_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date-frequency quarterly \
  --min-start-date 2022-01-01 \
  --max-start-date 2025-03-31 \
  --period-months 1,3,6,12 \
  --top-n-values 3,5 \
  --min-holding-days-values 3,5,10 \
  --keep-rank-offsets 2,4 \
  --keep-rank-threshold-values 5,7,9 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-filter-modes off \
  --entry-gate-modes off \
  --market-scopes KOSPI \
  --position-stop-loss-modes off,on \
  --position-stop-loss-pcts none,0.10 \
  --portfolio-dd-cut-modes off
```

옵션 메모:
- `--keep-rank-threshold-values`를 주면 absolute threshold grid를 사용하며, 미지정 시 기존 `--keep-rank-offsets`를 사용합니다.
- `--position-stop-loss-pcts` / `--portfolio-dd-cut-pcts`는 mode와 독립적으로 해석됩니다. 예: `none,0.10` → OFF + ON(10%).
- stability score는 1/3/6/12개월 가중치(0.10/0.20/0.30/0.40)를 고정 사용하며, missing horizon이 있어도 재정규화하지 않습니다.

산출물:
- CSV
  - `start_window_robustness_results_<batch_id>.csv`
  - `start_window_robustness_strategy_summary_<batch_id>.csv`
  - `start_window_robustness_period_summary_<batch_id>.csv`
- SQLite
  - `start_window_robustness_batches`
  - `start_window_robustness_results`
  - `start_window_robustness_strategy_summary`
  - `start_window_robustness_period_summary`
- Markdown
  - `start_window_robustness_summary_<batch_id>.md`

`rolling_liquidity` 사용 시에는 기존과 동일하게 `validate_rolling_universe_no_lookahead(...)`를 실행해  
`t`일 유니버스에 `t` 이후 데이터가 섞이지 않았는지(violations) 함께 기록합니다.

### 5) 백테스트 성과 비교 리포트 생성

```bash
python scripts/generate_performance_report.py \
  --db data/market_pipeline.db \
  --baseline-run-id <daily_run_id> \
  --improved-run-id <improved_old_run_id> \
  --improved-new-run-id <improved_new_run_id> \
  --benchmark KOSPI \
  --output-dir data/reports
```

옵션:
- `--baseline-run-id`: baseline 전략 run_id (일간 리밸런싱 권장)
- `--improved-run-id`: 개선 전략(run_id, 기존 scoring)
- `--improved-new-run-id`: 개선 전략(run_id, 새 scoring)
- `--run-id`: (하위호환) improved run_id alias
- `--benchmark`: `KOSPI` 또는 `KOSPI200` 선호값

생성 산출물:
- `performance_comparison_<run_id>.csv` (요약 지표)
- `equity_curve_comparison_<run_id>.csv` (누적 자산 곡선 비교 데이터)
- `monthly_returns_<run_id>.csv` (월별 수익률 요약)
- SQLite 테이블:
  - `performance_report_runs`
  - `performance_report_summary`
  - `performance_report_curve`
  - `performance_report_monthly`

비교 기준(동일 후보군/동일 교집합 기간):
1. `baseline_strategy`: 기존 일간 리밸런싱 전략
2. `improved_strategy_old`: 개선 전략(기존 scoring)
3. `improved_strategy_new`: 개선 전략(새 scoring, 선택 입력)
4. `equal_weight_universe`: 같은 날짜의 `daily_scores` 후보군 전체를 동일비중으로 보유
5. `benchmark_kospi`: pykrx 인덱스(`KOSPI=1001`, `KOSPI200=1028`) 조회를 우선 사용
   - 인덱스 데이터를 가져오지 못하면 `daily_prices` 전체 동일비중 프록시를 자동 사용
   - 실제 사용된 소스는 `performance_report_runs.benchmark_source`와 리포트 JSON 출력에서 확인 가능

지표 정의:
- 실제 초기 자본: `backtest_runs.initial_equity` (없으면 첫 기록/수익률 역산)
- 첫 기록 시점 자산: 백테스트 첫 행 자산(첫 리밸런싱 이후)
- 마지막 자산, 총 수익률, 연환산 수익률
- 최대 낙폭(MDD), 변동성(연환산), 샤프비율(무위험수익률 0 가정)
- 거래 횟수(추정): 일자별 편입 종목 집합 변화량(매수+매도)
- 평균 보유 종목 수

리포트 해석 팁:
- `improved_strategy_old/new`가 `baseline_strategy`보다 거래 횟수는 크게 낮고, 수익률 저하가 제한적이면 매매 구조 개선이 유효했다고 볼 수 있습니다.
- `improved_strategy_new`가 `improved_strategy_old`를 이기고, 동시에 `equal_weight_universe`/`benchmark_kospi` 격차를 줄이면 점수식 개선 효과로 해석할 수 있습니다.
- `실제 초기 자본`과 `첫 기록 시점 자산`이 다른 이유는, 본 백테스트가 `d0→d1` 수익률을 첫 행에 기록하기 때문입니다.

## Data model (핵심 테이블)

- `daily_prices(symbol, date, open, high, low, close, volume)`
- `daily_features(symbol, date, ret_1d, ret_5d, momentum_20d, momentum_60d, sma_20_gap, sma_60_gap, range_pct, volatility_20d, volume_z20)`
- `daily_scores(symbol, date, score, rank)`
- `backtest_runs(run_id, created_at, top_n, start_date, end_date, initial_equity, rebalance_frequency, min_holding_days, keep_rank_threshold, scoring_profile, market_filter_enabled, market_filter_ma20_reduce_by, market_filter_ma60_mode, ma20_trigger_count, ma60_trigger_count, reduced_target_count_days, blocked_new_buy_days, cash_mode_days)`
- `backtest_market_filter_events(run_id, date, market_proxy_value, market_proxy_ma20, market_proxy_ma60, below_ma20, below_ma60, original_target_count, adjusted_target_count, ma60_mode, action)`
- `backtest_results(run_id, date, equity, daily_return, position_count)`
- `performance_report_runs(report_id, base_run_id, benchmark_name, benchmark_source, start_date, end_date, ...)`
- `performance_report_summary(report_id, strategy_key, actual_initial_capital, first_recorded_equity, ending_equity, ... )`
- `performance_report_curve(report_id, date, strategy_equity, equal_weight_equity, benchmark_equity)`
- `performance_report_monthly(report_id, month, strategy_return, equal_weight_return, benchmark_return)`
- `paper_positions(symbol, qty, entry_price, entry_date, updated_at)`
- `paper_rebalance_log(as_of_date, executed_at, rebalance_frequency)`
- `paper_orders(order_id, created_at, symbol, side, qty, price, reason)`

## Scoring 철학과 해석 (노이즈 완화 중심)

`run_pipeline.py --scoring-version`으로 점수식을 선택할 수 있습니다.
- `old` (`improved_v1` 별칭): 기존 식(단기 신호 반응이 상대적으로 큼)
- `trend_v2` (`improved_v2` 별칭): 중기 추세 중심 + 노이즈/변동성 강한 억제
- `hybrid_v3` (`improved_v3` 별칭): `old` 기반에 추세 필터를 약하게 섞은 절충형
- `hybrid_v4` (`improved_v4` 별칭): **old 안정성을 최대한 유지**하면서 중기 추세를 아주 약하게 보강한 보수형

### 왜 지금 기준 전략은 old인가?
- robustness 해석 기준에서 `old`가 **stability_score 최상위**로 확인되어, 현 시점 기준 전략으로 유지합니다.
- `hybrid_v3`는 총수익률이 일부 구간에서 높아도, 샤프/최악 MDD에서 `old` 대비 열세가 나타나 변동성 구간 대응에서 보수적 운용 기준을 충족하지 못했습니다.
- 따라서 운영 기준선은 `old`로 고정하고, 신규 개선은 `old`의 구조를 크게 훼손하지 않는 `hybrid_v4` 중심으로 진행합니다.

### 기존 점수식 (`improved_v1`)
```text
score_v1 = 0.20*ret_1d + 0.35*ret_5d + 0.35*momentum_20d + 0.10*volume_z20 - 0.05*range_pct
```
- `ret_1d`는 하루 노이즈(뉴스/수급 급변)에 민감합니다.
- `range_pct`는 단기 변동폭이라 추세 신호보다 이벤트성 흔들림을 반영하기 쉽습니다.



### trend 점수식 (`improved_v2`)
```text
score_v2 =
  0.15*ret_5d
+ 0.35*momentum_20d
+ 0.30*momentum_60d
+ 0.12*sma_20_gap
+ 0.10*sma_60_gap
+ 0.05*volume_z20
- 0.03*range_pct
- 0.04*volatility_20d
```

해석:
- `ret_1d`를 제거하고 중기 추세 시그널 비중을 크게 높인 구조입니다.
- 대신 `volatility_20d`/`range_pct` 페널티가 강해, 변동성이 있는 추세 초입까지 과도하게 깎일 수 있습니다.

### hybrid 점수식 (`improved_v3`)
```text
score_v3 =
  0.10*ret_1d
+ 0.36*ret_5d
+ 0.30*momentum_20d
+ 0.08*momentum_60d
+ 0.06*sma_20_gap
+ 0.10*volume_z20
- 0.035*range_pct
- 0.02*volatility_20d
```

해석:
- **old 기본 골격(단기+중기 혼합)**을 유지합니다.
- `ret_1d`를 완전 제거하지 않고 절반 수준으로 축소해 급격한 노이즈 반응만 줄입니다.
- `momentum_60d`와 `sma_20_gap`은 약한 가중치로만 추가해 추세 확인용으로 사용합니다.
- `volatility_20d`/`range_pct` 페널티는 `trend_v2`보다 완화해 장기 robustness 저하를 줄이는 목적입니다.

### hybrid_v4 점수식 (`improved_v4`)
```text
score_v4 =
  0.16*ret_1d
+ 0.35*ret_5d
+ 0.33*momentum_20d
+ 0.04*momentum_60d
+ 0.03*sma_20_gap
+ 0.10*volume_z20
- 0.02*range_pct
- 0.01*volatility_20d
```

해석:
- `old` 구조를 유지하되, `ret_1d`만 소폭 축소(`0.20 → 0.16`)했습니다.
- `momentum_60d`/`sma_20_gap`은 매우 낮은 비중으로만 추가해 “추세 확인” 역할로 제한했습니다.
- `range_pct`/`volatility_20d` 페널티는 `hybrid_v3`보다 더 완화해 과도한 방어 페널티를 줄였습니다.

### scoring 버전 실행 예시 (동일 후보군/동일 기간 비교: old / hybrid_v3 / hybrid_v4)

아래처럼 **같은 DB/같은 기간**에서 scoring 버전만 바꿔 3회 실행하면 공정 비교가 가능합니다.

```bash
# 1) old scoring run
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 15 \
  --scoring-version old

# 2) hybrid_v3 scoring run (동일 조건)
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 15 \
  --scoring-version hybrid_v3

# 3) hybrid_v4 scoring run (동일 조건)
python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 10 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 15 \
  --scoring-version hybrid_v4

# 4) 리포트 비교
python scripts/generate_performance_report.py \
  --db data/market_pipeline.db \
  --baseline-run-id <baseline_run_id> \
  --improved-run-id <old_run_id> \
  --improved-new-run-id <hybrid_v4_run_id>
```

- Latest scoring: `generate_daily_scores(..., include_history=False)`
- Historical scoring: `generate_daily_scores(..., include_history=True)`

## 새 스코어링 비교 실행 예시

동일 후보군/동일 기간으로 old / hybrid_v3 / hybrid_v4를 비교하려면:

```bash
# baseline (daily)
python scripts/run_pipeline.py --source csv --db data/market_pipeline.db --prices-csv data/sample_daily_prices.csv --top-n 5 --rebalance-frequency daily --min-holding-days 0 --keep-rank-threshold 5 --scoring-version old

# improved_old (weekly + 기존 scoring)
python scripts/run_pipeline.py --source csv --db data/market_pipeline.db --prices-csv data/sample_daily_prices.csv --top-n 5 --rebalance-frequency weekly --min-holding-days 5 --keep-rank-threshold 7 --scoring-version old

# improved_hybrid_v3 (weekly + hybrid_v3 scoring)
python scripts/run_pipeline.py --source csv --db data/market_pipeline.db --prices-csv data/sample_daily_prices.csv --top-n 5 --rebalance-frequency weekly --min-holding-days 5 --keep-rank-threshold 7 --scoring-version hybrid_v3

# improved_hybrid_v4 (weekly + hybrid_v4 scoring)
python scripts/run_pipeline.py --source csv --db data/market_pipeline.db --prices-csv data/sample_daily_prices.csv --top-n 5 --rebalance-frequency weekly --min-holding-days 5 --keep-rank-threshold 7 --scoring-version hybrid_v4

# 성과 비교 리포트
python scripts/generate_performance_report.py \
  --db data/market_pipeline.db \
  --baseline-run-id <baseline_run_id> \
  --improved-run-id <improved_old_run_id> \
  --improved-new-run-id <improved_hybrid_v4_run_id> \
  --benchmark KOSPI
```

## KRX 운영 시 주의사항

1. **휴장일/거래일 차이**
   - 백테스트는 `daily_prices`에 존재하는 거래일 순서를 그대로 사용합니다.
   - 미국장과 거래일이 다르므로, 과거 미국장 데이터 전제를 섞으면 왜곡될 수 있습니다.

2. **룩어헤드 바이어스 방지 가정**
   - `d0`일 종가까지의 데이터로 스코어를 계산하고,
   - 수익률은 `d0 -> d1`(다음 거래일 종가)로 평가합니다.
   - 즉, 같은 날짜의 미래 가격을 참조하지 않도록 기존 가정을 유지합니다.

3. **pykrx 네트워크 의존성**
   - 실데이터 ingest는 네트워크 및 pykrx API 응답 상태에 따라 지연/실패할 수 있습니다.

## Colab에서 실행 (권장 명령)

```bash
!git clone https://github.com/jwlee-collab/krx-stock.git
%cd krx-stock
!python -m pip install -U pip pykrx
!python scripts/run_pipeline.py --source krx --market KOSPI --start-date 2025-01-01 --end-date 2025-12-31 --top-n 5
!python scripts/validate_pipeline.py --db data/market_pipeline.db --top-n 5
```

### Colab 점검 명령 (시장 유니버스 경로)

아래 명령으로 `--market` 자동 수집 경로를 안전하게 점검할 수 있습니다.

```bash
!python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline_kospi.db \
  --market KOSPI \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 5

!python scripts/run_pipeline.py \
  --source krx \
  --db data/market_pipeline_all.db \
  --market ALL \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --top-n 5 \
  --min-close-price 1000 \
  --min-avg-dollar-volume-20d 100000000 \
  --min-avg-volume-20d 10000
```

실패 시 점검 포인트:
- `pip install -U pykrx` 재설치
- Colab 런타임 네트워크 상태 확인
- 날짜 범위(`--start-date`, `--end-date`)가 모두 휴장일/미래일이 아닌지 확인
- 필터가 너무 강하면(`after=0`) 임계값을 완화해서 재실행

## 후보군 민감도 / 종목 편향 진단 리포트

후보군(유니버스) 변경에 따른 전략 민감도와 종목 집중 리스크를 자동으로 진단하려면 아래 스크립트를 사용합니다.

- 스크립트: `scripts/analyze_universe_sensitivity.py`
- 핵심 보장:
  - **스코어링 수식은 변경하지 않고** 기존 `generate_daily_scores`를 재사용
  - **백테스트 핵심 로직은 유지**하고 기존 `run_backtest`를 재사용
  - 진단/리포트 산출물(CSV + SQLite + JSON)에 집중

### 진단 기능

1. Universe 비교
- old/new 후보군 종목 수
- 공통 종목 수
- old-only / new-only 종목 리스트
- overlap ratio

2. Universe 민감도 백테스트 (`--universe-top-n` 기준)
- old universe 전체
- new universe 전체
- overlap universe only
- removed symbols only
- added symbols only

각 후보군별 비교 지표:
- 총 수익률
- MDD
- Sharpe
- 후보군 평균 대비 초과수익
- 거래 횟수
- 평균 보유 종목 수

3. top_n 민감도 비교 (`--top-n-values`, 기본 `3,5,10`)
- top_n 증가 시 수익률 변화량
- top_n 증가 시 MDD 변화량
- 후보군 대비 초과수익 유지 여부

4. 종목별 기여도 분석
- 종목별 보유 일수
- 종목별 선택 횟수
- 종목별 추정 기여수익률
- 상위 기여 10개 / 하위 기여 10개
- 상위 3개 종목 기여 비중

5. Concentration risk 요약
- 성과가 소수 종목에 과도하게 집중되었는지
- top_n=3이 과집중인지
- top_n=5/10이 drawdown 측면에서 상대적으로 안정적인지

### 실행 예시

```bash
python scripts/analyze_universe_sensitivity.py \
  --db data/market_pipeline.db \
  --old-universe-file data/kospi100_manual_old.csv \
  --new-universe-file data/kospi100_manual.csv \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --universe-top-n 3 \
  --top-n-values 3,5,10 \
  --topn-universe-scope new \
  --scoring-version hybrid_v4 \
  --output-dir data/reports/universe_sensitivity
```

### 산출물

- `universe_comparison.csv`
- `universe_sensitivity_metrics.csv`
- `topn_sensitivity_metrics.csv`
- `symbol_contribution_top10.csv`
- `symbol_contribution_bottom10.csv`
- `summary.json`

추가로 SQLite DB에도 아래 테이블로 적재됩니다.
- `universe_sensitivity_reports`
- `universe_sensitivity_metrics`
- `topn_sensitivity_metrics`
- `symbol_contribution_metrics`

### 실전 운영 팁 (후보군 고정/관리)

- 후보군 CSV를 날짜/버전으로 **파일명 고정**해서 보관하세요.
  - 예: `kospi100_manual_2025Q1.csv`, `kospi100_manual_2025Q2.csv`
- 실전/리서치 모두에서 **실행 시 사용한 universe 파일 경로를 로그로 남기고**, 리포트와 함께 아카이브하세요.
- 전략 변경 없이 후보군만 바꾼 A/B 실험을 정기 실행해, 민감도(수익/MDD/초과수익)를 모니터링하세요.
- top_n=3 성과가 높더라도 상위 3종목 기여 비중이 과도하면, top_n=5 또는 10으로 완화한 대안을 병행 검증하세요.

---

## KOSPI 전용 Dynamic Universe 실험 가이드 (2026-04 업데이트)

### Universe 파일 역할 구분 (중요)

- `data/kospi100_manual.csv`: **고정 100개 테스트용(static) 후보군**
- `data/krx_source_universe_500.csv`: **KOSPI+KOSDAQ 혼합 500개 실험용 후보군**
- `data/kospi_source_universe_500.csv`: **KOSPI 전용 dynamic universe 실험용 후보군**

> 주의: `kospi100_manual.csv`와 `krx_source_universe_500.csv`는 유지하고 삭제하지 않습니다.

### KOSPI 전용 source universe 생성/갱신

현재 저장소에는 `data/kospi_source_universe_500.csv`를 포함합니다.  
다만 이 파일은 현재 검증 가능한 KOSPI-only 스냅샷(300개)이며, Colab/로컬에서 아래 스크립트로 **목표 500개**를 재생성해 사용하세요.

```bash
python scripts/build_kospi_universe.py \
  --output data/kospi_source_universe_500.csv \
  --target-size 500 \
  --lookback-days 20 \
  --as-of-date 2025-03-31
```

- 생성 규칙: `pykrx`로 KOSPI 종목 목록을 가져오고, 최근 `lookback-days` 평균 거래대금 기준 상위 종목을 선택
- 출력 컬럼: `symbol,name,market,note`
- `symbol`: 6자리 문자열, `market`: `KOSPI`

### Universe CSV 검증

```bash
python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --require-kospi-only
```

> `validate_universe_csv.py`는 CSV 구조/형식(row 수, symbol 중복/형식, market 값)만 검사합니다.  
> **실제 OHLCV 조회 가능 여부(가격 데이터 존재)는 별도 검증**이 필요합니다.

500개를 강제 검증하려면:

```bash
python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --expected-rows 500 \
  --require-kospi-only
```

CSV 구조 검증 항목:
- row 수
- unique symbol 수
- 중복 여부
- 6자리 코드 여부
- `market` 컬럼 KOSPI-only 여부
- UTF-8 BOM 여부

가격 데이터 검증(실제 OHLCV 조회 가능 여부):

```bash
python scripts/validate_universe_price_data.py \
  --file data/kospi_source_universe_500.csv \
  --start-date 2025-03-03 \
  --end-date 2025-03-31 \
  --min-price-rows 5 \
  --output-valid data/kospi_source_universe_valid_price.csv \
  --output-invalid data/kospi_source_universe_invalid_price.csv
```

- `output-valid`: validation 기간에 `min-price-rows` 이상 OHLCV가 조회되는 symbol
- `output-invalid`: 조회 실패 또는 row 부족 symbol

### KOSPI-only dynamic universe 실행 예시

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/kospi_dynamic_500_to_100_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 5 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old
```

### KOSPI-only robustness 실험 예시 (1/3/6/12개월 동시 평가)

```bash
python scripts/run_robustness_experiments.py \
  --db data/kospi_dynamic_500_to_100_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --output-dir data/reports \
  --period-months 1,3,6,12 \
  --top-n-values 3,5,10 \
  --min-holding-days-values 3,5,10 \
  --keep-rank-offsets 2,4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-filter-modes off \
  --entry-gate-modes off,on \
  --min-entry-score-values 0.0 \
  --entry-gate-rule-set basic_trend \
  --market-scopes KOSPI
```

### 3개월/6개월 수익률 개선 가능성 확인 실험 흐름

1. `--period-months 1,3,6,12`로 전체 기간을 동시에 계산
2. 1개월은 참고 지표로만 보고, 3/6개월 구간에서 조합별 순위 변화 확인
3. `top-n`, `min-holding-days`, `keep-rank-offset` 조합 중 3/6개월 상위 반복 출현 조합 추리기
4. 같은 조합의 12개월/MDD/샤프/후보군 대비 초과수익까지 동시 확인
5. 3/6개월 개선 + 12개월 안정성까지 보이는 조합만 최종 후보로 채택

### 1개월 수익률 해석 원칙 (반드시 준수)

- 1개월 수익률은 노이즈가 크므로 최종 전략 선택 기준으로 과신하지 말 것
- 1개월 성과는 단기 손실 위험, 진입 타이밍 민감도, 전략 반응 속도 확인용으로 사용할 것
- 최종 판단은 3/6/12개월 성과, MDD, 샤프비율, 후보군 평균 대비 초과수익을 함께 볼 것
- 1개월은 좋지만 3/6/12개월이 나쁘면 단기 운이 좋았을 가능성이 크다
- 1개월은 약하지만 3/6/12개월이 안정적이면 스윙/중기 전략으로 볼 수 있다

### Colab 실행 흐름 (KOSPI 전용)

1. 최신 저장소 clone
2. `pykrx` 설치
3. KOSPI 500 후보군 생성 또는 검증
4. KOSPI dynamic universe DB 생성
5. 1/3/6/12개월 robustness 실험 실행
6. 결과 CSV 확인

예시:

```bash
# 1) clone
!git clone <YOUR_REPO_URL>
%cd krx-stock

# 2) install
!pip install pykrx

# 3) build / validate
!python scripts/build_kospi_universe.py --output data/kospi_source_universe_500.csv --target-size 500 --lookback-days 20
!python scripts/validate_universe_csv.py --file data/kospi_source_universe_500.csv --expected-rows 500 --require-kospi-only

# 4) run pipeline
!python scripts/run_pipeline.py \
  --source krx \
  --db data/kospi_dynamic_500_to_100_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 5 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old

# 5) robustness (1/3/6/12m)
!python scripts/run_robustness_experiments.py \
  --db data/kospi_dynamic_500_to_100_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --output-dir data/reports \
  --period-months 1,3,6,12 \
  --top-n-values 3,5,10 \
  --min-holding-days-values 3,5,10 \
  --keep-rank-offsets 2,4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-filter-modes off \
  --entry-gate-modes off,on \
  --min-entry-score-values 0.0 \
  --entry-gate-rule-set basic_trend \
  --market-scopes KOSPI

# 6) outputs
!ls -lh data/reports | tail -n 20
```

## KOSPI-only Dynamic Universe 운영 가이드 (안정화 버전)

### 1) KOSPI source universe 생성: strict vs partial

이번 버전의 `scripts/build_kospi_universe.py`는 아래 순서로 데이터를 수집합니다.

1. `pykrx` KOSPI 수집
2. 실패 시 기존 `data/kospi_source_universe_500.csv` fallback
3. 그래도 실패 시 `FinanceDataReader.StockListing('KOSPI')`
4. 모두 실패하면 친절한 에러 메시지로 종료

> 주의: 가짜/임의 종목코드는 생성하지 않습니다.

#### strict mode (500 미만이면 실패)

```bash
python scripts/build_kospi_universe.py \
  --output data/kospi_source_universe_500.csv \
  --target-size 500 \
  --min-size 500
```

#### partial mode (예: 300개 이상이면 진행)

```bash
python scripts/build_kospi_universe.py \
  --output data/kospi_source_universe_500.csv \
  --target-size 500 \
  --min-size 300 \
  --allow-partial \
  --validate-price-data \
  --validation-start-date 2025-03-03 \
  --validation-end-date 2025-03-31 \
  --min-price-rows 5 \
  --excluded-output data/kospi_source_universe_excluded_no_price.csv
```

검증:

```bash
python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --require-kospi-only
```

`validate_universe_csv.py`는 row 수, unique/duplicate, 6자리 symbol, `market=KOSPI`, UTF-8 BOM 여부를 점검합니다(구조 검증).  
실제 가격 데이터 검증은 `validate_universe_price_data.py` 또는 `build_kospi_universe.py --validate-price-data`를 사용하세요.

### 2) Colab 실행 예시

```bash
!python scripts/build_kospi_universe.py \
  --output data/kospi_source_universe_500.csv \
  --target-size 500 \
  --min-size 300 \
  --allow-partial

!python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --require-kospi-only

!python scripts/run_pipeline.py \
  --source krx \
  --db data/kospi_dynamic_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 5 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old
```

### 3) 파일 용도 구분

- `data/kospi100_manual.csv`: 고정 100개 테스트용
- `data/krx_source_universe_500.csv`: KOSPI+KOSDAQ 혼합 실험용
- `data/kospi_source_universe_500.csv`: KOSPI-only dynamic universe source pool

### 4) KOSPI-only 권장 실험 흐름

1. KOSPI source universe 생성 또는 검증
2. `rolling_liquidity`로 날짜별 거래대금 상위 100개 구성
3. `old` scoring 유지
4. `weekly` 리밸런싱
5. `top_n=3,5,10` 비교
6. 1/3/6/12개월 성과 비교

## Risk Cut (MDD 완화 목적)

### 핵심 원칙

- stop loss는 수익률을 항상 높이기 위한 기능이 아니라 **MDD를 줄이기 위한 기능**입니다.
- stop loss가 너무 빡세면 좋은 종목도 노이즈 구간에서 잘려나갈 수 있습니다.
- 1개월 성과는 노이즈가 커서 과신하지 말고, 3/6/12개월 + MDD + Sharpe + 후보군 대비 초과수익을 함께 봐야 합니다.
- 총수익률이 약간 낮아져도 MDD가 크게 줄고 Sharpe가 개선되면 실전성 측면에서 의미가 큽니다.

### 백테스트 옵션

- 개별 종목 손절(기본 OFF)
  - `--enable-position-stop-loss`
  - `--position-stop-loss-pct` (기본 0.08)
- 트레일링 스탑(기본 OFF, 진단 포함)
  - `--enable-trailing-stop`
  - `--trailing-stop-pct` (기본 0.10)
- 포트폴리오 DD 컷(기본 OFF)
  - `--enable-portfolio-dd-cut`
  - `--portfolio-dd-cut-pct` (기본 0.10)
  - `--portfolio-dd-cooldown-days` (기본 20)

**우선순위 규칙:** position stop loss는 리스크 관리 규칙이므로 `min_holding_days`보다 우선합니다.

### KOSPI-only + risk cut 실행 예시

```bash
python scripts/run_pipeline.py \
  --source krx \
  --db data/kospi_dynamic_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --start-date 2022-01-01 \
  --end-date 2025-03-31 \
  --top-n 5 \
  --rebalance-frequency weekly \
  --min-holding-days 5 \
  --keep-rank-threshold 7 \
  --scoring-version old \
  --enable-position-stop-loss \
  --position-stop-loss-pct 0.08 \
  --enable-portfolio-dd-cut \
  --portfolio-dd-cut-pct 0.10 \
  --portfolio-dd-cooldown-days 20
```

출력 JSON의 `risk_cut.diagnostics`에서 stop/dd-cut 카운터를 확인할 수 있습니다.

### Robustness에서 risk cut ON/OFF 비교

```bash
python scripts/run_robustness_experiments.py \
  --db data/kospi_dynamic_3y.db \
  --universe-file data/kospi_source_universe_500.csv \
  --universe-mode rolling_liquidity \
  --universe-size 100 \
  --universe-lookback-days 20 \
  --output-dir data/reports \
  --period-months 1,3,6,12 \
  --top-n-values 3,5,10 \
  --min-holding-days-values 3,5,10 \
  --keep-rank-offsets 2,4 \
  --scoring-versions old \
  --rebalance-frequency weekly \
  --market-filter-modes off \
  --entry-gate-modes off,on \
  --market-scopes KOSPI \
  --position-stop-loss-modes off,on \
  --position-stop-loss-pct-values 0.08,0.10
```

추가 확장(선택):

```bash
--portfolio-dd-cut-modes off,on \
--portfolio-dd-cut-pct-values 0.10,0.15 \
--portfolio-dd-cooldown-days-values 20
```

## Trade/Episode Attribution 진단 스크립트

```bash
python scripts/analyze_trade_attribution.py \
  --db data/market_pipeline.db \
  --run-id <BACKTEST_RUN_ID> \
  --output-dir outputs/trade_attribution
```

생성 파일:
- `trade_episode_attribution_<run_id>.csv`
- `trade_feature_summary_<run_id>.csv`
- `stop_loss_after_return_<run_id>.csv`
- `trade_attribution_report_<run_id>.md`

### KOSPI sector map 생성

`analyze_sector_attribution.py`에서 `UNKNOWN` 비중이 100%로 나오는 경우, 먼저 sector map CSV를 생성하세요.

```bash
python scripts/build_kospi_sector_map.py \
  --universe-file data/kospi_valid_universe_495.csv \
  --output data/kospi_sector_map.csv \
  --source auto \
  --overwrite
```

- `--source auto` 우선순위: `krx-file(--input-sector-file 지정 시) -> kind -> pykrx -> fdr -> manual(UNKNOWN)`.
- `--as-of-date`는 `YYYY-MM-DD` 또는 `YYYYMMDD`를 지원합니다. 미지정 시 최근 평일 기준으로 조회하고, 실패하면 `universe-file`의 `validation_end_date` 최대값 기준으로 재시도합니다.
- `--source kind`는 KIND 상장법인목록 URL(기본: `https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13`)을 `pandas.read_html`로 읽어 `업종` 컬럼 기반 매핑을 수행합니다.
- `--source krx-file`는 사용자가 직접 내려받은 KRX/KIND 파일(`.csv/.xls/.xlsx`)을 읽어 컬럼 자동 인식으로 매핑합니다.
  - CSV 인코딩은 `--encoding auto` 기본값에서 `cp949 -> utf-8-sig -> utf-8` 순서 fallback을 시도합니다.
  - 매핑률이 80% 미만이면 warning, 30% 미만이면 `--allow-partial` 없을 때 실패합니다.
- pykrx는 KOSPI 업종지수 구성종목(`get_index_ticker_list / get_index_portfolio_deposit_file`)을 사용해 sector를 보강합니다.
- 외부 조회 실패 시에도 각 종목은 `fallback-sector`(기본: `UNKNOWN`)으로 채워져 출력됩니다.
- 출력 CSV(`data/kospi_sector_map.csv`)에는 `broad_sector` 컬럼이 추가되며, KIND 세부 업종을 실험용 대분류(예: `전기전자/IT하드웨어`, `금융`, `화학/소재` 등)로 매핑합니다.
- 실행 마지막 줄에 `KOSPI_SECTOR_MAP_JSON={...}` 요약이 출력됩니다.
  - 요약에는 `broad_sector_count`, `unknown_broad_sector_rows`, `sample_mapped_rows[].broad_sector`가 포함됩니다.
- `symbol` 컬럼은 모든 CSV에서 6자리 문자열(`zfill(6)`)로 정규화되고, writer는 전체 컬럼 quoting을 사용합니다. pandas에서 재로드할 때는 `dtype={"symbol": str}` 권장.

자동 KIND 방식 예시:

```bash
python scripts/build_kospi_sector_map.py --source kind --overwrite --allow-partial
```

수동 KRX/KIND 파일 방식 예시:

```bash
python scripts/build_kospi_sector_map.py --source krx-file --input-sector-file data/raw/krx_sector.csv --overwrite --allow-partial
```

sector attribution 연결:

```bash
python scripts/analyze_sector_attribution.py --sector-file data/kospi_sector_map.csv --group-by both
```

- `--group-by`: `sector|broad_sector|both` (기본 `both`)
  - `sector`: 기존 상세 업종 기준 파일 생성
  - `broad_sector`: 대분류 업종 기준 파일 생성
  - `both`: 상세/대분류 모두 생성
- 추가 산출물(대분류):
  - `broad_sector_attribution_summary.csv`
  - `broad_sector_symbol_attribution.csv`
  - `daily_broad_sector_exposure.csv`
  - `broad_sector_comparison.csv`
  - `broad_sector_2024_stress_summary.csv`

## Broad Sector Guardrail 실험 (Experiment 전용)

> 이 실험은 **production rule이 아니라 실험 스크립트**입니다. 기존 baseline/prod 로직은 변경하지 않습니다.

Quick 실행(기본: quarterly + 12개월):

```bash
python scripts/run_broad_sector_guardrail_experiment.py \
  --db data/kospi_495_rolling_3y.db \
  --universe-file data/kospi_valid_universe_495.csv \
  --sector-file data/kospi_sector_map.csv \
  --mode quick \
  --outdir outputs/broad_sector_guardrail/quick
```

strict final report baseline parity 검증까지 같이 수행하려면:

```bash
python scripts/run_broad_sector_guardrail_experiment.py \
  --db data/kospi_495_rolling_3y.db \
  --universe-file data/kospi_valid_universe_495.csv \
  --sector-file data/kospi_sector_map.csv \
  --mode quick \
  --reference-final-report-dir reports/final_candidate_report/latest \
  --outdir outputs/broad_sector_guardrail/quick
```

Full 실행(monthly,quarterly + 1,3,6,12개월):

```bash
python scripts/run_broad_sector_guardrail_experiment.py \
  --db data/kospi_495_rolling_3y.db \
  --universe-file data/kospi_valid_universe_495.csv \
  --sector-file data/kospi_sector_map.csv \
  --mode full \
  --outdir outputs/broad_sector_guardrail/full
```

주요 산출물:

- `manifest.json`
- `guardrail_candidate_summary.csv`
- `guardrail_window_results.csv`
- `guardrail_worst_windows.csv`
- `guardrail_monthly_returns.csv`
- `guardrail_full_period_results.csv`
- `guardrail_daily_broad_sector_exposure.csv`
- `guardrail_sector_cap_compliance.csv`
- `guardrail_report.md`
- `equity_curve.csv`, `drawdown_curve.csv`
- `equity_curve.png`, `drawdown_curve.png`, `broad_sector_exposure_heatmap.png`

추가 검증/추적:

- `manifest.json`에 `score_signatures`(candidate별 scoring_profile, row_count, sample date top10 심볼 등)가 저장됩니다.
- `manifest.json`에 `no_cap_baseline_parity_check`가 저장됩니다.
  - `--reference-final-report-dir` 지정 시 strict final report의 `baseline_old`와 full-period/window metric parity를 검사합니다.
  - mismatch 시 스크립트는 실패하며 guardrail 결과를 invalid로 처리합니다.
- rolling universe는 `daily_universe` 기존 데이터가 있으면 재사용하고, `--rebuild-rolling-universe` 지정 시에만 재생성합니다.

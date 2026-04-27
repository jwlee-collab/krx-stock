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
- `pipeline/validator.py` — 전체 파이프라인 검증
- `scripts/generate_sample_prices.py` — KRX 6자리 코드 기반 샘플 데이터 생성
- `scripts/run_pipeline.py` — 전체 파이프라인 실행
- `scripts/validate_pipeline.py` — 전체 파이프라인 검증 실행

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

500개를 강제 검증하려면:

```bash
python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --expected-rows 500 \
  --require-kospi-only
```

검증 항목:
- row 수
- unique symbol 수
- 중복 여부
- 6자리 코드 여부
- `market` 컬럼 KOSPI-only 여부
- UTF-8 BOM 여부

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
  --allow-partial
```

검증:

```bash
python scripts/validate_universe_csv.py \
  --file data/kospi_source_universe_500.csv \
  --require-kospi-only
```

`validate_universe_csv.py`는 row 수, unique/duplicate, 6자리 symbol, `market=KOSPI`, UTF-8 BOM 여부를 함께 점검합니다.

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


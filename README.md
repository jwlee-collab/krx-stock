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

`trend_v2`를 보조 실험군으로 함께 포함하려면:

```bash
python scripts/run_robustness_experiments.py --db data/market_pipeline.db --include-trend-v2
```

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
- `backtest_runs(run_id, created_at, top_n, start_date, end_date, initial_equity, rebalance_frequency, min_holding_days, keep_rank_threshold, scoring_profile)`
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

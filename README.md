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
- `pipeline/backtest.py` — 일별 리밸런싱 백테스트
- `pipeline/paper_trading.py` — 일회성 모의매매 리밸런싱 사이클
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
  --top-n 3
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
  --top-n 3
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


### 5) 백테스트 성과 비교 리포트 생성

```bash
python scripts/generate_performance_report.py \
  --db data/market_pipeline.db \
  --benchmark KOSPI \
  --output-dir data/reports
```

옵션:
- `--run-id`: 특정 `backtest_runs.run_id`를 지정해서 리포트 생성 (미지정 시 최신 run 자동 선택)
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

비교 기준:
1. `strategy`: 기존 점수 기반 top-N 백테스트 (`backtest_results`) 결과 재사용
2. `equal_weight_universe`: 같은 날짜의 `daily_scores` 후보군 전체를 동일비중으로 보유
3. `benchmark_kospi`: pykrx 인덱스(`KOSPI=1001`, `KOSPI200=1028`) 조회를 우선 사용
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
- `strategy`가 `equal_weight_universe` 대비 초과성과를 내면, 스코어 랭킹 자체의 선택력이 있다고 해석할 수 있습니다.
- `strategy`가 `benchmark_kospi` 대비 우수하더라도 `MDD/변동성`이 과도하면 위험-보상 균형을 함께 확인해야 합니다.
- `실제 초기 자본`과 `첫 기록 시점 자산`이 다른 이유는, 본 백테스트가 `d0→d1` 수익률을 첫 행에 기록하기 때문입니다.

## Data model (핵심 테이블)

- `daily_prices(symbol, date, open, high, low, close, volume)`
- `daily_features(symbol, date, ret_1d, ret_5d, momentum_20d, range_pct, volume_z20)`
- `daily_scores(symbol, date, score, rank)`
- `backtest_runs(run_id, created_at, top_n, start_date, end_date, initial_equity)`
- `backtest_results(run_id, date, equity, daily_return, position_count)`
- `performance_report_runs(report_id, base_run_id, benchmark_name, benchmark_source, start_date, end_date, ...)`
- `performance_report_summary(report_id, strategy_key, actual_initial_capital, first_recorded_equity, ending_equity, ... )`
- `performance_report_curve(report_id, date, strategy_equity, equal_weight_equity, benchmark_equity)`
- `performance_report_monthly(report_id, month, strategy_return, equal_weight_return, benchmark_return)`
- `paper_positions(symbol, qty, entry_price, updated_at)`
- `paper_orders(order_id, created_at, symbol, side, qty, price, reason)`

## Unified scoring behavior

동일 스코어 공식을 최신/히스토리컬 모두에 사용합니다.

```text
score = 0.20*ret_1d + 0.35*ret_5d + 0.35*momentum_20d + 0.10*volume_z20 - 0.05*range_pct
```

- Latest scoring: `generate_daily_scores(..., include_history=False)`
- Historical scoring: `generate_daily_scores(..., include_history=True)`

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

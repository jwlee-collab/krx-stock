"""SQLite-native daily market data pipeline.

Modules:
- ingest: load daily prices into SQLite
- features: compute daily features from OHLCV
- scoring: generate daily model scores/ranks (latest + history)
- backtest: run SQLite-based historical backtests
- paper_trading: run SQLite-based paper trading cycle
- validator: end-to-end pipeline validator
"""

from .db import get_connection, init_db
from .ingest import ingest_daily_prices_csv
from .features import generate_daily_features
from .scoring import (
    DEFAULT_SCORING_PROFILE,
    SUPPORTED_SCORING_PROFILES,
    generate_daily_scores,
    score_formula_v1,
    score_formula_v2,
)
from .backtest import run_backtest
from .paper_trading import run_paper_trading_cycle
from .validator import validate_pipeline

__all__ = [
    "get_connection",
    "init_db",
    "ingest_daily_prices_csv",
    "generate_daily_features",
    "generate_daily_scores",
    "DEFAULT_SCORING_PROFILE",
    "SUPPORTED_SCORING_PROFILES",
    "score_formula_v1",
    "score_formula_v2",
    "run_backtest",
    "run_paper_trading_cycle",
    "validate_pipeline",
]

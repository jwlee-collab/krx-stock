from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, Union

Cell = Union[str, int, float, bool, None]
Record = dict[str, Cell]
ShadowLabel = Literal[
    "IMMEDIATE_CANDIDATE",
    "DELAY_REVIEW",
    "COOLDOWN_RECLAIM_WATCH",
    "HIGH_RISK_OVERHEAT",
    "LOWER_PRIORITY",
    "PENDING_DATA",
]

DEFAULT_DB: Final = "~/krx-stock-persist/data/kospi_495_rolling_3y.db"
DEFAULT_REPORTS_DIR: Final = "~/krx-stock-persist/reports/paper_trading"
DEFAULT_OPS_LEDGER: Final = "~/krx-stock-persist/reports/paper_trading/krx_daily_ops_ledger.csv"
DEFAULT_OUT_DIR: Final = "~/krx-stock-persist/reports/research/exp81"
DEFAULT_EXP80A_FORWARD: Final = (
    "~/krx-stock-persist/reports/research/exp80a_pool100/"
    "exp80a_candidate_forward_outcomes_20260605_015825.csv"
)
DEFAULT_EXP80A_SNAPSHOT: Final = (
    "~/krx-stock-persist/reports/research/exp80a_pool100/"
    "exp80a_candidate_snapshot_20260605_015825.csv"
)
DEFAULT_EXP80C_ASSIGNMENTS: Final = (
    "~/krx-stock-persist/reports/research/exp80c_pool100/"
    "exp80c_candidate_assignments_20260605_105915.csv"
)
DEFAULT_EXP80D_ENTRY_PATHS: Final = (
    "~/krx-stock-persist/reports/research/exp80d/"
    "exp80d_candidate_entry_paths_20260605_124812.csv"
)
DEFAULT_EXP80D_RULE_SUMMARY: Final = (
    "~/krx-stock-persist/reports/research/exp80d/"
    "exp80d_rule_timing_summary_20260605_124812.csv"
)
WATCH_SYMBOLS: Final[tuple[tuple[str, str], ...]] = (
    ("032830", "삼성생명"),
    ("066570", "LG전자"),
    ("000810", "삼성화재"),
    ("028260", "삼성물산"),
    ("064400", "LG CNS"),
    ("454910", "두산로보틱스"),
    ("011070", "LG이노텍"),
    ("000660", "SK하이닉스"),
    ("005930", "삼성전자"),
)


@dataclass(frozen=True)
class Config:
    db: Path
    reports_dir: Path
    ops_ledger: Path
    exp80a_forward: Path
    exp80a_snapshot: Path
    exp80c_assignments: Path
    exp80d_entry_paths: Path
    exp80d_rule_summary: Path
    out_dir: Path
    as_of_date: str | None
    candidate_n: int
    dry_run: bool


@dataclass(frozen=True)
class OutputPaths:
    dashboard_csv: Path
    dashboard_latest_csv: Path
    dashboard_md: Path
    dashboard_latest_md: Path
    metadata_json: Path
    metadata_latest_json: Path

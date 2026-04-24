from __future__ import annotations

import sqlite3
from pathlib import Path


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column in columns:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_prices (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (symbol, date)
        );

        CREATE TABLE IF NOT EXISTS daily_features (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            ret_1d REAL,
            ret_5d REAL,
            momentum_20d REAL,
            range_pct REAL,
            volume_z20 REAL,
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol, date) REFERENCES daily_prices(symbol, date)
        );

        CREATE TABLE IF NOT EXISTS daily_scores (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            score REAL NOT NULL,
            rank INTEGER NOT NULL,
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol, date) REFERENCES daily_prices(symbol, date)
        );

        CREATE TABLE IF NOT EXISTS backtest_runs (
            run_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            start_date TEXT,
            end_date TEXT,
            initial_equity REAL,
            rebalance_frequency TEXT NOT NULL DEFAULT 'daily',
            min_holding_days INTEGER NOT NULL DEFAULT 0,
            keep_rank_threshold INTEGER
        );

        CREATE TABLE IF NOT EXISTS backtest_results (
            run_id TEXT NOT NULL,
            date TEXT NOT NULL,
            equity REAL NOT NULL,
            daily_return REAL NOT NULL,
            position_count INTEGER NOT NULL,
            PRIMARY KEY (run_id, date),
            FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS performance_report_runs (
            report_id TEXT PRIMARY KEY,
            base_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            benchmark_name TEXT NOT NULL,
            benchmark_source TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            notes TEXT,
            FOREIGN KEY (base_run_id) REFERENCES backtest_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS performance_report_summary (
            report_id TEXT NOT NULL,
            strategy_key TEXT NOT NULL,
            strategy_label TEXT NOT NULL,
            actual_initial_capital REAL NOT NULL,
            first_recorded_equity REAL NOT NULL,
            ending_equity REAL NOT NULL,
            total_return REAL NOT NULL,
            annualized_return REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            volatility REAL NOT NULL,
            sharpe REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            avg_holdings REAL NOT NULL,
            PRIMARY KEY (report_id, strategy_key),
            FOREIGN KEY (report_id) REFERENCES performance_report_runs(report_id)
        );

        CREATE TABLE IF NOT EXISTS performance_report_curve (
            report_id TEXT NOT NULL,
            date TEXT NOT NULL,
            strategy_equity REAL NOT NULL,
            equal_weight_equity REAL NOT NULL,
            benchmark_equity REAL NOT NULL,
            PRIMARY KEY (report_id, date),
            FOREIGN KEY (report_id) REFERENCES performance_report_runs(report_id)
        );

        CREATE TABLE IF NOT EXISTS performance_report_monthly (
            report_id TEXT NOT NULL,
            month TEXT NOT NULL,
            strategy_return REAL NOT NULL,
            equal_weight_return REAL NOT NULL,
            benchmark_return REAL NOT NULL,
            PRIMARY KEY (report_id, month),
            FOREIGN KEY (report_id) REFERENCES performance_report_runs(report_id)
        );

        CREATE TABLE IF NOT EXISTS paper_positions (
            symbol TEXT PRIMARY KEY,
            qty REAL NOT NULL,
            entry_price REAL NOT NULL,
            entry_date TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty REAL NOT NULL,
            price REAL NOT NULL,
            reason TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_rebalance_log (
            as_of_date TEXT PRIMARY KEY,
            executed_at TEXT NOT NULL,
            rebalance_frequency TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(date);
        CREATE INDEX IF NOT EXISTS idx_features_date ON daily_features(date);
        CREATE INDEX IF NOT EXISTS idx_scores_date_rank ON daily_scores(date, rank);
        """
    )

    _ensure_column(conn, "backtest_runs", "initial_equity", "initial_equity REAL")
    _ensure_column(conn, "backtest_runs", "rebalance_frequency", "rebalance_frequency TEXT NOT NULL DEFAULT 'daily'")
    _ensure_column(conn, "backtest_runs", "min_holding_days", "min_holding_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "keep_rank_threshold", "keep_rank_threshold INTEGER")
    _ensure_column(conn, "paper_positions", "entry_date", "entry_date TEXT")

    conn.commit()

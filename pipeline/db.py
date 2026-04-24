from __future__ import annotations

import sqlite3
from pathlib import Path


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


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
            end_date TEXT
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

        CREATE TABLE IF NOT EXISTS paper_positions (
            symbol TEXT PRIMARY KEY,
            qty REAL NOT NULL,
            entry_price REAL NOT NULL,
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

        CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(date);
        CREATE INDEX IF NOT EXISTS idx_features_date ON daily_features(date);
        CREATE INDEX IF NOT EXISTS idx_scores_date_rank ON daily_scores(date, rank);
        """
    )
    conn.commit()

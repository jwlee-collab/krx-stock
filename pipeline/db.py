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
            momentum_60d REAL,
            sma_20_gap REAL,
            sma_60_gap REAL,
            range_pct REAL,
            volatility_20d REAL,
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

        CREATE TABLE IF NOT EXISTS daily_universe (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            universe_rank INTEGER NOT NULL,
            avg_dollar_volume REAL NOT NULL,
            universe_mode TEXT NOT NULL,
            universe_size INTEGER NOT NULL,
            lookback_days INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (date, symbol, universe_mode, universe_size, lookback_days),
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
            keep_rank_threshold INTEGER,
            scoring_profile TEXT NOT NULL DEFAULT 'improved_v1',
            market_filter_enabled INTEGER NOT NULL DEFAULT 0,
            market_filter_ma20_reduce_by INTEGER NOT NULL DEFAULT 1,
            market_filter_ma60_mode TEXT NOT NULL DEFAULT 'block_new_buys',
            ma20_trigger_count INTEGER NOT NULL DEFAULT 0,
            ma60_trigger_count INTEGER NOT NULL DEFAULT 0,
            reduced_target_count_days INTEGER NOT NULL DEFAULT 0,
            blocked_new_buy_days INTEGER NOT NULL DEFAULT 0,
            cash_mode_days INTEGER NOT NULL DEFAULT 0,
            entry_gate_enabled INTEGER NOT NULL DEFAULT 0,
            min_entry_score REAL NOT NULL DEFAULT 0.0,
            require_positive_momentum20 INTEGER NOT NULL DEFAULT 0,
            require_positive_momentum60 INTEGER NOT NULL DEFAULT 0,
            require_above_sma20 INTEGER NOT NULL DEFAULT 0,
            require_above_sma60 INTEGER NOT NULL DEFAULT 0,
            entry_gate_rejected_count INTEGER NOT NULL DEFAULT 0,
            entry_gate_cash_days INTEGER NOT NULL DEFAULT 0,
            average_actual_position_count REAL NOT NULL DEFAULT 0.0,
            min_actual_position_count INTEGER NOT NULL DEFAULT 0,
            max_actual_position_count INTEGER NOT NULL DEFAULT 0,
            enable_position_stop_loss INTEGER NOT NULL DEFAULT 0,
            position_stop_loss_pct REAL NOT NULL DEFAULT 0.08,
            enable_trailing_stop INTEGER NOT NULL DEFAULT 0,
            trailing_stop_pct REAL NOT NULL DEFAULT 0.10,
            enable_portfolio_dd_cut INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_pct REAL NOT NULL DEFAULT 0.10,
            portfolio_dd_cooldown_days INTEGER NOT NULL DEFAULT 20,
            position_stop_loss_count INTEGER NOT NULL DEFAULT 0,
            trailing_stop_count INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_count INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cooldown_days_count INTEGER NOT NULL DEFAULT 0,
            risk_cut_cash_days INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS backtest_market_filter_events (
            run_id TEXT NOT NULL,
            date TEXT NOT NULL,
            market_proxy_value REAL,
            market_proxy_ma20 REAL,
            market_proxy_ma60 REAL,
            below_ma20 INTEGER NOT NULL,
            below_ma60 INTEGER NOT NULL,
            original_target_count INTEGER NOT NULL,
            adjusted_target_count INTEGER NOT NULL,
            ma60_mode TEXT NOT NULL,
            action TEXT NOT NULL,
            PRIMARY KEY (run_id, date),
            FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
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

        CREATE TABLE IF NOT EXISTS backtest_holdings (
            run_id TEXT NOT NULL,
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            weight REAL NOT NULL,
            entry_date TEXT,
            entry_price REAL,
            close REAL,
            unrealized_return REAL,
            rank INTEGER,
            score REAL,
            PRIMARY KEY (run_id, date, symbol),
            FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS backtest_risk_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            date TEXT NOT NULL,
            symbol TEXT,
            event_type TEXT NOT NULL,
            trigger_price REAL,
            reference_price REAL,
            return_pct REAL,
            action TEXT NOT NULL,
            created_at TEXT NOT NULL,
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


        CREATE TABLE IF NOT EXISTS robustness_experiment_batches (
            batch_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            db_path TEXT NOT NULL,
            end_date TEXT NOT NULL,
            rebalance_frequency TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS robustness_experiment_results (
            experiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            period_months INTEGER NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            keep_rank_offset INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            rebalance_frequency TEXT NOT NULL,
            universe_mode TEXT NOT NULL DEFAULT 'static',
            universe_size INTEGER,
            universe_lookback_days INTEGER,
            market_filter_enabled INTEGER NOT NULL DEFAULT 0,
            market_filter_ma20_reduce_by INTEGER NOT NULL DEFAULT 1,
            market_filter_ma60_mode TEXT NOT NULL DEFAULT 'block_new_buys',
            ma20_trigger_count INTEGER NOT NULL DEFAULT 0,
            ma60_trigger_count INTEGER NOT NULL DEFAULT 0,
            reduced_target_count_days INTEGER NOT NULL DEFAULT 0,
            blocked_new_buy_days INTEGER NOT NULL DEFAULT 0,
            cash_mode_days INTEGER NOT NULL DEFAULT 0,
            entry_gate_enabled INTEGER NOT NULL DEFAULT 0,
            min_entry_score REAL NOT NULL DEFAULT 0.0,
            require_positive_momentum20 INTEGER NOT NULL DEFAULT 0,
            require_positive_momentum60 INTEGER NOT NULL DEFAULT 0,
            require_above_sma20 INTEGER NOT NULL DEFAULT 0,
            require_above_sma60 INTEGER NOT NULL DEFAULT 0,
            entry_gate_rejected_count INTEGER NOT NULL DEFAULT 0,
            entry_gate_cash_days INTEGER NOT NULL DEFAULT 0,
            average_actual_position_count REAL NOT NULL DEFAULT 0.0,
            min_actual_position_count INTEGER NOT NULL DEFAULT 0,
            max_actual_position_count INTEGER NOT NULL DEFAULT 0,
            enable_position_stop_loss INTEGER NOT NULL DEFAULT 0,
            position_stop_loss_pct REAL NOT NULL DEFAULT 0.08,
            enable_trailing_stop INTEGER NOT NULL DEFAULT 0,
            trailing_stop_pct REAL NOT NULL DEFAULT 0.10,
            enable_portfolio_dd_cut INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_pct REAL NOT NULL DEFAULT 0.10,
            portfolio_dd_cooldown_days INTEGER NOT NULL DEFAULT 20,
            position_stop_loss_count INTEGER NOT NULL DEFAULT 0,
            trailing_stop_count INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_count INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cooldown_days_count INTEGER NOT NULL DEFAULT 0,
            risk_cut_cash_days INTEGER NOT NULL DEFAULT 0,
            market_scope TEXT NOT NULL DEFAULT 'ALL',
            source_symbol_count INTEGER NOT NULL DEFAULT 0,
            average_daily_universe_count REAL NOT NULL DEFAULT 0.0,
            selected_kospi_count INTEGER NOT NULL DEFAULT 0,
            selected_kosdaq_count INTEGER NOT NULL DEFAULT 0,
            kospi_contribution_return REAL NOT NULL DEFAULT 0.0,
            kosdaq_contribution_return REAL NOT NULL DEFAULT 0.0,
            total_return REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            sharpe REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            candidate_avg_return REAL NOT NULL,
            excess_return_vs_universe REAL NOT NULL,
            robustness_score REAL NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES robustness_experiment_batches(batch_id),
            FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS robustness_experiment_stability (
            stability_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            stability_group_key TEXT NOT NULL,
            num_periods INTEGER NOT NULL,
            mean_total_return REAL NOT NULL,
            std_total_return REAL NOT NULL,
            mean_sharpe REAL NOT NULL,
            worst_mdd REAL NOT NULL,
            mean_excess_return_vs_universe REAL NOT NULL,
            mean_trade_count REAL NOT NULL,
            stability_score REAL NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES robustness_experiment_batches(batch_id)
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
        CREATE INDEX IF NOT EXISTS idx_daily_universe_date_rank ON daily_universe(date, universe_mode, universe_rank);
        CREATE INDEX IF NOT EXISTS idx_backtest_risk_events_run_date ON backtest_risk_events(run_id, date);
        CREATE INDEX IF NOT EXISTS idx_backtest_holdings_run_date ON backtest_holdings(run_id, date);
        CREATE INDEX IF NOT EXISTS idx_robustness_results_batch_score ON robustness_experiment_results(batch_id, robustness_score DESC);
        CREATE INDEX IF NOT EXISTS idx_robustness_stability_batch_score ON robustness_experiment_stability(batch_id, stability_score DESC);
        """
    )

    _ensure_column(conn, "backtest_runs", "initial_equity", "initial_equity REAL")
    _ensure_column(conn, "backtest_runs", "rebalance_frequency", "rebalance_frequency TEXT NOT NULL DEFAULT 'daily'")
    _ensure_column(conn, "backtest_runs", "min_holding_days", "min_holding_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "keep_rank_threshold", "keep_rank_threshold INTEGER")
    _ensure_column(conn, "backtest_runs", "scoring_profile", "scoring_profile TEXT NOT NULL DEFAULT 'improved_v1'")
    _ensure_column(conn, "backtest_runs", "market_filter_enabled", "market_filter_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "market_filter_ma20_reduce_by", "market_filter_ma20_reduce_by INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "backtest_runs", "market_filter_ma60_mode", "market_filter_ma60_mode TEXT NOT NULL DEFAULT 'block_new_buys'")
    _ensure_column(conn, "backtest_runs", "ma20_trigger_count", "ma20_trigger_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "ma60_trigger_count", "ma60_trigger_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "reduced_target_count_days", "reduced_target_count_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "blocked_new_buy_days", "blocked_new_buy_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "cash_mode_days", "cash_mode_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "entry_gate_enabled", "entry_gate_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "min_entry_score", "min_entry_score REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "backtest_runs", "require_positive_momentum20", "require_positive_momentum20 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "require_positive_momentum60", "require_positive_momentum60 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "require_above_sma20", "require_above_sma20 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "require_above_sma60", "require_above_sma60 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "entry_gate_rejected_count", "entry_gate_rejected_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "entry_gate_cash_days", "entry_gate_cash_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "average_actual_position_count", "average_actual_position_count REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "backtest_runs", "min_actual_position_count", "min_actual_position_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "max_actual_position_count", "max_actual_position_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "enable_position_stop_loss", "enable_position_stop_loss INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "position_stop_loss_pct", "position_stop_loss_pct REAL NOT NULL DEFAULT 0.08")
    _ensure_column(conn, "backtest_runs", "enable_trailing_stop", "enable_trailing_stop INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "trailing_stop_pct", "trailing_stop_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "backtest_runs", "enable_portfolio_dd_cut", "enable_portfolio_dd_cut INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "portfolio_dd_cut_pct", "portfolio_dd_cut_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "backtest_runs", "portfolio_dd_cooldown_days", "portfolio_dd_cooldown_days INTEGER NOT NULL DEFAULT 20")
    _ensure_column(conn, "backtest_runs", "position_stop_loss_count", "position_stop_loss_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "trailing_stop_count", "trailing_stop_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "portfolio_dd_cut_count", "portfolio_dd_cut_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "portfolio_dd_cooldown_days_count", "portfolio_dd_cooldown_days_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "backtest_runs", "risk_cut_cash_days", "risk_cut_cash_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "paper_positions", "entry_date", "entry_date TEXT")
    _ensure_column(conn, "daily_features", "momentum_60d", "momentum_60d REAL")
    _ensure_column(conn, "daily_features", "sma_20_gap", "sma_20_gap REAL")
    _ensure_column(conn, "daily_features", "sma_60_gap", "sma_60_gap REAL")
    _ensure_column(conn, "daily_features", "volatility_20d", "volatility_20d REAL")
    _ensure_column(conn, "backtest_holdings", "entry_date", "entry_date TEXT")
    _ensure_column(conn, "backtest_holdings", "entry_price", "entry_price REAL")
    _ensure_column(conn, "backtest_holdings", "close", "close REAL")
    _ensure_column(conn, "backtest_holdings", "unrealized_return", "unrealized_return REAL")
    _ensure_column(conn, "backtest_holdings", "rank", "rank INTEGER")
    _ensure_column(conn, "backtest_holdings", "score", "score REAL")

    _ensure_column(conn, "robustness_experiment_results", "enable_position_stop_loss", "enable_position_stop_loss INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "position_stop_loss_pct", "position_stop_loss_pct REAL NOT NULL DEFAULT 0.08")
    _ensure_column(conn, "robustness_experiment_results", "enable_trailing_stop", "enable_trailing_stop INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "trailing_stop_pct", "trailing_stop_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "robustness_experiment_results", "enable_portfolio_dd_cut", "enable_portfolio_dd_cut INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "portfolio_dd_cut_pct", "portfolio_dd_cut_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "robustness_experiment_results", "portfolio_dd_cooldown_days", "portfolio_dd_cooldown_days INTEGER NOT NULL DEFAULT 20")
    _ensure_column(conn, "robustness_experiment_results", "position_stop_loss_count", "position_stop_loss_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "trailing_stop_count", "trailing_stop_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "portfolio_dd_cut_count", "portfolio_dd_cut_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "portfolio_dd_cooldown_days_count", "portfolio_dd_cooldown_days_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "risk_cut_cash_days", "risk_cut_cash_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "market_filter_enabled", "market_filter_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "universe_mode", "universe_mode TEXT NOT NULL DEFAULT 'static'")
    _ensure_column(conn, "robustness_experiment_results", "universe_size", "universe_size INTEGER")
    _ensure_column(conn, "robustness_experiment_results", "universe_lookback_days", "universe_lookback_days INTEGER")
    _ensure_column(conn, "robustness_experiment_results", "market_filter_ma20_reduce_by", "market_filter_ma20_reduce_by INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "robustness_experiment_results", "market_filter_ma60_mode", "market_filter_ma60_mode TEXT NOT NULL DEFAULT 'block_new_buys'")
    _ensure_column(conn, "robustness_experiment_results", "ma20_trigger_count", "ma20_trigger_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "ma60_trigger_count", "ma60_trigger_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "reduced_target_count_days", "reduced_target_count_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "blocked_new_buy_days", "blocked_new_buy_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "cash_mode_days", "cash_mode_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "entry_gate_enabled", "entry_gate_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "min_entry_score", "min_entry_score REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "robustness_experiment_results", "require_positive_momentum20", "require_positive_momentum20 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "require_positive_momentum60", "require_positive_momentum60 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "require_above_sma20", "require_above_sma20 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "require_above_sma60", "require_above_sma60 INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "entry_gate_rejected_count", "entry_gate_rejected_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "entry_gate_cash_days", "entry_gate_cash_days INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "average_actual_position_count", "average_actual_position_count REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "robustness_experiment_results", "min_actual_position_count", "min_actual_position_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "max_actual_position_count", "max_actual_position_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "market_scope", "market_scope TEXT NOT NULL DEFAULT 'ALL'")
    _ensure_column(conn, "robustness_experiment_results", "source_symbol_count", "source_symbol_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "average_daily_universe_count", "average_daily_universe_count REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "robustness_experiment_results", "selected_kospi_count", "selected_kospi_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "selected_kosdaq_count", "selected_kosdaq_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "robustness_experiment_results", "kospi_contribution_return", "kospi_contribution_return REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "robustness_experiment_results", "kosdaq_contribution_return", "kosdaq_contribution_return REAL NOT NULL DEFAULT 0.0")

    conn.commit()

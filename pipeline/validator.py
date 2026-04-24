from __future__ import annotations

import sqlite3

from .features import generate_daily_features
from .scoring import generate_daily_scores
from .universe_filter import UniverseFilterConfig, filter_universe
from .backtest import run_backtest
from .paper_trading import run_paper_trading_cycle


def validate_pipeline(conn: sqlite3.Connection, top_n: int = 3, validate_universe_filter: bool = True) -> dict:
    """Run end-to-end pipeline checks against current SQLite DB contents."""
    results: dict[str, object] = {}

    feat_changes = generate_daily_features(conn)
    selected_symbols = None
    universe_summary = None
    if validate_universe_filter:
        cfg = UniverseFilterConfig(
            min_close_price=0.0,
            min_avg_dollar_volume_20d=0.0,
            min_avg_volume_20d=0.0,
            min_data_days_60d=1,
            shock_lookback_days=20,
            shock_abs_return_threshold=1.0,
            shock_max_hits=999,
        )
        universe_summary = filter_universe(conn, cfg)
        selected_symbols = universe_summary["selected_symbols"]

        removed_total = sum(universe_summary["removed_by_reason"].values())
        assert universe_summary["before_count"] >= universe_summary["after_count"], "Universe filter count mismatch"
        assert removed_total >= universe_summary["removed_count"], "Universe filter reason summary mismatch"
        results["universe_filter"] = universe_summary

    score_changes = generate_daily_scores(conn, include_history=True, allowed_symbols=selected_symbols)
    results["feature_rows_written"] = feat_changes
    results["score_rows_written"] = score_changes

    price_count = conn.execute("SELECT COUNT(*) c FROM daily_prices").fetchone()["c"]
    feature_count = conn.execute("SELECT COUNT(*) c FROM daily_features").fetchone()["c"]
    score_count = conn.execute("SELECT COUNT(*) c FROM daily_scores").fetchone()["c"]
    results["price_count"] = price_count
    results["feature_count"] = feature_count
    results["score_count"] = score_count

    assert price_count > 0, "No prices ingested"
    assert feature_count > 0, "No features generated"
    assert score_count > 0, "No scores generated"

    run_id = run_backtest(conn, top_n=top_n)
    bt_rows = conn.execute("SELECT COUNT(*) c FROM backtest_results WHERE run_id=?", (run_id,)).fetchone()["c"]
    assert bt_rows > 0, "Backtest produced no rows"
    results["backtest_run_id"] = run_id
    results["backtest_rows"] = bt_rows

    paper = run_paper_trading_cycle(conn, target_positions=top_n)
    results["paper"] = paper
    return results

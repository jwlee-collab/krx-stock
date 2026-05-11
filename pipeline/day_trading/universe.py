from __future__ import annotations

import sqlite3

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.models import UniverseSelection


class DayUniverseProvider:
    def __init__(self, conn: sqlite3.Connection, config: DayTradingConfig):
        self.conn = conn
        self.config = config

    def resolve_score_date(self, trade_date: str | None = None) -> tuple[str | None, bool, bool, list[str]]:
        cfg = self.config
        reason_codes: list[str] = []
        if cfg.score_date_override:
            score_date = cfg.score_date_override
            same_day = bool(trade_date and score_date == trade_date)
            if trade_date and score_date > trade_date:
                return None, same_day, False, ["SCORE_DATE_AFTER_TRADE_DATE"]
            if same_day and not cfg.allow_same_day_scores:
                return None, True, False, ["SAME_DAY_SCORE_FORBIDDEN"]
            if same_day and cfg.same_day_score_requires_override and not cfg.score_date_override:
                return None, True, False, ["SAME_DAY_SCORE_REQUIRES_OVERRIDE"]
            if same_day:
                reason_codes.append("SAME_DAY_SCORE_OVERRIDE_USED")
            return score_date, same_day, not same_day, reason_codes

        if trade_date is None:
            row = self.conn.execute("SELECT MAX(date) AS d FROM daily_scores").fetchone()
            return (row["d"] if row else None), False, True, ["TRADE_DATE_NOT_PROVIDED"]

        op = "<=" if cfg.allow_same_day_scores and not cfg.same_day_score_requires_override else "<"
        row = self.conn.execute(f"SELECT MAX(date) AS d FROM daily_scores WHERE date {op} ?", (trade_date,)).fetchone()
        score_date = row["d"] if row else None
        if score_date is None:
            same_day_row = self.conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (trade_date,)).fetchone()
            if same_day_row:
                return None, False, False, ["ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN"]
            return None, False, True, ["NO_PRIOR_SCORE_DATE"]
        same_day = score_date == trade_date
        if same_day and not cfg.allow_same_day_scores:
            return None, True, False, ["SAME_DAY_SCORE_FORBIDDEN"]
        if same_day:
            reason_codes.append("SAME_DAY_SCORE_USED")
        return score_date, same_day, not same_day, reason_codes

    def get_universe_selection(self, trade_date: str | None = None) -> UniverseSelection:
        cfg = self.config
        if cfg.universe_source == "INDEPENDENT" and not cfg.allow_independent_universe:
            raise ValueError("independent DAY universe is disabled")
        score_date, same_day, lookahead_safe, reason_codes = self.resolve_score_date(trade_date)
        if not score_date:
            return UniverseSelection(
                trade_date=trade_date,
                score_date=None,
                candidates=[],
                source_universe=cfg.universe_source,
                same_day_score_used=False,
                lookahead_safe=lookahead_safe,
                reason_codes=reason_codes,
            )
        if cfg.universe_source == "INDEPENDENT":
            rows = self.conn.execute(
                """
                SELECT symbol
                FROM daily_universe
                WHERE date=?
                ORDER BY universe_rank ASC, symbol ASC
                LIMIT ?
                """,
                (score_date, int(cfg.max_universe_symbols)),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT symbol
                FROM daily_scores
                WHERE date=?
                ORDER BY rank ASC, symbol ASC
                LIMIT ?
                """,
                (score_date, int(cfg.max_universe_symbols)),
            ).fetchall()
        return UniverseSelection(
            trade_date=trade_date,
            score_date=score_date,
            candidates=[str(r["symbol"]) for r in rows],
            source_universe=cfg.universe_source,
            same_day_score_used=bool(same_day),
            lookahead_safe=bool(lookahead_safe),
            reason_codes=reason_codes,
        )

    def get_candidates(self, as_of_date: str | None = None) -> list[str]:
        return self.get_universe_selection(as_of_date).candidates


class StaticDayUniverseProvider:
    def __init__(self, candidates: list[str]):
        self.candidates = list(candidates)

    def get_candidates(self, as_of_date: str | None = None) -> list[str]:
        return list(self.candidates)

    def get_universe_selection(self, trade_date: str | None = None) -> UniverseSelection:
        return UniverseSelection(
            trade_date=trade_date,
            score_date=trade_date,
            candidates=list(self.candidates),
            source_universe="STATIC",
            same_day_score_used=False,
            lookahead_safe=True,
        )

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from pipeline.day_trading.models import DayPosition, DayTradeLogEvent, DayTradeResult


class DayTradeLogger:
    def __init__(self, conn: sqlite3.Connection | None = None):
        self.conn = conn
        self.events: list[DayTradeLogEvent] = []

    def log_event(
        self,
        event_type: str,
        strategy_id: str,
        mode: str,
        symbol: str | None = None,
        reason_codes: list[str] | None = None,
        raw_metrics: dict[str, Any] | None = None,
        message: str = "",
        created_at: datetime | None = None,
    ) -> DayTradeLogEvent:
        event = DayTradeLogEvent(
            event_type=event_type,
            created_at=created_at or datetime.now(timezone.utc),
            strategy_id=strategy_id,
            mode=mode,
            symbol=symbol,
            reason_codes=list(reason_codes or []),
            raw_metrics=dict(raw_metrics or {}),
            message=message,
        )
        self.events.append(event)
        if self.conn is not None:
            self.conn.execute(
                """
                INSERT INTO day_trade_logs(
                    created_at,strategy_id,mode,symbol,event_type,reason_codes_json,raw_metrics_json,message
                )
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    event.created_at.isoformat(),
                    event.strategy_id,
                    event.mode,
                    event.symbol,
                    event.event_type,
                    json.dumps(event.reason_codes, ensure_ascii=False),
                    json.dumps(event.raw_metrics, ensure_ascii=False, default=str),
                    event.message,
                ),
            )
            self.conn.commit()
        return event

    def log_universe(
        self,
        strategy_id: str,
        mode: str,
        candidates: list[str],
        source: str,
        score_date: str | None = None,
        trade_date: str | None = None,
        same_day_score_used: bool = False,
        lookahead_safe: bool = True,
        reason_codes: list[str] | None = None,
    ) -> None:
        self.log_event(
            "UNIVERSE_COLLECTED",
            strategy_id=strategy_id,
            mode=mode,
            reason_codes=reason_codes or [],
            raw_metrics={
                "candidate_count": len(candidates),
                "source_universe": source,
                "trade_date": trade_date,
                "score_date": score_date,
                "same_day_score_used": bool(same_day_score_used),
                "lookahead_safe": bool(lookahead_safe),
            },
            message="DAY universe collected",
        )

    def log_signal(self, signal_dict: dict[str, Any]) -> None:
        self.log_event(
            "SIGNAL_CREATED",
            strategy_id=str(signal_dict["strategy_id"]),
            mode=str(signal_dict["mode"]),
            symbol=str(signal_dict["symbol"]),
            reason_codes=list(signal_dict.get("signal_reason_codes", [])),
            raw_metrics=signal_dict,
            message="DAY buy signal created",
        )

    def log_paper_entry(self, position: DayPosition) -> None:
        self.log_event(
            "PAPER_ENTRY",
            strategy_id=position.strategy_id,
            mode="PAPER",
            symbol=position.symbol,
            reason_codes=list(position.signal_reason_codes),
            raw_metrics={
                "qty": position.qty,
                "entry_price": position.entry_price,
                "stop_loss_price": position.stop_loss_price,
                "take_profit_price": position.take_profit_price,
                "entry_cost": position.entry_cost,
                "trade_date": position.trade_date,
            },
            message="DAY paper position opened",
        )
        if self.conn is not None:
            self.conn.execute(
                """
                INSERT INTO day_paper_orders(created_at,strategy_id,mode,symbol,side,qty,price,reason,cost,raw_json)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    position.opened_at.isoformat(),
                    position.strategy_id,
                    "PAPER",
                    position.symbol,
                    "BUY",
                    position.qty,
                    position.entry_price,
                    "DAY_ENTRY",
                    position.entry_cost,
                    json.dumps(position.raw_metrics, ensure_ascii=False, default=str),
                ),
            )
            self.conn.execute(
                """
                INSERT INTO day_paper_positions(
                    strategy_id,symbol,trade_date,status,qty,entry_price,stop_loss_price,take_profit_price,
                    opened_at,closed_at,exit_price,exit_reason,realized_pnl,updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(strategy_id,symbol,trade_date) DO UPDATE SET
                    status=excluded.status,
                    qty=excluded.qty,
                    entry_price=excluded.entry_price,
                    stop_loss_price=excluded.stop_loss_price,
                    take_profit_price=excluded.take_profit_price,
                    opened_at=excluded.opened_at,
                    updated_at=excluded.updated_at
                """,
                (
                    position.strategy_id,
                    position.symbol,
                    position.trade_date,
                    "OPEN",
                    position.qty,
                    position.entry_price,
                    position.stop_loss_price,
                    position.take_profit_price,
                    position.opened_at.isoformat(),
                    None,
                    None,
                    None,
                    None,
                    position.opened_at.isoformat(),
                ),
            )
            self.conn.commit()

    def log_paper_exit(self, trade: DayTradeResult) -> None:
        self.log_event(
            "PAPER_EXIT",
            strategy_id=trade.strategy_id,
            mode="PAPER",
            symbol=trade.symbol,
            reason_codes=[trade.exit_reason],
            raw_metrics={
                "qty": trade.qty,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "gross_pnl": trade.gross_pnl,
                "net_pnl": trade.net_pnl,
                "costs": trade.costs,
                "exit_reason": trade.exit_reason,
            },
            message="DAY paper position closed",
        )
        if self.conn is not None:
            self.conn.execute(
                """
                INSERT INTO day_paper_orders(created_at,strategy_id,mode,symbol,side,qty,price,reason,cost,raw_json)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trade.exit_time.isoformat(),
                    trade.strategy_id,
                    "PAPER",
                    trade.symbol,
                    "SELL",
                    trade.qty,
                    trade.exit_price,
                    trade.exit_reason,
                    trade.costs,
                    json.dumps({"net_pnl": trade.net_pnl}, ensure_ascii=False),
                ),
            )
            self.conn.execute(
                """
                UPDATE day_paper_positions
                SET status='CLOSED',
                    closed_at=?,
                    exit_price=?,
                    exit_reason=?,
                    realized_pnl=?,
                    updated_at=?
                WHERE strategy_id=? AND symbol=? AND trade_date=?
                """,
                (
                    trade.exit_time.isoformat(),
                    trade.exit_price,
                    trade.exit_reason,
                    trade.net_pnl,
                    trade.exit_time.isoformat(),
                    trade.strategy_id,
                    trade.symbol,
                    trade.entry_time.date().isoformat(),
                ),
            )
            self.conn.commit()

    def log_daily_summary(self, strategy_id: str, mode: str, summary: dict[str, Any]) -> None:
        self.log_event(
            "DAILY_SUMMARY",
            strategy_id=strategy_id,
            mode=mode,
            raw_metrics=summary,
            message="DAY daily summary",
        )

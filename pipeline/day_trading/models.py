from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class IntradayBar:
    symbol: str
    timestamp: datetime
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float | None = None
    source: str | None = None

    @property
    def trade_value(self) -> float:
        if self.amount is not None:
            return float(self.amount)
        return float(self.close) * float(self.volume)


@dataclass(frozen=True)
class DaySignal:
    strategy_id: str
    symbol: str
    side: str
    timestamp: datetime
    expected_entry_price: float
    stop_loss_price: float
    take_profit_price: float
    confidence: float
    signal_reason_codes: list[str]
    raw_metrics: dict[str, Any]
    mode: str
    source_universe: str
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": self.side,
            "timestamp": self.timestamp.isoformat(),
            "expected_entry_price": self.expected_entry_price,
            "stop_loss_price": self.stop_loss_price,
            "take_profit_price": self.take_profit_price,
            "confidence": self.confidence,
            "signal_reason_codes": list(self.signal_reason_codes),
            "raw_metrics": dict(self.raw_metrics),
            "mode": self.mode,
            "source_universe": self.source_universe,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class SignalEvaluation:
    signal: DaySignal | None
    rejected: bool
    reason_codes: list[str]
    raw_metrics: dict[str, Any]


@dataclass(frozen=True)
class IntradayContext:
    symbol: str
    timestamp: datetime
    price: float
    vwap: float | None
    vwap_distance_pct: float | None
    relative_volume: float
    traded_value: float
    traded_value_score: float
    breakout_score: float
    market_context_score: float
    sector_context_score: float
    trade_strength_score: float
    foreign_flow_score: float
    institution_flow_score: float
    program_flow_score: float
    total_intraday_score: float
    risk_flags: list[str] = field(default_factory=list)
    stale_data_flags: list[str] = field(default_factory=list)
    missing_data_flags: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    raw_metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.risk_flags and not any(code.startswith("REJECT_") for code in self.reason_codes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "price": self.price,
            "vwap": self.vwap,
            "vwap_distance_pct": self.vwap_distance_pct,
            "relative_volume": self.relative_volume,
            "traded_value": self.traded_value,
            "traded_value_score": self.traded_value_score,
            "breakout_score": self.breakout_score,
            "market_context_score": self.market_context_score,
            "sector_context_score": self.sector_context_score,
            "trade_strength_score": self.trade_strength_score,
            "foreign_flow_score": self.foreign_flow_score,
            "institution_flow_score": self.institution_flow_score,
            "program_flow_score": self.program_flow_score,
            "total_intraday_score": self.total_intraday_score,
            "risk_flags": list(self.risk_flags),
            "stale_data_flags": list(self.stale_data_flags),
            "missing_data_flags": list(self.missing_data_flags),
            "reason_codes": list(self.reason_codes),
            "raw_metrics": dict(self.raw_metrics),
        }


@dataclass(frozen=True)
class UniverseSelection:
    trade_date: str | None
    score_date: str | None
    candidates: list[str]
    source_universe: str
    same_day_score_used: bool
    lookahead_safe: bool
    reason_codes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DayEntryDecision:
    approved: bool
    reason_code: str
    notional: float = 0.0
    raw_metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class DayPosition:
    strategy_id: str
    symbol: str
    qty: float
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    opened_at: datetime
    trade_date: str
    side: str = "LONG"
    highest_price: float = 0.0
    status: str = "OPEN"
    entry_cost: float = 0.0
    entry_reference_price: float = 0.0
    entry_fee_krw: float = 0.0
    entry_slippage_cost_krw: float = 0.0
    signal_reason_codes: list[str] = field(default_factory=list)
    raw_metrics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.highest_price <= 0.0:
            self.highest_price = self.entry_price

    @property
    def notional(self) -> float:
        return abs(self.qty * self.entry_price)


@dataclass(frozen=True)
class DayExitSignal:
    strategy_id: str
    symbol: str
    timestamp: datetime
    expected_exit_price: float
    reason: str
    raw_metrics: dict[str, Any]


@dataclass(frozen=True)
class DayTradeResult:
    strategy_id: str
    symbol: str
    qty: float
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    gross_pnl: float
    net_pnl: float
    gross_return_pct: float
    net_return_pct: float
    costs: float
    exit_reason: str
    signal_reason_codes: list[str]
    entry_notional_krw: float = 0.0
    exit_notional_krw: float = 0.0
    fees_krw: float = 0.0
    tax_krw: float = 0.0
    slippage_cost_krw: float = 0.0
    entry_fee_krw: float = 0.0
    exit_fee_krw: float = 0.0
    exit_tax_krw: float = 0.0
    entry_slippage_cost_krw: float = 0.0
    exit_slippage_cost_krw: float = 0.0


@dataclass(frozen=True)
class DayTradeLogEvent:
    event_type: str
    created_at: datetime
    strategy_id: str
    mode: str
    symbol: str | None = None
    reason_codes: list[str] = field(default_factory=list)
    raw_metrics: dict[str, Any] = field(default_factory=dict)
    message: str = ""

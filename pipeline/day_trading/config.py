from __future__ import annotations

from dataclasses import dataclass, field


VALID_DAY_TRADING_MODES = {"SIGNAL_ONLY", "PAPER", "LIVE"}
VALID_UNIVERSE_SOURCES = {"SWING_CANDIDATES", "INDEPENDENT"}
VALID_CONTEXT_MISSING_POLICIES = {"fail_closed", "neutral", "reject"}
VALID_ZERO_VOLUME_BAR_POLICIES = {"strict_invalid", "no_trade_context", "drop_no_trade"}


@dataclass(frozen=True)
class DayTradingConfig:
    enabled: bool = False
    mode: str = "SIGNAL_ONLY"
    strategy_id: str = "DAY"
    universe_source: str = "SWING_CANDIDATES"
    allow_independent_universe: bool = False
    no_overnight: bool = True
    timeframe_primary: str = "5m"
    timeframe_confirm: str = "15m"
    max_universe_symbols: int = 20
    allow_same_day_scores: bool = False
    same_day_score_requires_override: bool = True
    score_date_override: str | None = None

    max_trades_per_day: int = 3
    max_trades_per_symbol_per_day: int = 1
    max_open_positions: int = 2
    max_symbol_exposure_pct: float = 0.20
    max_total_exposure_pct: float = 0.40
    notional_per_trade: float = 1_000_000.0
    initial_equity: float = 10_000_000.0

    force_exit_time: str = "15:10"
    stop_loss_pct: float = 0.012
    take_profit_pct: float = 0.024
    trailing_stop_enabled: bool = False
    trailing_stop_pct: float = 0.010
    daily_loss_limit_pct: float = 0.020
    consecutive_loss_limit: int = 2
    block_reentry_after_loss: bool = True
    fail_closed_on_missing_data: bool = True
    zero_volume_bar_policy: str = "strict_invalid"

    min_primary_bars: int = 6
    min_confirm_bars: int = 4
    breakout_lookback_bars: int = 3
    confirm_breakout_lookback_bars: int = 2
    require_confirm_breakout: bool = True
    min_avg_trade_value: float = 50_000_000.0
    min_latest_trade_value: float = 30_000_000.0
    min_volume_surge_ratio: float = 1.20
    require_vwap_above: bool = True
    require_market_trend_data: bool = True
    market_proxy_symbol: str | None = None
    market_proxy_by_market: dict[str, str] = field(default_factory=dict)
    market_drop_limit_pct: float = 0.010
    sector_proxy_by_symbol: dict[str, str] = field(default_factory=dict)

    optional_context_data_policy: str = "neutral"
    trade_strength_data_policy: str = "neutral"
    investor_flow_data_policy: str = "neutral"
    program_flow_data_policy: str = "neutral"
    max_optional_data_age_minutes: int = 20
    min_total_intraday_score: float = 0.35
    min_market_context_score: float = -0.20

    commission_pct: float = 0.00015
    transaction_tax_pct: float = 0.00180
    slippage_pct: float = 0.00050

    allow_live_trading: bool = False

    def validate(self) -> None:
        mode = self.mode.upper()
        if mode not in VALID_DAY_TRADING_MODES:
            raise ValueError(f"unsupported day_trading.mode: {self.mode}")
        if self.universe_source not in VALID_UNIVERSE_SOURCES:
            raise ValueError(f"unsupported day_trading.universe_source: {self.universe_source}")
        if self.universe_source == "INDEPENDENT" and not self.allow_independent_universe:
            raise ValueError("independent DAY universe requires allow_independent_universe=True")
        for policy in [
            self.optional_context_data_policy,
            self.trade_strength_data_policy,
            self.investor_flow_data_policy,
            self.program_flow_data_policy,
        ]:
            if policy not in VALID_CONTEXT_MISSING_POLICIES:
                raise ValueError(f"unsupported missing data policy: {policy}")
        if self.zero_volume_bar_policy not in VALID_ZERO_VOLUME_BAR_POLICIES:
            raise ValueError(f"unsupported zero_volume_bar_policy: {self.zero_volume_bar_policy}")
        if self.strategy_id != "DAY":
            raise ValueError("DayTradingConfig.strategy_id must remain 'DAY'")
        if mode == "LIVE" and not self.allow_live_trading:
            raise PermissionError("LIVE mode requires allow_live_trading=True and explicit human review")
        if self.max_trades_per_day <= 0:
            raise ValueError("max_trades_per_day must be > 0")
        if self.max_trades_per_symbol_per_day <= 0:
            raise ValueError("max_trades_per_symbol_per_day must be > 0")
        if self.max_open_positions <= 0:
            raise ValueError("max_open_positions must be > 0")
        if self.stop_loss_pct <= 0.0 or self.take_profit_pct <= 0.0:
            raise ValueError("stop_loss_pct and take_profit_pct must be > 0")
        if self.daily_loss_limit_pct <= 0.0:
            raise ValueError("daily_loss_limit_pct must be > 0")
        if self.notional_per_trade <= 0.0 or self.initial_equity <= 0.0:
            raise ValueError("notional_per_trade and initial_equity must be > 0")

    @property
    def normalized_mode(self) -> str:
        return self.mode.upper()

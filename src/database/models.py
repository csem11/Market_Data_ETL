"""
Data models for options chain data
"""

from datetime import datetime, date
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class OptionsChainData:
    """Data class for options chain information"""
    symbol: str
    expiration_date: str
    strike_price: float
    option_type: str  # 'call' or 'put'
    bid: Optional[float] = None
    ask: Optional[float] = None
    last_price: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    implied_volatility: Optional[float] = None
    contract_name: Optional[str] = None
    last_trade_date: Optional[datetime] = None
    eff_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.eff_date is None:
            self.eff_date = datetime.now()


@dataclass
class StockInfo:
    """Data class for stock information"""
    symbol: str
    company_name: Optional[str] = None
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    eff_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.eff_date is None:
            self.eff_date = datetime.now()


@dataclass
class StockPrices:
    """Data class for stock price history"""
    symbol: str
    date: date
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class EarningsDates:
    """Data class for earnings dates information"""
    symbol: str
    earnings_date: datetime
    earnings_type: str  # 'quarterly' or 'annual'
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class OptionMetrics:
    """Data class for option metrics and analytics"""
    symbol: str
    expiration_date: str
    strike_price: float
    option_type: str  # 'call' or 'put'
    current_price: Optional[float] = None  # Current stock price
    option_price: Optional[float] = None  # Option price (mid of bid/ask)
    intrinsic_value: Optional[float] = None
    time_value: Optional[float] = None
    moneyness: Optional[str] = None  # 'ITM', 'ATM', 'OTM'
    days_to_expiration: Optional[int] = None
    implied_volatility: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    bid_ask_spread: Optional[float] = None
    volume_oi_ratio: Optional[float] = None
    max_pain: Optional[float] = None
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class TreasuryRates:
    """Data class for US Treasury rates data"""
    date: datetime
    one_month: Optional[float] = None
    two_month: Optional[float] = None
    three_month: Optional[float] = None
    six_month: Optional[float] = None
    one_year: Optional[float] = None
    two_year: Optional[float] = None
    three_year: Optional[float] = None
    five_year: Optional[float] = None
    seven_year: Optional[float] = None
    ten_year: Optional[float] = None
    twenty_year: Optional[float] = None
    thirty_year: Optional[float] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


def options_chain_to_dict(options_data: OptionsChainData) -> Dict[str, Any]:
    """Convert OptionsChainData to dictionary for database storage"""
    return {
        'symbol': options_data.symbol,
        'expiration_date': options_data.expiration_date,
        'strike_price': options_data.strike_price,
        'option_type': options_data.option_type,
        'bid': options_data.bid,
        'ask': options_data.ask,
        'last_price': options_data.last_price,
        'volume': options_data.volume,
        'open_interest': options_data.open_interest,
        'implied_volatility': options_data.implied_volatility,
        'contract_name': options_data.contract_name,
        'last_trade_date': options_data.last_trade_date.isoformat() if options_data.last_trade_date else None,
        'eff_date': options_data.eff_date.isoformat() if options_data.eff_date else None,
        'created_at': options_data.created_at.isoformat() if options_data.created_at else None
    }


def stock_info_to_dict(stock_info: StockInfo) -> Dict[str, Any]:
    """Convert StockInfo to dictionary for database storage"""
    return {
        'symbol': stock_info.symbol,
        'company_name': stock_info.company_name,
        'current_price': stock_info.current_price,
        'market_cap': stock_info.market_cap,
        'sector': stock_info.sector,
        'industry': stock_info.industry,
        'eff_date': stock_info.eff_date.isoformat() if stock_info.eff_date else None,
        'created_at': stock_info.created_at.isoformat() if stock_info.created_at else None
    }


def stock_prices_to_dict(stock_prices: StockPrices) -> Dict[str, Any]:
    """Convert StockPrices to dictionary for database storage"""
    return {
        'symbol': stock_prices.symbol,
        'date': stock_prices.date.isoformat(),
        'open_price': stock_prices.open_price,
        'high_price': stock_prices.high_price,
        'low_price': stock_prices.low_price,
        'close_price': stock_prices.close_price,
        'volume': stock_prices.volume,
        'adjusted_close': stock_prices.adjusted_close,
        'created_at': stock_prices.created_at.isoformat() if stock_prices.created_at else None
    }


def earnings_dates_to_dict(earnings_dates: EarningsDates) -> Dict[str, Any]:
    """Convert EarningsDates to dictionary for database storage"""
    return {
        'symbol': earnings_dates.symbol,
        'earnings_date': earnings_dates.earnings_date.isoformat(),
        'earnings_type': earnings_dates.earnings_type,
        'fiscal_year': earnings_dates.fiscal_year,
        'fiscal_quarter': earnings_dates.fiscal_quarter,
        'created_at': earnings_dates.created_at.isoformat() if earnings_dates.created_at else None
    }


def option_metrics_to_dict(option_metrics: OptionMetrics) -> Dict[str, Any]:
    """Convert OptionMetrics to dictionary for database storage"""
    return {
        'symbol': option_metrics.symbol,
        'expiration_date': option_metrics.expiration_date,
        'strike_price': option_metrics.strike_price,
        'option_type': option_metrics.option_type,
        'current_price': option_metrics.current_price,
        'option_price': option_metrics.option_price,
        'intrinsic_value': option_metrics.intrinsic_value,
        'time_value': option_metrics.time_value,
        'moneyness': option_metrics.moneyness,
        'days_to_expiration': option_metrics.days_to_expiration,
        'implied_volatility': option_metrics.implied_volatility,
        'delta': option_metrics.delta,
        'gamma': option_metrics.gamma,
        'theta': option_metrics.theta,
        'vega': option_metrics.vega,
        'rho': option_metrics.rho,
        'volume': option_metrics.volume,
        'open_interest': option_metrics.open_interest,
        'bid_ask_spread': option_metrics.bid_ask_spread,
        'volume_oi_ratio': option_metrics.volume_oi_ratio,
        'max_pain': option_metrics.max_pain,
        'support_level': option_metrics.support_level,
        'resistance_level': option_metrics.resistance_level,
        'created_at': option_metrics.created_at.isoformat() if option_metrics.created_at else None
    }


def treasury_rates_to_dict(treasury_rates: TreasuryRates) -> Dict[str, Any]:
    """Convert TreasuryRates to dictionary for database storage"""
    return {
        'date': treasury_rates.date.isoformat(),
        'one_month': treasury_rates.one_month,
        'two_month': treasury_rates.two_month,
        'three_month': treasury_rates.three_month,
        'six_month': treasury_rates.six_month,
        'one_year': treasury_rates.one_year,
        'two_year': treasury_rates.two_year,
        'three_year': treasury_rates.three_year,
        'five_year': treasury_rates.five_year,
        'seven_year': treasury_rates.seven_year,
        'ten_year': treasury_rates.ten_year,
        'twenty_year': treasury_rates.twenty_year,
        'thirty_year': treasury_rates.thirty_year,
        'created_at': treasury_rates.created_at.isoformat() if treasury_rates.created_at else None
    }

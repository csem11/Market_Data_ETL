"""
Data models for options chain data
"""

from datetime import datetime
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
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
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
        'delta': options_data.delta,
        'gamma': options_data.gamma,
        'theta': options_data.theta,
        'vega': options_data.vega,
        'rho': options_data.rho,
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

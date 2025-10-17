"""
Options data processor
Handles transformation and processing of options data
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import OptionsChainData, OptionMetrics


class OptionsProcessor:
    """Processor for options data transformation and analysis"""
    
    def __init__(self):
        """Initialize options processor"""
        pass
    
    def process_options_chain(self, options_data: List[OptionsChainData]) -> List[OptionsChainData]:
        """
        Process options chain data
        
        Args:
            options_data: List of OptionsChainData objects
            
        Returns:
            Processed list of OptionsChainData objects
        """
        processed_data = []
        
        for option in options_data:
            # Add any processing logic here
            # For now, just pass through
            processed_data.append(option)
        
        return processed_data
    
    def calculate_option_metrics(self, options_data: List[OptionsChainData], 
                                current_price: float) -> List[OptionMetrics]:
        """
        Calculate option metrics from options chain data
        
        Args:
            options_data: List of OptionsChainData objects
            current_price: Current stock price
            
        Returns:
            List of OptionMetrics objects
        """
        metrics = []
        
        for option in options_data:
            # Calculate basic metrics
            intrinsic_value = self._calculate_intrinsic_value(
                option.option_type, option.strike_price, current_price
            )
            
            time_value = None
            if option.last_price and intrinsic_value is not None:
                time_value = option.last_price - intrinsic_value
            
            moneyness = self._determine_moneyness(
                option.option_type, option.strike_price, current_price
            )
            
            # Create OptionMetrics object
            metric = OptionMetrics(
                symbol=option.symbol,
                expiration_date=option.expiration_date,
                strike_price=option.strike_price,
                option_type=option.option_type,
                current_price=current_price,
                option_price=option.last_price,
                intrinsic_value=intrinsic_value,
                time_value=time_value,
                moneyness=moneyness,
                days_to_expiration=self._calculate_days_to_expiration(option.expiration_date),
                implied_volatility=option.implied_volatility,
                volume=option.volume,
                open_interest=option.open_interest,
                bid_ask_spread=self._calculate_bid_ask_spread(option.bid, option.ask)
            )
            
            metrics.append(metric)
        
        return metrics
    
    def _calculate_intrinsic_value(self, option_type: str, strike_price: float, 
                                 current_price: float) -> Optional[float]:
        """Calculate intrinsic value of an option"""
        if option_type == 'call':
            return max(0, current_price - strike_price)
        elif option_type == 'put':
            return max(0, strike_price - current_price)
        return None
    
    def _determine_moneyness(self, option_type: str, strike_price: float, 
                           current_price: float) -> str:
        """Determine moneyness of an option"""
        if option_type == 'call':
            if current_price > strike_price:
                return 'ITM'
            elif current_price == strike_price:
                return 'ATM'
            else:
                return 'OTM'
        elif option_type == 'put':
            if current_price < strike_price:
                return 'ITM'
            elif current_price == strike_price:
                return 'ATM'
            else:
                return 'OTM'
        return 'Unknown'
    
    def _calculate_days_to_expiration(self, expiration_date: str) -> Optional[int]:
        """Calculate days to expiration"""
        try:
            exp_date = datetime.strptime(expiration_date, '%Y-%m-%d')
            today = datetime.now()
            return (exp_date - today).days
        except:
            return None
    
    def _calculate_bid_ask_spread(self, bid: Optional[float], ask: Optional[float]) -> Optional[float]:
        """Calculate bid-ask spread"""
        if bid and ask:
            return ask - bid
        return None

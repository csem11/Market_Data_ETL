"""
Options metrics calculations
Individual functions for calculating options metrics
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import OptionMetrics


def calculate_intrinsic_value(option_type: str, strike_price: float, current_price: float) -> Optional[float]:
    """Calculate intrinsic value of an option"""
    if option_type == 'call':
        return max(0, current_price - strike_price)
    elif option_type == 'put':
        return max(0, strike_price - current_price)
    return None


def determine_moneyness(option_type: str, strike_price: float, current_price: float) -> str:
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


def calculate_days_to_expiration(expiration_date: str) -> Optional[int]:
    """Calculate days to expiration"""
    try:
        exp_date = datetime.strptime(expiration_date, '%Y-%m-%d')
        today = datetime.now()
        return (exp_date - today).days
    except:
        return None


def calculate_bid_ask_spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Calculate bid-ask spread"""
    if bid and ask:
        return ask - bid
    return None


def calculate_time_value(option_price: float, intrinsic_value: Optional[float]) -> Optional[float]:
    """Calculate time value of an option"""
    if option_price is not None and intrinsic_value is not None:
        return option_price - intrinsic_value
    return None


def calculate_option_metrics(options_df: pd.DataFrame, current_price: float = None) -> List[OptionMetrics]:
    """
    Calculate option metrics from options DataFrame
    
    Args:
        options_df: DataFrame with options chain data
        current_price: Current stock price
        
    Returns:
        List of OptionMetrics objects
    """
    metrics = []
    
    if options_df.empty:
        return metrics
    
    # If no current price provided, skip metrics calculation
    if current_price is None:
        print("      No current price provided, skipping metrics calculation")
        return metrics
    
    for _, row in options_df.iterrows():
        try:
            # Calculate basic metrics
            intrinsic_value = calculate_intrinsic_value(
                row['option_type'], row['strike_price'], current_price
            )
            
            time_value = calculate_time_value(row.get('last_price'), intrinsic_value)
            
            moneyness = determine_moneyness(
                row['option_type'], row['strike_price'], current_price
            )
            
            # Create OptionMetrics object
            metric = OptionMetrics(
                symbol=row['symbol'],
                expiration_date=row['expiration_date'],
                strike_price=row['strike_price'],
                option_type=row['option_type'],
                current_price=current_price,
                option_price=row.get('last_price'),
                intrinsic_value=intrinsic_value,
                time_value=time_value,
                moneyness=moneyness,
                days_to_expiration=calculate_days_to_expiration(row['expiration_date']),
                implied_volatility=row.get('implied_volatility'),
                volume=row.get('volume'),
                open_interest=row.get('open_interest'),
                bid_ask_spread=calculate_bid_ask_spread(row.get('bid'), row.get('ask'))
            )
            
            metrics.append(metric)
            
        except Exception as e:
            print(f"Error processing option {row.get('symbol', 'unknown')}: {e}")
            continue
    
    return metrics


def calculate_advanced_metrics(options_df: pd.DataFrame, current_price: float) -> Dict[str, Any]:
    """
    Calculate advanced options metrics
    
    Args:
        options_df: DataFrame with options chain data
        current_price: Current stock price
        
    Returns:
        Dictionary with advanced metrics
    """
    if options_df.empty:
        return {}
    
    metrics = {}
    
    # Volume analysis
    metrics['total_volume'] = options_df['volume'].sum() if 'volume' in options_df.columns else 0
    metrics['avg_volume'] = options_df['volume'].mean() if 'volume' in options_df.columns else 0
    
    # Open interest analysis
    metrics['total_open_interest'] = options_df['open_interest'].sum() if 'open_interest' in options_df.columns else 0
    metrics['avg_open_interest'] = options_df['open_interest'].mean() if 'open_interest' in options_df.columns else 0
    
    # Implied volatility analysis
    if 'implied_volatility' in options_df.columns:
        iv_data = options_df['implied_volatility'].dropna()
        if not iv_data.empty:
            metrics['avg_implied_volatility'] = iv_data.mean()
            metrics['max_implied_volatility'] = iv_data.max()
            metrics['min_implied_volatility'] = iv_data.min()
    
    # Moneyness distribution
    calls = options_df[options_df['option_type'] == 'call']
    puts = options_df[options_df['option_type'] == 'put']
    
    metrics['call_put_ratio'] = len(calls) / len(puts) if len(puts) > 0 else 0
    
    # Strike price analysis
    if not options_df.empty:
        metrics['strike_range'] = {
            'min_strike': options_df['strike_price'].min(),
            'max_strike': options_df['strike_price'].max(),
            'strike_count': len(options_df)
        }
    
    return metrics
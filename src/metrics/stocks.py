"""
Stock metrics calculations
Individual functions for calculating stock metrics
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import StockPrices


def calculate_volatility(prices: pd.Series) -> float:
    """Calculate price volatility"""
    if len(prices) < 2:
        return 0.0
    
    returns = prices.pct_change().dropna()
    return returns.std() * (252 ** 0.5)  # Annualized volatility


def calculate_price_change(current_price: float, previous_price: float) -> float:
    """Calculate absolute price change"""
    return current_price - previous_price


def calculate_price_change_percentage(current_price: float, previous_price: float) -> float:
    """Calculate percentage price change"""
    if previous_price == 0:
        return 0.0
    return ((current_price - previous_price) / previous_price) * 100


def calculate_simple_moving_average(prices: pd.Series, window: int = 20) -> pd.Series:
    """Calculate simple moving average"""
    return prices.rolling(window=window).mean()


def determine_trend(prices: pd.Series, window: int = 20) -> str:
    """Determine trend direction based on moving average"""
    if len(prices) < window:
        return "insufficient_data"
    
    sma = calculate_simple_moving_average(prices, window)
    current_price = prices.iloc[-1]
    current_sma = sma.iloc[-1]
    
    if current_price > current_sma * 1.02:  # 2% above SMA
        return "uptrend"
    elif current_price < current_sma * 0.98:  # 2% below SMA
        return "downtrend"
    else:
        return "sideways"


def calculate_exponential_moving_average(prices: pd.Series, window: int = 20) -> pd.Series:
    """Calculate exponential moving average"""
    return prices.ewm(span=window).mean()


def calculate_rsi(prices: pd.Series, window: int = 14) -> pd.Series:
    """Calculate Relative Strength Index"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_bollinger_bands(prices: pd.Series, window: int = 20, std_dev: float = 2) -> Dict[str, pd.Series]:
    """Calculate Bollinger Bands"""
    sma = calculate_simple_moving_average(prices, window)
    std = prices.rolling(window=window).std()
    
    return {
        'upper': sma + (std * std_dev),
        'middle': sma,
        'lower': sma - (std * std_dev)
    }


def calculate_price_metrics(stock_prices: List[StockPrices]) -> Dict[str, Any]:
    """
    Calculate price metrics from stock price data
    
    Args:
        stock_prices: List of StockPrices objects
        
    Returns:
        Dictionary with price metrics
    """
    if not stock_prices:
        return {}
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame([{
        'date': price.date,
        'open': price.open_price,
        'high': price.high_price,
        'low': price.low_price,
        'close': price.close_price,
        'volume': price.volume
    } for price in stock_prices])
    
    if df.empty:
        return {}
    
    # Calculate basic metrics
    latest_price = df['close'].iloc[-1]
    first_price = df['close'].iloc[0]
    
    metrics = {
        'current_price': latest_price,
        'price_change': calculate_price_change(latest_price, first_price),
        'price_change_pct': calculate_price_change_percentage(latest_price, first_price),
        'high_52w': df['high'].max(),
        'low_52w': df['low'].min(),
        'avg_volume': df['volume'].mean(),
        'volatility': calculate_volatility(df['close']),
        'trend': determine_trend(df['close'])
    }
    
    return metrics


def calculate_technical_indicators(stock_prices: List[StockPrices]) -> Dict[str, Any]:
    """
    Calculate technical indicators from stock price data
    
    Args:
        stock_prices: List of StockPrices objects
        
    Returns:
        Dictionary with technical indicators
    """
    if not stock_prices:
        return {}
    
    # Convert to DataFrame
    df = pd.DataFrame([{
        'date': price.date,
        'open': price.open_price,
        'high': price.high_price,
        'low': price.low_price,
        'close': price.close_price,
        'volume': price.volume
    } for price in stock_prices])
    
    if df.empty:
        return {}
    
    close_prices = df['close']
    
    indicators = {}
    
    # Moving averages
    indicators['sma_20'] = calculate_simple_moving_average(close_prices, 20).iloc[-1] if len(close_prices) >= 20 else None
    indicators['sma_50'] = calculate_simple_moving_average(close_prices, 50).iloc[-1] if len(close_prices) >= 50 else None
    indicators['ema_20'] = calculate_exponential_moving_average(close_prices, 20).iloc[-1] if len(close_prices) >= 20 else None
    
    # RSI
    rsi = calculate_rsi(close_prices, 14)
    indicators['rsi'] = rsi.iloc[-1] if not rsi.empty else None
    
    # Bollinger Bands
    bb = calculate_bollinger_bands(close_prices, 20, 2)
    indicators['bb_upper'] = bb['upper'].iloc[-1] if not bb['upper'].empty else None
    indicators['bb_middle'] = bb['middle'].iloc[-1] if not bb['middle'].empty else None
    indicators['bb_lower'] = bb['lower'].iloc[-1] if not bb['lower'].empty else None
    
    return indicators

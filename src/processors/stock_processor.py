"""
Stock data processor
Handles transformation and processing of stock data
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import StockInfo, StockPrices


class StockProcessor:
    """Processor for stock data transformation and analysis"""
    
    def __init__(self):
        """Initialize stock processor"""
        pass
    
    def process_stock_info(self, stock_info: StockInfo) -> StockInfo:
        """
        Process stock information
        
        Args:
            stock_info: StockInfo object
            
        Returns:
            Processed StockInfo object
        """
        # Normalize symbol to uppercase
        stock_info.symbol = stock_info.symbol.upper()
        
        # Add any other processing logic here
        return stock_info
    
    def process_stock_prices(self, stock_prices: List[StockPrices]) -> List[StockPrices]:
        """
        Process stock price data
        
        Args:
            stock_prices: List of StockPrices objects
            
        Returns:
            Processed list of StockPrices objects
        """
        processed_data = []
        
        for price in stock_prices:
            # Add any processing logic here
            # For now, just pass through
            processed_data.append(price)
        
        return processed_data
    
    def calculate_price_metrics(self, stock_prices: List[StockPrices]) -> Dict[str, Any]:
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
        
        # Calculate metrics
        latest_price = df['close'].iloc[-1]
        first_price = df['close'].iloc[0]
        
        metrics = {
            'current_price': latest_price,
            'price_change': latest_price - first_price,
            'price_change_pct': ((latest_price - first_price) / first_price) * 100 if first_price != 0 else 0,
            'high_52w': df['high'].max(),
            'low_52w': df['low'].min(),
            'avg_volume': df['volume'].mean(),
            'volatility': self._calculate_volatility(df['close']),
            'trend': self._determine_trend(df['close'])
        }
        
        return metrics
    
    def _calculate_volatility(self, prices: pd.Series) -> float:
        """Calculate price volatility"""
        if len(prices) < 2:
            return 0.0
        
        returns = prices.pct_change().dropna()
        return returns.std() * (252 ** 0.5)  # Annualized volatility
    
    def _determine_trend(self, prices: pd.Series) -> str:
        """Determine price trend"""
        if len(prices) < 2:
            return 'Unknown'
        
        first_price = prices.iloc[0]
        last_price = prices.iloc[-1]
        
        if last_price > first_price * 1.05:  # 5% threshold
            return 'Uptrend'
        elif last_price < first_price * 0.95:  # 5% threshold
            return 'Downtrend'
        else:
            return 'Sideways'

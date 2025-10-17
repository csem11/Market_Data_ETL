"""
Stock data processor
Handles transformation and processing of stock data
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import StockInfo, StockPrices
from ..metrics.stocks import calculate_price_metrics, calculate_technical_indicators


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
        return calculate_price_metrics(stock_prices)
    
    def calculate_technical_indicators(self, stock_prices: List[StockPrices]) -> Dict[str, Any]:
        """
        Calculate technical indicators from stock price data
        
        Args:
            stock_prices: List of StockPrices objects
            
        Returns:
            Dictionary with technical indicators
        """
        return calculate_technical_indicators(stock_prices)

"""
Database loader for ETL operations
Handles data loading into the database
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import time

from ..database import OptionsDatabase
from ..database.models import OptionsChainData, StockInfo, StockPrices, EarningsDates, OptionMetrics, TreasuryRates


class DatabaseLoader:
    """Database loader for ETL operations"""
    
    def __init__(self, db_path: str = "data/options/market_data.db"):
        """
        Initialize database loader
        
        Args:
            db_path: Path to database file
        """
        self.db = OptionsDatabase(db_path)
    
    def load_options_chain(self, options_data: List[OptionsChainData]) -> int:
        """
        Load options chain data into the database
        
        Args:
            options_data: List of OptionsChainData objects
            
        Returns:
            Number of records inserted
        """
        return self.db.insert_options_chain(options_data)
    
    def load_stock_info(self, stock_info: StockInfo) -> bool:
        """
        Load stock information into the database
        
        Args:
            stock_info: StockInfo object
            
        Returns:
            True if successful
        """
        return self.db.insert_stock_info(stock_info)
    
    def load_stock_prices(self, stock_prices: List[StockPrices]) -> int:
        """
        Load stock price history into the database
        
        Args:
            stock_prices: List of StockPrices objects
            
        Returns:
            Number of records inserted
        """
        return self.db.insert_stock_prices(stock_prices)
    
    def load_earnings_dates(self, earnings_dates: List[EarningsDates]) -> int:
        """
        Load earnings dates into the database
        
        Args:
            earnings_dates: List of EarningsDates objects
            
        Returns:
            Number of records inserted
        """
        return self.db.insert_earnings_dates(earnings_dates)
    
    def load_option_metrics(self, option_metrics: List[OptionMetrics]) -> int:
        """
        Load option metrics into the database
        
        Args:
            option_metrics: List of OptionMetrics objects
            
        Returns:
            Number of records inserted
        """
        return self.db.insert_option_metrics(option_metrics)
    
    def load_treasury_rates(self, treasury_rates: List[TreasuryRates]) -> int:
        """
        Load treasury rates into the database
        
        Args:
            treasury_rates: List of TreasuryRates objects
            
        Returns:
            Number of records inserted
        """
        return self.db.insert_treasury_rates(treasury_rates)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        return self.db.get_database_stats()
    
    def cleanup_old_data(self, days_old: int = 30) -> int:
        """
        Clean up old data from the database
        
        Args:
            days_old: Number of days to keep data
            
        Returns:
            Number of records deleted
        """
        return self.db.delete_old_data(days_old)

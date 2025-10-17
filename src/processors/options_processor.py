"""
Options data processor
Handles transformation and processing of options data
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import OptionsChainData, OptionMetrics
from ..metrics.options import calculate_option_metrics, calculate_advanced_metrics


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
    
    def calculate_option_metrics(self, options_df: pd.DataFrame, current_price: float = None) -> List[OptionMetrics]:
        """
        Calculate option metrics from options DataFrame
        
        Args:
            options_df: DataFrame with options chain data
            current_price: Current stock price (if not provided, will try to get from stock data)
            
        Returns:
            List of OptionMetrics objects
        """
        return calculate_option_metrics(options_df, current_price)
    
    def calculate_advanced_metrics(self, options_df: pd.DataFrame, current_price: float) -> Dict[str, Any]:
        """
        Calculate advanced options metrics
        
        Args:
            options_df: DataFrame with options chain data
            current_price: Current stock price
            
        Returns:
            Dictionary with advanced metrics
        """
        return calculate_advanced_metrics(options_df, current_price)

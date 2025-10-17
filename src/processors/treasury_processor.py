"""
Treasury data processor
Handles transformation and processing of treasury rates data
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from ..database.models import TreasuryRates


class TreasuryProcessor:
    """Processor for treasury rates data transformation and analysis"""
    
    def __init__(self):
        """Initialize treasury processor"""
        pass
    
    def process_treasury_rates(self, treasury_data: List[TreasuryRates]) -> List[TreasuryRates]:
        """
        Process treasury rates data
        
        Args:
            treasury_data: List of TreasuryRates objects
            
        Returns:
            Processed list of TreasuryRates objects
        """
        processed_data = []
        
        for rates in treasury_data:
            # Add any processing logic here
            # For now, just pass through
            processed_data.append(rates)
        
        return processed_data
    
    def calculate_yield_curve_metrics(self, treasury_data: List[TreasuryRates]) -> Dict[str, Any]:
        """
        Calculate yield curve metrics from treasury data
        
        Args:
            treasury_data: List of TreasuryRates objects
            
        Returns:
            Dictionary with yield curve metrics
        """
        if not treasury_data:
            return {}
        
        # Get the most recent data
        latest_rates = treasury_data[-1]
        
        metrics = {
            'date': latest_rates.date,
            'yield_curve_slope': self._calculate_yield_curve_slope(latest_rates),
            'yield_curve_curvature': self._calculate_yield_curve_curvature(latest_rates),
            'short_term_rates': {
                'one_month': latest_rates.one_month,
                'three_month': latest_rates.three_month,
                'six_month': latest_rates.six_month
            },
            'long_term_rates': {
                'ten_year': latest_rates.ten_year,
                'thirty_year': latest_rates.thirty_year
            }
        }
        
        return metrics
    
    def _calculate_yield_curve_slope(self, rates: TreasuryRates) -> Optional[float]:
        """Calculate yield curve slope (10Y - 2Y)"""
        if rates.ten_year and rates.two_year:
            return rates.ten_year - rates.two_year
        return None
    
    def _calculate_yield_curve_curvature(self, rates: TreasuryRates) -> Optional[float]:
        """Calculate yield curve curvature"""
        if rates.two_year and rates.ten_year and rates.thirty_year:
            # Simplified curvature calculation
            return (rates.two_year + rates.thirty_year) / 2 - rates.ten_year
        return None
    
    def analyze_rate_trends(self, treasury_data: List[TreasuryRates], 
                           days: int = 30) -> Dict[str, Any]:
        """
        Analyze rate trends over a period
        
        Args:
            treasury_data: List of TreasuryRates objects
            days: Number of days to analyze
            
        Returns:
            Dictionary with trend analysis
        """
        if len(treasury_data) < 2:
            return {}
        
        # Get data for the specified period
        recent_data = treasury_data[-days:] if len(treasury_data) >= days else treasury_data
        
        trends = {}
        
        # Calculate trends for each rate
        rate_fields = ['one_month', 'three_month', 'six_month', 'one_year', 
                      'two_year', 'ten_year', 'thirty_year']
        
        for field in rate_fields:
            values = [getattr(rate, field) for rate in recent_data if getattr(rate, field) is not None]
            if len(values) >= 2:
                trends[field] = {
                    'current': values[-1],
                    'change': values[-1] - values[0],
                    'change_pct': ((values[-1] - values[0]) / values[0]) * 100 if values[0] != 0 else 0
                }
        
        return trends

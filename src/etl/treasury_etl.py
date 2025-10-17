"""
Treasury ETL pipeline
Complete ETL process for treasury rates data
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from ..scrapers.treasury import TreasuryScraper
from ..processors.treasury_processor import TreasuryProcessor
from ..loaders.database_loader import DatabaseLoader
from ..database.models import TreasuryRates


class TreasuryETL:
    """Complete ETL pipeline for treasury rates data"""
    
    def __init__(self, db_path: str = "data/options/market_data.db", 
                 rate_limit_delay: float = 0.1):
        """
        Initialize Treasury ETL pipeline
        
        Args:
            db_path: Database path
            rate_limit_delay: Delay between API calls
        """
        self.scraper = TreasuryScraper(rate_limit_delay=rate_limit_delay)
        self.processor = TreasuryProcessor()
        self.loader = DatabaseLoader(db_path)
    
    def extract_treasury_data(self, year: int, month: int) -> List[TreasuryRates]:
        """
        Extract treasury data for a specific month
        
        Args:
            year: Year to extract
            month: Month to extract
            
        Returns:
            List of TreasuryRates objects
        """
        print(f"Extracting treasury data for {year}-{month:02d}...")
        return self.scraper.fetch_and_process_month(year, month)
    
    def transform_treasury_data(self, treasury_data: List[TreasuryRates]) -> List[TreasuryRates]:
        """
        Transform treasury data
        
        Args:
            treasury_data: Raw treasury data
            
        Returns:
            Processed treasury data
        """
        print(f"Transforming {len(treasury_data)} treasury records...")
        return self.processor.process_treasury_rates(treasury_data)
    
    def load_treasury_data(self, treasury_data: List[TreasuryRates]) -> int:
        """
        Load treasury data into database
        
        Args:
            treasury_data: Processed treasury data
            
        Returns:
            Number of records loaded
        """
        print(f"Loading {len(treasury_data)} treasury records...")
        return self.loader.load_treasury_rates(treasury_data)
    
    def run_etl_pipeline(self, year: int, month: int) -> Dict[str, Any]:
        """
        Run complete ETL pipeline for treasury data
        
        Args:
            year: Year to process
            month: Month to process
            
        Returns:
            Dictionary with ETL results
        """
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = self.extract_treasury_data(year, month)
            if not raw_data:
                return {
                    'success': False,
                    'error': f'No data found for {year}-{month:02d}',
                    'records_processed': 0,
                    'duration': 0
                }
            
            # Transform
            processed_data = self.transform_treasury_data(raw_data)
            
            # Load
            records_loaded = self.load_treasury_data(processed_data)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return {
                'success': True,
                'records_processed': len(processed_data),
                'records_loaded': records_loaded,
                'duration': duration,
                'year': year,
                'month': month
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return {
                'success': False,
                'error': str(e),
                'records_processed': 0,
                'duration': duration
            }
    
    def get_treasury_analytics(self, start_date: Optional[str] = None, 
                              end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get treasury analytics for a date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary with analytics
        """
        # Get treasury data from database
        df = self.loader.db.get_treasury_rates(start_date, end_date)
        
        if df.empty:
            return {'error': 'No treasury data found for the specified date range'}
        
        # Convert to TreasuryRates objects
        treasury_data = []
        for _, row in df.iterrows():
            rates = TreasuryRates(
                date=datetime.fromisoformat(row['date'].replace('T', ' ')),
                one_month=row.get('one_month'),
                two_month=row.get('two_month'),
                three_month=row.get('three_month'),
                six_month=row.get('six_month'),
                one_year=row.get('one_year'),
                two_year=row.get('two_year'),
                three_year=row.get('three_year'),
                five_year=row.get('five_year'),
                seven_year=row.get('seven_year'),
                ten_year=row.get('ten_year'),
                twenty_year=row.get('twenty_year'),
                thirty_year=row.get('thirty_year')
            )
            treasury_data.append(rates)
        
        # Calculate analytics
        yield_curve_metrics = self.processor.calculate_yield_curve_metrics(treasury_data)
        rate_trends = self.processor.analyze_rate_trends(treasury_data)
        
        return {
            'yield_curve_metrics': yield_curve_metrics,
            'rate_trends': rate_trends,
            'data_points': len(treasury_data),
            'date_range': {
                'start': treasury_data[0].date.isoformat(),
                'end': treasury_data[-1].date.isoformat()
            }
        }

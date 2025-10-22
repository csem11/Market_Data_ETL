

"""
Treasury rates scraper for US Treasury data
Fetches and processes daily treasury bill rates from treasury.gov
"""

import requests
import pandas as pd
from datetime import datetime
from typing import List, Optional
import time

from ..database.models import TreasuryRates


class TreasuryScraper:
    """Scraper for US Treasury rates data"""
    
    def __init__(self, rate_limit_delay: float = 0.1):
        """
        Initialize treasury scraper
        
        Args:
            rate_limit_delay: Delay between requests in seconds
        """
        self.rate_limit_delay = rate_limit_delay
        self.base_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv"
    
    def get_daily_treasury_rates(self, year: int = None, month: int = None) -> pd.DataFrame:
        """
        Fetch daily treasury bill rates from US Treasury for a given month.
        Uses the csv download link for robust parsing.

        Args:
            year (int): Year for which rates are needed. Defaults to current year.
            month (int): Month for which rates are needed. Defaults to current month.

        Returns:
            pd.DataFrame: Rates table with columns as on treasury.gov csv.
        """
        if year is None or month is None:
            now = datetime.now()
            year = now.year
            month = now.month

        # Construct the year-month string for 'field_tdr_date_value' param: format 'YYYYMM'
        if year and month:
            ym_str = f"{year}{month:02d}"
        else:
            now = datetime.now()
            ym_str = f"{now.year}{now.month:02d}"

        # This is the CSV download URL pattern found on the treasury website for the table:
        csv_url = f"{self.base_url}/all/{ym_str}?type=daily_treasury_bill_rates&field_tdr_date_value_month={ym_str}&page&_format=csv"

        try:
            resp = requests.get(csv_url, timeout=30)
            resp.raise_for_status()

            # The csv file might be encoded with BOM or badly formatted, so allow for flexibility
            from io import StringIO
            s = resp.content.decode("utf-8-sig")
            df = pd.read_csv(StringIO(s))

            # Clean up column names and types
            df = self._clean_treasury_data(df)
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return df
            
        except requests.RequestException as e:
            if year and month:
                print(f"Error fetching treasury data for {year}-{month:02d}: {e}")
            else:
                print(f"Error fetching treasury data for current month: {e}")
            return pd.DataFrame()
    
    def _clean_treasury_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize treasury data
        
        Args:
            df: Raw treasury data DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        # Standardize column names based on actual treasury.gov format
        column_mapping = {
            'Date': 'date',
            '4 WEEKS BANK DISCOUNT': 'one_month',
            '13 WEEKS BANK DISCOUNT': 'three_month',
            '26 WEEKS BANK DISCOUNT': 'six_month',
            '52 WEEKS BANK DISCOUNT': 'one_year'
        }
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Convert date column to datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Convert rate columns to numeric, handling 'N/A' and other non-numeric values
        rate_columns = [col for col in df.columns if col != 'date']
        for col in rate_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with invalid dates
        df = df.dropna(subset=['date'])
        
        return df
    
    def process_treasury_data(self, df: pd.DataFrame) -> List[TreasuryRates]:
        """
        Process treasury DataFrame into TreasuryRates objects
        
        Args:
            df: Treasury data DataFrame
            
        Returns:
            List of TreasuryRates objects
        """
        treasury_rates = []
        
        for _, row in df.iterrows():
            try:
                # Create TreasuryRates object
                rates = TreasuryRates(
                    date=row['date'],
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
                treasury_rates.append(rates)
                
            except Exception as e:
                print(f"Error processing treasury data for date {row.get('date', 'unknown')}: {e}")
                continue
        
        return treasury_rates
    
    def fetch_and_process_month(self, year: int, month: int) -> List[TreasuryRates]:
        """
        Fetch and process treasury data for a specific month
        
        Args:
            year: Year to fetch
            month: Month to fetch
            
        Returns:
            List of TreasuryRates objects
        """
        if year and month:
            print(f"Fetching treasury data for {year}-{month:02d}...")
        else:
            print("Fetching treasury data for current month...")
        
        # Fetch raw data
        df = self.get_daily_treasury_rates(year, month)
        
        if df.empty:
            if year and month:
                print(f"No data found for {year}-{month:02d}")
            else:
                print("No data found for current month")
            return []
        
        # Process into TreasuryRates objects
        treasury_rates = self.process_treasury_data(df)
        
        if year and month:
            print(f"Processed {len(treasury_rates)} treasury rate records for {year}-{month:02d}")
        else:
            print(f"Processed {len(treasury_rates)} treasury rate records for current month")
        return treasury_rates


# Backward compatibility function
def get_daily_treasury_rates(year: int = None, month: int = None) -> pd.DataFrame:
    """
    Legacy function for backward compatibility
    
    Args:
        year: Year for which rates are needed
        month: Month for which rates are needed
        
    Returns:
        DataFrame with treasury rates
    """
    scraper = TreasuryScraper()
    return scraper.get_daily_treasury_rates(year, month)
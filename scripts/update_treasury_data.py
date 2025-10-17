#!/usr/bin/env python3
"""
Script to update treasury rates data from US Treasury
Fetches and stores daily treasury bill rates
"""

import sys
import os
import argparse
from datetime import datetime, date

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scrapers.treasury import TreasuryScraper
from src.database import OptionsDatabase


def update_treasury_data(year: int = None, month: int = None, db_path: str = "data/options/market_data.db"):
    """
    Update treasury rates data for a specific month
    
    Args:
        year: Year to fetch (defaults to current year)
        month: Month to fetch (defaults to current month)
        db_path: Database path
    """
    if year is None or month is None:
        now = datetime.now()
        year = now.year
        month = now.month
    
    print(f"Updating treasury data for {year}-{month:02d}")
    print(f"Database: {db_path}")
    
    # Initialize scraper and database
    scraper = TreasuryScraper(rate_limit_delay=0.1)
    db = OptionsDatabase(db_path)
    
    try:
        # Fetch and process treasury data
        treasury_rates = scraper.fetch_and_process_month(year, month)
        
        if not treasury_rates:
            print(f"No treasury data found for {year}-{month:02d}")
            return 0
        
        # Insert into database (will overwrite overlapping days due to UNIQUE constraint)
        rows_inserted = db.insert_treasury_rates(treasury_rates)
        
        print(f" Successfully inserted {rows_inserted} treasury rate records")
        
        # Show summary
        latest_rates = db.get_latest_treasury_rates()
        if not latest_rates.empty:
            latest_date = latest_rates.iloc[0]['date']
            print(f"Latest treasury data: {latest_date}")
        
        return rows_inserted
        
    except Exception as e:
        print(f" Error updating treasury data: {e}")
        return 0


def update_current_month(db_path: str = "data/options/market_data.db"):
    """Update treasury data for current month"""
    now = datetime.now()
    return update_treasury_data(now.year, now.month, db_path)


def update_multiple_months(start_year: int, start_month: int, 
                          end_year: int, end_month: int, 
                          db_path: str = "data/options/market_data.db"):
    """
    Update treasury data for multiple months
    
    Args:
        start_year: Starting year
        start_month: Starting month
        end_year: Ending year
        end_month: Ending month
        db_path: Database path
    """
    print(f"Updating treasury data from {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
    
    total_inserted = 0
    current_year = start_year
    current_month = start_month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        try:
            rows_inserted = update_treasury_data(current_year, current_month, db_path)
            total_inserted += rows_inserted
            
            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
                
        except Exception as e:
            print(f" Error updating {current_year}-{current_month:02d}: {e}")
            # Continue with next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
    
    print(f" Total treasury records inserted: {total_inserted}")
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description='Update treasury rates data')
    parser.add_argument('--year', '-y', type=int, help='Year to fetch (defaults to current year)')
    parser.add_argument('--month', '-m', type=int, help='Month to fetch (defaults to current month)')
    parser.add_argument('--start-year', type=int, help='Start year for multi-month update')
    parser.add_argument('--start-month', type=int, help='Start month for multi-month update')
    parser.add_argument('--end-year', type=int, help='End year for multi-month update')
    parser.add_argument('--end-month', type=int, help='End month for multi-month update')
    parser.add_argument('--db-path', default='data/options/market_data.db', help='Database path')
    parser.add_argument('--current-month', action='store_true', help='Update current month only')
    
    args = parser.parse_args()
    
    try:
        if args.current_month:
            # Update current month
            rows_inserted = update_current_month(args.db_path)
        elif args.start_year and args.start_month and args.end_year and args.end_month:
            # Update multiple months
            rows_inserted = update_multiple_months(
                args.start_year, args.start_month,
                args.end_year, args.end_month,
                args.db_path
            )
        else:
            # Update specific month
            rows_inserted = update_treasury_data(args.year, args.month, args.db_path)
        
        if rows_inserted > 0:
            print(f" Treasury data update completed successfully!")
            return 0
        else:
            print(" No treasury data was inserted")
            return 1
            
    except Exception as e:
        print(f" Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

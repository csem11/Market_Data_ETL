#!/usr/bin/env python3
"""
Script to update options chain data from yfinance API
"""

import sys
import os
import argparse
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scrapers import YahooScraper
from src.database import OptionsDatabase, StockInfo


def load_sp500_symbols(csv_path: str = "data/sp500_companies.csv") -> list:
    """
    Load S&P 500 symbols from CSV file
    
    Args:
        csv_path: Path to S&P 500 companies CSV file
        
    Returns:
        List of stock symbols
    """
    try:
        df = pd.read_csv(csv_path)
        symbols = df['Symbol'].tolist()
        print(f"Loaded {len(symbols)} S&P 500 symbols")
        return symbols
    except Exception as e:
        print(f"Error loading S&P 500 symbols: {e}")
        return []


def update_single_symbol(symbol: str, db: OptionsDatabase, scraper: YahooScraper, 
                        max_expiration_dates: int = 15, fetch_stock_info: bool = True):
    """
    Update options data for a single symbol
    
    Args:
        symbol: Stock symbol
        db: Database instance
        scraper: Options scraper instance
        max_expiration_dates: Maximum number of expiration dates to fetch
        fetch_stock_info: Whether to fetch and store stock info
    """
    print(f"\n=== Processing {symbol} ===")
    
    try:
        # Fetch and store stock info
        if fetch_stock_info:
            print(f"Fetching stock info for {symbol}")
            stock_info = scraper.get_stock_info(symbol)
            if stock_info:
                success = db.insert_stock_info(stock_info)
                if success:
                    print(f"Stock info updated for {symbol}")
                else:
                    print(f"Failed to update stock info for {symbol}")
            else:
                print(f"No stock info found for {symbol}")
        
        # Fetch options data
        print(f"Fetching options data for {symbol}")
        options_data = scraper.get_multiple_expiration_dates(symbol, max_expiration_dates)
        
        if options_data:
            rows_inserted = db.insert_options_chain(options_data)
            print(f"Inserted {rows_inserted} options contracts for {symbol}")
        else:
            print(f"No options data found for {symbol}")
            
    except Exception as e:
        print(f"Error processing {symbol}: {e}")


def main():
    """Main function to update options data"""
    parser = argparse.ArgumentParser(description="Update options chain data from yfinance")
    parser.add_argument("--symbol", "-s", help="Single symbol to update")
    parser.add_argument("--symbols", "-l", nargs="+", help="List of symbols to update")
    parser.add_argument("--sp500", action="store_true", help="Update all S&P 500 symbols")
    parser.add_argument("--max-expiration-dates", "-e", type=int, default=3,
                       help="Maximum number of expiration dates per symbol")
    parser.add_argument("--db-path", default="data/options/market_data.db",
                       help="Path to SQLite database file")
    parser.add_argument("--rate-limit", "-r", type=float, default=0.1,
                       help="Rate limit delay between API calls (seconds)")
    parser.add_argument("--skip-stock-info", action="store_true",
                       help="Skip fetching stock information")
    parser.add_argument("--stats", action="store_true",
                       help="Show database statistics and exit")
    
    args = parser.parse_args()
    
    # Initialize database and scraper
    db = OptionsDatabase(args.db_path)
    scraper = YahooScraper(rate_limit_delay=args.rate_limit)
    
    # Show statistics if requested
    if args.stats:
        stats = db.get_database_stats()
        print("\n=== Database Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
        return 0
    
    # Determine symbols to process
    symbols_to_process = []
    
    if args.symbol:
        symbols_to_process = [args.symbol.upper()]
    elif args.symbols:
        symbols_to_process = [s.upper() for s in args.symbols]
    elif args.sp500:
        symbols_to_process = load_sp500_symbols()
        if not symbols_to_process:
            print("Error: Could not load S&P 500 symbols")
            return 1
    else:
        print("Error: Must specify --symbol, --symbols, or --sp500")
        return 1
    
    if not symbols_to_process:
        print("No symbols to process")
        return 1
    
    print(f"\n=== Starting Options Data Update ===")
    print(f"Symbols to process: {len(symbols_to_process)}")
    print(f"Max expiration dates per symbol: {args.max_expiration_dates}")
    print(f"Rate limit delay: {args.rate_limit}s")
    print(f"Database path: {args.db_path}")
    print(f"Fetch stock info: {not args.skip_stock_info}")
    
    # Process symbols
    start_time = datetime.now()
    successful_updates = 0
    
    for i, symbol in enumerate(symbols_to_process, 1):
        try:
            update_single_symbol(
                symbol, 
                db, 
                scraper, 
                args.max_expiration_dates, 
                not args.skip_stock_info
            )
            successful_updates += 1
            
        except KeyboardInterrupt:
            print(f"\nInterrupted by user after processing {i-1} symbols")
            break
        except Exception as e:
            print(f"Unexpected error processing {symbol}: {e}")
            continue
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n=== Update Complete ===")
    print(f"Successfully processed: {successful_updates}/{len(symbols_to_process)} symbols")
    print(f"Total time: {duration}")
    
    # Show final statistics
    stats = db.get_database_stats()
    print(f"\nFinal database statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    return 0


if __name__ == "__main__":
    exit(main())

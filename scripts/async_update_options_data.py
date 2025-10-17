#!/usr/bin/env python3
"""
Async script to update options chain data using aiohttp for faster concurrent requests
"""

import asyncio
import sys
import os
import argparse
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scrapers.async_options_scraper import AsyncOptionsScraper
from src.database import OptionsDatabase, StockInfo


def load_sp500_symbols(csv_path: str = "data/sp500_companies.csv") -> List[str]:
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


async def update_single_symbol(symbol: str, db: OptionsDatabase, scraper: AsyncOptionsScraper, 
                             max_expiration_dates: int = 30, fetch_stock_info: bool = True):
    """
    Update options data for a single symbol asynchronously
    
    Args:
        symbol: Stock symbol
        db: Database instance
        scraper: Async options scraper instance
        max_expiration_dates: Maximum number of expiration dates to fetch
        fetch_stock_info: Whether to fetch and store stock info
    """
    print(f"\n=== Processing {symbol} ===")
    
    try:
        import aiohttp
        
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Fetch and store stock info
            if fetch_stock_info:
                print(f"Fetching stock info for {symbol}")
                stock_info = await scraper.get_stock_info(session, symbol)
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
            options_data = await scraper.get_multiple_expiration_dates(session, symbol, max_expiration_dates)
            
            if options_data:
                rows_inserted = db.insert_options_chain(options_data)
                print(f"Inserted {rows_inserted} options contracts for {symbol}")
            else:
                print(f"No options data found for {symbol}")
                
    except Exception as e:
        print(f"Error processing {symbol}: {e}")


async def update_symbols_batch(symbols: List[str], db: OptionsDatabase, scraper: AsyncOptionsScraper,
                              max_expiration_dates: int = 30, fetch_stock_info: bool = True,
                              batch_size: int = 50):
    """
    Update options data for multiple symbols in batches using async operations
    
    Args:
        symbols: List of stock symbols
        db: Database instance
        scraper: Async options scraper instance
        max_expiration_dates: Maximum number of expiration dates per symbol
        fetch_stock_info: Whether to fetch and store stock info
        batch_size: Number of symbols to process in each batch
    """
    print(f"Starting batch processing of {len(symbols)} symbols in batches of {batch_size}")
    
    total_successful = 0
    
    # Process symbols in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        print(f"\n=== Processing Batch {batch_num}/{total_batches} ({len(batch)} symbols) ===")
        batch_start_time = datetime.now()
        
        try:
            if fetch_stock_info:
                # Get stock info for the batch
                print("Fetching stock info for batch...")
                stock_info_results = await scraper.get_stock_info_batch(batch)
                
                # Store stock info
                stock_info_inserted = 0
                for symbol, stock_info in stock_info_results.items():
                    if stock_info:
                        success = db.insert_stock_info(stock_info)
                        if success:
                            stock_info_inserted += 1
                
                print(f"Inserted stock info for {stock_info_inserted}/{len(batch)} symbols")
            
            # Get options data for the batch
            print("Fetching options data for batch...")
            options_data_results = await scraper.get_options_data_batch(batch, max_expiration_dates)
            
            # Store options data
            options_inserted = 0
            for symbol, options_data in options_data_results.items():
                if options_data:
                    rows_inserted = db.insert_options_chain(options_data)
                    options_inserted += rows_inserted
            
            batch_end_time = datetime.now()
            batch_duration = batch_end_time - batch_start_time
            
            print(f"Batch {batch_num} completed in {batch_duration}")
            print(f"Inserted {options_inserted} options contracts for {len([d for d in options_data_results.values() if d])} symbols")
            
            total_successful += len([d for d in options_data_results.values() if d])
            
        except Exception as e:
            print(f"Error processing batch {batch_num}: {e}")
            continue
    
    return total_successful


async def update_sp500_async(symbols: List[str], db: OptionsDatabase, scraper: AsyncOptionsScraper,
                           max_expiration_dates: int = 30, fetch_stock_info: bool = True):
    """
    Update options data for all S&P 500 symbols using the most efficient async method
    
    Args:
        symbols: List of S&P 500 symbols
        db: Database instance
        scraper: Async options scraper instance
        max_expiration_dates: Maximum number of expiration dates per symbol
        fetch_stock_info: Whether to fetch and store stock info
    """
    print(f"Starting async S&P 500 update for {len(symbols)} symbols...")
    start_time = datetime.now()
    
    try:
        # Use the most efficient method - concurrent fetching of both stock info and options data
        stock_info_results, options_data_results = await scraper.get_sp500_options_data(
            symbols, max_expiration_dates
        )
        
        # Store stock info
        if fetch_stock_info:
            stock_info_inserted = 0
            print("Storing stock info...")
            for symbol, stock_info in stock_info_results.items():
                if stock_info:
                    success = db.insert_stock_info(stock_info)
                    if success:
                        stock_info_inserted += 1
            print(f"Inserted stock info for {stock_info_inserted}/{len(symbols)} symbols")
        
        # Store options data
        options_inserted = 0
        symbols_with_options = 0
        print("Storing options data...")
        for symbol, options_data in options_data_results.items():
            if options_data:
                rows_inserted = db.insert_options_chain(options_data)
                options_inserted += rows_inserted
                symbols_with_options += 1
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n=== Async Update Complete ===")
        print(f"Successfully processed: {symbols_with_options}/{len(symbols)} symbols")
        print(f"Total options contracts inserted: {options_inserted}")
        print(f"Total time: {duration}")
        print(f"Average time per symbol: {duration.total_seconds() / len(symbols):.2f} seconds")
        
        return symbols_with_options, options_inserted
        
    except Exception as e:
        print(f"Error in async S&P 500 update: {e}")
        return 0, 0


async def main():
    """Main async function to update options data"""
    parser = argparse.ArgumentParser(description="Async update options chain data using aiohttp")
    parser.add_argument("--symbol", "-s", help="Single symbol to update")
    parser.add_argument("--symbols", "-l", nargs="+", help="List of symbols to update")
    parser.add_argument("--sp500", action="store_true", help="Update all S&P 500 symbols")
    parser.add_argument("--max-expiration-dates", "-e", type=int, default=30,
                       help="Maximum number of expiration dates per symbol")
    parser.add_argument("--db-path", default="data/options/market_data.db",
                       help="Path to SQLite database file")
    parser.add_argument("--rate-limit", "-r", type=float, default=10.0,
                       help="Rate limit per second")
    parser.add_argument("--max-concurrent", "-c", type=int, default=20,
                       help="Maximum concurrent requests")
    parser.add_argument("--skip-stock-info", action="store_true",
                       help="Skip fetching stock information")
    parser.add_argument("--stats", action="store_true",
                       help="Show database statistics and exit")
    parser.add_argument("--batch-size", "-b", type=int, default=50,
                       help="Batch size for processing symbols (when not using --sp500)")
    
    args = parser.parse_args()
    
    # Initialize database and scraper
    db = OptionsDatabase(args.db_path)
    scraper = AsyncOptionsScraper(
        rate_limit_per_second=args.rate_limit,
        max_concurrent_requests=args.max_concurrent,
        max_retries=3
    )
    
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
    
    print(f"\n=== Starting Async Options Data Update ===")
    print(f"Symbols to process: {len(symbols_to_process)}")
    print(f"Max expiration dates per symbol: {args.max_expiration_dates}")
    print(f"Rate limit: {args.rate_limit} requests/second")
    print(f"Max concurrent requests: {args.max_concurrent}")
    print(f"Database path: {args.db_path}")
    print(f"Fetch stock info: {not args.skip_stock_info}")
    
    # Process symbols
    start_time = datetime.now()
    
    try:
        if args.sp500 and len(symbols_to_process) > 100:
            # Use the most efficient method for large S&P 500 updates
            successful_updates, total_options = await update_sp500_async(
                symbols_to_process, 
                db, 
                scraper, 
                args.max_expiration_dates, 
                not args.skip_stock_info
            )
        elif len(symbols_to_process) == 1:
            # Single symbol
            await update_single_symbol(
                symbols_to_process[0], 
                db, 
                scraper, 
                args.max_expiration_dates, 
                not args.skip_stock_info
            )
            successful_updates = 1
        else:
            # Batch processing for smaller sets
            successful_updates = await update_symbols_batch(
                symbols_to_process, 
                db, 
                scraper, 
                args.max_expiration_dates, 
                not args.skip_stock_info,
                args.batch_size
            )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n=== Update Complete ===")
        print(f"Successfully processed: {successful_updates}/{len(symbols_to_process)} symbols")
        print(f"Total time: {duration}")
        print(f"Average time per symbol: {duration.total_seconds() / len(symbols_to_process):.2f} seconds")
        
    except KeyboardInterrupt:
        print(f"\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    
    # Show final statistics
    stats = db.get_database_stats()
    print(f"\nFinal database statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))

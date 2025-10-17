#!/usr/bin/env python3
"""
Main script to fetch options data for S&P 500 companies using async processing
"""

import asyncio
import sys
import os
import pandas as pd
from datetime import datetime
import argparse

# Add project root to path
sys.path.append(os.path.dirname(__file__))

from src.scrapers import HybridAsyncOptionsScraper
from src.database import OptionsDatabase


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


def load_index_etfs(csv_path: str = "data/index_etfs.csv") -> list:
    """
    Load index ETF symbols from CSV file
    
    Args:
        csv_path: Path to index ETFs CSV file
        
    Returns:
        List of ETF symbols
    """
    try:
        df = pd.read_csv(csv_path)
        symbols = df['Symbol'].tolist()
        print(f"Loaded {len(symbols)} index ETF symbols: {symbols}")
        return symbols
    except Exception as e:
        print(f"Error loading index ETFs: {e}")
        return []


async def fetch_options_data(symbols: list, 
                           max_expiration_dates: int = 30,
                           fetch_stock_info: bool = True,
                           rate_limit_delay: float = 0.05,
                           max_concurrent: int = 15,
                           batch_size: int = 100):
    """
    Fetch options data for symbols using async processing
    
    Args:
        symbols: List of symbols (stocks and/or ETFs)
        max_expiration_dates: Maximum number of expiration dates per symbol
        fetch_stock_info: Whether to fetch stock information
        rate_limit_delay: Delay between requests
        max_concurrent: Maximum concurrent requests
        batch_size: Number of symbols to process in each batch
    
    Returns:
        Tuple of (stock_info_results, options_data_results, timing_info)
    """
    print(f"\n=== Starting Options Data Fetch ===")
    print(f"Total symbols: {len(symbols)}")
    print(f"Max expiration dates per symbol: {max_expiration_dates}")
    print(f"Fetch stock info: {fetch_stock_info}")
    print(f"Rate limit delay: {rate_limit_delay}s")
    print(f"Max concurrent requests: {max_concurrent}")
    print(f"Batch size: {batch_size}")
    
    # Initialize scraper
    scraper = HybridAsyncOptionsScraper(
        rate_limit_delay=rate_limit_delay,
        max_concurrent_requests=max_concurrent
    )
    
    # Initialize database
    db = OptionsDatabase()
    
    start_time = datetime.now()
    total_stock_info_inserted = 0
    total_options_inserted = 0
    successful_symbols = 0
    
    # Process symbols in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        print(f"\n=== Processing Batch {batch_num}/{total_batches} ({len(batch)} symbols) ===")
        batch_start_time = datetime.now()
        
        try:
            if fetch_stock_info:
                # Fetch stock info for the batch
                print("Fetching stock info for batch...")
                stock_info_results = await scraper.get_stock_info_batch(batch)
                
                # Store stock info
                batch_stock_inserted = 0
                for symbol, stock_info in stock_info_results.items():
                    if stock_info:
                        success = db.insert_stock_info(stock_info)
                        if success:
                            batch_stock_inserted += 1
                
                print(f"Inserted stock info for {batch_stock_inserted}/{len(batch)} symbols")
                total_stock_info_inserted += batch_stock_inserted
            
            # Fetch options data for the batch
            print("Fetching options data for batch...")
            options_data_results = await scraper.get_options_data_batch(batch, max_expiration_dates)
            
            # Store options data
            batch_options_inserted = 0
            batch_successful_symbols = 0
            for symbol, options_data in options_data_results.items():
                if options_data:
                    rows_inserted = db.insert_options_chain(options_data)
                    batch_options_inserted += rows_inserted
                    batch_successful_symbols += 1
            
            batch_end_time = datetime.now()
            batch_duration = batch_end_time - batch_start_time
            
            print(f"Batch {batch_num} completed in {batch_duration}")
            print(f"Inserted {batch_options_inserted} options contracts for {batch_successful_symbols} symbols")
            
            total_options_inserted += batch_options_inserted
            successful_symbols += batch_successful_symbols
            
        except Exception as e:
            print(f"Error processing batch {batch_num}: {e}")
            continue
    
    end_time = datetime.now()
    total_duration = end_time - start_time
    
    # Compile results
    timing_info = {
        'total_duration': total_duration,
        'successful_symbols': successful_symbols,
        'total_symbols': len(symbols),
        'total_stock_info_inserted': total_stock_info_inserted,
        'total_options_inserted': total_options_inserted,
        'average_time_per_symbol': total_duration.total_seconds() / len(symbols) if symbols else 0
    }
    
    return None, None, timing_info


async def main():
    """Main async function"""
    parser = argparse.ArgumentParser(description="Fetch options data for stocks and ETFs using async processing")
    parser.add_argument("--max-expiration-dates", "-e", type=int, default=30,
                       help="Maximum number of expiration dates per symbol")
    parser.add_argument("--db-path", default="data/options/market_data.db",
                       help="Path to SQLite database file")
    parser.add_argument("--rate-limit", "-r", type=float, default=0.05,
                       help="Rate limit delay between requests (seconds)")
    parser.add_argument("--max-concurrent", "-c", type=int, default=15,
                       help="Maximum concurrent requests")
    parser.add_argument("--batch-size", "-b", type=int, default=100,
                       help="Batch size for processing symbols")
    parser.add_argument("--skip-stock-info", action="store_true",
                       help="Skip fetching stock information")
    parser.add_argument("--symbols", "-s", nargs="+",
                       help="Specific symbols to fetch (instead of all S&P 500)")
    parser.add_argument("--limit", "-l", type=int,
                       help="Limit number of symbols to process (for testing)")
    parser.add_argument("--include-etfs", action="store_true",
                       help="Include index ETFs (SPY, QQQ, IWM, DIA)")
    parser.add_argument("--etfs-only", action="store_true",
                       help="Process only index ETFs")
    parser.add_argument("--stats", action="store_true",
                       help="Show database statistics before and after")
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Show statistics if requested
    if args.stats:
        print("\n=== Database Statistics (Before) ===")
        stats = db.get_database_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
    
    # Determine symbols to process
    if args.etfs_only:
        # Process only index ETFs
        symbols_to_process = load_index_etfs()
        if not symbols_to_process:
            print("Error: Could not load index ETFs")
            return 1
        print(f"Processing only index ETFs: {symbols_to_process}")
    elif args.symbols:
        # Process specific symbols
        symbols_to_process = [s.upper() for s in args.symbols]
        print(f"Processing specific symbols: {symbols_to_process}")
    else:
        # Load S&P 500 symbols
        symbols_to_process = load_sp500_symbols()
        if not symbols_to_process:
            print("Error: Could not load S&P 500 symbols")
            return 1
        
        # Add index ETFs if requested
        if args.include_etfs:
            etf_symbols = load_index_etfs()
            if etf_symbols:
                symbols_to_process.extend(etf_symbols)
                print(f"Added {len(etf_symbols)} index ETFs to S&P 500 symbols")
        
        # Apply limit if specified
        if args.limit:
            symbols_to_process = symbols_to_process[:args.limit]
            print(f"Limited to first {args.limit} symbols")
    
    if not symbols_to_process:
        print("No symbols to process")
        return 1
    
    try:
        # Fetch options data
        _, _, timing_info = await fetch_options_data(
            symbols_to_process,
            max_expiration_dates=args.max_expiration_dates,
            fetch_stock_info=not args.skip_stock_info,
            rate_limit_delay=args.rate_limit,
            max_concurrent=args.max_concurrent,
            batch_size=args.batch_size
        )
        
        # Print results
        print(f"\n=== Fetch Complete ===")
        print(f"Successfully processed: {timing_info['successful_symbols']}/{timing_info['total_symbols']} symbols")
        print(f"Stock info inserted: {timing_info['total_stock_info_inserted']}")
        print(f"Options contracts inserted: {timing_info['total_options_inserted']}")
        print(f"Total time: {timing_info['total_duration']}")
        print(f"Average time per symbol: {timing_info['average_time_per_symbol']:.2f} seconds")
        
        # Calculate performance metrics
        if timing_info['successful_symbols'] > 0:
            contracts_per_second = timing_info['total_options_inserted'] / timing_info['total_duration'].total_seconds()
            print(f"Options contracts per second: {contracts_per_second:.1f}")
            
            symbols_per_minute = (timing_info['successful_symbols'] / timing_info['total_duration'].total_seconds()) * 60
            print(f"Symbols processed per minute: {symbols_per_minute:.1f}")
        
    except KeyboardInterrupt:
        print(f"\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Show final statistics if requested
    if args.stats:
        print(f"\n=== Database Statistics (After) ===")
        stats = db.get_database_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
    
    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))

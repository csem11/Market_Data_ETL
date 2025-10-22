#!/usr/bin/env python3
"""
YTD Backfill Script for Market Data ETL
Backfills stock and index prices from January 1st of current year to present
"""

import sys
import os
import argparse
from datetime import datetime
from typing import List

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase
from scripts.collect_data import collect_stock_data, load_indices_symbols


def clear_existing_price_data(db: OptionsDatabase, symbols: List[str]) -> int:
    """
    Clear existing price data for given symbols to ensure fresh YTD data
    
    Args:
        db: Database instance
        symbols: List of symbols to clear
        
    Returns:
        Number of records deleted
    """
    print(f"\n Clearing existing price data for {len(symbols)} symbols...")
    
    total_deleted = 0
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        for symbol in symbols:
            cursor.execute('DELETE FROM stock_prices WHERE symbol = ?', (symbol,))
            deleted_count = cursor.rowcount
            total_deleted += deleted_count
            if deleted_count > 0:
                print(f"  ✓ Cleared {deleted_count} records for {symbol}")
        
        conn.commit()
    
    print(f"Total records deleted: {total_deleted}")
    return total_deleted


def backfill_ytd_data(symbols: List[str], db: OptionsDatabase, 
                     rate_limit: float = 0.1, clear_existing: bool = True) -> dict:
    """
    Backfill YTD data for given symbols
    
    Args:
        symbols: List of stock symbols
        db: Database instance
        rate_limit: Rate limit delay
        clear_existing: Whether to clear existing data first
        
    Returns:
        Dictionary with backfill statistics
    """
    print(f"\n Starting YTD backfill for {len(symbols)} symbols...")
    print(f"Date range: 2025-01-01 to {datetime.now().strftime('%Y-%m-%d')}")
    
    # Clear existing data if requested
    if clear_existing:
        clear_existing_price_data(db, symbols)
    
    # Collect YTD stock data
    stats = collect_stock_data(symbols, db, rate_limit, 15, "ytd")
    
    return stats


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="YTD Backfill Script for Market Data")
    
    # Data sources
    parser.add_argument("--sp500", action="store_true", help="Include S&P 500 companies")
    parser.add_argument("--etfs", action="store_true", help="Include index ETFs")
    parser.add_argument("--indices", action="store_true", help="Include primary indices")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to process")
    parser.add_argument("--all", action="store_true", help="Include all data sources")
    
    # Configuration
    parser.add_argument("--db-path", default="data/options/market_data.db", help="Database path")
    parser.add_argument("--rate-limit", type=float, default=0.1, help="Rate limit delay")
    parser.add_argument("--limit", type=int, help="Limit number of symbols (for testing)")
    parser.add_argument("--no-clear", action="store_true", help="Don't clear existing data")
    parser.add_argument("--batch-size", type=int, default=50, help="Process symbols in batches")
    
    # Output
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Show database statistics if requested
    if args.stats:
        print(" DATABASE STATISTICS (Before Backfill)")
        print("="*60)
        stats = db.get_database_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("="*60)
        return 0
    
    # Determine symbols to process
    symbols = []
    
    if args.symbols:
        symbols = args.symbols
    elif args.all or (not args.sp500 and not args.etfs and not args.indices):
        # Default to all data sources
        try:
            import pandas as pd
            # Load S&P 500 symbols
            sp500_df = pd.read_csv("data/sp500_companies.csv")
            symbols.extend(sp500_df['Symbol'].tolist())
            print(f"Loaded {len(sp500_df)} S&P 500 symbols")
            
            # Load ETF symbols
            etf_df = pd.read_csv("data/index_etfs.csv")
            symbols.extend(etf_df['Symbol'].tolist())
            print(f"Loaded {len(etf_df)} ETF symbols")
            
            # Load indices symbols
            indices_symbols = load_indices_symbols()
            symbols.extend(indices_symbols)
            print(f"Loaded {len(indices_symbols)} indices symbols")
            
        except Exception as e:
            print(f"Error loading symbols: {e}")
            return 1
    else:
        if args.sp500:
            try:
                import pandas as pd
                sp500_df = pd.read_csv("data/sp500_companies.csv")
                symbols.extend(sp500_df['Symbol'].tolist())
                print(f"Loaded {len(sp500_df)} S&P 500 symbols")
            except Exception as e:
                print(f"Error loading S&P 500 symbols: {e}")
                return 1
        
        if args.etfs:
            try:
                import pandas as pd
                etf_df = pd.read_csv("data/index_etfs.csv")
                symbols.extend(etf_df['Symbol'].tolist())
                print(f"Loaded {len(etf_df)} ETF symbols")
            except Exception as e:
                print(f"Error loading ETF symbols: {e}")
                return 1
        
        if args.indices:
            indices_symbols = load_indices_symbols()
            symbols.extend(indices_symbols)
            print(f"Loaded {len(indices_symbols)} indices symbols")
    
    # Remove duplicates
    symbols = list(set(symbols))
    
    # Apply limit if specified
    if args.limit:
        symbols = symbols[:args.limit]
        print(f"Limited to {len(symbols)} symbols")
    
    print(f"Total symbols to backfill: {len(symbols)}")
    
    # Process in batches if specified
    if args.batch_size and len(symbols) > args.batch_size:
        print(f"Processing in batches of {args.batch_size}")
        
        total_stats = {
            'stock_info_collected': 0,
            'stock_prices_collected': 0,
            'errors': 0
        }
        
        for i in range(0, len(symbols), args.batch_size):
            batch = symbols[i:i + args.batch_size]
            batch_num = (i // args.batch_size) + 1
            total_batches = (len(symbols) + args.batch_size - 1) // args.batch_size
            
            print(f"\n Processing batch {batch_num}/{total_batches} ({len(batch)} symbols)")
            
            batch_stats = backfill_ytd_data(
                batch, db, args.rate_limit, not args.no_clear
            )
            
            # Update total stats
            for key in total_stats:
                total_stats[key] += batch_stats.get(key, 0)
            
            # Don't clear data for subsequent batches
            args.no_clear = True
        
        stats = total_stats
    else:
        stats = backfill_ytd_data(
            symbols, db, args.rate_limit, not args.no_clear
        )
    
    # Print final statistics
    print(f"\n YTD BACKFILL COMPLETED")
    print("="*60)
    print(f"Stock info records: {stats['stock_info_collected']}")
    print(f"Stock price records: {stats['stock_prices_collected']}")
    print(f"Total errors: {stats['errors']}")
    print("="*60)
    
    # Show final database statistics
    print(f"\n DATABASE STATISTICS (After Backfill)")
    print("="*60)
    db_stats = db.get_database_stats()
    for key, value in db_stats.items():
        print(f"{key}: {value}")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    exit(main())

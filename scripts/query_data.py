#!/usr/bin/env python3
"""
General data query script for market data ETL
Handles querying options, stock prices, earnings, treasury data, and metrics
"""

import sys
import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase


def query_options_data(db: OptionsDatabase, symbol: str = None, 
                       expiration_date: str = None, option_type: str = None,
                       min_volume: int = None, limit: int = 20) -> pd.DataFrame:
    """
    Query options chain data
    
    Args:
        db: Database instance
        symbol: Stock symbol to query
        expiration_date: Expiration date filter (YYYY-MM-DD)
        option_type: Option type filter (call/put)
        min_volume: Minimum volume threshold
        limit: Maximum number of results
        
    Returns:
        DataFrame with options data
    """
    if min_volume:
        return db.get_high_volume_options(min_volume=min_volume, limit=limit)
    else:
        return db.get_options_chain(
            symbol=symbol,
            expiration_date=expiration_date,
            option_type=option_type
        )


def query_stock_data(db: OptionsDatabase, symbol: str = None, 
                     start_date: str = None, end_date: str = None,
                     limit: int = 100) -> pd.DataFrame:
    """
    Query stock price data
    
    Args:
        db: Database instance
        symbol: Stock symbol to query
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        limit: Maximum number of results
        
    Returns:
        DataFrame with stock price data
    """
    return db.get_stock_prices(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )


def query_earnings_data(db: OptionsDatabase, symbol: str = None,
                        start_date: str = None, end_date: str = None,
                        limit: int = 50) -> pd.DataFrame:
    """
    Query earnings dates data
    
    Args:
        db: Database instance
        symbol: Stock symbol to query
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        limit: Maximum number of results
        
    Returns:
        DataFrame with earnings data
    """
    return db.get_earnings_dates(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )


def query_treasury_data(db: OptionsDatabase, start_date: str = None,
                        end_date: str = None, limit: int = 100) -> pd.DataFrame:
    """
    Query treasury rates data
    
    Args:
        db: Database instance
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        limit: Maximum number of results
        
    Returns:
        DataFrame with treasury data
    """
    return db.get_treasury_rates(
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )


def query_options_metrics(db: OptionsDatabase, symbol: str = None,
                          expiration_date: str = None, option_type: str = None,
                          moneyness: str = None, min_volume: int = None,
                          limit: int = 20) -> pd.DataFrame:
    """
    Query options metrics data
    
    Args:
        db: Database instance
        symbol: Stock symbol to query
        expiration_date: Expiration date filter (YYYY-MM-DD)
        option_type: Option type filter (call/put)
        moneyness: Moneyness filter (ITM/ATM/OTM)
        min_volume: Minimum volume threshold
        limit: Maximum number of results
        
    Returns:
        DataFrame with options metrics
    """
    if min_volume:
        return db.get_high_volume_options(min_volume=min_volume, limit=limit)
    elif moneyness:
        return db.get_options_by_moneyness(symbol=symbol, moneyness=moneyness)
    else:
        return db.get_option_metrics(
            symbol=symbol,
            expiration_date=expiration_date,
            option_type=option_type,
            moneyness=moneyness,
            limit=limit
        )


def query_stock_info(db: OptionsDatabase, symbol: str = None,
                     sector: str = None, limit: int = 100) -> pd.DataFrame:
    """
    Query stock information data
    
    Args:
        db: Database instance
        symbol: Stock symbol to query
        sector: Sector filter
        limit: Maximum number of results
        
    Returns:
        DataFrame with stock info
    """
    return db.get_stock_info(
        symbol=symbol,
        sector=sector,
        limit=limit
    )


def print_dataframe_summary(df: pd.DataFrame, data_type: str):
    """Print summary of DataFrame results"""
    if df.empty:
        print(f" No {data_type} data found")
        return
    
    print(f"\n {data_type.upper()} DATA SUMMARY")
    print(f"{'='*60}")
    print(f"Records found: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Show first few rows
    print(f"\nFirst {min(5, len(df))} records:")
    print(df.head().to_string(index=False))
    
    # Show summary statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print(f"\n Summary Statistics:")
        print(df[numeric_cols].describe().to_string())


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="General market data query script")
    
    # Data type arguments
    parser.add_argument('--options', action='store_true', help='Query options data')
    parser.add_argument('--stock-prices', action='store_true', help='Query stock price data')
    parser.add_argument('--earnings', action='store_true', help='Query earnings data')
    parser.add_argument('--treasury', action='store_true', help='Query treasury data')
    parser.add_argument('--metrics', action='store_true', help='Query options metrics')
    parser.add_argument('--stock-info', action='store_true', help='Query stock info')
    parser.add_argument('--all', action='store_true', help='Query all data types')
    
    # Common filters
    parser.add_argument('--symbol', '-s', help='Stock symbol to query')
    parser.add_argument('--expiration', '-e', help='Expiration date (YYYY-MM-DD)')
    parser.add_argument('--type', '-t', choices=['call', 'put'], help='Option type')
    parser.add_argument('--moneyness', '-m', choices=['ITM', 'ATM', 'OTM'], help='Moneyness filter')
    parser.add_argument('--min-volume', '-v', type=int, help='Minimum volume threshold')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--sector', help='Sector filter for stock info')
    parser.add_argument('--limit', '-l', type=int, default=20, help='Maximum number of results')
    
    # Database arguments
    parser.add_argument('--db-path', default="data/options/market_data.db", help='Database path')
    
    # Output arguments
    parser.add_argument('--output', '-o', help='Output file path (CSV)')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Show database statistics if requested
    if args.stats:
        print(" DATABASE STATISTICS")
        print("="*60)
        stats = db.get_database_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("="*60)
        return 0
    
    # Determine what data to query
    query_options = args.options or args.all
    query_stock_prices = args.stock_prices or args.all
    query_earnings = args.earnings or args.all
    query_treasury = args.treasury or args.all
    query_metrics = args.metrics or args.all
    query_stock_info = args.stock_info or args.all
    
    # If no specific data type specified, show help
    if not any([query_options, query_stock_prices, query_earnings, 
                query_treasury, query_metrics, query_stock_info]):
        print(" No data type specified. Use --options, --stock-prices, --earnings, --treasury, --metrics, --stock-info, or --all")
        parser.print_help()
        return 1
    
    try:
        # Query options data
        if query_options:
            print(" Querying options data...")
            df = query_options_data(
                db, symbol=args.symbol, expiration_date=args.expiration,
                option_type=args.type, min_volume=args.min_volume, limit=args.limit
            )
            print_dataframe_summary(df, "Options")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_options.csv", index=False)
                print(f" Saved to {args.output}_options.csv")
        
        # Query stock price data
        if query_stock_prices:
            print("\n Querying stock price data...")
            df = query_stock_data(
                db, symbol=args.symbol, start_date=args.start_date,
                end_date=args.end_date, limit=args.limit
            )
            print_dataframe_summary(df, "Stock Prices")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_stock_prices.csv", index=False)
                print(f" Saved to {args.output}_stock_prices.csv")
        
        # Query earnings data
        if query_earnings:
            print("\n Querying earnings data...")
            df = query_earnings_data(
                db, symbol=args.symbol, start_date=args.start_date,
                end_date=args.end_date, limit=args.limit
            )
            print_dataframe_summary(df, "Earnings")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_earnings.csv", index=False)
                print(f" Saved to {args.output}_earnings.csv")
        
        # Query treasury data
        if query_treasury:
            print("\n Querying treasury data...")
            df = query_treasury_data(
                db, start_date=args.start_date, end_date=args.end_date, limit=args.limit
            )
            print_dataframe_summary(df, "Treasury")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_treasury.csv", index=False)
                print(f" Saved to {args.output}_treasury.csv")
        
        # Query options metrics
        if query_metrics:
            print("\n Querying options metrics...")
            df = query_options_metrics(
                db, symbol=args.symbol, expiration_date=args.expiration,
                option_type=args.type, moneyness=args.moneyness,
                min_volume=args.min_volume, limit=args.limit
            )
            print_dataframe_summary(df, "Options Metrics")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_metrics.csv", index=False)
                print(f" Saved to {args.output}_metrics.csv")
        
        # Query stock info
        if query_stock_info:
            print("\n Querying stock info...")
            df = query_stock_info(
                db, symbol=args.symbol, sector=args.sector, limit=args.limit
            )
            print_dataframe_summary(df, "Stock Info")
            
            if args.output and not df.empty:
                df.to_csv(f"{args.output}_stock_info.csv", index=False)
                print(f" Saved to {args.output}_stock_info.csv")
        
        print(f"\n Query completed successfully!")
        
    except Exception as e:
        print(f" Error during query: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

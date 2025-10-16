#!/usr/bin/env python3
"""
Script to query options chain data from SQLite database
"""

import sys
import os
import argparse
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase


def query_options_by_symbol(db: OptionsDatabase, symbol: str, expiration_date: str = None):
    """
    Query options data for a specific symbol
    
    Args:
        db: Database instance
        symbol: Stock symbol
        expiration_date: Optional expiration date filter
    """
    print(f"\n=== Options Data for {symbol} ===")
    
    # Show stock info first
    stock_info = db.get_stock_info(symbol)
    if stock_info:
        print(f"Stock Information:")
        print(f"  Company: {stock_info.get('company_name', 'N/A')}")
        print(f"  Current Price: ${stock_info.get('current_price', 'N/A')}")
        market_cap = stock_info.get('market_cap')
        if market_cap:
            print(f"  Market Cap: ${market_cap:,.0f}")
        else:
            print(f"  Market Cap: N/A")
        print(f"  Effective Date: {stock_info.get('eff_date', 'N/A')}")
        print(f"  Sector: {stock_info.get('sector', 'N/A')}")
        print(f"  Industry: {stock_info.get('industry', 'N/A')}")
    
    df = db.get_options_chain(symbol, expiration_date)
    
    if df.empty:
        print("\nNo options data found")
        return
    
    print(f"\nFound {len(df)} options contracts")
    
    # Show summary statistics
    if not df.empty:
        print(f"\nSummary:")
        print(f"  Expiration dates: {df['expiration_date'].nunique()}")
        print(f"  Strike price range: ${df['strike_price'].min():.2f} - ${df['strike_price'].max():.2f}")
        print(f"  Call contracts: {len(df[df['option_type'] == 'call'])}")
        print(f"  Put contracts: {len(df[df['option_type'] == 'put'])}")
        
        # Show recent data
        print(f"\nRecent options data (first 10 rows):")
        display_columns = ['symbol', 'expiration_date', 'strike_price', 'option_type', 
                          'bid', 'ask', 'last_price', 'volume', 'open_interest']
        available_columns = [col for col in display_columns if col in df.columns]
        print(df[available_columns].head(10).to_string(index=False))


def query_available_symbols(db: OptionsDatabase):
    """Query available symbols in the database"""
    print("\n=== Available Symbols ===")
    
    symbols = db.get_available_symbols()
    if symbols:
        print(f"Found {len(symbols)} symbols with options data:")
        for i, symbol in enumerate(sorted(symbols), 1):
            print(f"  {i:3d}. {symbol}")
    else:
        print("No symbols found in database")


def query_expiration_dates(db: OptionsDatabase, symbol: str):
    """Query available expiration dates for a symbol"""
    print(f"\n=== Expiration Dates for {symbol} ===")
    
    dates = db.get_available_expiration_dates(symbol)
    if dates:
        print(f"Found {len(dates)} expiration dates:")
        for i, date in enumerate(dates, 1):
            print(f"  {i:2d}. {date}")
    else:
        print(f"No expiration dates found for {symbol}")


def query_high_volume_options(db: OptionsDatabase, min_volume: int = 100, limit: int = 20):
    """Query options with high volume"""
    print(f"\n=== High Volume Options (volume >= {min_volume}) ===")
    
    with db.get_connection() as conn:
        query = """
            SELECT symbol, expiration_date, strike_price, option_type, 
                   last_price, volume, open_interest, created_at
            FROM options_chain 
            WHERE volume >= ?
            ORDER BY volume DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(min_volume, limit))
        
        if df.empty:
            print(f"No options found with volume >= {min_volume}")
        else:
            print(f"Found {len(df)} high-volume options:")
            print(df.to_string(index=False))


def query_options_by_greeks(db: OptionsDatabase, greek: str, min_value: float = None, 
                           max_value: float = None, limit: int = 20):
    """Query options filtered by Greek values"""
    print(f"\n=== Options by {greek.title()} ===")
    
    if greek not in ['delta', 'gamma', 'theta', 'vega', 'rho']:
        print(f"Invalid Greek: {greek}. Valid options: delta, gamma, theta, vega, rho")
        return
    
    with db.get_connection() as conn:
        query = f"""
            SELECT symbol, expiration_date, strike_price, option_type, 
                   last_price, {greek}, volume, open_interest
            FROM options_chain 
            WHERE {greek} IS NOT NULL
        """
        params = []
        
        if min_value is not None:
            query += f" AND {greek} >= ?"
            params.append(min_value)
        
        if max_value is not None:
            query += f" AND {greek} <= ?"
            params.append(max_value)
        
        query += f" ORDER BY ABS({greek}) DESC LIMIT ?"
        params.append(limit)
        
        df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            print(f"No options found with specified {greek} criteria")
        else:
            print(f"Found {len(df)} options:")
            print(df.to_string(index=False))


def main():
    """Main function to query options data"""
    parser = argparse.ArgumentParser(description="Query options chain data from SQLite database")
    parser.add_argument("--db-path", default="data/options/options_data.db",
                       help="Path to SQLite database file")
    parser.add_argument("--symbol", "-s", help="Symbol to query")
    parser.add_argument("--expiration-date", "-e", help="Expiration date filter (YYYY-MM-DD)")
    parser.add_argument("--list-symbols", action="store_true", help="List all available symbols")
    parser.add_argument("--list-expirations", action="store_true", help="List expiration dates for symbol")
    parser.add_argument("--high-volume", type=int, metavar="MIN_VOLUME", 
                       help="Query high volume options (specify minimum volume)")
    parser.add_argument("--greek", choices=['delta', 'gamma', 'theta', 'vega', 'rho'],
                       help="Query options by Greek value")
    parser.add_argument("--min-greek", type=float, help="Minimum Greek value")
    parser.add_argument("--max-greek", type=float, help="Maximum Greek value")
    parser.add_argument("--limit", type=int, default=20, help="Limit number of results")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Show statistics if requested
    if args.stats:
        stats = db.get_database_stats()
        print("=== Database Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
        return 0
    
    # List symbols if requested
    if args.list_symbols:
        query_available_symbols(db)
        return 0
    
    # List expiration dates if requested
    if args.list_expirations:
        if not args.symbol:
            print("Error: --symbol is required with --list-expirations")
            return 1
        query_expiration_dates(db, args.symbol.upper())
        return 0
    
    # Query high volume options
    if args.high_volume:
        query_high_volume_options(db, args.high_volume, args.limit)
        return 0
    
    # Query by Greek values
    if args.greek:
        query_options_by_greeks(db, args.greek, args.min_greek, args.max_greek, args.limit)
        return 0
    
    # Query options by symbol (default behavior)
    if args.symbol:
        query_options_by_symbol(db, args.symbol.upper(), args.expiration_date)
    else:
        print("Error: Must specify --symbol or use one of the query options")
        print("Use --help for available options")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

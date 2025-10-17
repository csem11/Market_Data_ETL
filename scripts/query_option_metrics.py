#!/usr/bin/env python3
"""
Script to query option metrics data from the database
"""

import sys
import os
import argparse
import pandas as pd

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase


def query_option_metrics(symbol: str, expiration_date: str = None, option_type: str = None, 
                        moneyness: str = None, min_volume: int = None):
    """Query option metrics with various filters"""
    db = OptionsDatabase()
    
    if min_volume:
        # Use high volume options query
        df = db.get_high_volume_options(min_volume=min_volume, limit=50)
        if symbol:
            df = df[df['symbol'] == symbol]
    else:
        # Use standard option metrics query
        df = db.get_option_metrics(
            symbol=symbol,
            expiration_date=expiration_date,
            option_type=option_type,
            moneyness=moneyness
        )
    
    return df


def main():
    parser = argparse.ArgumentParser(description='Query option metrics data')
    parser.add_argument('--symbol', '-s', required=True, help='Stock symbol to query')
    parser.add_argument('--expiration', '-e', help='Expiration date (YYYY-MM-DD)')
    parser.add_argument('--type', '-t', choices=['call', 'put'], help='Option type (call or put)')
    parser.add_argument('--moneyness', '-m', choices=['ITM', 'ATM', 'OTM'], help='Moneyness filter')
    parser.add_argument('--min-volume', '-v', type=int, help='Minimum volume threshold')
    parser.add_argument('--limit', '-l', type=int, default=20, help='Maximum number of results')
    parser.add_argument('--db-path', default='data/options/market_data.db', help='Database path')
    
    args = parser.parse_args()
    
    # Override database path if provided
    if args.db_path != 'data/options/market_data.db':
        os.environ['DATABASE_PATH'] = args.db_path
    
    try:
        df = query_option_metrics(
            symbol=args.symbol,
            expiration_date=args.expiration,
            option_type=args.type,
            moneyness=args.moneyness,
            min_volume=args.min_volume
        )
        
        if df.empty:
            print(f"No option metrics found for {args.symbol}")
            return
        
        # Limit results
        df = df.head(args.limit)
        
        print(f"\n=== Option Metrics for {args.symbol} ===")
        print(f"Found {len(df)} records\n")
        
        # Display key columns
        display_cols = ['symbol', 'expiration_date', 'strike_price', 'option_type', 
                        'current_price', 'option_price', 'moneyness', 'volume', 
                        'implied_volatility', 'delta', 'gamma', 'theta', 'vega']
        
        # Only show columns that exist in the dataframe
        available_cols = [col for col in display_cols if col in df.columns]
        print(df[available_cols].to_string(index=False))
        
        # Show summary statistics if we have numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            print(f"\n=== Summary Statistics ===")
            print(df[numeric_cols].describe())
        
    except Exception as e:
        print(f"Error querying option metrics: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Main orchestration script for market data ETL
Coordinates data collection, processing, and analysis
"""

import asyncio
import sys
import os
import argparse
from datetime import datetime
import subprocess

# Add project root to path
sys.path.append(os.path.dirname(__file__))

from src.database import OptionsDatabase


def run_script(script_path: str, args: list = None) -> int:
    """
    Run a script with given arguments
    
    Args:
        script_path: Path to script to run
        args: List of arguments to pass to script
        
    Returns:
        Exit code of the script
    """
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    
    print(f" Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(f"Warnings: {result.stderr}")
        return 0
    except subprocess.CalledProcessError as e:
        print(f" Error running {script_path}: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return e.returncode


def show_database_stats(db_path: str = "data/options/market_data.db"):
    """Show database statistics"""
    print("\n DATABASE STATISTICS")
    print("="*60)
    try:
        db = OptionsDatabase(db_path)
        stats = db.get_database_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"Error getting database stats: {e}")
    print("="*60)


def main():
    """Main orchestration function"""
    parser = argparse.ArgumentParser(description="Market Data ETL Orchestration")
    
    # Data collection modes
    parser.add_argument("--collect-all", action="store_true", 
                       help="Collect all data types (stock, options, treasury, metrics, indices)")
    parser.add_argument("--collect-stock", action="store_true", 
                       help="Collect stock data (info, prices, earnings)")
    parser.add_argument("--collect-options", action="store_true", 
                       help="Collect options data")
    parser.add_argument("--collect-treasury", action="store_true", 
                       help="Collect treasury data")
    parser.add_argument("--calculate-metrics", action="store_true", 
                       help="Calculate options metrics")
    
    # Data sources
    parser.add_argument("--sp500", action="store_true", help="Include S&P 500 companies")
    parser.add_argument("--etfs", action="store_true", help="Include index ETFs")
    parser.add_argument("--indices", action="store_true", help="Include primary indices")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to process")
    parser.add_argument("--limit", type=int, help="Limit number of symbols (for testing)")
    
    # Query modes
    parser.add_argument("--query-all", action="store_true", help="Query all data types")
    parser.add_argument("--query-options", action="store_true", help="Query options data")
    parser.add_argument("--query-stock", action="store_true", help="Query stock data")
    parser.add_argument("--query-stock-info", action="store_true", help="Query stock info")
    parser.add_argument("--query-treasury", action="store_true", help="Query treasury data")
    parser.add_argument("--query-metrics", action="store_true", help="Query options metrics")
    
    # Configuration
    parser.add_argument("--db-path", default="data/options/market_data.db", help="Database path")
    parser.add_argument("--rate-limit", type=float, default=0.1, help="Rate limit delay")
    parser.add_argument("--max-concurrent", type=int, default=15, help="Max concurrent requests")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    parser.add_argument("--max-expiration-dates", type=int, default=30, help="Max expiration dates")
    parser.add_argument("--price-period", default="ytd", 
                       choices=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
                       help="Period for stock price data collection (default: ytd for year-to-date)")
    
    # Treasury specific
    parser.add_argument("--treasury-year", type=int, help="Year for treasury data")
    parser.add_argument("--treasury-month", type=int, help="Month for treasury data")
    
    # Query filters
    parser.add_argument("--symbol", "-s", help="Symbol to query")
    parser.add_argument("--expiration", "-e", help="Expiration date filter")
    parser.add_argument("--type", "-t", choices=['call', 'put'], help="Option type filter")
    parser.add_argument("--moneyness", "-m", choices=['ITM', 'ATM', 'OTM'], help="Moneyness filter")
    parser.add_argument("--min-volume", "-v", type=int, help="Minimum volume threshold")
    parser.add_argument("--start-date", help="Start date filter")
    parser.add_argument("--end-date", help="End date filter")
    parser.add_argument("--limit-results", type=int, default=20, help="Limit query results")
    
    # Output
    parser.add_argument("--output", "-o", help="Output file prefix")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    
    args = parser.parse_args()
    
    # Show database statistics if requested
    if args.stats:
        show_database_stats(args.db_path)
        if not any([args.collect_all, args.collect_stock, args.collect_options, 
                   args.collect_treasury, args.calculate_metrics, args.query_all,
                   args.query_options, args.query_stock, args.query_stock_info, args.query_treasury, args.query_metrics]):
            return 0
    
    # Determine what to do
    collect_data = any([args.collect_all, args.collect_stock, args.collect_options, 
                       args.collect_treasury, args.calculate_metrics])
    query_data = any([args.query_all, args.query_options, args.query_stock, 
                     args.query_stock_info, args.query_treasury, args.query_metrics])
    
    if not collect_data and not query_data:
        print(" No action specified. Use --collect-* or --query-* options")
        parser.print_help()
        return 1
    
    # Build collection-specific arguments
    collect_args = [
        "--db-path", args.db_path,
        "--rate-limit", str(args.rate_limit),
        "--max-concurrent", str(args.max_concurrent),
        "--batch-size", str(args.batch_size),
        "--price-period", args.price_period
    ]
    
    if args.symbols:
        collect_args.extend(["--symbols"] + args.symbols)
    elif args.sp500:
        collect_args.append("--sp500")
    elif args.etfs:
        collect_args.append("--etfs")
    elif args.indices:
        collect_args.append("--indices")
    else:
        # Default to S&P 500 if no data source specified
        collect_args.append("--sp500")
    
    if args.limit:
        collect_args.extend(["--limit", str(args.limit)])
    
    if args.output:
        collect_args.extend(["--output", args.output])
    
    # Build query-specific arguments
    query_args = ["--db-path", args.db_path]
    
    if args.output:
        query_args.extend(["--output", args.output])
    
    # Data Collection
    if collect_data:
        print(" Starting data collection...")
        
        # Build collect_data.py arguments
        collect_script_args = ["scripts/collect_data.py"] + collect_args
        
        if args.collect_all:
            collect_script_args.append("--all-data")
        else:
            if args.collect_stock:
                collect_script_args.append("--stock-data")
            if args.collect_options:
                collect_script_args.append("--options-data")
            if args.collect_treasury:
                collect_script_args.append("--treasury-data")
            if args.calculate_metrics:
                collect_script_args.append("--metrics")
        
        if args.treasury_year:
            collect_script_args.extend(["--treasury-year", str(args.treasury_year)])
        if args.treasury_month:
            collect_script_args.extend(["--treasury-month", str(args.treasury_month)])
        
        collect_script_args.extend(["--max-expiration-dates", str(args.max_expiration_dates)])
        
        # Run data collection
        exit_code = run_script("scripts/collect_data.py", collect_script_args[1:])  # Skip script name
        if exit_code != 0:
            print(f" Data collection failed with exit code {exit_code}")
            return exit_code
        
        print(" Data collection completed successfully!")
    
    # Data Querying
    if query_data:
        print("\n Starting data querying...")
        
        # Build query_data.py arguments
        query_script_args = ["scripts/query_data.py"] + query_args
        
        if args.query_all:
            query_script_args.append("--all")
        else:
            if args.query_options:
                query_script_args.append("--options")
            if args.query_stock:
                query_script_args.append("--stock-prices")
            if args.query_stock_info:
                query_script_args.append("--stock-info")
            if args.query_treasury:
                query_script_args.append("--treasury")
            if args.query_metrics:
                query_script_args.append("--metrics")
        
        # Add query filters
        if args.symbol:
            query_script_args.extend(["--symbol", args.symbol])
        if args.expiration:
            query_script_args.extend(["--expiration", args.expiration])
        if args.type:
            query_script_args.extend(["--type", args.type])
        if args.moneyness:
            query_script_args.extend(["--moneyness", args.moneyness])
        if args.min_volume:
            query_script_args.extend(["--min-volume", str(args.min_volume)])
        if args.start_date:
            query_script_args.extend(["--start-date", args.start_date])
        if args.end_date:
            query_script_args.extend(["--end-date", args.end_date])
        if args.limit_results:
            query_script_args.extend(["--limit", str(args.limit_results)])
        
        # Run data querying
        exit_code = run_script("scripts/query_data.py", query_script_args[1:])  # Skip script name
        if exit_code != 0:
            print(f" Data querying failed with exit code {exit_code}")
            return exit_code
        
        print(" Data querying completed successfully!")
    
    # Show final statistics
    if args.stats or collect_data:
        show_database_stats(args.db_path)
    
    print(f"\n Market Data ETL orchestration completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python3
"""
Data collection script for market data ETL
Handles collection of stock, options, treasury, and indices data
"""

import sys
import os
import argparse
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase
from src.scrapers.yahoo_scraper import YahooScraper
from src.scrapers.treasury import TreasuryScraper
from src.processors.stock_processor import StockProcessor
from src.processors.options_processor import OptionsProcessor
from src.processors.treasury_processor import TreasuryProcessor
from src.metrics.options import OptionsMetricsCalculator


def load_indices_symbols(csv_path: str = "data/indicies.csv") -> List[str]:
    """
    Load indices symbols from CSV file
    
    Args:
        csv_path: Path to indices CSV file
        
    Returns:
        List of indices symbols
    """
    import pandas as pd
    
    try:
        df = pd.read_csv(csv_path)
        # Filter out empty rows and get ticker column
        symbols = df['ticker'].dropna().tolist()
        print(f"Loaded {len(symbols)} indices symbols from {csv_path}")
        return symbols
    except Exception as e:
        print(f"Error loading indices symbols: {e}")
        return []


def collect_stock_data(symbols: List[str], db: OptionsDatabase, 
                      rate_limit: float = 0.1, max_concurrent: int = 15, 
                      price_period: str = "ytd") -> Dict[str, int]:
    """
    Collect stock data (info and prices) for given symbols
    
    Args:
        symbols: List of stock symbols
        db: Database instance
        rate_limit: Rate limit delay
        max_concurrent: Max concurrent requests
        price_period: Period for price data collection (ytd, 1y, etc.)
        
    Returns:
        Dictionary with collection statistics
    """
    print(f"\n Collecting stock data for {len(symbols)} symbols...")
    
    scraper = YahooScraper(rate_limit_delay=rate_limit)
    processor = StockProcessor()
    
    stats = {
        'stock_info_collected': 0,
        'stock_prices_collected': 0,
        'errors': 0
    }
    
    for i, symbol in enumerate(symbols, 1):
        print(f"Processing {symbol} ({i}/{len(symbols)})")
        
        try:
            # Collect stock info
            stock_info = scraper.get_stock_info(symbol)
            if stock_info:
                processed_info = processor.process_stock_info(stock_info)
                if db.insert_stock_info(processed_info):
                    stats['stock_info_collected'] += 1
                    print(f"  ✓ Stock info collected for {symbol}")
                else:
                    print(f"  ✗ Failed to insert stock info for {symbol}")
            
            # Collect stock prices (configurable period daily data)
            if price_period == "ytd":
                stock_prices = scraper.get_stock_price_history_ytd(symbol, interval="1d")
            else:
                stock_prices = scraper.get_stock_price_history(symbol, period=price_period, interval="1d")
            if stock_prices:
                processed_prices = processor.process_stock_prices(stock_prices)
                rows_inserted = db.insert_stock_prices(processed_prices)
                if rows_inserted > 0:
                    stats['stock_prices_collected'] += rows_inserted
                    print(f"  ✓ Stock prices collected for {symbol} ({rows_inserted} records)")
                else:
                    print(f"  ✗ Failed to insert stock prices for {symbol}")
            
        except Exception as e:
            print(f"  ✗ Error processing {symbol}: {e}")
            stats['errors'] += 1
            continue
    
    return stats


def collect_options_data(symbols: List[str], db: OptionsDatabase,
                        rate_limit: float = 0.1, max_expiration_dates: int = 3) -> Dict[str, int]:
    """
    Collect options data for given symbols
    
    Args:
        symbols: List of stock symbols
        db: Database instance
        rate_limit: Rate limit delay
        max_expiration_dates: Max expiration dates per symbol
        
    Returns:
        Dictionary with collection statistics
    """
    print(f"\n Collecting options data for {len(symbols)} symbols...")
    
    scraper = YahooScraper(rate_limit_delay=rate_limit)
    processor = OptionsProcessor()
    
    stats = {
        'options_collected': 0,
        'symbols_processed': 0,
        'errors': 0
    }
    
    for i, symbol in enumerate(symbols, 1):
        print(f"Processing options for {symbol} ({i}/{len(symbols)})")
        
        try:
            # Get options data for multiple expiration dates
            options_data = scraper.get_multiple_expiration_dates(symbol, max_expiration_dates)
            
            if options_data:
                processed_options = processor.process_options_chain(options_data)
                rows_inserted = db.insert_options_chain(processed_options)
                
                if rows_inserted > 0:
                    stats['options_collected'] += rows_inserted
                    stats['symbols_processed'] += 1
                    print(f"  ✓ Options collected for {symbol} ({rows_inserted} contracts)")
                else:
                    print(f"  ✗ Failed to insert options for {symbol}")
            else:
                print(f"  - No options data available for {symbol}")
                
        except Exception as e:
            print(f"  ✗ Error processing options for {symbol}: {e}")
            stats['errors'] += 1
            continue
    
    return stats


def collect_treasury_data(db: OptionsDatabase, year: Optional[int] = None, 
                         month: Optional[int] = None) -> Dict[str, int]:
    """
    Collect treasury rates data
    
    Args:
        db: Database instance
        year: Specific year for treasury data
        month: Specific month for treasury data
        
    Returns:
        Dictionary with collection statistics
    """
    print(f"\n Collecting treasury data...")
    
    scraper = TreasuryScraper()
    processor = TreasuryProcessor()
    
    stats = {
        'treasury_records_collected': 0,
        'errors': 0
    }
    
    try:
        # Get treasury data
        treasury_data = scraper.get_treasury_rates(year=year, month=month)
        
        if treasury_data:
            processed_data = processor.process_treasury_rates(treasury_data)
            rows_inserted = db.insert_treasury_rates(processed_data)
            
            if rows_inserted > 0:
                stats['treasury_records_collected'] = rows_inserted
                print(f"  ✓ Treasury data collected ({rows_inserted} records)")
            else:
                print(f"  ✗ Failed to insert treasury data")
        else:
            print(f"  - No treasury data available")
            
    except Exception as e:
        print(f"  ✗ Error collecting treasury data: {e}")
        stats['errors'] += 1
    
    return stats


def calculate_options_metrics(db: OptionsDatabase) -> Dict[str, int]:
    """
    Calculate options metrics for all symbols
    
    Args:
        db: Database instance
        
    Returns:
        Dictionary with calculation statistics
    """
    print(f"\n Calculating options metrics...")
    
    calculator = OptionsMetricsCalculator()
    
    stats = {
        'metrics_calculated': 0,
        'symbols_processed': 0,
        'errors': 0
    }
    
    try:
        # Get all symbols with options data
        symbols = db.get_available_symbols()
        
        for symbol in symbols:
            try:
                print(f"Calculating metrics for {symbol}...")
                
                # Get options data for this symbol
                options_df = db.get_options_chain(symbol)
                
                if not options_df.empty:
                    # Calculate metrics
                    metrics = calculator.calculate_all_metrics(options_df)
                    
                    if metrics:
                        rows_inserted = db.insert_option_metrics(metrics)
                        if rows_inserted > 0:
                            stats['metrics_calculated'] += rows_inserted
                            stats['symbols_processed'] += 1
                            print(f"  ✓ Metrics calculated for {symbol} ({rows_inserted} records)")
                        else:
                            print(f"  ✗ Failed to insert metrics for {symbol}")
                    else:
                        print(f"  - No metrics calculated for {symbol}")
                else:
                    print(f"  - No options data found for {symbol}")
                    
            except Exception as e:
                print(f"  ✗ Error calculating metrics for {symbol}: {e}")
                stats['errors'] += 1
                continue
    
    except Exception as e:
        print(f"  ✗ Error in metrics calculation: {e}")
        stats['errors'] += 1
    
    return stats


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Market data collection script")
    
    # Data collection modes
    parser.add_argument("--all-data", action="store_true", 
                       help="Collect all data types (stock, options, treasury, metrics)")
    parser.add_argument("--stock-data", action="store_true", 
                       help="Collect stock data (info, prices)")
    parser.add_argument("--options-data", action="store_true", 
                       help="Collect options data")
    parser.add_argument("--treasury-data", action="store_true", 
                       help="Collect treasury data")
    parser.add_argument("--metrics", action="store_true", 
                       help="Calculate options metrics")
    parser.add_argument("--indices", action="store_true", 
                       help="Include primary indices data")
    
    # Data sources
    parser.add_argument("--sp500", action="store_true", help="Include S&P 500 companies")
    parser.add_argument("--etfs", action="store_true", help="Include index ETFs")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to process")
    parser.add_argument("--limit", type=int, help="Limit number of symbols (for testing)")
    
    # Configuration
    parser.add_argument("--db-path", default="data/options/market_data.db", help="Database path")
    parser.add_argument("--rate-limit", type=float, default=0.1, help="Rate limit delay")
    parser.add_argument("--max-concurrent", type=int, default=15, help="Max concurrent requests")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    parser.add_argument("--max-expiration-dates", type=int, default=3, help="Max expiration dates")
    parser.add_argument("--price-period", default="ytd", 
                       choices=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
                       help="Period for stock price data collection")
    
    # Treasury specific
    parser.add_argument("--treasury-year", type=int, help="Year for treasury data")
    parser.add_argument("--treasury-month", type=int, help="Month for treasury data")
    
    # Output
    parser.add_argument("--output", help="Output file prefix")
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Determine what data to collect
    collect_stock = args.stock_data or args.all_data
    collect_options = args.options_data or args.all_data
    collect_treasury = args.treasury_data or args.all_data
    calculate_metrics = args.metrics or args.all_data
    include_indices = args.indices or args.all_data
    
    # If no specific data type specified, show help
    if not any([collect_stock, collect_options, collect_treasury, calculate_metrics]):
        print(" No data type specified. Use --stock-data, --options-data, --treasury-data, --metrics, or --all-data")
        parser.print_help()
        return 1
    
    # Determine symbols to process
    symbols = []
    
    if args.symbols:
        symbols = args.symbols
    elif args.sp500:
        # Load S&P 500 symbols
        try:
            import pandas as pd
            sp500_df = pd.read_csv("data/sp500_companies.csv")
            symbols = sp500_df['Symbol'].tolist()
            print(f"Loaded {len(symbols)} S&P 500 symbols")
        except Exception as e:
            print(f"Error loading S&P 500 symbols: {e}")
            return 1
    elif args.etfs:
        # Load ETF symbols
        try:
            import pandas as pd
            etf_df = pd.read_csv("data/index_etfs.csv")
            symbols = etf_df['ticker'].tolist()
            print(f"Loaded {len(symbols)} ETF symbols")
        except Exception as e:
            print(f"Error loading ETF symbols: {e}")
            return 1
    else:
        # Default to S&P 500
        try:
            import pandas as pd
            sp500_df = pd.read_csv("data/sp500_companies.csv")
            symbols = sp500_df['Symbol'].tolist()
            print(f"Loaded {len(symbols)} S&P 500 symbols")
        except Exception as e:
            print(f"Error loading S&P 500 symbols: {e}")
            return 1
    
    # Add indices symbols if requested
    if include_indices:
        indices_symbols = load_indices_symbols()
        symbols.extend(indices_symbols)
        print(f"Added {len(indices_symbols)} indices symbols")
    
    # Apply limit if specified
    if args.limit:
        symbols = symbols[:args.limit]
        print(f"Limited to {len(symbols)} symbols")
    
    print(f"Total symbols to process: {len(symbols)}")
    
    # Collection statistics
    total_stats = {
        'stock_info_collected': 0,
        'stock_prices_collected': 0,
        'options_collected': 0,
        'treasury_records_collected': 0,
        'metrics_calculated': 0,
        'total_errors': 0
    }
    
    try:
        # Collect stock data
        if collect_stock:
            stock_stats = collect_stock_data(symbols, db, args.rate_limit, args.max_concurrent, args.price_period)
            total_stats.update(stock_stats)
            total_stats['total_errors'] += stock_stats.get('errors', 0)
        
        # Collect options data
        if collect_options:
            options_stats = collect_options_data(symbols, db, args.rate_limit, args.max_expiration_dates)
            total_stats['options_collected'] += options_stats.get('options_collected', 0)
            total_stats['total_errors'] += options_stats.get('errors', 0)
        
        # Collect treasury data
        if collect_treasury:
            treasury_stats = collect_treasury_data(db, args.treasury_year, args.treasury_month)
            total_stats['treasury_records_collected'] += treasury_stats.get('treasury_records_collected', 0)
            total_stats['total_errors'] += treasury_stats.get('errors', 0)
        
        # Calculate options metrics
        if calculate_metrics:
            metrics_stats = calculate_options_metrics(db)
            total_stats['metrics_calculated'] += metrics_stats.get('metrics_calculated', 0)
            total_stats['total_errors'] += metrics_stats.get('errors', 0)
        
        # Print final statistics
        print(f"\n COLLECTION COMPLETED")
        print("="*60)
        print(f"Stock info records: {total_stats['stock_info_collected']}")
        print(f"Stock price records: {total_stats['stock_prices_collected']}")
        print(f"Options contracts: {total_stats['options_collected']}")
        print(f"Treasury records: {total_stats['treasury_records_collected']}")
        print(f"Metrics calculated: {total_stats['metrics_calculated']}")
        print(f"Total errors: {total_stats['total_errors']}")
        print("="*60)
        
        # Show database statistics
        print(f"\n DATABASE STATISTICS")
        print("="*60)
        db_stats = db.get_database_stats()
        for key, value in db_stats.items():
            print(f"{key}: {value}")
        print("="*60)
        
    except Exception as e:
        print(f" Error during collection: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

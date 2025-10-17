#!/usr/bin/env python3
"""
General data collection script for market data ETL
Handles options, stock prices, earnings, treasury data, and metrics calculation
"""

import sys
import os
import argparse
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import asyncio

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase, StockInfo, StockPrices, EarningsDates, OptionMetrics, TreasuryRates
from src.scrapers.yahoo_scraper import YahooScraper
from src.scrapers import HybridAsyncOptionsScraper
from src.scrapers.treasury import TreasuryScraper
from src.processors.options_processor import OptionsProcessor
from src.processors.stock_processor import StockProcessor


def load_symbols_from_csv(csv_path: str) -> List[str]:
    """Load symbols from CSV file"""
    try:
        df = pd.read_csv(csv_path)
        if 'Symbol' in df.columns:
            return df['Symbol'].tolist()
        else:
            print(f"Warning: No 'Symbol' column found in {csv_path}")
            return []
    except Exception as e:
        print(f"Error loading {csv_path}: {e}")
        return []


def get_monday_date() -> datetime:
    """Get the date of the most recent Monday"""
    today = datetime.now()
    days_since_monday = today.weekday()  # Monday is 0
    monday = today - timedelta(days=days_since_monday)
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def collect_stock_data(db: OptionsDatabase, scraper: YahooScraper, symbols: List[str], 
                       monday_date: datetime, rate_limit_delay: float = 0.1) -> Dict[str, Any]:
    """
    Collect comprehensive stock data (info, prices, earnings) for symbols
    
    Args:
        db: Database instance
        scraper: Yahoo scraper instance
        symbols: List of symbols to process
        monday_date: Monday date for returns calculation
        rate_limit_delay: Delay between API calls
        
    Returns:
        Dictionary with collection results
    """
    print(f"Collecting stock data for {len(symbols)} symbols...")
    
    results = {
        'total_symbols': len(symbols),
        'processed_symbols': 0,
        'stock_info_inserted': 0,
        'stock_prices_inserted': 0,
        'earnings_dates_inserted': 0,
        'returns_summary': [],
        'errors': []
    }
    
    for i, symbol in enumerate(symbols):
        print(f"\n[{i+1}/{len(symbols)}] Processing {symbol}...")
        
        try:
            # Get stock info
            stock_info = scraper.get_stock_info(symbol)
            if stock_info:
                db.insert_stock_info(stock_info)
                results['stock_info_inserted'] += 1
            
            # Get stock price history (last 30 days to ensure we have Monday data)
            stock_prices = scraper.get_stock_price_history(symbol, period="1mo", interval="1d")
            if stock_prices:
                inserted = db.insert_stock_prices(stock_prices)
                results['stock_prices_inserted'] += inserted
                
                # Calculate returns since Monday
                monday_date_only = monday_date.date()
                monday_prices = [p for p in stock_prices if p.date == monday_date_only]
                latest_prices = [p for p in stock_prices if p.date >= monday_date_only]
                
                if monday_prices and latest_prices:
                    monday_close = monday_prices[0].close_price
                    latest_close = latest_prices[-1].close_price
                    if monday_close and latest_close and monday_close != 0:
                        returns = ((latest_close - monday_close) / monday_close) * 100
                        results['returns_summary'].append({
                            'symbol': symbol,
                            'returns': returns
                        })
                        print(f"    Returns since Monday: {returns:.2f}%")
            
            # Get earnings dates
            earnings_dates = scraper.get_earnings_dates(symbol)
            if earnings_dates:
                inserted = db.insert_earnings_dates(earnings_dates)
                results['earnings_dates_inserted'] += inserted
                print(f"    Retrieved {len(earnings_dates)} earnings dates")
            
            results['processed_symbols'] += 1
            
        except Exception as e:
            error_msg = f"{symbol}: {str(e)}"
            results['errors'].append(error_msg)
            print(f"     Error: {error_msg}")
    
    return results


async def collect_options_data(db: OptionsDatabase, symbols: List[str], 
                              max_expiration_dates: int = 30,
                              rate_limit_delay: float = 0.05,
                              max_concurrent: int = 15,
                              batch_size: int = 100) -> Dict[str, Any]:
    """
    Collect options data for symbols using async processing
    
    Args:
        db: Database instance
        symbols: List of symbols to process
        max_expiration_dates: Maximum expiration dates per symbol
        rate_limit_delay: Delay between requests
        max_concurrent: Maximum concurrent requests
        batch_size: Batch size for processing
        
    Returns:
        Dictionary with collection results
    """
    print(f" Collecting options data for {len(symbols)} symbols...")
    
    scraper = HybridAsyncOptionsScraper(
        rate_limit_delay=rate_limit_delay,
        max_concurrent_requests=max_concurrent
    )
    
    results = {
        'total_symbols': len(symbols),
        'processed_symbols': 0,
        'options_inserted': 0,
        'errors': []
    }
    
    # Process symbols in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        print(f"\n=== Processing Options Batch {batch_num}/{total_batches} ({len(batch)} symbols) ===")
        
        try:
            # Fetch options data for the batch
            options_data_results = await scraper.get_options_data_batch(batch, max_expiration_dates)
            
            # Store options data
            batch_options_inserted = 0
            batch_successful_symbols = 0
            for symbol, options_data in options_data_results.items():
                if options_data:
                    rows_inserted = db.insert_options_chain(options_data)
                    batch_options_inserted += rows_inserted
                    batch_successful_symbols += 1
            
            print(f"    Inserted {batch_options_inserted} options contracts for {batch_successful_symbols} symbols")
            
            results['options_inserted'] += batch_options_inserted
            results['processed_symbols'] += batch_successful_symbols
            
        except Exception as e:
            error_msg = f"Batch {batch_num}: {str(e)}"
            results['errors'].append(error_msg)
            print(f"     Error: {error_msg}")
    
    return results


def collect_treasury_data(db: OptionsDatabase, year: int = None, month: int = None) -> Dict[str, Any]:
    """
    Collect treasury rates data
    
    Args:
        db: Database instance
        year: Year to fetch (defaults to current)
        month: Month to fetch (defaults to current)
        
    Returns:
        Dictionary with collection results
    """
    print(f" Collecting treasury data for {year or 'current'}-{month or 'current':02d}...")
    
    scraper = TreasuryScraper()
    treasury_rates_list = scraper.fetch_and_process_month(year, month)
    
    results = {
        'treasury_rates_inserted': 0,
        'errors': []
    }
    
    if treasury_rates_list:
        try:
            inserted_count = db.insert_treasury_rates(treasury_rates_list)
            results['treasury_rates_inserted'] = inserted_count
            print(f"     Inserted {inserted_count} treasury rate records")
        except Exception as e:
            error_msg = f"Treasury data: {str(e)}"
            results['errors'].append(error_msg)
            print(f"     Error: {error_msg}")
    else:
        print("     No treasury data found")
    
    return results


def calculate_options_metrics(db: OptionsDatabase, symbols: List[str] = None) -> Dict[str, Any]:
    """
    Calculate and store options metrics for symbols
    
    Args:
        db: Database instance
        symbols: List of symbols to process (if None, processes all symbols with options data)
        
    Returns:
        Dictionary with calculation results
    """
    print(f" Calculating options metrics...")
    
    processor = OptionsProcessor()
    
    # Get symbols to process
    if symbols is None:
        try:
            symbols_df = db.get_unique_options_symbols()
            symbols = symbols_df['symbol'].tolist()
        except Exception as e:
            print(f"Error getting symbols: {e}")
            return {'metrics_inserted': 0, 'errors': [str(e)]}
    
    results = {
        'total_symbols': len(symbols),
        'processed_symbols': 0,
        'metrics_inserted': 0,
        'errors': []
    }
    
    for i, symbol in enumerate(symbols):
        print(f"  [{i+1}/{len(symbols)}] Processing {symbol}...")
        
        try:
            # Get options data for the symbol
            options_df = db.get_options_chain(symbol=symbol)
            
            if options_df.empty:
                print(f"    No options data found for {symbol}")
                continue
            
            # Get current stock price
            current_price = None
            try:
                stock_prices_df = db.get_stock_prices(symbol=symbol)
                if not stock_prices_df.empty:
                    current_price = stock_prices_df.iloc[-1]['close_price']
            except Exception as e:
                print(f"     Could not get current price: {e}")
            
            # Calculate metrics
            metrics_list = processor.calculate_option_metrics(options_df, current_price)
            
            if metrics_list:
                inserted_count = db.insert_option_metrics(metrics_list)
                results['metrics_inserted'] += inserted_count
                print(f"     Created {inserted_count} metrics records")
            else:
                print(f"     No metrics calculated")
            
            results['processed_symbols'] += 1
            
        except Exception as e:
            error_msg = f"{symbol}: {str(e)}"
            results['errors'].append(error_msg)
            print(f"     Error: {error_msg}")
    
    return results


def print_summary(results: Dict[str, Any], data_type: str):
    """Print summary of data collection results"""
    print(f"\n{'='*60}")
    print(f" {data_type.upper()} COLLECTION SUMMARY".center(60))
    print(f"{'='*60}")
    
    for key, value in results.items():
        if key != 'errors' and key != 'returns_summary':
            print(f"{key.replace('_', ' ').title()}: {value}")
    
    if 'returns_summary' in results and results['returns_summary']:
        sorted_returns = sorted(results['returns_summary'], key=lambda x: x['returns'], reverse=True)
        print(f"\n TOP PERFORMERS SINCE MONDAY:")
        for i, item in enumerate(sorted_returns[:5]):
            print(f"   {i+1}. {item['symbol'].ljust(5)}: {item['returns']:>7.2f}%")
    
    if results.get('errors'):
        print(f"\n ERRORS:")
        for error in results['errors'][:5]:  # Show first 5 errors
            print(f"  • {error}")
        if len(results['errors']) > 5:
            print(f"  ... and {len(results['errors']) - 5} more errors")
    
    print(f"{'='*60}")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="General market data collection script")
    
    # Data source arguments
    parser.add_argument('--sp500', action='store_true', help='Include S&P 500 companies')
    parser.add_argument('--etfs', action='store_true', help='Include index ETFs')
    parser.add_argument('--symbols', nargs='+', help='Specific symbols to process')
    parser.add_argument('--limit', type=int, help='Limit number of symbols (for testing)')
    
    # Data type arguments
    parser.add_argument('--stock-data', action='store_true', help='Collect stock data (info, prices, earnings)')
    parser.add_argument('--options-data', action='store_true', help='Collect options data')
    parser.add_argument('--treasury-data', action='store_true', help='Collect treasury data')
    parser.add_argument('--metrics', action='store_true', help='Calculate options metrics')
    parser.add_argument('--all-data', action='store_true', help='Collect all data types')
    
    # Configuration arguments
    parser.add_argument('--db-path', default="data/options/market_data.db", help='Database path')
    parser.add_argument('--rate-limit', type=float, default=0.1, help='Rate limit delay (seconds)')
    parser.add_argument('--max-concurrent', type=int, default=15, help='Max concurrent requests')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--max-expiration-dates', type=int, default=30, help='Max expiration dates per symbol')
    
    # Treasury data arguments
    parser.add_argument('--treasury-year', type=int, help='Year for treasury data')
    parser.add_argument('--treasury-month', type=int, help='Month for treasury data')
    
    args = parser.parse_args()
    
    # Initialize database
    db = OptionsDatabase(args.db_path)
    
    # Determine symbols to process
    symbols = []
    
    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
        print(f"Processing specific symbols: {symbols}")
    else:
        if args.sp500:
            sp500_symbols = load_symbols_from_csv("data/sp500_companies.csv")
            symbols.extend(sp500_symbols)
            print(f"Loaded {len(sp500_symbols)} S&P 500 symbols")
        
        if args.etfs:
            etf_symbols = load_symbols_from_csv("data/index_etfs.csv")
            symbols.extend(etf_symbols)
            print(f"Loaded {len(etf_symbols)} ETF symbols")
        
        if not symbols:
            print("No symbols specified. Use --sp500, --etfs, or --symbols")
            return 1
    
    # Apply limit if specified
    if args.limit:
        symbols = symbols[:args.limit]
        print(f"Limited to {len(symbols)} symbols for testing")
    
    if not symbols:
        print("No symbols to process")
        return 1
    
    # Determine what data to collect
    collect_stock = args.stock_data or args.all_data
    collect_options = args.options_data or args.all_data
    collect_treasury = args.treasury_data or args.all_data
    calculate_metrics = args.metrics or args.all_data
    
    print(f"\n Starting data collection...")
    print(f"Symbols: {len(symbols)}")
    print(f"Stock data: {'' if collect_stock else ''}")
    print(f"Options data: {'' if collect_options else ''}")
    print(f"Treasury data: {'' if collect_treasury else ''}")
    print(f"Metrics calculation: {'' if calculate_metrics else ''}")
    
    # Initialize scrapers
    yahoo_scraper = YahooScraper(rate_limit_delay=args.rate_limit)
    monday_date = get_monday_date()
    
    try:
        # Collect stock data
        if collect_stock:
            stock_results = collect_stock_data(db, yahoo_scraper, symbols, monday_date, args.rate_limit)
            print_summary(stock_results, "Stock Data")
        
        # Collect options data
        if collect_options:
            options_results = await collect_options_data(
                db, symbols, 
                max_expiration_dates=args.max_expiration_dates,
                rate_limit_delay=args.rate_limit,
                max_concurrent=args.max_concurrent,
                batch_size=args.batch_size
            )
            print_summary(options_results, "Options Data")
        
        # Collect treasury data
        if collect_treasury:
            treasury_results = collect_treasury_data(db, args.treasury_year, args.treasury_month)
            print_summary(treasury_results, "Treasury Data")
        
        # Calculate metrics
        if calculate_metrics:
            metrics_results = calculate_options_metrics(db, symbols)
            print_summary(metrics_results, "Options Metrics")
        
        print(f"\n Data collection completed successfully!")
        
    except KeyboardInterrupt:
        print(f"\n Interrupted by user")
        return 1
    except Exception as e:
        print(f" Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))

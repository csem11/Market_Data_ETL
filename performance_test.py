#!/usr/bin/env python3
"""
Performance comparison between sync and async options data fetching
"""

import asyncio
import sys
import os
import time
from datetime import datetime

sys.path.append(os.path.dirname(__file__))

from src.scrapers import OptionsScraper, HybridAsyncOptionsScraper


async def test_async_performance(symbols: list, max_expiration_dates: int = 2):
    """Test async performance"""
    print(f"Testing async performance with {len(symbols)} symbols...")
    
    scraper = HybridAsyncOptionsScraper(
        rate_limit_delay=0.05,
        max_concurrent_requests=15
    )
    
    start_time = time.time()
    
    # Fetch options data for all symbols concurrently
    results = await scraper.get_options_data_batch(symbols, max_expiration_dates)
    
    end_time = time.time()
    
    total_contracts = sum(len(data) for data in results.values())
    successful_symbols = len([data for data in results.values() if data])
    
    return {
        'duration': end_time - start_time,
        'total_contracts': total_contracts,
        'successful_symbols': successful_symbols,
        'contracts_per_second': total_contracts / (end_time - start_time) if (end_time - start_time) > 0 else 0
    }


def test_sync_performance(symbols: list, max_expiration_dates: int = 2):
    """Test sync performance"""
    print(f"Testing sync performance with {len(symbols)} symbols...")
    
    scraper = OptionsScraper(rate_limit_delay=0.1)
    
    start_time = time.time()
    
    # Fetch options data sequentially
    results = scraper.get_sp500_options_data(symbols, max_expiration_dates)
    
    end_time = time.time()
    
    total_contracts = sum(len(data) for data in results.values())
    successful_symbols = len([data for data in results.values() if data])
    
    return {
        'duration': end_time - start_time,
        'total_contracts': total_contracts,
        'successful_symbols': successful_symbols,
        'contracts_per_second': total_contracts / (end_time - start_time) if (end_time - start_time) > 0 else 0
    }


async def main():
    """Main function to run performance comparison"""
    print("Options Data Fetching Performance Comparison")
    print("=" * 50)
    
    # Test with a subset of symbols for faster comparison
    test_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "JPM", "JNJ"]
    max_expiration_dates = 2
    
    print(f"Test symbols: {test_symbols}")
    print(f"Max expiration dates per symbol: {max_expiration_dates}")
    
    # Test async performance
    print("\n" + "=" * 30)
    async_results = await test_async_performance(test_symbols, max_expiration_dates)
    
    print(f"\nAsync Results:")
    print(f"  Duration: {async_results['duration']:.2f} seconds")
    print(f"  Successful symbols: {async_results['successful_symbols']}/{len(test_symbols)}")
    print(f"  Total contracts: {async_results['total_contracts']}")
    print(f"  Contracts per second: {async_results['contracts_per_second']:.1f}")
    
    # Test sync performance
    print("\n" + "=" * 30)
    sync_results = test_sync_performance(test_symbols, max_expiration_dates)
    
    print(f"\nSync Results:")
    print(f"  Duration: {sync_results['duration']:.2f} seconds")
    print(f"  Successful symbols: {sync_results['successful_symbols']}/{len(test_symbols)}")
    print(f"  Total contracts: {sync_results['total_contracts']}")
    print(f"  Contracts per second: {sync_results['contracts_per_second']:.1f}")
    
    # Calculate performance improvement
    print("\n" + "=" * 30)
    print("Performance Comparison:")
    
    if sync_results['duration'] > 0 and async_results['duration'] > 0:
        speedup = sync_results['duration'] / async_results['duration']
        print(f"  Speedup: {speedup:.1f}x faster with async")
        print(f"  Time saved: {sync_results['duration'] - async_results['duration']:.2f} seconds")
        
        if async_results['contracts_per_second'] > 0 and sync_results['contracts_per_second'] > 0:
            throughput_improvement = async_results['contracts_per_second'] / sync_results['contracts_per_second']
            print(f"  Throughput improvement: {throughput_improvement:.1f}x more contracts per second")
    
    # Estimate S&P 500 performance
    print("\n" + "=" * 30)
    print("S&P 500 Performance Estimate:")
    
    sp500_symbols = 500
    if async_results['duration'] > 0 and async_results['successful_symbols'] > 0:
        estimated_time_async = (async_results['duration'] / async_results['successful_symbols']) * sp500_symbols
        print(f"  Estimated time for all S&P 500 (async): {estimated_time_async/60:.1f} minutes")
    
    if sync_results['duration'] > 0 and sync_results['successful_symbols'] > 0:
        estimated_time_sync = (sync_results['duration'] / sync_results['successful_symbols']) * sp500_symbols
        print(f"  Estimated time for all S&P 500 (sync): {estimated_time_sync/60:.1f} minutes")
        
        if estimated_time_async > 0:
            time_saved = estimated_time_sync - estimated_time_async
            print(f"  Time saved for S&P 500: {time_saved/60:.1f} minutes")


if __name__ == "__main__":
    asyncio.run(main())

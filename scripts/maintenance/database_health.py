#!/usr/bin/env python3
"""
Database health check and maintenance script
Monitors database health and performs maintenance tasks
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.scrapers.treasury import TreasuryScraper
from src.database import OptionsDatabase


def check_database_health(db_path: str = "data/options/market_data.db") -> Dict[str, Any]:
    """
    Check database health and return statistics
    
    Args:
        db_path: Database path
        
    Returns:
        Dictionary with health statistics
    """
    db = OptionsDatabase(db_path)
    stats = db.get_database_stats()
    
    health_report = {
        'timestamp': datetime.now().isoformat(),
        'database_path': db_path,
        'total_options_records': stats.get('total_options_records', 0),
        'unique_symbols': stats.get('unique_symbols', 0),
        'total_stocks': stats.get('total_stocks', 0),
        'latest_data_timestamp': stats.get('latest_data_timestamp'),
        'database_size_mb': get_database_size(db_path)
    }
    
    return health_report


def get_database_size(db_path: str) -> float:
    """Get database file size in MB"""
    try:
        size_bytes = os.path.getsize(db_path)
        return round(size_bytes / (1024 * 1024), 2)
    except:
        return 0.0


def cleanup_old_data(db_path: str, days_old: int = 30) -> int:
    """
    Clean up old data from database
    
    Args:
        db_path: Database path
        days_old: Number of days to keep data
        
    Returns:
        Number of records deleted
    """
    db = OptionsDatabase(db_path)
    return db.cleanup_old_data(days_old)


def check_treasury_data_health(db_path: str = "data/options/market_data.db") -> Dict[str, Any]:
    """
    Check treasury data health
    
    Args:
        db_path: Database path
        
    Returns:
        Dictionary with treasury data health
    """
    db = OptionsDatabase(db_path)
    
    # Get treasury data from database
    df = db.get_treasury_rates()
    
    if df.empty:
        return {
            'status': 'error',
            'message': 'No treasury data found'
        }
    
    return {
        'status': 'healthy',
        'data_points': len(df),
        'date_range': {
            'start': df['date'].min(),
            'end': df['date'].max()
        },
        'latest_rates': {
            'one_month': df.iloc[-1].get('one_month'),
            'three_month': df.iloc[-1].get('three_month'),
            'ten_year': df.iloc[-1].get('ten_year')
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Database health check and maintenance')
    parser.add_argument('--db-path', default='data/options/market_data.db', help='Database path')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old data')
    parser.add_argument('--days-old', type=int, default=30, help='Days of data to keep')
    parser.add_argument('--treasury-health', action='store_true', help='Check treasury data health')
    
    args = parser.parse_args()
    
    try:
        print("=== Database Health Check ===")
        
        # General database health
        health = check_database_health(args.db_path)
        print(f"Database: {health['database_path']}")
        print(f"Size: {health['database_size_mb']} MB")
        print(f"Options records: {health['total_options_records']:,}")
        print(f"Unique symbols: {health['unique_symbols']:,}")
        print(f"Stock records: {health['total_stocks']:,}")
        print(f"Latest data: {health['latest_data_timestamp']}")
        
        # Treasury data health
        if args.treasury_health:
            print("\n=== Treasury Data Health ===")
            treasury_health = check_treasury_data_health(args.db_path)
            print(f"Status: {treasury_health['status']}")
            if treasury_health['status'] == 'healthy':
                print(f"Data points: {treasury_health['data_points']}")
                print(f"Date range: {treasury_health['date_range']}")
        
        # Cleanup old data
        if args.cleanup:
            print(f"\n=== Cleaning up data older than {args.days_old} days ===")
            deleted_count = cleanup_old_data(args.db_path, args.days_old)
            print(f"Deleted {deleted_count} old records")
        
        print("\n✅ Database health check completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

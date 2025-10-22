#!/usr/bin/env python3
"""
Script to update effective dates in the database
Updates all records with today's date to yesterday's date
"""

import sys
import os
from datetime import datetime, timedelta
import sqlite3

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import OptionsDatabase


def update_effective_dates(db_path: str = "data/options/market_data.db"):
    """
    Update effective dates from today to yesterday for all recent data
    
    Args:
        db_path: Path to the database file
    """
    print("Updating effective dates from today to yesterday...")
    
    # Calculate dates
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    print(f"Today: {today}")
    print(f"Yesterday: {yesterday}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Update stock_info table
        print("\nUpdating stock_info table...")
        cursor.execute("""
            UPDATE stock_info 
            SET eff_date = ? 
            WHERE DATE(eff_date) = ?
        """, (yesterday.isoformat(), today.isoformat()))
        
        stock_updated = cursor.rowcount
        print(f"Updated {stock_updated} stock_info records")
        
        # Update options_chain table
        print("\nUpdating options_chain table...")
        cursor.execute("""
            UPDATE options_chain 
            SET eff_date = ? 
            WHERE DATE(eff_date) = ?
        """, (yesterday.isoformat(), today.isoformat()))
        
        options_updated = cursor.rowcount
        print(f"Updated {options_updated} options_chain records")
        
        # Update stock_prices table if it exists and has eff_date column
        print("\nChecking stock_prices table...")
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='stock_prices'
        """)
        
        if cursor.fetchone():
            # Check if eff_date column exists
            cursor.execute("PRAGMA table_info(stock_prices)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'eff_date' in columns:
                cursor.execute("""
                    UPDATE stock_prices 
                    SET eff_date = ? 
                    WHERE DATE(eff_date) = ?
                """, (yesterday.isoformat(), today.isoformat()))
                
                prices_updated = cursor.rowcount
                print(f"Updated {prices_updated} stock_prices records")
            else:
                print("stock_prices table exists but has no eff_date column")
        else:
            print("stock_prices table not found")
        
        # Update treasury_rates table if it exists and has eff_date column
        print("\nChecking treasury_rates table...")
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='treasury_rates'
        """)
        
        if cursor.fetchone():
            # Check if eff_date column exists
            cursor.execute("PRAGMA table_info(treasury_rates)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'eff_date' in columns:
                cursor.execute("""
                    UPDATE treasury_rates 
                    SET eff_date = ? 
                    WHERE DATE(eff_date) = ?
                """, (yesterday.isoformat(), today.isoformat()))
                
                treasury_updated = cursor.rowcount
                print(f"Updated {treasury_updated} treasury_rates records")
            else:
                print("treasury_rates table exists but has no eff_date column")
        else:
            print("treasury_rates table not found")
        
        # Commit changes
        conn.commit()
        
        print(f"\n✅ Successfully updated effective dates!")
        print(f"Total records updated:")
        print(f"  - Stock info: {stock_updated}")
        print(f"  - Options: {options_updated}")
        
        # Show updated statistics
        print(f"\n📊 Updated Database Statistics:")
        print("="*50)
        
        # Get updated counts
        cursor.execute("SELECT COUNT(*) FROM stock_info WHERE DATE(eff_date) = ?", (yesterday.isoformat(),))
        stock_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM options_chain WHERE DATE(eff_date) = ?", (yesterday.isoformat(),))
        options_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM options_chain WHERE DATE(eff_date) = ?", (yesterday.isoformat(),))
        unique_symbols = cursor.fetchone()[0]
        
        print(f"Records with yesterday's date ({yesterday}):")
        print(f"  - Stock info records: {stock_count}")
        print(f"  - Options records: {options_count}")
        print(f"  - Unique symbols: {unique_symbols}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error updating effective dates: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update effective dates in database")
    parser.add_argument("--db-path", default="data/options/market_data.db", 
                       help="Database path")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be updated without making changes")
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 DRY RUN - No changes will be made")
        # TODO: Implement dry run functionality
        return 0
    
    return update_effective_dates(args.db_path)


if __name__ == "__main__":
    exit(main())

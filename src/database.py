"""
Database module for SQLite operations
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

from .models import OptionsChainData, StockInfo, options_chain_to_dict, stock_info_to_dict


class OptionsDatabase:
    """SQLite database manager for options data"""
    
    def __init__(self, db_path: str = "data/options/options_data.db"):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.ensure_data_directory()
        self.init_database()
    
    def ensure_data_directory(self):
        """Ensure the data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn
    
    def init_database(self):
        """Initialize database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if eff_date column exists in stock_info table and add it if missing
            self._migrate_stock_info_table(cursor)
            
            # Check if eff_date column exists in options_chain table and add it if missing
            self._migrate_options_chain_table(cursor)
            
            # Create options_chain table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS options_chain (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    expiration_date TEXT NOT NULL,
                    strike_price REAL NOT NULL,
                    option_type TEXT NOT NULL,
                    bid REAL,
                    ask REAL,
                    last_price REAL,
                    volume INTEGER,
                    open_interest INTEGER,
                    implied_volatility REAL,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    contract_name TEXT,
                    last_trade_date TEXT,
                    eff_date TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            
            # Create stock_info table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    company_name TEXT,
                    current_price REAL,
                    market_cap REAL,
                    sector TEXT,
                    industry TEXT,
                    eff_date TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_options_symbol_expiration 
                ON options_chain(symbol, expiration_date)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_options_symbol 
                ON options_chain(symbol)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_options_symbol_eff_date 
                ON options_chain(symbol, eff_date)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_symbol 
                ON stock_info(symbol)
            """)
            
            conn.commit()
    
    def _migrate_stock_info_table(self, cursor):
        """Migrate existing stock_info table to add eff_date column if missing"""
        try:
            # Check if eff_date column exists
            cursor.execute("PRAGMA table_info(stock_info)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'eff_date' not in columns:
                print("Adding eff_date column to stock_info table...")
                cursor.execute("ALTER TABLE stock_info ADD COLUMN eff_date TEXT")
                
                # Update existing records to have eff_date = created_at
                cursor.execute("""
                    UPDATE stock_info 
                    SET eff_date = created_at 
                    WHERE eff_date IS NULL
                """)
        except sqlite3.Error as e:
            print(f"Migration warning: {e}")
            # If table doesn't exist yet, that's fine - it will be created with the new schema
    
    def _migrate_options_chain_table(self, cursor):
        """Migrate existing options_chain table to add eff_date column if missing"""
        try:
            # Check if eff_date column exists
            cursor.execute("PRAGMA table_info(options_chain)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'eff_date' not in columns:
                print("Adding eff_date column to options_chain table...")
                cursor.execute("ALTER TABLE options_chain ADD COLUMN eff_date TEXT")
                
                # Update existing records to have eff_date = created_at
                cursor.execute("""
                    UPDATE options_chain 
                    SET eff_date = created_at 
                    WHERE eff_date IS NULL
                """)
                
                # Create the new index for symbol + eff_date
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_options_symbol_eff_date 
                    ON options_chain(symbol, eff_date)
                """)
        except sqlite3.Error as e:
            print(f"Migration warning: {e}")
            # If table doesn't exist yet, that's fine - it will be created with the new schema
    
    def insert_options_chain(self, options_data: List[OptionsChainData]) -> int:
        """
        Insert options chain data into database.
        Overwrites existing data for the same symbol+eff_date combination.
        
        Args:
            options_data: List of OptionsChainData objects
            
        Returns:
            Number of rows inserted/updated
        """
        if not options_data:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the eff_date from the first option (assuming all have the same eff_date)
            first_option = options_data[0]
            eff_date = first_option.eff_date.isoformat() if first_option.eff_date else None
            symbol = first_option.symbol
            
            # Delete existing data for this symbol+eff_date combination
            if eff_date:
                cursor.execute("""
                    DELETE FROM options_chain 
                    WHERE symbol = ? AND eff_date = ?
                """, (symbol, eff_date))
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    print(f"Deleted {deleted_count} existing records for {symbol} on {eff_date}")
            
            rows_inserted = 0
            for option in options_data:
                try:
                    data_dict = options_chain_to_dict(option)
                    cursor.execute("""
                        INSERT INTO options_chain 
                        (symbol, expiration_date, strike_price, option_type, bid, ask, 
                         last_price, volume, open_interest, implied_volatility, delta, 
                         gamma, theta, vega, rho, contract_name, last_trade_date, eff_date, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['symbol'], data_dict['expiration_date'], data_dict['strike_price'],
                        data_dict['option_type'], data_dict['bid'], data_dict['ask'],
                        data_dict['last_price'], data_dict['volume'], data_dict['open_interest'],
                        data_dict['implied_volatility'], data_dict['delta'], data_dict['gamma'],
                        data_dict['theta'], data_dict['vega'], data_dict['rho'],
                        data_dict['contract_name'], data_dict['last_trade_date'], 
                        data_dict['eff_date'], data_dict['created_at']
                    ))
                    rows_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting option data: {e}")
                    continue
            
            conn.commit()
            return rows_inserted
    
    def insert_stock_info(self, stock_info: StockInfo) -> bool:
        """
        Insert or update stock information
        
        Args:
            stock_info: StockInfo object
            
        Returns:
            True if successful, False otherwise
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            data_dict = stock_info_to_dict(stock_info)
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_info 
                    (symbol, company_name, current_price, market_cap, sector, industry, eff_date, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data_dict['symbol'], data_dict['company_name'], data_dict['current_price'],
                    data_dict['market_cap'], data_dict['sector'], data_dict['industry'],
                    data_dict['eff_date'], data_dict['created_at']
                ))
                conn.commit()
                return True
            except sqlite3.Error as e:
                print(f"Error inserting stock info: {e}")
                return False
    
    def get_options_chain(self, symbol: str, expiration_date: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve options chain data for a symbol
        
        Args:
            symbol: Stock symbol
            expiration_date: Optional expiration date filter (YYYY-MM-DD format)
            
        Returns:
            DataFrame with options chain data
        """
        with self.get_connection() as conn:
            if expiration_date:
                query = """
                    SELECT * FROM options_chain 
                    WHERE symbol = ? AND expiration_date = ?
                    ORDER BY expiration_date, strike_price, option_type
                """
                df = pd.read_sql_query(query, conn, params=(symbol, expiration_date))
            else:
                query = """
                    SELECT * FROM options_chain 
                    WHERE symbol = ?
                    ORDER BY expiration_date, strike_price, option_type
                """
                df = pd.read_sql_query(query, conn, params=(symbol,))
            
            return df
    
    def get_stock_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve stock information
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with stock info or None if not found
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stock_info WHERE symbol = ?", (symbol,))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
    
    def get_available_symbols(self) -> List[str]:
        """
        Get list of all symbols in the database
        
        Returns:
            List of stock symbols
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT symbol FROM options_chain")
            return [row[0] for row in cursor.fetchall()]
    
    def get_available_expiration_dates(self, symbol: str) -> List[str]:
        """
        Get available expiration dates for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            List of expiration dates
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT expiration_date 
                FROM options_chain 
                WHERE symbol = ? 
                ORDER BY expiration_date
            """, (symbol,))
            return [row[0] for row in cursor.fetchall()]
    
    def delete_old_data(self, days_old: int = 30) -> int:
        """
        Delete options data older than specified days
        
        Args:
            days_old: Number of days to keep data
            
        Returns:
            Number of rows deleted
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM options_chain 
                WHERE created_at < datetime('now', '-{} days')
            """.format(days_old))
            
            rows_deleted = cursor.rowcount
            conn.commit()
            return rows_deleted
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Count total options records
            cursor.execute("SELECT COUNT(*) FROM options_chain")
            total_options = cursor.fetchone()[0]
            
            # Count unique symbols
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM options_chain")
            unique_symbols = cursor.fetchone()[0]
            
            # Count unique stocks
            cursor.execute("SELECT COUNT(*) FROM stock_info")
            total_stocks = cursor.fetchone()[0]
            
            # Get latest data timestamp
            cursor.execute("SELECT MAX(created_at) FROM options_chain")
            latest_data = cursor.fetchone()[0]
            
            return {
                'total_options_records': total_options,
                'unique_symbols': unique_symbols,
                'total_stocks': total_stocks,
                'latest_data_timestamp': latest_data
            }

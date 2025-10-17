"""
Database module for SQLite operations
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

from .models import OptionsChainData, StockInfo, StockPrices, EarningsDates, OptionMetrics, TreasuryRates, options_chain_to_dict, stock_info_to_dict, stock_prices_to_dict, earnings_dates_to_dict, option_metrics_to_dict, treasury_rates_to_dict


class OptionsDatabase:
    """SQLite database manager for options data"""
    
    def __init__(self, db_path: str = "data/options/market_data.db"):
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
            
            # Remove greeks columns from options_chain table if they exist
            self._migrate_remove_greeks_columns(cursor)
            
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
            
            # Create stock_prices table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    adjusted_close REAL,
                    created_at TEXT NOT NULL,
                    UNIQUE(symbol, date)
                )
            """)
            
            # Create earnings_dates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS earnings_dates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    earnings_date TEXT NOT NULL,
                    earnings_type TEXT NOT NULL,
                    fiscal_year INTEGER,
                    fiscal_quarter INTEGER,
                    created_at TEXT NOT NULL,
                    UNIQUE(symbol, earnings_date, earnings_type)
                )
            """)
            
            # Create option_metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS option_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    expiration_date TEXT NOT NULL,
                    strike_price REAL NOT NULL,
                    option_type TEXT NOT NULL,
                    current_price REAL,
                    option_price REAL,
                    intrinsic_value REAL,
                    time_value REAL,
                    moneyness TEXT,
                    days_to_expiration INTEGER,
                    implied_volatility REAL,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    volume INTEGER,
                    open_interest INTEGER,
                    bid_ask_spread REAL,
                    volume_oi_ratio REAL,
                    max_pain REAL,
                    support_level REAL,
                    resistance_level REAL,
                    created_at TEXT NOT NULL,
                    UNIQUE(symbol, expiration_date, strike_price, option_type)
                )
            """)
            
            # Create treasury_rates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS treasury_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    one_month REAL,
                    two_month REAL,
                    three_month REAL,
                    six_month REAL,
                    one_year REAL,
                    two_year REAL,
                    three_year REAL,
                    five_year REAL,
                    seven_year REAL,
                    ten_year REAL,
                    twenty_year REAL,
                    thirty_year REAL,
                    created_at TEXT NOT NULL,
                    UNIQUE(date)
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
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date 
                ON stock_prices(symbol, date)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_earnings_symbol_date 
                ON earnings_dates(symbol, earnings_date)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_option_metrics_symbol_expiration 
                ON option_metrics(symbol, expiration_date)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_option_metrics_symbol_strike 
                ON option_metrics(symbol, strike_price)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_option_metrics_moneyness 
                ON option_metrics(moneyness)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_treasury_rates_date 
                ON treasury_rates(date)
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
    
    def _migrate_remove_greeks_columns(self, cursor):
        """Remove greeks columns from options_chain table if they exist"""
        try:
            # Check if greeks columns exist
            cursor.execute("PRAGMA table_info(options_chain)")
            columns = [column[1] for column in cursor.fetchall()]
            
            greeks_columns = ['delta', 'gamma', 'theta', 'vega', 'rho']
            existing_greeks = [col for col in greeks_columns if col in columns]
            
            if existing_greeks:
                print(f"Removing greeks columns: {existing_greeks}")
                
                # SQLite doesn't support DROP COLUMN directly, so we need to recreate the table
                # First, create a backup table with the new schema
                cursor.execute("""
                    CREATE TABLE options_chain_new (
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
                        contract_name TEXT,
                        last_trade_date TEXT,
                        eff_date TEXT,
                        created_at TEXT NOT NULL
                    )
                """)
                
                # Copy data from old table to new table (excluding greeks columns)
                cursor.execute("""
                    INSERT INTO options_chain_new 
                    (id, symbol, expiration_date, strike_price, option_type, bid, ask, 
                     last_price, volume, open_interest, implied_volatility, contract_name, 
                     last_trade_date, eff_date, created_at)
                    SELECT id, symbol, expiration_date, strike_price, option_type, bid, ask,
                           last_price, volume, open_interest, implied_volatility, contract_name,
                           last_trade_date, eff_date, created_at
                    FROM options_chain
                """)
                
                # Drop the old table and rename the new one
                cursor.execute("DROP TABLE options_chain")
                cursor.execute("ALTER TABLE options_chain_new RENAME TO options_chain")
                
                # Recreate indexes
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
                
                print(f"Successfully removed greeks columns: {existing_greeks}")
                
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
                         last_price, volume, open_interest, implied_volatility, contract_name, 
                         last_trade_date, eff_date, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['symbol'], data_dict['expiration_date'], data_dict['strike_price'],
                        data_dict['option_type'], data_dict['bid'], data_dict['ask'],
                        data_dict['last_price'], data_dict['volume'], data_dict['open_interest'],
                        data_dict['implied_volatility'], data_dict['contract_name'], 
                        data_dict['last_trade_date'], data_dict['eff_date'], data_dict['created_at']
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
    
    def insert_stock_prices(self, stock_prices_data: List[StockPrices]) -> int:
        """
        Insert stock price history data into database
        
        Args:
            stock_prices_data: List of StockPrices objects
            
        Returns:
            Number of rows inserted/updated
        """
        if not stock_prices_data:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows_inserted = 0
            
            for price_data in stock_prices_data:
                try:
                    data_dict = stock_prices_to_dict(price_data)
                    cursor.execute("""
                        INSERT OR REPLACE INTO stock_prices 
                        (symbol, date, open_price, high_price, low_price, close_price, 
                         volume, adjusted_close, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['symbol'], data_dict['date'], data_dict['open_price'],
                        data_dict['high_price'], data_dict['low_price'], data_dict['close_price'],
                        data_dict['volume'], data_dict['adjusted_close'], data_dict['created_at']
                    ))
                    rows_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting stock price data: {e}")
                    continue
            
            conn.commit()
            return rows_inserted
    
    def insert_earnings_dates(self, earnings_data: List[EarningsDates]) -> int:
        """
        Insert earnings dates data into database
        
        Args:
            earnings_data: List of EarningsDates objects
            
        Returns:
            Number of rows inserted/updated
        """
        if not earnings_data:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows_inserted = 0
            
            for earnings in earnings_data:
                try:
                    data_dict = earnings_dates_to_dict(earnings)
                    cursor.execute("""
                        INSERT OR REPLACE INTO earnings_dates 
                        (symbol, earnings_date, earnings_type, fiscal_year, fiscal_quarter, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['symbol'], data_dict['earnings_date'], data_dict['earnings_type'],
                        data_dict['fiscal_year'], data_dict['fiscal_quarter'], data_dict['created_at']
                    ))
                    rows_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting earnings data: {e}")
                    continue
            
            conn.commit()
            return rows_inserted
    
    def get_stock_prices(self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve stock price history for a symbol
        
        Args:
            symbol: Stock symbol
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            
        Returns:
            DataFrame with stock price data
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM stock_prices 
                WHERE symbol = ?
            """
            params = [symbol]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            query += " ORDER BY date"
            
            df = pd.read_sql_query(query, conn, params=params)
            return df
    
    def get_earnings_dates(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve earnings dates for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            DataFrame with earnings dates data
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM earnings_dates 
                WHERE symbol = ?
                ORDER BY earnings_date
            """
            df = pd.read_sql_query(query, conn, params=(symbol,))
            return df
    
    def insert_option_metrics(self, option_metrics: List[OptionMetrics]) -> int:
        """
        Insert option metrics data into the database
        
        Args:
            option_metrics: List of OptionMetrics objects
            
        Returns:
            Number of rows inserted/updated
        """
        if not option_metrics:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows_inserted = 0
            
            for metrics in option_metrics:
                try:
                    data_dict = option_metrics_to_dict(metrics)
                    cursor.execute("""
                        INSERT OR REPLACE INTO option_metrics 
                        (symbol, expiration_date, strike_price, option_type, current_price, option_price,
                         intrinsic_value, time_value, moneyness, days_to_expiration, implied_volatility,
                         delta, gamma, theta, vega, rho, volume, open_interest, bid_ask_spread,
                         volume_oi_ratio, max_pain, support_level, resistance_level, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['symbol'], data_dict['expiration_date'], data_dict['strike_price'], 
                        data_dict['option_type'], data_dict['current_price'], data_dict['option_price'],
                        data_dict['intrinsic_value'], data_dict['time_value'], data_dict['moneyness'],
                        data_dict['days_to_expiration'], data_dict['implied_volatility'], data_dict['delta'],
                        data_dict['gamma'], data_dict['theta'], data_dict['vega'], data_dict['rho'],
                        data_dict['volume'], data_dict['open_interest'], data_dict['bid_ask_spread'],
                        data_dict['volume_oi_ratio'], data_dict['max_pain'], data_dict['support_level'],
                        data_dict['resistance_level'], data_dict['created_at']
                    ))
                    rows_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting option metrics: {e}")
                    continue
            
            conn.commit()
            return rows_inserted
    
    def get_option_metrics(self, symbol: str, expiration_date: Optional[str] = None, 
                          option_type: Optional[str] = None, moneyness: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve option metrics for a symbol with optional filters
        
        Args:
            symbol: Stock symbol
            expiration_date: Filter by expiration date (YYYY-MM-DD format)
            option_type: Filter by option type ('call' or 'put')
            moneyness: Filter by moneyness ('ITM', 'ATM', 'OTM')
            
        Returns:
            DataFrame with option metrics data
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM option_metrics 
                WHERE symbol = ?
            """
            params = [symbol]
            
            if expiration_date:
                query += " AND expiration_date = ?"
                params.append(expiration_date)
            
            if option_type:
                query += " AND option_type = ?"
                params.append(option_type)
            
            if moneyness:
                query += " AND moneyness = ?"
                params.append(moneyness)
            
            query += " ORDER BY expiration_date, strike_price"
            
            df = pd.read_sql_query(query, conn, params=params)
            return df
    
    def get_high_volume_options(self, min_volume: int = 1000, limit: int = 20) -> pd.DataFrame:
        """
        Retrieve options with high trading volume
        
        Args:
            min_volume: Minimum volume threshold
            limit: Maximum number of results
            
        Returns:
            DataFrame with high volume options
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM option_metrics 
                WHERE volume >= ?
                ORDER BY volume DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(min_volume, limit))
            return df
    
    def get_options_by_moneyness(self, symbol: str, moneyness: str) -> pd.DataFrame:
        """
        Retrieve options filtered by moneyness
        
        Args:
            symbol: Stock symbol
            moneyness: Moneyness filter ('ITM', 'ATM', 'OTM')
            
        Returns:
            DataFrame with filtered options
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM option_metrics 
                WHERE symbol = ? AND moneyness = ?
                ORDER BY expiration_date, strike_price
            """
            df = pd.read_sql_query(query, conn, params=(symbol, moneyness))
            return df
    
    def insert_treasury_rates(self, treasury_rates: List[TreasuryRates]) -> int:
        """
        Insert treasury rates data into the database
        
        Args:
            treasury_rates: List of TreasuryRates objects
            
        Returns:
            Number of rows inserted/updated
        """
        if not treasury_rates:
            return 0
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows_inserted = 0
            
            for rates in treasury_rates:
                try:
                    data_dict = treasury_rates_to_dict(rates)
                    cursor.execute("""
                        INSERT OR REPLACE INTO treasury_rates 
                        (date, one_month, two_month, three_month, six_month, one_year,
                         two_year, three_year, five_year, seven_year, ten_year, twenty_year,
                         thirty_year, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_dict['date'], data_dict['one_month'], data_dict['two_month'],
                        data_dict['three_month'], data_dict['six_month'], data_dict['one_year'],
                        data_dict['two_year'], data_dict['three_year'], data_dict['five_year'],
                        data_dict['seven_year'], data_dict['ten_year'], data_dict['twenty_year'],
                        data_dict['thirty_year'], data_dict['created_at']
                    ))
                    rows_inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting treasury rates: {e}")
                    continue
            
            conn.commit()
            return rows_inserted
    
    def get_treasury_rates(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve treasury rates data
        
        Args:
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            
        Returns:
            DataFrame with treasury rates data
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM treasury_rates 
                WHERE 1=1
            """
            params = []
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            query += " ORDER BY date"
            
            df = pd.read_sql_query(query, conn, params=params)
            return df
    
    def get_latest_treasury_rates(self) -> pd.DataFrame:
        """
        Retrieve the most recent treasury rates data
        
        Returns:
            DataFrame with latest treasury rates
        """
        with self.get_connection() as conn:
            query = """
                SELECT * FROM treasury_rates 
                ORDER BY date DESC 
                LIMIT 1
            """
            df = pd.read_sql_query(query, conn)
            return df

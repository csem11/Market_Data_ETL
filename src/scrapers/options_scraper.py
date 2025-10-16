"""
Options chain scraper using yfinance API
"""

import yfinance as yf
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, date
import time

from ..models import OptionsChainData, StockInfo


class OptionsScraper:
    """Options chain scraper using yfinance"""
    
    def __init__(self, rate_limit_delay: float = 0.1):
        """
        Initialize options scraper
        
        Args:
            rate_limit_delay: Delay between API calls in seconds
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Implement rate limiting for API calls"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = time.time()
    
    def get_stock_info(self, symbol: str) -> Optional[StockInfo]:
        """
        Get stock information
        
        Args:
            symbol: Stock symbol
            
        Returns:
            StockInfo object or None if not found
        """
        try:
            self._rate_limit()
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            return StockInfo(
                symbol=symbol.upper(),
                company_name=info.get('longName'),
                current_price=info.get('currentPrice'),
                market_cap=info.get('marketCap'),
                sector=info.get('sector'),
                industry=info.get('industry'),
                eff_date=datetime.now().date()  # Set effective date to today
            )
        except Exception as e:
            print(f"Error fetching stock info for {symbol}: {e}")
            return None
    
    def get_options_expiration_dates(self, symbol: str) -> List[str]:
        """
        Get available options expiration dates
        
        Args:
            symbol: Stock symbol
            
        Returns:
            List of expiration dates in YYYY-MM-DD format
        """
        try:
            self._rate_limit()
            ticker = yf.Ticker(symbol)
            expirations = ticker.options
            
            if expirations:
                # Convert to YYYY-MM-DD format
                formatted_dates = []
                for exp_date in expirations:
                    try:
                        # Parse the date and format it
                        date_obj = datetime.strptime(exp_date, '%Y-%m-%d')
                        formatted_dates.append(date_obj.strftime('%Y-%m-%d'))
                    except ValueError:
                        # If parsing fails, use the original string
                        formatted_dates.append(exp_date)
                return sorted(formatted_dates)
            
            return []
        except Exception as e:
            print(f"Error fetching expiration dates for {symbol}: {e}")
            return []
    
    def get_options_chain(self, symbol: str, expiration_date: Optional[str] = None) -> List[OptionsChainData]:
        """
        Get options chain data for a symbol
        
        Args:
            symbol: Stock symbol
            expiration_date: Specific expiration date (YYYY-MM-DD format). If None, gets nearest expiration.
            
        Returns:
            List of OptionsChainData objects
        """
        try:
            self._rate_limit()
            ticker = yf.Ticker(symbol)
            
            # Get available expiration dates
            available_dates = ticker.options
            if not available_dates:
                print(f"No options data available for {symbol}")
                return []
            
            # Select expiration date
            if expiration_date:
                if expiration_date not in available_dates:
                    print(f"Expiration date {expiration_date} not available for {symbol}")
                    return []
                target_date = expiration_date
            else:
                # Use nearest expiration date
                target_date = available_dates[0]
            
            # Get options chain data
            options_chain = ticker.option_chain(target_date)
            
            options_data = []
            
            # Get current date for effective date
            current_date = datetime.now().date()
            
            # Process calls
            if options_chain.calls is not None and not options_chain.calls.empty:
                for _, row in options_chain.calls.iterrows():
                    option = OptionsChainData(
                        symbol=symbol.upper(),
                        expiration_date=target_date,
                        strike_price=float(row.get('strike', 0)),
                        option_type='call',
                        bid=float(row.get('bid', 0)) if pd.notna(row.get('bid')) else None,
                        ask=float(row.get('ask', 0)) if pd.notna(row.get('ask')) else None,
                        last_price=float(row.get('lastPrice', 0)) if pd.notna(row.get('lastPrice')) else None,
                        volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                        open_interest=int(row.get('openInterest', 0)) if pd.notna(row.get('openInterest')) else None,
                        implied_volatility=float(row.get('impliedVolatility', 0)) if pd.notna(row.get('impliedVolatility')) else None,
                        delta=float(row.get('delta', 0)) if pd.notna(row.get('delta')) else None,
                        gamma=float(row.get('gamma', 0)) if pd.notna(row.get('gamma')) else None,
                        theta=float(row.get('theta', 0)) if pd.notna(row.get('theta')) else None,
                        vega=float(row.get('vega', 0)) if pd.notna(row.get('vega')) else None,
                        rho=float(row.get('rho', 0)) if pd.notna(row.get('rho')) else None,
                        contract_name=row.get('contractSymbol'),
                        last_trade_date=row.get('lastTradeDate'),
                        eff_date=current_date
                    )
                    options_data.append(option)
            
            # Process puts
            if options_chain.puts is not None and not options_chain.puts.empty:
                for _, row in options_chain.puts.iterrows():
                    option = OptionsChainData(
                        symbol=symbol.upper(),
                        expiration_date=target_date,
                        strike_price=float(row.get('strike', 0)),
                        option_type='put',
                        bid=float(row.get('bid', 0)) if pd.notna(row.get('bid')) else None,
                        ask=float(row.get('ask', 0)) if pd.notna(row.get('ask')) else None,
                        last_price=float(row.get('lastPrice', 0)) if pd.notna(row.get('lastPrice')) else None,
                        volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                        open_interest=int(row.get('openInterest', 0)) if pd.notna(row.get('openInterest')) else None,
                        implied_volatility=float(row.get('impliedVolatility', 0)) if pd.notna(row.get('impliedVolatility')) else None,
                        delta=float(row.get('delta', 0)) if pd.notna(row.get('delta')) else None,
                        gamma=float(row.get('gamma', 0)) if pd.notna(row.get('gamma')) else None,
                        theta=float(row.get('theta', 0)) if pd.notna(row.get('theta')) else None,
                        vega=float(row.get('vega', 0)) if pd.notna(row.get('vega')) else None,
                        rho=float(row.get('rho', 0)) if pd.notna(row.get('rho')) else None,
                        contract_name=row.get('contractSymbol'),
                        last_trade_date=row.get('lastTradeDate'),
                        eff_date=current_date
                    )
                    options_data.append(option)
            
            print(f"Retrieved {len(options_data)} options contracts for {symbol} expiring {target_date}")
            return options_data
            
        except Exception as e:
            print(f"Error fetching options chain for {symbol}: {e}")
            return []
    
    def get_multiple_expiration_dates(self, symbol: str, max_dates: int = 5) -> List[OptionsChainData]:
        """
        Get options chain data for multiple expiration dates
        
        Args:
            symbol: Stock symbol
            max_dates: Maximum number of expiration dates to fetch
            
        Returns:
            List of OptionsChainData objects
        """
        try:
            self._rate_limit()
            ticker = yf.Ticker(symbol)
            available_dates = ticker.options
            
            if not available_dates:
                print(f"No options data available for {symbol}")
                return []
            
            # Limit to max_dates or all available dates
            dates_to_fetch = available_dates[:max_dates]
            
            all_options = []
            for exp_date in dates_to_fetch:
                print(f"Fetching options for {symbol} expiring {exp_date}")
                options_data = self.get_options_chain(symbol, exp_date)
                all_options.extend(options_data)
                
                # Add delay between different expiration date requests
                if len(dates_to_fetch) > 1:
                    time.sleep(self.rate_limit_delay)
            
            print(f"Total options contracts retrieved for {symbol}: {len(all_options)}")
            return all_options
            
        except Exception as e:
            print(f"Error fetching multiple expiration dates for {symbol}: {e}")
            return []
    
    def get_sp500_options_data(self, symbols: List[str], max_expiration_dates: int = 3) -> Dict[str, List[OptionsChainData]]:
        """
        Get options data for multiple S&P 500 symbols
        
        Args:
            symbols: List of stock symbols
            max_expiration_dates: Maximum number of expiration dates per symbol
            
        Returns:
            Dictionary mapping symbols to their options data
        """
        results = {}
        
        for i, symbol in enumerate(symbols, 1):
            print(f"Processing {symbol} ({i}/{len(symbols)})")
            
            try:
                options_data = self.get_multiple_expiration_dates(symbol, max_expiration_dates)
                if options_data:
                    results[symbol] = options_data
                else:
                    print(f"No options data found for {symbol}")
                    
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
            
            # Add delay between symbols to avoid rate limiting
            if i < len(symbols):
                time.sleep(self.rate_limit_delay * 2)
        
        return results

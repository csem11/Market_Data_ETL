"""
Hybrid async options chain scraper using yfinance with asyncio for concurrent execution
This approach uses yfinance (which works reliably) but executes calls asynchronously
"""

import asyncio
import yfinance as yf
import pandas as pd
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, date
import time

from ..database.models import OptionsChainData, StockInfo


class HybridAsyncOptionsScraper:
    """Hybrid async options chain scraper using yfinance with asyncio concurrency"""
    
    def __init__(self, 
                 rate_limit_delay: float = 0.1,
                 max_concurrent_requests: int = 20,
                 timeout: int = 30):
        """
        Initialize hybrid async options scraper
        
        Args:
            rate_limit_delay: Delay between API calls in seconds
            max_concurrent_requests: Maximum concurrent requests (not used with yfinance)
            timeout: Request timeout in seconds (not used with yfinance)
        """
        self.rate_limit_delay = rate_limit_delay
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
    
    async def _rate_limit(self):
        """Implement rate limiting for API calls"""
        await asyncio.sleep(self.rate_limit_delay)
    
    async def get_stock_info(self, symbol: str) -> Optional[StockInfo]:
        """
        Get stock information asynchronously using yfinance
        
        Args:
            symbol: Stock symbol
            
        Returns:
            StockInfo object or None if not found
        """
        async with self._semaphore:
            try:
                await self._rate_limit()
                
                # Run yfinance call in thread pool to avoid blocking
                def _fetch_stock_info():
                    ticker = yf.Ticker(symbol)
                    return ticker.info
                
                info = await asyncio.to_thread(_fetch_stock_info)
                
                return StockInfo(
                    symbol=symbol.upper(),
                    company_name=info.get('longName'),
                    current_price=info.get('currentPrice'),
                    market_cap=info.get('marketCap'),
                    sector=info.get('sector'),
                    industry=info.get('industry'),
                    eff_date=datetime.now().date()
                )
                
            except Exception as e:
                print(f"Error fetching stock info for {symbol}: {e}")
                return None
    
    async def get_options_expiration_dates(self, symbol: str) -> List[str]:
        """
        Get available options expiration dates asynchronously
        
        Args:
            symbol: Stock symbol
            
        Returns:
            List of expiration dates in YYYY-MM-DD format
        """
        async with self._semaphore:
            try:
                await self._rate_limit()
                
                def _fetch_expiration_dates():
                    ticker = yf.Ticker(symbol)
                    return ticker.options
                
                expirations = await asyncio.to_thread(_fetch_expiration_dates)
                
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
    
    async def get_options_chain(self, symbol: str, expiration_date: str) -> List[OptionsChainData]:
        """
        Get options chain data for a specific expiration date asynchronously
        
        Args:
            symbol: Stock symbol
            expiration_date: Expiration date in YYYY-MM-DD format
            
        Returns:
            List of OptionsChainData objects
        """
        async with self._semaphore:
            try:
                await self._rate_limit()
                
                def _fetch_options_chain():
                    ticker = yf.Ticker(symbol)
                    return ticker.option_chain(expiration_date)
                
                options_chain = await asyncio.to_thread(_fetch_options_chain)
                
                options_data = []
                current_date = datetime.now().date()
                
                # Process calls
                if options_chain.calls is not None and not options_chain.calls.empty:
                    for _, row in options_chain.calls.iterrows():
                        option = OptionsChainData(
                            symbol=symbol.upper(),
                            expiration_date=expiration_date,
                            strike_price=float(row.get('strike', 0)),
                            option_type='call',
                            bid=float(row.get('bid', 0)) if pd.notna(row.get('bid')) else None,
                            ask=float(row.get('ask', 0)) if pd.notna(row.get('ask')) else None,
                            last_price=float(row.get('lastPrice', 0)) if pd.notna(row.get('lastPrice')) else None,
                            volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                            open_interest=int(row.get('openInterest', 0)) if pd.notna(row.get('openInterest')) else None,
                            implied_volatility=float(row.get('impliedVolatility', 0)) if pd.notna(row.get('impliedVolatility')) else None,
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
                            expiration_date=expiration_date,
                            strike_price=float(row.get('strike', 0)),
                            option_type='put',
                            bid=float(row.get('bid', 0)) if pd.notna(row.get('bid')) else None,
                            ask=float(row.get('ask', 0)) if pd.notna(row.get('ask')) else None,
                            last_price=float(row.get('lastPrice', 0)) if pd.notna(row.get('lastPrice')) else None,
                            volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                            open_interest=int(row.get('openInterest', 0)) if pd.notna(row.get('openInterest')) else None,
                            implied_volatility=float(row.get('impliedVolatility', 0)) if pd.notna(row.get('impliedVolatility')) else None,
                            contract_name=row.get('contractSymbol'),
                            last_trade_date=row.get('lastTradeDate'),
                            eff_date=current_date
                        )
                        options_data.append(option)
                
                return options_data
                
            except Exception as e:
                print(f"Error fetching options chain for {symbol} expiring {expiration_date}: {e}")
                return []
    
    async def get_multiple_expiration_dates(self, symbol: str, max_dates: int = 30) -> List[OptionsChainData]:
        """
        Get options chain data for multiple expiration dates concurrently
        
        Args:
            symbol: Stock symbol
            max_dates: Maximum number of expiration dates to fetch
            
        Returns:
            List of OptionsChainData objects
        """
        try:
            # Get available expiration dates
            exp_dates = await self.get_options_expiration_dates(symbol)
            if not exp_dates:
                print(f"No options data available for {symbol}")
                return []
            
            # Limit to max_dates
            dates_to_fetch = exp_dates[:max_dates]
            
            # Create tasks for concurrent execution
            tasks = []
            for exp_date in dates_to_fetch:
                task = self.get_options_chain(symbol, exp_date)
                tasks.append(task)
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Combine results
            all_options = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Error fetching options for {symbol} expiring {dates_to_fetch[i]}: {result}")
                else:
                    all_options.extend(result)
            
            print(f"Retrieved {len(all_options)} options contracts for {symbol}")
            return all_options
            
        except Exception as e:
            print(f"Error fetching multiple expiration dates for {symbol}: {e}")
            return []
    
    async def get_stock_info_batch(self, symbols: List[str]) -> Dict[str, Optional[StockInfo]]:
        """
        Get stock information for multiple symbols concurrently
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to StockInfo objects
        """
        tasks = []
        for symbol in symbols:
            task = self.get_stock_info(symbol)
            tasks.append((symbol, task))
        
        results = {}
        for symbol, task in tasks:
            try:
                result = await task
                results[symbol] = result
            except Exception as e:
                print(f"Error fetching stock info for {symbol}: {e}")
                results[symbol] = None
        
        return results
    
    async def get_options_data_batch(self, symbols: List[str], max_expiration_dates: int = 30) -> Dict[str, List[OptionsChainData]]:
        """
        Get options data for multiple symbols concurrently
        
        Args:
            symbols: List of stock symbols
            max_expiration_dates: Maximum number of expiration dates per symbol
            
        Returns:
            Dictionary mapping symbols to their options data
        """
        tasks = []
        for symbol in symbols:
            task = self.get_multiple_expiration_dates(symbol, max_expiration_dates)
            tasks.append((symbol, task))
        
        results = {}
        for symbol, task in tasks:
            try:
                result = await task
                results[symbol] = result
            except Exception as e:
                print(f"Error fetching options data for {symbol}: {e}")
                results[symbol] = []
        
        return results
    
    async def get_sp500_options_data(self, symbols: List[str], max_expiration_dates: int = 30) -> Tuple[Dict[str, Optional[StockInfo]], Dict[str, List[OptionsChainData]]]:
        """
        Get both stock info and options data for S&P 500 symbols concurrently
        
        Args:
            symbols: List of stock symbols
            max_expiration_dates: Maximum number of expiration dates per symbol
            
        Returns:
            Tuple of (stock_info_dict, options_data_dict)
        """
        print(f"Starting concurrent fetch for {len(symbols)} symbols...")
        start_time = time.time()
        
        # Run stock info and options data fetching concurrently
        stock_info_task = self.get_stock_info_batch(symbols)
        options_data_task = self.get_options_data_batch(symbols, max_expiration_dates)
        
        stock_info_results, options_data_results = await asyncio.gather(
            stock_info_task, 
            options_data_task
        )
        
        end_time = time.time()
        print(f"Completed concurrent fetch in {end_time - start_time:.2f} seconds")
        
        return stock_info_results, options_data_results

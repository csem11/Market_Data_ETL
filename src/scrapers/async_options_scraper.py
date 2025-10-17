"""
Async options chain scraper using aiohttp for faster concurrent requests
"""

import asyncio
import aiohttp
import ssl
import json
import pandas as pd
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, date
import time
from asyncio_throttle import Throttler

from ..database.models import OptionsChainData, StockInfo


class AsyncOptionsScraper:
    """Async options chain scraper using aiohttp for concurrent requests"""
    
    def __init__(self, 
                 rate_limit_per_second: float = 10.0,
                 max_concurrent_requests: int = 20,
                 timeout: int = 30,
                 max_retries: int = 3):
        """
        Initialize async options scraper
        
        Args:
            rate_limit_per_second: Maximum requests per second
            max_concurrent_requests: Maximum concurrent requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
        """
        self.rate_limit_per_second = rate_limit_per_second
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout
        self.max_retries = max_retries
        self.throttler = Throttler(rate_limit=rate_limit_per_second, period=1.0)
        
        # Yahoo Finance API endpoints
        self.base_url = "https://query1.finance.yahoo.com"
        self.options_url = f"{self.base_url}/v7/finance/options"
        self.quote_url = f"{self.base_url}/v8/finance/chart"
        
    async def _make_request(self, session: aiohttp.ClientSession, url: str, params: Dict = None) -> Optional[Dict]:
        """
        Make an async HTTP request with retry logic
        
        Args:
            session: aiohttp session
            url: Request URL
            params: Query parameters
            
        Returns:
            JSON response data or None if failed
        """
        async with self.throttler:
            for attempt in range(self.max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=self.timeout)
                    async with session.get(url, params=params, timeout=timeout) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data
                        elif response.status == 429:  # Rate limited
                            wait_time = 2 ** attempt
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            print(f"HTTP {response.status} for {url}")
                            return None
                            
                except asyncio.TimeoutError:
                    print(f"Timeout for {url} (attempt {attempt + 1})")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue
                except Exception as e:
                    print(f"Request error for {url}: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue
                    
            return None
    
    async def get_stock_info(self, session: aiohttp.ClientSession, symbol: str) -> Optional[StockInfo]:
        """
        Get stock information asynchronously
        
        Args:
            session: aiohttp session
            symbol: Stock symbol
            
        Returns:
            StockInfo object or None if not found
        """
        try:
            url = f"{self.quote_url}/{symbol}"
            params = {
                'range': '1d',
                'interval': '1d',
                'includePrePost': 'false',
                'corsDomain': 'finance.yahoo.com'
            }
            
            data = await self._make_request(session, url, params)
            if not data or 'chart' not in data or not data['chart']['result']:
                return None
                
            result = data['chart']['result'][0]
            meta = result.get('meta', {})
            
            # Get current price from latest quote
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            if quotes.get('close'):
                current_price = quotes['close'][-1] if quotes['close'][-1] is not None else None
            else:
                current_price = meta.get('regularMarketPrice')
            
            return StockInfo(
                symbol=symbol.upper(),
                company_name=meta.get('longName'),
                current_price=current_price,
                market_cap=meta.get('marketCap'),
                sector=meta.get('sector'),
                industry=meta.get('industry'),
                eff_date=datetime.now().date()
            )
            
        except Exception as e:
            print(f"Error fetching stock info for {symbol}: {e}")
            return None
    
    async def get_options_expiration_dates(self, session: aiohttp.ClientSession, symbol: str) -> List[str]:
        """
        Get available options expiration dates asynchronously
        
        Args:
            session: aiohttp session
            symbol: Stock symbol
            
        Returns:
            List of expiration dates in YYYY-MM-DD format
        """
        try:
            url = f"{self.options_url}/{symbol}"
            data = await self._make_request(session, url)
            
            if not data or 'optionChain' not in data:
                return []
                
            option_chain = data['optionChain']
            if not option_chain['result']:
                return []
                
            expirations = option_chain['result'][0].get('expirationDates', [])
            
            # Convert timestamps to YYYY-MM-DD format
            formatted_dates = []
            for timestamp in expirations:
                try:
                    date_obj = datetime.fromtimestamp(timestamp)
                    formatted_dates.append(date_obj.strftime('%Y-%m-%d'))
                except (ValueError, OSError):
                    continue
                    
            return sorted(formatted_dates)
            
        except Exception as e:
            print(f"Error fetching expiration dates for {symbol}: {e}")
            return []
    
    async def get_options_chain(self, session: aiohttp.ClientSession, symbol: str, expiration_date: str) -> List[OptionsChainData]:
        """
        Get options chain data for a specific expiration date asynchronously
        
        Args:
            session: aiohttp session
            symbol: Stock symbol
            expiration_date: Expiration date in YYYY-MM-DD format
            
        Returns:
            List of OptionsChainData objects
        """
        try:
            # Convert date to timestamp
            date_obj = datetime.strptime(expiration_date, '%Y-%m-%d')
            timestamp = int(date_obj.timestamp())
            
            url = f"{self.options_url}/{symbol}"
            params = {'date': timestamp}
            
            data = await self._make_request(session, url, params)
            if not data or 'optionChain' not in data:
                return []
                
            option_chain = data['optionChain']
            if not option_chain['result']:
                return []
                
            result = option_chain['result'][0]
            options_data = []
            current_date = datetime.now().date()
            
            # Process calls
            calls = result.get('options', [{}])[0].get('calls', [])
            for call in calls:
                option = OptionsChainData(
                    symbol=symbol.upper(),
                    expiration_date=expiration_date,
                    strike_price=float(call.get('strike', 0)),
                    option_type='call',
                    bid=float(call.get('bid', 0)) if call.get('bid') is not None else None,
                    ask=float(call.get('ask', 0)) if call.get('ask') is not None else None,
                    last_price=float(call.get('lastPrice', 0)) if call.get('lastPrice') is not None else None,
                    volume=int(call.get('volume', 0)) if call.get('volume') is not None else None,
                    open_interest=int(call.get('openInterest', 0)) if call.get('openInterest') is not None else None,
                    implied_volatility=float(call.get('impliedVolatility', 0)) if call.get('impliedVolatility') is not None else None,
                    contract_name=call.get('contractSymbol'),
                    last_trade_date=datetime.fromtimestamp(call['lastTradeDate']) if call.get('lastTradeDate') else None,
                    eff_date=current_date
                )
                options_data.append(option)
            
            # Process puts
            puts = result.get('options', [{}])[0].get('puts', [])
            for put in puts:
                option = OptionsChainData(
                    symbol=symbol.upper(),
                    expiration_date=expiration_date,
                    strike_price=float(put.get('strike', 0)),
                    option_type='put',
                    bid=float(put.get('bid', 0)) if put.get('bid') is not None else None,
                    ask=float(put.get('ask', 0)) if put.get('ask') is not None else None,
                    last_price=float(put.get('lastPrice', 0)) if put.get('lastPrice') is not None else None,
                    volume=int(put.get('volume', 0)) if put.get('volume') is not None else None,
                    open_interest=int(put.get('openInterest', 0)) if put.get('openInterest') is not None else None,
                    implied_volatility=float(put.get('impliedVolatility', 0)) if put.get('impliedVolatility') is not None else None,
                    delta=float(put.get('delta', 0)) if put.get('delta') is not None else None,
                    gamma=float(put.get('gamma', 0)) if put.get('gamma') is not None else None,
                    theta=float(put.get('theta', 0)) if put.get('theta') is not None else None,
                    vega=float(put.get('vega', 0)) if put.get('vega') is not None else None,
                    rho=float(put.get('rho', 0)) if put.get('rho') is not None else None,
                    contract_name=put.get('contractSymbol'),
                    last_trade_date=datetime.fromtimestamp(put['lastTradeDate']) if put.get('lastTradeDate') else None,
                    eff_date=current_date
                )
                options_data.append(option)
            
            return options_data
            
        except Exception as e:
            print(f"Error fetching options chain for {symbol} expiring {expiration_date}: {e}")
            return []
    
    async def get_multiple_expiration_dates(self, session: aiohttp.ClientSession, symbol: str, max_dates: int = 30) -> List[OptionsChainData]:
        """
        Get options chain data for multiple expiration dates concurrently
        
        Args:
            session: aiohttp session
            symbol: Stock symbol
            max_dates: Maximum number of expiration dates to fetch
            
        Returns:
            List of OptionsChainData objects
        """
        try:
            # Get available expiration dates
            exp_dates = await self.get_options_expiration_dates(session, symbol)
            if not exp_dates:
                print(f"No options data available for {symbol}")
                return []
            
            # Limit to max_dates
            dates_to_fetch = exp_dates[:max_dates]
            
            # Create tasks for concurrent execution
            tasks = []
            for exp_date in dates_to_fetch:
                task = self.get_options_chain(session, symbol, exp_date)
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
        # Create SSL context that doesn't verify certificates (for macOS compatibility)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests, ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in symbols:
                task = self.get_stock_info(session, symbol)
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
        # Create SSL context that doesn't verify certificates (for macOS compatibility)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests, ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in symbols:
                task = self.get_multiple_expiration_dates(session, symbol, max_expiration_dates)
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

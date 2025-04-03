import os
import re
import gzip
import time

import pandas as pd
import requests
import pytz
from datetime import datetime
from io import BytesIO
from decimal import Decimal
from typing import Dict, Optional
from services.database import DatabaseService
from config import Config

class OptionChainService:
    def __init__(self, max_retries=3):
        self.BASE_URL = "https://assets.upstox.com/market-quote/instruments/exchange"
        self.UPSTOX_BASE_URL = "https://api.upstox.com"
        self.symbol_to_instrument = {}
        self.market_open = Config.MARKET_OPEN
        self.market_close = Config.MARKET_CLOSE
        self.trading_days = Config.TRADING_DAYS
        self.instruments_data = None
        self.max_retries = max_retries
        self._load_instruments_with_retry()
        self.fno_stocks = self._load_fno_stocks()
        self.database = DatabaseService()


    def get_fno_stocks_with_symbols(self):
        """Return F&O stocks list with proper Yahoo Finance symbols"""
        return [f"{stock}.NS" for stock in self.fno_stocks.keys()]

    def _load_instruments_with_retry(self):
        """Load instruments data with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._load_instruments_data()
                if self.instruments_data is not None:
                    return
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Failed to load instruments after {self.max_retries} attempts")
                time.sleep(2 ** attempt)  # Exponential backoff

    def _load_instruments_data(self):
        """Load all instruments data from Upstox"""
        try:
            url = f"{self.BASE_URL}/complete.csv.gz"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
                df = pd.read_csv(f)
                df.columns = df.columns.str.lower()

                # Ensure required columns exist
                required_columns = {'exchange', 'tradingsymbol', 'lot_size', 'instrument_key'}
                if not required_columns.issubset(df.columns):
                    raise ValueError("Missing required columns in instruments data")

                self.instruments_data = df
                print("Successfully loaded instruments data")

        except Exception as e:
            print(f"Error loading instruments data: {str(e)}")
            self.instruments_data = None
            raise

    def _load_fno_stocks(self) -> Dict[str, int]:
        """Load F&O stocks list with lot sizes"""
        if self.instruments_data is None:
            print("Instruments data not available")
            return {}

        try:
            # Get NSE F&O stocks (excluding indices)
            nse_fno = self.instruments_data[
                (self.instruments_data['exchange'] == 'NSE_FO') &
                (~self.instruments_data['tradingsymbol'].str.contains('NIFTY|BANKNIFTY|FINNIFTY', regex=True))
                ]

            # Create mapping of base symbol to lot size
            fno_stocks = {}
            for _, row in nse_fno.iterrows():
                try:
                    # Extract base symbol (e.g., "RELIANCE" from "RELIANCE21OCT2000CE")
                    base_symbol = re.split(r'\d', row['tradingsymbol'], 1)[0]
                    fno_stocks[base_symbol] = int(row['lot_size'])
                except (ValueError, TypeError) as e:
                    print(f"Skipping invalid row: {row['tradingsymbol']} - {str(e)}")
                    continue

            print(f"Loaded {len(fno_stocks)} F&O stocks")
            return fno_stocks

        except Exception as e:
            print(f"Error processing F&O stocks: {str(e)}")
            return {}

    def _get_instrument_key(self, symbol: str) -> Optional[str]:
        """Get instrument key for a stock"""
        if self.instruments_data is None:
            print("Instruments data not loaded")
            return None

        try:
            # Try NSE equities first
            eq_match = self.instruments_data[
                (self.instruments_data['exchange'] == 'NSE_EQ') &
                (self.instruments_data['tradingsymbol'] == symbol.upper())
                ]

            if not eq_match.empty:
                return eq_match.iloc[0]['instrument_key']

            # Try indices if not found in equities
            index_map = {
                'NIFTY': 'NSE_INDEX|Nifty 50',
                'BANKNIFTY': 'NSE_INDEX|Nifty Bank',
                'FINNIFTY': 'NSE_INDEX|Nifty Financial Services'
            }

            return index_map.get(symbol.upper())

        except Exception as e:
            print(f"Error looking up instrument key for {symbol}: {str(e)}")
            return None

    def _get_futures_instrument_key(self, symbol: str) -> Optional[str]:
        """Get futures instrument key for current expiry"""
        if self.instruments_data is None:
            print("Instruments data not loaded")
            return None

        try:
            # Convert expiry date to futures symbol format (e.g., "25APR" for April 2025)
            expiry_year = Config.EXPIRY_DATE[2:4]  # Last two digits of year
            expiry_month = Config.EXPIRY_DATE[5:7]  # Two-digit month

            month_map = {
                '01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR',
                '05': 'MAY', '06': 'JUN', '07': 'JUL', '08': 'AUG',
                '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'
            }
            month_abbr = month_map.get(expiry_month, '')

            fut_symbol = f"{symbol}{expiry_year}{month_abbr}FUT"

            # Find matching futures contract
            fut_match = self.instruments_data[
                (self.instruments_data['exchange'] == 'NSE_FO') &
                (self.instruments_data['tradingsymbol'] == fut_symbol)
                ]

            if not fut_match.empty:
                return fut_match.iloc[0]['instrument_key']

            print(f"No futures contract found for {fut_symbol}")
            return None

        except Exception as e:
            print(f"Error looking up futures instrument key for {symbol}: {str(e)}")
            return None

        # Load F&O stocks

    def get_fno_stocks(self):
        print("inside get_fno_stocks")
        return self.fno_stocks

    def fetch_stocks(self):
        df = self._fetch_csv_data(f"{self.BASE_URL}/complete.csv.gz")
        if df is None:
            return {"error": "Failed to fetch stocks"}

        df.columns = df.columns.str.lower()
        if not {'exchange', 'tradingsymbol', 'lot_size', 'instrument_key'}.issubset(df.columns):
            return {"error": "Required columns not found"}

        # Process NSE and BSE F&O data
        nse_fno = df[df['exchange'] == 'NSE_FO'][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()
        bse_fno = df[(df['exchange'] == 'BSE_FO') &
                     (df['tradingsymbol'].str.contains('SENSEX|BANKEX', regex=True))][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()

        fno_stocks_raw = pd.concat([nse_fno, bse_fno])
        stock_data = {}
        self.symbol_to_instrument = {}

        for _, row in fno_stocks_raw.iterrows():
            parsed = self._parse_option_symbol(row['tradingsymbol'])
            if parsed:
                stock = parsed["stock"]
                if stock not in stock_data:
                    stock_data[stock] = {"expiries": set(), "options": [], "instrument_key": None}

                stock_data[stock]["expiries"].add(parsed["expiry"])
                stock_data[stock]["options"].append({
                    "expiry": parsed["expiry"],
                    "strike_price": parsed["strike_price"],
                    "option_type": parsed["option_type"],
                    "lot_size": row['lot_size'],
                    "instrument_key": row['instrument_key'],
                    "tradingsymbol": row['tradingsymbol']
                })
                self.symbol_to_instrument[row['tradingsymbol']] = row['instrument_key']

        # Add equity instrument keys
        nse_eq = df[df['exchange'] == 'NSE_EQ'][['tradingsymbol', 'instrument_key']].dropna()
        for _, row in nse_eq.iterrows():
            stock_name = row['tradingsymbol']
            self.symbol_to_instrument[stock_name] = row['instrument_key']
            if stock_name in stock_data:
                stock_data[stock_name]["instrument_key"] = row['instrument_key']

        # Add index instrument keys
        index_keys = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
            "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
            "SENSEX": "BSE_INDEX|SENSEX",
            "BANKEX": "BSE_INDEX|BANKEX"
        }

        for index, key in index_keys.items():
            if index in stock_data:
                stock_data[index]["instrument_key"] = key
                self.symbol_to_instrument[index] = key

        for stock in stock_data:
            stock_data[stock]["expiries"] = sorted(stock_data[stock]["expiries"])

        return stock_data

    def fetch_price(self, instrument_key):
        if not instrument_key:
            return {"error": "Instrument key is required"}

        trading_symbol = next((sym for sym, key in self.symbol_to_instrument.items() if key == instrument_key), None)
        if not trading_symbol:
            return {"error": "Trading symbol not found"}

        parsed_data = self._parse_option_symbol(trading_symbol)
        if not parsed_data:
            return {"error": "Failed to parse option symbol"}

        stock_name = parsed_data["stock"]
        stock_instrument_key = self.symbol_to_instrument.get(stock_name)
        instrument_keys = [instrument_key, stock_instrument_key]

        url = f"{self.UPSTOX_BASE_URL}/v2/market-quote/quotes"
        headers = {"Authorization": f"Bearer {Config.ACCESS_TOKEN}"}
        params = {'instrument_key': ','.join(instrument_keys)}

        response = requests.get(url, headers=headers, params=params)
        data = response.json().get("data", {})
        # Get option price
        if "BSE_FO" in instrument_key:
            option_key = f"BSE_FO:{trading_symbol}"
        else:
            option_key = f"NSE_FO:{trading_symbol}"
        option_price = data.get(option_key, {}).get("last_price", "N/A")

        # Get stock price
        index_symbols = ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"]
        if stock_name in index_symbols:
            stock_price = data.get(stock_instrument_key.replace('|', ':'), {}).get("last_price", "N/A")
        else:
            if "BSE_EQ" in stock_instrument_key:
                stock_price = data.get(f"BSE_EQ:{stock_name}", {}).get("last_price", "N/A")
            else:
                stock_price = data.get(f"NSE_EQ:{stock_name}", {}).get("last_price", "N/A")

        return {
            "option_price": option_price,
            "stock_price": stock_price
        }

    def fetch_bulk_prices(self, instrument_keys):
        if not instrument_keys:
            return {"error": "Instrument keys are required"}

        all_keys = []
        stock_keys = set()

        for instrument_key in instrument_keys:
            trading_symbol = next((sym for sym, key in self.symbol_to_instrument.items() if key == instrument_key), None)
            if trading_symbol:
                parsed_data = self._parse_option_symbol(trading_symbol)
                if parsed_data:
                    stock_name = parsed_data["stock"]
                    stock_key = self.symbol_to_instrument.get(stock_name)
                    if stock_key:
                        stock_keys.add(stock_key)

        all_keys.extend(instrument_keys)
        all_keys.extend(stock_keys)

        url = f"{self.UPSTOX_BASE_URL}/v2/market-quote/quotes"
        headers = {"Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}"}
        params = {'instrument_key': ','.join(all_keys)}

        try:
            response = requests.get(url, headers=headers, params=params)
            data = response.json().get("data", {})

            result = {}
            index_symbols = ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"]

            for instrument_key in instrument_keys:
                trading_symbol = next((sym for sym, key in self.symbol_to_instrument.items() if key == instrument_key), None)
                if trading_symbol:
                    parsed_data = self._parse_option_symbol(trading_symbol)
                    if parsed_data:
                        stock_name = parsed_data["stock"]

                        # Get option price
                        if "BSE_FO" in instrument_key:
                            option_key = f"BSE_FO:{trading_symbol}"
                        else:
                            option_key = f"NSE_FO:{trading_symbol}"
                        option_data = data.get(option_key, {})
                        option_price = option_data.get("last_price", 0)

                        # Get stock price
                        if stock_name in index_symbols:
                            stock_key = self.symbol_to_instrument.get(stock_name).replace('|', ':')
                            stock_price = data.get(stock_key, {}).get("last_price", 0)
                        else:
                            if "BSE_EQ" in self.symbol_to_instrument.get(stock_name, ""):
                                stock_price = data.get(f"BSE_EQ:{stock_name}", {}).get("last_price", 0)
                            else:
                                stock_price = data.get(f"NSE_EQ:{stock_name}", {}).get("last_price", 0)

                        result[instrument_key] = {
                            "option_price": float(option_price) if option_price else 0,
                            "stock_price": float(stock_price) if stock_price else 0
                        }

            return result
        except Exception as e:
            return {"error": str(e)}

    def run_market_processing(self):
        """Background worker for market hours processing"""
        last_clear_date = None
        IST = pytz.timezone("Asia/Kolkata")
    
        while True:
            now = datetime.now(IST)

            # Clear old data at market open
            if (now.weekday() < 5 and Config.MARKET_OPEN >= now.time() <= Config.MARKET_CLOSE and
                    (last_clear_date is None or last_clear_date != now.date())):
                try:
                    self.database.clear_old_data()
                    last_clear_date = now.date()
                    print("Cleared all previous day's data")
                except Exception as e:
                    print(f"Failed to clear old data: {e}")

            # Process during market hours
            if now.weekday() < 5 and Config.MARKET_OPEN <= now.time() <= Config.MARKET_CLOSE:
                try:
                    self._fetch_and_store_orders()
                    time.sleep(30)  # Run every 30 seconds
                except Exception as e:
                    print(f"Script error: {e}")
                    time.sleep(60)
            else:
                time.sleep(300)  # 5 min sleep outside market hours

    def _fetch_and_store_orders(self):
        """Fetch and store option chain data"""
        total_start = time.time()
        processed_stocks = 0
        MIN_REQUEST_INTERVAL = 0.3  # Rate limiting
        last_api_call = 0

        for stock, lot_size in self.fno_stocks.items():
            try:
                # Rate limiting
                elapsed = time.time() - last_api_call
                if elapsed < MIN_REQUEST_INTERVAL:
                    time.sleep(MIN_REQUEST_INTERVAL - elapsed)

                last_api_call = time.time()
                result = self._fetch_option_chain(stock, lot_size)

                if not result:
                    continue

                # Save data
                self.database.save_options_data(stock, result['options_orders'])
                self.database.save_futures_data(stock, result['futures_orders'])
                self.database.save_oi_volume_batch(result['oi_records'])

                processed_stocks += 1
                print(f"Saved {stock} data")

            except Exception as e:
                print(f"Failed {stock}: {type(e).__name__} - {str(e)}")
                continue

        total_time = time.time() - total_start
        print(f"\nCompleted {processed_stocks}/{len(self.fno_stocks)} stocks in {total_time:.1f}s")

    def _fetch_option_chain(self, stock_symbol, lot_size, expiry_date="2025-04-24"):
        """Fetch option chain data for a stock"""
        try:
            # Get instrument key
            instrument_key = self._get_instrument_key(stock_symbol)
            if not instrument_key:
                print(f"No instrument key for {stock_symbol}")
                return None

            # Fetch option chain
            url = 'https://api.upstox.com/v2/option/chain'
            headers = {'Authorization': f'Bearer {os.getenv("ACCESS_TOKEN")}'}
            params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

            response = requests.get(url, params=params, headers=headers)
            if response.status_code != 200:
                print(f"Option chain API failed for {stock_symbol}: {response.status_code}")
                return None

            data = response.json().get('data', [])
            if not data:
                print(f"No option chain data for {stock_symbol}")
                return None

            # Prepare market quotes request
            spot_price = data[0].get('underlying_spot_price', 0)
            strikes = sorted(set(option['strike_price'] for option in data))
            closest_strikes = [s for s in strikes if s <= spot_price][-3:] + [s for s in strikes if s >= spot_price][:3]

            instrument_keys = []
            for option in data:
                if option['strike_price'] in closest_strikes:
                    if option['call_options'].get('instrument_key'):
                        instrument_keys.append(option['call_options']['instrument_key'])
                    if option['put_options'].get('instrument_key'):
                        instrument_keys.append(option['put_options']['instrument_key'])

            # Add futures instrument key
            fut_instrument_key = self._get_futures_instrument_key(stock_symbol)
            if fut_instrument_key:
                instrument_keys.append(fut_instrument_key)

            if not instrument_keys:
                print(f"No valid instrument keys for {stock_symbol}")
                return None

            # Fetch market quotes
            market_quotes = self._fetch_market_quotes(instrument_keys)
            if not market_quotes:
                print(f"No market quotes for {stock_symbol}")
                return None

            # Process data
            result = {
                'options_orders': [],
                'futures_orders': [],
                'oi_records': []
            }

            # Process futures
            futures_orders = self._process_futures_orders(market_quotes, stock_symbol, lot_size)
            if futures_orders:
                result['futures_orders'] = futures_orders

            # Process options
            for key, quote_data in market_quotes.items():
                symbol = quote_data.get('symbol', '')

                if key == fut_instrument_key:
                    continue

                if symbol.endswith("CE"):
                    option_type = "CE"
                elif symbol.endswith("PE"):
                    option_type = "PE"
                else:
                    continue

                strike_match = re.search(r'(\d+)(CE|PE)$', symbol)
                if not strike_match:
                    continue

                strike_price = int(strike_match.group(1))
                depth = quote_data.get('depth', {})
                top_bids = depth.get('buy', [])[:5]
                top_asks = depth.get('sell', [])[:5]
                ltp = quote_data.get('last_price', 0)

                threshold = lot_size * 87
                has_large_bid = any(bid['quantity'] >= threshold for bid in top_bids)
                has_large_ask = any(ask['quantity'] >= threshold for ask in top_asks)

                if (has_large_bid or has_large_ask) and ltp > 2:
                    result['options_orders'].append({
                        'stock': stock_symbol,
                        'strike_price': strike_price,
                        'type': option_type,
                        'ltp': ltp,
                        'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                        'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                        'lot_size': lot_size,
                        'timestamp': datetime.now(pytz.timezone('Asia/Kolkata'))
                    })

                # Prepare OI data
                result['oi_records'].append({
                    'symbol': stock_symbol,
                    'expiry': expiry_date,
                    'strike': strike_price,
                    'option_type': option_type,
                    'oi': Decimal(str(quote_data.get('oi', 0))),
                    'volume': Decimal(str(quote_data.get('volume', 0))),
                    'price': Decimal(str(ltp)),
                    'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M")
                })

            print(f"{stock_symbol}: {len(result['options_orders'])} orders, {len(result['oi_records'])} OI records")
            return result

        except Exception as e:
            print(f"Error in fetch_option_chain for {stock_symbol}: {str(e)}")
            return None

    def _process_futures_orders(self, market_quotes, stock_symbol, lot_size):
        """Detect large futures orders"""
        large_orders = []
        prefix = f"NSE_FO:{stock_symbol}"
        suffix = "APRFUT"

        pattern = re.compile(rf"{prefix}\d+{suffix}")
        fut_instrument_key = next((key for key in market_quotes if pattern.match(key)), None)

        if not fut_instrument_key:
            print(f"No data found for {stock_symbol} futures")
            return large_orders

        futures_data = market_quotes[fut_instrument_key]
        depth_data = futures_data.get('depth', {})
        top_bids = depth_data.get('buy', [])[:5]
        top_asks = depth_data.get('sell', [])[:5]

        threshold = lot_size * 36
        ltp = futures_data.get('last_price', 0)
        valid_bid = any(bid['quantity'] >= threshold for bid in top_bids)
        valid_ask = any(ask['quantity'] >= threshold for ask in top_asks)

        if (valid_bid or valid_ask) and ltp > 2:
            large_orders.append({
                'stock': stock_symbol,
                'ltp': ltp,
                'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                'lot_size': lot_size,
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata'))
            })

        return large_orders

    def _fetch_market_quotes(self, instrument_keys):
        """Fetch market quotes with rate limiting"""
        try:
            url = 'https://api.upstox.com/v2/market-quote/quotes'
            headers = {'Authorization': f'Bearer {os.getenv("ACCESS_TOKEN")}'}
            params = {'instrument_key': ','.join(instrument_keys)}

            response = requests.get(url, headers=headers, params=params)
            return response.json().get('data', {}) if response.status_code == 200 else {}
        except Exception as e:
            print(f"Request failed: {str(e)}")
            return {}

    def _fetch_csv_data(self, url):
        """Fetch and parse gzipped CSV data"""
        response = requests.get(url)
        if response.status_code == 200:
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
                return pd.read_csv(f)
        return None

    def _parse_option_symbol(self, symbol):
        """Parse option symbol into components"""
        match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)", symbol)
        if match:
            return {
                "stock": match.group(1),
                "expiry": match.group(2),
                "strike_price": int(match.group(3)),
                "option_type": match.group(4)
            }
        return None

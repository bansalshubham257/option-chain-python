import os
import re
import gzip
import time

import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta, date
from io import BytesIO
from decimal import Decimal
from typing import Dict, Optional
from services.database import DatabaseService
from config import Config
from psycopg2.extras import execute_batch

from concurrent.futures import ThreadPoolExecutor, as_completed

import functools
from typing import List
from calendar import monthrange

class OptionChainService:
    def __init__(self, max_retries=3):
        self.BASE_URL = "https://assets.upstox.com/market-quote/instruments/exchange"
        self.UPSTOX_BASE_URL = "https://api.upstox.com"
        self.symbol_to_instrument = {}
        self.instruments_data = None
        self.market_open = Config.MARKET_OPEN
        self.market_close = Config.MARKET_CLOSE
        self.max_retries = max_retries
        self._load_instruments_with_retry()
        self.fno_stocks = self._load_fno_stocks()
        self.database = DatabaseService()
        self._last_instruments_load = None
        self._instruments_cache_ttl = 3600  # 1 hour cache

    @functools.lru_cache(maxsize=1)
    def _load_instruments_data_cached(self) -> Optional[pd.DataFrame]:
        """Cached version of instruments data loader"""
        try:
            url = f"{self.BASE_URL}/complete.csv.gz"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
                df = pd.read_csv(f, usecols=['exchange', 'tradingsymbol', 'lot_size', 'instrument_key'])
                df.columns = df.columns.str.lower()
                return df

        except Exception as e:
            print(f"Error loading instruments data: {str(e)}")
            return None

    def _load_instruments_data(self):
        """Load instruments data with cache support"""
        now = time.time()
        if (self._last_instruments_load is None or
                (now - self._last_instruments_load) > self._instruments_cache_ttl):
            self.instruments_data = self._load_instruments_data_cached()
            self._last_instruments_load = now
            if self.instruments_data is not None:
                print("Successfully loaded fresh instruments data")
            else:
                print("Failed to load instruments data")
        else:
            print("Using cached instruments data")

    def _process_fno_stocks_parallel(self, df: pd.DataFrame) -> Dict[str, int]:
        """Parallel processing of F&O stocks"""
        try:
            # Filter NSE F&O stocks (excluding indices)
            nse_fno = df[
                (df['exchange'] == 'NSE_FO') &
                (~df['tradingsymbol'].str.contains('NIFTY|BANKNIFTY|FINNIFTY', regex=True))
                ].copy()

            # Parallel processing
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for _, row in nse_fno.iterrows():
                    futures.append(executor.submit(self._process_fno_row, row))

                fno_stocks = {}
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        fno_stocks.update(result)

                return fno_stocks

        except Exception as e:
            print(f"Error processing F&O stocks: {str(e)}")
            return {}

    def _process_fno_row(self, row) -> Dict[str, int]:
        """Process a single F&O row"""
        try:
            base_symbol = re.split(r'\d', row['tradingsymbol'], 1)[0]
            return {base_symbol: int(row['lot_size'])}
        except (ValueError, TypeError) as e:
            print(f"Skipping invalid row: {row['tradingsymbol']} - {str(e)}")
            return {}

    def fetch_stocks(self):
        """Fetch all stocks options details from oi_volume_history table"""
        try:
            with self.database._get_cursor() as cur:
                # Query to get distinct options data from the database with lot_size
                # and join with stock_data_cache to get spot price
                # Use DISTINCT ON to ensure each option appears only once
                query = """
                    SELECT DISTINCT ON (ovh.symbol, ovh.expiry_date, ovh.strike_price, ovh.option_type, ovh.display_time)
                        ovh.symbol,
                        ovh.expiry_date,
                        ovh.strike_price,
                        ovh.option_type,
                        ovh.display_time as latest_time,
                        ovh.oi as oi,
                        ovh.volume as volume,
                        ovh.price as price,
                        COALESCE(ik.lot_size, 0) as lot_size,
                        sdc.close as spot_price,
                        ovh.vega,
                        ovh.theta,
                        ovh.gamma,
                        ovh.delta,
                        ovh.iv,
                        ovh.pop
                    FROM oi_volume_history ovh
                    LEFT JOIN instrument_keys ik ON ovh.symbol = ik.symbol
                    LEFT JOIN (
                        SELECT symbol, close
                        FROM stock_data_cache 
                        WHERE interval = '1d'
                        AND close > 0
                    ) sdc ON ovh.symbol = REPLACE(sdc.symbol, '.NS', '')
                    ORDER BY ovh.symbol, ovh.expiry_date, ovh.strike_price, ovh.option_type, ovh.display_time DESC, ik.lot_size DESC
                """
                cur.execute(query)

                # Fetch all results
                rows = cur.fetchall()

                # Process the data into the desired format
                stock_data = {}
                for row in rows:
                    symbol = row[0]
                    expiry = row[1].strftime('%Y-%m-%d') if row[1] else None
                    strike_price = float(row[2]) if row[2] else None
                    option_type = row[3]
                    # Use COALESCE in the SQL query to handle NULL values
                    lot_size = int(row[8]) if row[8] is not None else 0
                    spot_price = float(row[9]) if row[9] is not None else None

                    # Extract option greeks
                    vega = float(row[10]) if row[10] is not None else None
                    theta = float(row[11]) if row[11] is not None else None
                    gamma = float(row[12]) if row[12] is not None else None
                    delta = float(row[13]) if row[13] is not None else None
                    iv = float(row[14]) if row[14] is not None else None
                    pop = float(row[15]) if row[15] is not None else None

                    # Initialize stock entry if not exists
                    if symbol not in stock_data:
                        stock_data[symbol] = {
                            "expiries": set(),
                            "options": [],
                            "lot_size": lot_size,
                            "spot_price": spot_price
                        }
                    elif spot_price is not None and stock_data[symbol]["spot_price"] is None:
                        # Update spot price if it was None before but we have a value now
                        stock_data[symbol]["spot_price"] = spot_price

                    # Update lot_size if we get a non-zero value
                    if lot_size > 0 and stock_data[symbol]["lot_size"] == 0:
                        stock_data[symbol]["lot_size"] = lot_size

                    # Add expiry date to set
                    if expiry:
                        stock_data[symbol]["expiries"].add(expiry)

                    # Add option details with lot_size and greeks
                    option = {
                        "expiry": expiry,
                        "strike_price": strike_price,
                        "option_type": option_type,
                        "oi": int(row[5]) if row[5] else 0,
                        "volume": int(row[6]) if row[6] else 0,
                        "price": float(row[7]) if row[7] else 0,
                        "lot_size": lot_size,  # Use the lot_size directly from the query
                        "vega": vega,
                        "theta": theta,
                        "gamma": gamma,
                        "delta": delta,
                        "iv": iv,
                        "pop": pop
                    }

                    # Check if this option is already in the list to avoid duplicates
                    # This is a safeguard in case DISTINCT ON doesn't work as expected
                    duplicate = False
                    for existing_option in stock_data[symbol]["options"]:
                        if (existing_option["expiry"] == option["expiry"] and
                                existing_option["strike_price"] == option["strike_price"] and
                                existing_option["option_type"] == option["option_type"]):
                            duplicate = True
                            break

                    if not duplicate:
                        stock_data[symbol]["options"].append(option)

                # Convert sets to sorted lists for expiries
                for symbol in stock_data:
                    stock_data[symbol]["expiries"] = sorted(list(stock_data[symbol]["expiries"]))

                    # If we still don't have a lot_size for this symbol, try to get it from the fno_stocks
                    if stock_data[symbol]["lot_size"] == 0:
                        if symbol in self.fno_stocks:
                            stock_data[symbol]["lot_size"] = self.fno_stocks[symbol]

                            # Update all options with the symbol's lot_size
                            for option in stock_data[symbol]["options"]:
                                if option["lot_size"] == 0:
                                    option["lot_size"] = stock_data[symbol]["lot_size"]

                return stock_data

        except Exception as e:
            print(f"Error fetching stocks from database: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": f"Failed to fetch stocks data: {str(e)}"}

    def _process_fno_data(self, df: pd.DataFrame) -> Dict:
        """Process F&O data in optimized way"""
        stock_data = {}
        self.symbol_to_instrument = {}

        # Process NSE and BSE F&O data
        nse_fno = df[df['exchange'] == 'NSE_FO'][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()
        bse_fno = df[(df['exchange'] == 'BSE_FO') &
                     (df['tradingsymbol'].str.contains('SENSEX|BANKEX', regex=True))][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()

        fno_stocks_raw = pd.concat([nse_fno, bse_fno])

        for _, row in fno_stocks_raw.iterrows():
            parsed = self._parse_option_symbol(row['tradingsymbol'])
            if parsed:
                if parsed["strike_price"] in (5, 5.0):
                    continue
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

        # Convert sets to lists before returning
        for stock in stock_data:
            stock_data[stock]["expiries"] = sorted(list(stock_data[stock]["expiries"]))

        return stock_data

    def _process_equity_data(self, df: pd.DataFrame) -> Dict:
        """Process equity and index data"""
        equity_data = {}

        # Process NSE equities
        nse_eq = df[df['exchange'] == 'NSE_EQ'][['tradingsymbol', 'instrument_key']].dropna()
        for _, row in nse_eq.iterrows():
            stock_name = row['tradingsymbol']
            self.symbol_to_instrument[stock_name] = row['instrument_key']
            equity_data[stock_name] = {"instrument_key": row['instrument_key']}

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
            self.symbol_to_instrument[index] = key
            equity_data[index] = {"instrument_key": key}

        return equity_data

    def _get_access_token(self):
        """Get access token from database"""
        token = self.database.get_access_token(account_id=1)
        if not token:
            # Fallback to environment variable if not in database
            token = os.getenv("ACCESS_TOKEN")
        return token

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
                'FINNIFTY': 'NSE_INDEX|Nifty Fin Service',
                'MIDCPNIFTY': 'NSE_INDEX|NIFTY MID SELECT',
                'SENSEX': 'BSE_INDEX|SENSEX',
                'BANKEX': 'BSE_INDEX|BANKEX'
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

            if symbol in ["SENSEX", "BANKEX"]:
                exchange = "BSE_FO"
            else:
                exchange = "NSE_FO"

            # Find matching futures contract
            fut_match = self.instruments_data[
                (self.instruments_data['exchange'] == exchange) &
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
        """Return F&O stocks list with indices included."""
        indices = {
            "NIFTY": 75,  # Example lot size
            "BANKNIFTY": 30,
            "SENSEX": 20,
            "BANKEX": 30,
            "MIDCPNIFTY": 120,
            "FINNIFTY": 65
        }
        return {**self.fno_stocks, **indices}

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
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
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
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
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

    def fetch_prev_close_prices(self, instrument_keys):
        """
        Fetch previous close prices for a list of instrument keys in batches of 500.
        :param instrument_keys: List of instrument keys
        :return: List of dictionaries with instrument_key and prev_close
        """
        url = f"{self.UPSTOX_BASE_URL}/v2/market-quote/ltp"
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        batch_size = 500
        result = []

        # Create a mapping of instrument_key to trading_symbol for reverse lookup
        instrument_key_to_symbol = {key[0]: key[1] for key in instrument_keys}

        for i in range(0, len(instrument_keys), batch_size):
            batch = instrument_keys[i:i + batch_size]
            params = {'instrument_key': ','.join([key[0] for key in batch])}

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json().get("data", {})

                for instrument_key, tradingsymbol in batch:
                    # Determine the exchange type and construct the response key
                    if "BSE_FO" in instrument_key:
                        response_key = f"BSE_FO:{tradingsymbol}"
                    elif "NSE_FO" in instrument_key:
                        response_key = f"NSE_FO:{tradingsymbol}"
                    elif "NSE_EQ" in instrument_key:
                        response_key = f"NSE_EQ:{tradingsymbol}"
                    elif "NSE_INDEX" in instrument_key:
                        response_key = f"NSE_INDEX:{tradingsymbol}"
                    elif "BSE_INDEX" in instrument_key:
                        response_key = f"BSE_INDEX:{tradingsymbol}"
                    else:
                        continue  # Skip if the exchange type is not recognized

                    # Fetch the last price from the response
                    quote_data = data.get(response_key, {})
                    prev_close = quote_data.get("last_price", None)

                    if prev_close is not None:
                        result.append({
                            "instrument_key": instrument_key,
                            "prev_close": float(prev_close)
                        })

            except Exception as e:
                print(f"Error fetching previous close prices for batch {i // batch_size + 1}: {e}")

            # Pause for 2 seconds before processing the next batch
            time.sleep(2)

        return result


    def run_market_processing(self):
        """Background worker for market hours processing"""
        last_clear_date = None
        IST = pytz.timezone("Asia/Kolkata")

        while True:
            now = datetime.now(IST)
            # Clear old data at market open
            if (now.weekday() < 7 and Config.MARKET_OPEN >= now.time() <= Config.MARKET_CLOSE and
                    (last_clear_date is None or last_clear_date != now.date())):
                try:
                    self.database.clear_old_data()
                    last_clear_date = now.date()
                    print("Cleared all previous day's data")
                except Exception as e:
                    print(f"Failed to clear old data: {e}")

            # Process during market hours

            # Process during market hours - removed the clearing logic
            if now.weekday() in Config.TRADING_DAYS and Config.MARKET_OPEN <= now.time() <= Config.MARKET_CLOSE:
                try:
                    self._fetch_and_store_orders()
                    print("fetched and stored orders completed")
                    time.sleep(120)  # Run every 30 seconds
                except Exception as e:
                    print(f"Script error: {e}")
                    time.sleep(60)
            else:
                time.sleep(300)  # 5 min sleep outside market hours

    def detect_buildups(self, lookback_minutes=30):
        """Detect long/short buildups in F&O stocks"""
        buildups = {
            "futures_long_buildup": [],
            "futures_short_buildup": [],
            "options_long_buildup": [],
            "options_short_buildup": [],
            "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
        }
        # Get all F&O stocks
        # Get all F&O stocks
        fno_stocks = list(self.get_fno_stocks().keys())

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=min(8, len(fno_stocks))) as executor:
            futures = {
                executor.submit(self._analyze_stock_buildup, stock, lookback_minutes): stock
                for stock in fno_stocks
            }

            for future in as_completed(futures):
                stock = futures[future]
                try:
                    result = future.result()
                    if result:
                        if result['futures']:
                            if result['futures']['type'] == 'long':
                                buildups["futures_long_buildup"].append(result['futures'])
                            else:
                                buildups["futures_short_buildup"].append(result['futures'])
                        buildups["options_long_buildup"].extend(result['options'].get('long', []))
                        buildups["options_short_buildup"].extend(result['options'].get('short', []))
                except Exception as e:
                    print(f"Error processing {stock}: {str(e)}")
                    continue

        return buildups

    def _analyze_stock_buildup(self, stock, lookback_minutes):
        """Analyze both futures and options buildup for a single stock"""
        try:
            futures_result = self._analyze_futures_buildup(stock, lookback_minutes)
            options_result = self._analyze_options_buildup(stock, lookback_minutes)
            return {
                'futures': futures_result,
                'options': options_result
            }
        except Exception as e:
            print(f"Error analyzing buildups for {stock}: {str(e)}")
            return None


    # In OptionChainService class:

    def run_analytics_worker(self):
        """Parallel implementation of analytics worker"""
        while True:
            try:
                # Check if it's currently market hours (weekday and time within market hours)
                now = datetime.now(pytz.timezone('Asia/Kolkata'))
                is_weekday = now.weekday() in Config.TRADING_DAYS
                is_market_hours = Config.MARKET_OPEN <= now.time() <= Config.MARKET_CLOSE

                if is_weekday and is_market_hours:
                    start_time = time.time()

                    # Run all analytics in parallel
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        buildups_future = executor.submit(self.detect_buildups)
                        oi_extremes_future = executor.submit(self.detect_oi_extremes)

                        # Wait for both to complete
                        buildups = buildups_future.result()
                        oi_analytics = oi_extremes_future.result()

                    # Store results
                    self.database.save_buildup_results({
                        'futures_long_buildup': buildups['futures_long_buildup'],
                        'futures_short_buildup': buildups['futures_short_buildup'],
                        'options_long_buildup': buildups['options_long_buildup'],
                        'options_short_buildup': buildups['options_short_buildup'],
                        'oi_gainers': oi_analytics['oi_gainers'],
                        'oi_losers': oi_analytics['oi_losers']
                    })

                    elapsed = time.time() - start_time
                    print(f"Analytics completed in {elapsed:.2f} seconds")

                    time.sleep(300 - elapsed if elapsed < 300 else 60)  # Maintain ~5 minute interval
                else:
                    print(f"Outside market hours (Weekday: {is_weekday}, Market hours: {is_market_hours}). Sleeping...")
                    time.sleep(300)  # Sleep for 5 minutes outside market hours

            except Exception as e:
                print(f"Error in analytics worker: {e}")
                time.sleep(60)

    def detect_oi_extremes(self, lookback_minutes=30, top_n=10):
        """Parallel implementation of OI extremes detection with proper time handling"""
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            threshold_time = (now - timedelta(minutes=lookback_minutes)).strftime('%H:%M')

            with self.database._get_cursor() as cur:
                # Get all unique symbols with recent data
                cur.execute("""
                    SELECT DISTINCT symbol 
                    FROM oi_volume_history
                    WHERE display_time >= %s
                """, (threshold_time,))
                symbols = [row[0] for row in cur.fetchall()]

            oi_analytics = {'oi_gainers': [], 'oi_losers': []}

            # Process symbols in parallel
            max_workers = max(1, min(8, len(symbols)))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._analyze_symbol_oi, symbol, threshold_time, top_n): symbol
                    for symbol in symbols
                }

                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        result = future.result()
                        if result:
                            oi_analytics['oi_gainers'].extend(result['oi_gainers'])
                            oi_analytics['oi_losers'].extend(result['oi_losers'])
                    except Exception as e:
                        print(f"Error processing {symbol}: {str(e)}")
                        continue

            # Sort and limit the final results
            oi_analytics['oi_gainers'].sort(key=lambda x: abs(x['oi_change']), reverse=True)
            oi_analytics['oi_losers'].sort(key=lambda x: abs(x['oi_change']), reverse=True)

            return {
                'oi_gainers': oi_analytics['oi_gainers'][:top_n],
                'oi_losers': oi_analytics['oi_losers'][:top_n],
                'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            print(f"Error in detect_oi_extremes: {str(e)}")
            return {'oi_gainers': [], 'oi_losers': [], 'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}

    def _analyze_symbol_oi(self, symbol, threshold_time, top_n):
        """Analyze OI changes for a single symbol with proper time handling"""
        try:
            with self.database._get_cursor() as cur:
                cur.execute("""
                    WITH oi_data AS (
                        SELECT 
                            strike_price as strike,
                            option_type as type,
                            oi as absolute_oi,
                            display_time,
                            oi - LAG(oi) OVER (
                                PARTITION BY strike_price, option_type 
                                ORDER BY display_time
                            ) as oi_diff,
                            LAG(oi) OVER (
                                PARTITION BY strike_price, option_type 
                                ORDER BY display_time
                            ) as prev_oi
                        FROM oi_volume_history
                        WHERE 
                            symbol = %s AND
                            display_time >= %s
                    ),
                    calculated_changes AS (
                        SELECT 
                            *,
                            CASE 
                                WHEN prev_oi = 0 OR prev_oi IS NULL THEN 0
                                ELSE (oi_diff::float / prev_oi) * 100 
                            END as oi_change
                        FROM oi_data
                        WHERE oi_diff IS NOT NULL
                    )
                    SELECT 
                        strike, type, absolute_oi, 
                        ROUND(oi_change::numeric, 2) as oi_change,
                        display_time
                    FROM calculated_changes
                    ORDER BY ABS(oi_change) DESC
                    LIMIT %s
                """, (symbol, threshold_time, top_n * 2))

                results = cur.fetchall()
                symbol_results = {'oi_gainers': [], 'oi_losers': []}
                ist = pytz.timezone('Asia/Kolkata')
                today = datetime.now(ist).date()

                for row in results:
                    strike, opt_type, absolute_oi, oi_change, display_time = row

                    # Create proper timestamp by combining today's date with the time string
                    try:
                        timestamp = datetime.strptime(f"{today} {display_time}", "%Y-%m-%d %H:%M").astimezone(ist)
                        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M')
                    except ValueError:
                        timestamp_str = display_time  # Fallback to just the time if parsing fails

                    item = {
                        'symbol': symbol,
                        'strike': float(strike),
                        'type': opt_type,
                        'oi': int(absolute_oi),
                        'oi_change': float(oi_change),
                        'timestamp': timestamp_str
                    }
                    if oi_change >= 0:
                        symbol_results['oi_gainers'].append(item)
                    else:
                        symbol_results['oi_losers'].append(item)

                return symbol_results
        except Exception as e:
            print(f"Error analyzing OI for {symbol}: {str(e)}")
            return None

    def _analyze_futures_buildup(self, stock, lookback_minutes):
        """Analyze futures buildup with proper time handling"""
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            threshold_time = (now - timedelta(minutes=lookback_minutes)).strftime('%H:%M')

            with self.database._get_cursor() as cur:
                cur.execute("""
                    WITH recent_data AS (
                        SELECT 
                            price, oi, volume, display_time,
                            LAG(price) OVER (ORDER BY display_time) as prev_price,
                            LAG(oi) OVER (ORDER BY display_time) as prev_oi,
                            LAG(volume) OVER (ORDER BY display_time) as prev_volume
                        FROM oi_volume_history
                        WHERE 
                            symbol = %s AND 
                            option_type = 'FU' AND
                            display_time >= %s
                        ORDER BY display_time DESC
                        LIMIT 10
                    )
                    SELECT 
                        price, oi, volume, display_time,
                        CASE WHEN prev_price = 0 THEN 0 ELSE (price - prev_price) / prev_price * 100 END as price_pct,
                        CASE WHEN prev_oi = 0 THEN 0 ELSE (oi - prev_oi) / prev_oi * 100 END as oi_pct,
                        CASE WHEN prev_volume = 0 THEN 0 ELSE (volume - prev_volume) / prev_volume * 100 END as volume_pct
                    FROM recent_data
                    WHERE prev_price IS NOT NULL
                    ORDER BY display_time DESC
                    LIMIT 1
                """, (stock, threshold_time))

                result = cur.fetchone()
                if not result:
                    return None

                price, oi, volume, display_time, price_pct, oi_pct, volume_pct = result

                # Create proper timestamp
                try:
                    timestamp = datetime.strptime(f"{now.date()} {display_time}", "%Y-%m-%d %H:%M").astimezone(ist)
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M')
                except ValueError:
                    timestamp_str = display_time  # Fallback to just the time

                if price_pct > 0.3 and oi_pct > 5:
                    print("Long buildup detected for futures")
                    return {
                        'symbol': stock,
                        'price_change': round(float(price_pct), 2),
                        'oi_change': round(float(oi_pct), 2),
                        'volume_change': round(float(volume_pct), 2),
                        'type': 'long',
                        'timestamp': timestamp_str
                    }
                elif price_pct < -0.3 and oi_pct > 5:
                    print("Short buildup detected for futures")
                    return {
                        'symbol': stock,
                        'price_change': round(float(price_pct), 2),
                        'oi_change': round(float(oi_pct), 2),
                        'volume_change': round(float(volume_pct), 2),
                        'type': 'short',
                        'timestamp': timestamp_str
                    }
                return None
        except Exception as e:
            print(f"Error in _analyze_futures_buildup for {stock}: {str(e)}")
            return None

    def _analyze_options_buildup(self, stock, lookback_minutes):
        """Analyze options buildup with proper time handling"""
        long_buildup = []
        short_buildup = []

        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            threshold_time = (now - timedelta(minutes=lookback_minutes)).strftime('%H:%M')

            with self.database._get_cursor() as cur:
                cur.execute("""
                    WITH option_data AS (
                        SELECT 
                            strike_price, option_type, price, oi, volume, display_time,
                            LAG(price) OVER (PARTITION BY strike_price, option_type ORDER BY display_time) as prev_price,
                            LAG(oi) OVER (PARTITION BY strike_price, option_type ORDER BY display_time) as prev_oi,
                            LAG(volume) OVER (PARTITION BY strike_price, option_type ORDER BY display_time) as prev_volume
                        FROM oi_volume_history
                        WHERE 
                            symbol = %s AND 
                            option_type IN ('CE', 'PE') AND
                            display_time >= %s
                    )
                    SELECT 
                        strike_price, option_type, price, oi, volume, display_time,
                        CASE WHEN prev_price = 0 THEN 0 ELSE (price - prev_price) / prev_price * 100 END as price_pct,
                        CASE WHEN prev_oi = 0 THEN 0 ELSE (oi - prev_oi) / prev_oi * 100 END as oi_pct,
                        CASE WHEN prev_volume = 0 THEN 0 ELSE (volume - prev_volume) / prev_volume * 100 END as volume_pct
                    FROM option_data
                    WHERE prev_price IS NOT NULL
                    ORDER BY strike_price, option_type, display_time DESC
                """, (stock, threshold_time))

                for row in cur.fetchall():
                    strike, opt_type, price, oi, volume, display_time, price_pct, oi_pct, volume_pct = row

                    # Create proper timestamp
                    try:
                        timestamp = datetime.strptime(f"{now.date()} {display_time}", "%Y-%m-%d %H:%M").astimezone(ist)
                        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M')
                    except ValueError:
                        timestamp_str = display_time  # Fallback to just the time

                    if opt_type == 'CE' and price_pct > 1 and oi_pct > 10:
                        print("Long buildup detected for options")
                        long_buildup.append({
                            'symbol': stock,
                            'strike': float(strike),
                            'type': opt_type,
                            'price_change': round(float(price_pct), 2),
                            'oi_change': round(float(oi_pct), 2),
                            'volume_change': round(float(volume_pct), 2),
                            'timestamp': timestamp_str
                        })
                    elif opt_type == 'PE' and price_pct < -1 and oi_pct > 10:
                        print("Short buildup detected for options")
                        short_buildup.append({
                            'symbol': stock,
                            'strike': float(strike),
                            'type': opt_type,
                            'price_change': round(float(price_pct), 2),
                            'oi_change': round(float(oi_pct), 2),
                            'volume_change': round(float(volume_pct), 2),
                            'timestamp': timestamp_str
                        })
        except Exception as e:
            print(f"Error in _analyze_options_buildup for {stock}: {str(e)}")

        return {'long': long_buildup, 'short': short_buildup}

    def _fetch_and_store_orders(self):
        """Fetch and store option chain data"""
        total_start = time.time()
        processed_stocks = 0
        MIN_REQUEST_INTERVAL = 0.3  # Rate limiting
        last_api_call = 0

        # Add indices manually to fno_stocks
        indices = {
            "NIFTY": 75,  # Example lot size
            "BANKNIFTY": 30,
            "SENSEX": 20,
            "BANKEX": 30,
            "MIDCPNIFTY": 120,
            "FINNIFTY": 65
        }
        fno_stocks_with_indices = {**self.fno_stocks, **indices}
        #fno_stocks_with_indices = {**indices}

        for stock, lot_size in fno_stocks_with_indices.items():
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
        print(f"\nCompleted {processed_stocks}/{len(fno_stocks_with_indices)} stocks in {total_time:.1f}s")

    def _fetch_option_chain(self, stock_symbol, lot_size):
        """Fetch option chain data for a stock or index."""
        try:
            # Handle instrument_key for indices separately
            index_keys = {
                "NIFTY": "NSE_INDEX|Nifty 50",
                "SENSEX": "BSE_INDEX|SENSEX",
                "BANKNIFTY": "NSE_INDEX|Nifty Bank",
                "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
                "BANKEX": "BSE_INDEX|BANKEX",
                "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT"
            }
            instrument_key = index_keys.get(stock_symbol)
            if not instrument_key:
                # Fallback to normal instrument key lookup for non-indices
                instrument_key = self._get_instrument_key(stock_symbol)

            if not instrument_key:
                print(f"No instrument key for {stock_symbol}")
                return None

            # Fetch all expiries for NIFTY and SENSEX
            if stock_symbol == "NIFTY":
                expiries = Config.NIFTY_EXPIRIES
            elif stock_symbol == "SENSEX":
                expiries = Config.SENSEX_EXPIRIES  # Use predefined expiries from config
            elif stock_symbol == "BANKEX":
                expiries = Config.BANKEX_EXPIRIES  # Use predefined expiries from config
            else:
                expiries = [Config.EXPIRY_DATE]  # Default expiry for other stocks

            result = {
                'options_orders': [],
                'futures_orders': [],
                'oi_records': []
            }
            print("Fetching option chain for", stock_symbol)
            print("instrument_key:", instrument_key)

            for expiry_date in expiries:
                # Fetch option chain for each expiry
                url = 'https://api.upstox.com/v2/option/chain'
                access_token = self._get_access_token()
                headers = {'Authorization': f'Bearer {access_token}'}
                params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

                response = requests.get(url, params=params, headers=headers)
                if response.status_code != 200:
                    print(f"Option chain API failed for {stock_symbol} ({expiry_date}): {response.status_code}")
                    continue

                data = response.json().get('data', [])
                if not data:
                    print(f"No option chain data for {stock_symbol} ({expiry_date})")
                    continue

                # Process data
                self._process_option_chain_data(data, stock_symbol, expiry_date, lot_size, result)

            print(f"{stock_symbol}: {len(result['options_orders'])} orders, {len(result['oi_records'])} OI records")
            return result

        except Exception as e:
            print(f"Error in fetch_option_chain for {stock_symbol}: {str(e)}")
            return None

    def _process_option_chain_data(self, data, stock_symbol, expiry_date, lot_size, result):
        """Process option chain data and populate result."""
        try:
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
                print(f"No valid instrument keys for {stock_symbol} ({expiry_date})")
                return

            # Fetch market quotes
            market_quotes = self._fetch_market_quotes(instrument_keys)
            if not market_quotes:
                print(f"No market quotes for {stock_symbol} ({expiry_date})")
                return

            # Process futures
            futures_orders = self._process_futures_orders(market_quotes, stock_symbol, lot_size)
            if futures_orders:
                result['futures_orders'].extend(futures_orders)

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

                # Check if it's NIFTY or SENSEX (weekly expiry format: NIFTY{DDMMM}{STRIKE}{CE/PE})
                if "NIFTY" in symbol or "SENSEX" in symbol:
                    strike_match = re.search(r'(?:NIFTY|SENSEX)\d{5}(\d+)(CE|PE)$', symbol)
                else:
                    strike_match = re.search(r'(\d+)(CE|PE)$', symbol)

                if "BANKNIFTY" in symbol or "FINNIFTY" in symbol or "MIDCPNIFTY" in symbol:
                    strike_match = re.search(r'(\d+)(CE|PE)$', symbol)

                if not strike_match:
                    continue  # Skip if no match

                strike_price = int(strike_match.group(1))  # Extract strike price
                depth = quote_data.get('depth', {})
                top_bids = depth.get('buy', [])[:5]
                top_asks = depth.get('sell', [])[:5]
                ltp = quote_data.get('last_price', 0)
                net_change = quote_data.get('net_change', 0)
                prev_close = ltp - net_change if ltp and net_change else None
                pct_change = (net_change / prev_close * 100) if prev_close else 0

                threshold = lot_size * 87
                has_large_bid = any(bid['quantity'] >= threshold for bid in top_bids)
                has_large_ask = any(ask['quantity'] >= threshold for ask in top_asks)

                # Extract option greeks if available
                vega = theta = gamma = delta = iv = pop = 0.0
                oi = quote_data.get('oi', 0)
                volume = quote_data.get('volume', 0)

                # Find the matching option in the original data to get Greeks
                for option_data in data:
                    if option_data['strike_price'] == strike_price:
                        # Get the appropriate option type data
                        option_detail = option_data['call_options'] if option_type == 'CE' else option_data['put_options']

                        # Extract greeks properly
                        if 'option_greeks' in option_detail:
                            greeks = option_detail['option_greeks']
                            vega = greeks.get('vega', 0)
                            theta = greeks.get('theta', 0)
                            gamma = greeks.get('gamma', 0)
                            delta = greeks.get('delta', 0)
                            iv = greeks.get('iv', 0)
                            pop = greeks.get('pop', 0)
                        break

                if (has_large_bid or has_large_ask) and ltp > 2:
                    result['options_orders'].append({
                        'stock': stock_symbol,
                        'strike_price': strike_price,
                        'type': option_type,
                        'ltp': ltp,
                        'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                        'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                        'lot_size': lot_size,
                        'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'oi': oi,
                        'volume': volume,
                        'vega': vega,
                        'theta': theta,
                        'gamma': gamma,
                        'delta': delta,
                        'iv': iv,
                        'pop': pop
                    })

                # Prepare OI data with option greeks data
                result['oi_records'].append({
                    'symbol': stock_symbol,
                    'expiry': expiry_date,
                    'strike': strike_price,
                    'option_type': option_type,
                    'oi': Decimal(str(quote_data.get('oi', 0))),
                    'volume': Decimal(str(quote_data.get('volume', 0))),
                    'price': Decimal(str(ltp)),
                    'pct_change': Decimal(str(pct_change)),
                    'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M"),
                    'vega': Decimal(str(vega)) if vega is not None else Decimal('0'),
                    'theta': Decimal(str(theta)) if theta is not None else Decimal('0'),
                    'gamma': Decimal(str(gamma)) if gamma is not None else Decimal('0'),
                    'delta': Decimal(str(delta)) if delta is not None else Decimal('0'),
                    'iv': Decimal(str(iv)) if iv is not None else Decimal('0'),
                    'pop': Decimal(str(pop)) if pop is not None else Decimal('0')
                })

        except Exception as e:
            print(f"Error processing option chain data for {stock_symbol} ({expiry_date}): {str(e)}")

    def _process_futures_orders(self, market_quotes, stock_symbol, lot_size):
        """Detect large futures orders"""
        large_orders = []
        if stock_symbol in ["SENSEX", "BANKEX"]:
            prefix = f"BSE_FO:{stock_symbol}"
        else:
            prefix = f"NSE_FO:{stock_symbol}"
        suffix = "JUNFUT"

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
            access_token = self._get_access_token()
            headers = {'Authorization': f'Bearer {access_token}'}
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
        """Parse option symbol into components with enhanced support for weekly options"""
        # Try standard monthly pattern (RELIANCE24APR4000CE)
        match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+(?:\.\d+)?)(CE|PE)$", symbol)
        if match:
            return {
                "stock": match.group(1),
                "expiry": match.group(2),
                "strike_price": float(match.group(3)),  # Convert to float to handle decimals
                "option_type": match.group(4)
            }

        # For weekly options with specific 5-digit strike price handling
        weekly_indices = ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

        for index in weekly_indices:
            if symbol.startswith(index) and symbol.endswith(("CE", "PE")):
                option_type = symbol[-2:]  # CE or PE
                remaining = symbol[len(index):-2]  # Remove index name and option type

                if len(remaining) < 9:  # Not enough chars for a weekly symbol, skip
                    continue

                # For NIFTY indices:
                # First 2 chars: YY (year)
                # Next 1-2 chars: MM or M (month)
                # Next 2 chars: DD (day)
                # Remaining: Strike price (should be 5 digits for NIFTY, SENSEX)

                # Extract year
                year_digits = remaining[:2]

                # Check if month is 1 or 2 digits
                if remaining[2:4].isdigit() and 1 <= int(remaining[2:4]) <= 12:
                    # Two-digit month
                    month_digits = remaining[2:4]
                    remaining_after_month = remaining[4:]
                else:
                    # One-digit month
                    month_digits = remaining[2:3]
                    remaining_after_month = remaining[3:]

                # Extract day
                day_digits = remaining_after_month[:2]

                # Extract strike price - all remaining digits
                strike_part = remaining_after_month[2:]

                if not strike_part.isdigit():
                    # Not a valid strike price
                    continue

                # For NIFTY/SENSEX, strike should be 5 digits
                expected_strike_length = 5
                if len(strike_part) != expected_strike_length and index in ["NIFTY", "SENSEX", "BANKNIFTY"]:
                    print(f"Warning: Unexpected strike length for {index}: {strike_part} (expected {expected_strike_length} digits)")
                    # Continue to try other patterns
                    continue

                # Format the expiry as DDMMYY for consistency
                expiry = f"{day_digits}{month_digits.zfill(2)}{year_digits}"

                # Construct the result
                result = {
                    "stock": index,
                    "expiry": expiry,
                    "strike_price": float(strike_part),
                    "option_type": option_type,
                    "is_weekly": True
                }

                return result

        # If no weekly pattern matched, return None
        return None

    def _parse_option_upstox_symbol(self, symbol):
        """Parse option symbol into components"""
        try:
            # Check for futures contract first (e.g., INFY25MAYFUT)
            fut_match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})FUT$", symbol)
            if fut_match:
                return {
                    "stock": fut_match.group(1),
                    "expiry": fut_match.group(2),
                    "strike_price": None,
                    "option_type": "FU"
                }

            # Try to match patterns like RELIANCE24APR4000CE or NIFTY24APR24000CE
            opt_match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$", symbol)
            if opt_match:
                return {
                    "stock": opt_match.group(1),
                    "expiry": opt_match.group(2),
                    "strike_price": int(opt_match.group(3)),
                    "option_type": opt_match.group(4)
                }

            # Try to match weekly expiry pattern for SENSEX/NIFTY: SENSEX2552089700PE (YYMM format)
            # Format: Symbol + YYMM + DD + Strike + CE/PE
            # Examples: SENSEX2552089700PE (SENSEX 25/05/20 89700 PE)
            weekly_match = re.match(r"(SENSEX|NIFTY)(\d{2})(\d{2})(\d{2})(\d+)(CE|PE)$", symbol)
            if weekly_match:
                stock = weekly_match.group(1)
                yy = weekly_match.group(2)    # YY format
                mm = weekly_match.group(3)    # MM format
                dd = weekly_match.group(4)    # DD format

                # Format expiry as DDMMYY for consistency with our system
                expiry = f"{dd}{mm}{yy}"

                return {
                    "stock": stock,
                    "expiry": expiry,
                    "strike_price": int(weekly_match.group(5)),
                    "option_type": weekly_match.group(6),
                    "is_weekly": True
                }

            return None
        except Exception as e:
            print(f"Error parsing symbol {symbol}: {e}")
            return None

    def _is_current_month_weekly_option(self, trading_symbol, current_month_digit):
        """
        Check if a weekly option trading symbol contains the current month.
        For symbols like SENSEX2552089700PE, checks if positions 3-4 contain the month.
        """
        try:
            # Extract the YYMM portion for indices
            if any(trading_symbol.startswith(index) for index in ['NIFTY', 'SENSEX']):
                # Find where the digits start (after the index name)
                index_len = 0
                for prefix in ['NIFTY', 'SENSEX']:
                    if trading_symbol.startswith(prefix):
                        index_len = len(prefix)
                        break

                if index_len > 0 and len(trading_symbol) > index_len + 4:
                    # Get the month digits (positions 3-4 in the numeric part)
                    month_digits = trading_symbol[index_len+2:index_len+4]
                    return month_digits == current_month_digit
            return False
        except Exception as e:
            print(f"Error checking weekly option month for {trading_symbol}: {e}")
            return False

    def fetch_instrument_keys(self) -> Dict[str, Dict]:
        """
        Fetch instrument keys from Upstox complete.csv.gz file.
        Filter by instrument_type (OPTSTK, OPTIDX, OPTFUT, FUTIDX, FUTSTK)
        and exchange (BSE_FO or NSE_FO).
        Also includes NSE_EQ for FNO stocks.
        Handles both monthly expiry format (SENSEX25MAY89100CE) and
        weekly expiry format (SENSEX2552089700PE).

        If the current month's expiry has already passed, fetches next month's expiry instead.
        For weekly expiries, if none available in current month, fetches next month's weekly expiries.

        Returns:
            Dict[str, Dict]: Dictionary with instrument_key as key and details as value
        """
        print("Fetching instrument data from Upstox...")
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

        try:
            # Download the gzipped CSV file
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # Decompress the gzipped content
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
                # Read the decompressed content into a pandas DataFrame
                df = pd.read_csv(f)

            print(f"Downloaded instrument data: {len(df)} instruments")

            # Define the filters
            instrument_types = ['OPTSTK', 'OPTIDX', 'OPTFUT', 'FUTIDX', 'FUTSTK']
            exchanges = ['BSE_FO', 'NSE_FO']

            # Get current month, year information
            now = datetime.now()
            current_month = now.strftime('%b').upper()  # MAY, JUN, etc.
            current_month_digit = now.strftime('%m')    # 05, 06, etc.
            current_month_single_digit = str(now.month) # 5, 6, etc. (single digit for months < 10)
            current_year_digit = now.strftime('%y')     # 23, 24, etc.

            # Check if the monthly expiry has passed for this month
            # For Indian market, typically last Thursday of the month
            last_day = monthrange(now.year, now.month)[1]
            last_date = date(now.year, now.month, last_day)
            # Find the last Thursday of the month
            while last_date.weekday() != 3:  # 3 = Thursday
                last_date -= timedelta(days=1)

            # Now check if we've passed this date
            expiry_passed = now.date() > last_date
            print(f"Current date: {now.date()}, Last Thursday: {last_date}, Expiry passed: {expiry_passed}")

            # If expiry has passed, use next month for monthly options
            if expiry_passed:
                next_month = now.month + 1 if now.month < 12 else 1
                next_year = now.year if now.month < 12 else now.year + 1

                # Update month info for monthly options
                next_month_date = date(next_year, next_month, 1)
                current_month = next_month_date.strftime('%b').upper()  # JUN, JUL, etc.
                current_month_digit = next_month_date.strftime('%m')    # 06, 07, etc.
                current_month_single_digit = str(next_month)  # 6, 7, etc.
                if next_month == 1:  # If next month is January, update year digit
                    current_year_digit = next_month_date.strftime('%y')  # 24, 25, etc.

                print(f"Monthly expiry for current month has passed. Using next month: {current_month}")
            else:
                print(f"Using current month for options: {current_month} ({current_month_digit})")

            # Apply filters for instrument types and exchanges
            filtered_df = df[
                (df['instrument_type'].isin(instrument_types)) &
                (df['exchange'].isin(exchanges))
                ]

            print(f"Filtered by type/exchange: {len(filtered_df)} instruments")

            # Get FNO stock symbols for equity filtering
            fno_stock_names = set(self.fno_stocks.keys())

            # Get equity instruments for FNO stocks
            equity_df = df[
                (df['exchange'] == 'NSE_EQ') &
                (df['tradingsymbol'].isin(fno_stock_names))
                ]
            print(f"Found {len(equity_df)} NSE_EQ instruments for FNO stocks")

            # Separate futures and options
            # For futures, identify by tradingsymbol ending with FUT and containing current month
            futures_df = filtered_df[
                (filtered_df['tradingsymbol'].str.contains(current_month)) &
                (filtered_df['tradingsymbol'].str.endswith('FUT'))
                ]
            print(f"Found {len(futures_df)} futures contracts for {current_month}")

            # For monthly options, filter by trading symbols containing current month abbreviation
            monthly_options_df = filtered_df[
                (filtered_df['tradingsymbol'].str.contains(current_month)) &
                ~(filtered_df['tradingsymbol'].str.endswith('FUT'))  # Exclude futures
                ]

            print(f"Found {len(monthly_options_df)} monthly option contracts for {current_month}")

            # For weekly options (SENSEX and NIFTY), we need a different approach
            # First try to get weekly options for current month
            now = datetime.now()
            weekly_month_digit = now.strftime('%m')      # Current month for weeklies
            weekly_month_single_digit = str(now.month)   # Current month for weeklies
            weekly_year_digit = now.strftime('%y')       # Current year for weeklies

            # Create patterns for both single and double-digit months for weeklies
            current_year_month_double = f"{weekly_year_digit}{weekly_month_digit}"  # e.g., "2405" for May 2024
            current_year_month_single = f"{weekly_year_digit}{weekly_month_single_digit}"  # e.g., "245" for May 2024

            print("Current month weekly options year-month patterns:", current_year_month_double, current_year_month_single)

            # Find all weekly options for NIFTY and SENSEX for the current month - try both patterns
            weekly_options_dfs = []

            # First try with double-digit month pattern
            weekly_options_pattern_double = f"(NIFTY|SENSEX){current_year_month_double}\\d{{2}}\\d+(CE|PE)"
            print("Weekly options pattern (double-digit):", weekly_options_pattern_double)

            double_digit_matches = filtered_df[
                filtered_df['tradingsymbol'].str.match(weekly_options_pattern_double)
            ]

            if len(double_digit_matches) > 0:
                weekly_options_dfs.append(double_digit_matches)
                print(f"Found {len(double_digit_matches)} weekly option contracts with double-digit month pattern")

            # Then try with single-digit month pattern (only if month < 10)
            if int(weekly_month_digit) < 10:
                weekly_options_pattern_single = f"(NIFTY|SENSEX){current_year_month_single}\\d{{2}}\\d+(CE|PE)"
                print("Weekly options pattern (single-digit):", weekly_options_pattern_single)

                single_digit_matches = filtered_df[
                    filtered_df['tradingsymbol'].str.match(weekly_options_pattern_single)
                ]

                if len(single_digit_matches) > 0:
                    weekly_options_dfs.append(single_digit_matches)
                    print(f"Found {len(single_digit_matches)} weekly option contracts with single-digit month pattern")

            # Combine all matches
            if weekly_options_dfs:
                weekly_options_df = pd.concat(weekly_options_dfs)
                print(f"Found total {len(weekly_options_df)} weekly option contracts for NIFTY/SENSEX in current month")
            else:
                weekly_options_df = pd.DataFrame()
                print("No weekly options found for NIFTY/SENSEX in current month")

            # Add more debug information about weekly options
            weekly_symbols = []
            if len(weekly_options_df) > 0:
                weekly_symbols = weekly_options_df['tradingsymbol'].tolist()[:10]  # Show first 10
                print(f"Example weekly options: {weekly_symbols}")

                # Test parsing for some weekly options
                for sym in weekly_symbols[:3]:
                    parsed = self._parse_option_symbol(sym)
                    if parsed:
                        print(f"Successfully parsed weekly symbol: {sym} -> {parsed}")
                    else:
                        print(f"Failed to parse weekly symbol: {sym}")

            # If no weekly options found for current month, try next month
            if len(weekly_options_df) == 0:
                print("No weekly options found for current month, looking for next month's weekly options...")

                # Calculate next month details
                next_month = now.month + 1 if now.month < 12 else 1
                next_year = now.year if now.month < 12 else now.year + 1
                next_year_digit = now.strftime('%y') if next_month > 1 else str(int(now.strftime('%y')) + 1)

                # Format next month digits
                next_month_digit = f"{next_month:02d}"  # Double digit
                next_month_single_digit = str(next_month)  # Single digit

                # Create patterns for next month's weekly options
                next_year_month_double = f"{next_year_digit}{next_month_digit}"
                next_year_month_single = f"{next_year_digit}{next_month_single_digit}"

                print(f"Next month weekly options year-month patterns: {next_year_month_double}, {next_year_month_single}")

                # Try double-digit month pattern for next month
                next_weekly_options_pattern_double = f"(NIFTY|SENSEX){next_year_month_double}\\d{{2}}\\d+(CE|PE)"
                print("Next month weekly options pattern (double-digit):", next_weekly_options_pattern_double)

                next_double_digit_matches = filtered_df[
                    filtered_df['tradingsymbol'].str.match(next_weekly_options_pattern_double)
                ]

                if len(next_double_digit_matches) > 0:
                    weekly_options_dfs.append(next_double_digit_matches)
                    print(f"Found {len(next_double_digit_matches)} next month weekly option contracts with double-digit pattern")

                # Try single-digit month pattern for next month (only if month < 10)
                if next_month < 10:
                    next_weekly_options_pattern_single = f"(NIFTY|SENSEX){next_year_month_single}\\d{{2}}\\d+(CE|PE)"
                    print("Next month weekly options pattern (single-digit):", next_weekly_options_pattern_single)

                    next_single_digit_matches = filtered_df[
                        filtered_df['tradingsymbol'].str.match(next_weekly_options_pattern_single)
                    ]

                    if len(next_single_digit_matches) > 0:
                        weekly_options_dfs.append(next_single_digit_matches)
                        print(f"Found {len(next_single_digit_matches)} next month weekly option contracts with single-digit pattern")

                # Combine all matches including next month's weekly options
                if weekly_options_dfs:
                    weekly_options_df = pd.concat(weekly_options_dfs)
                    print(f"Found total {len(weekly_options_df)} weekly option contracts for NIFTY/SENSEX across current and next month")

            # Check if we need a fallback approach
            if len(weekly_options_df) == 0:
                print("No weekly options found with specific patterns, trying alternative approach...")
                # Try a more general pattern and then filter manually
                for index in ["NIFTY", "SENSEX"]:
                    # Pattern like "NIFTY24" or "SENSEX24" (current year)
                    base_pattern = f"{index}{current_year_digit}"
                    matches = filtered_df[
                        filtered_df['tradingsymbol'].str.startswith(base_pattern) &
                        (filtered_df['tradingsymbol'].str.endswith('CE') | filtered_df['tradingsymbol'].str.endswith('PE')) &
                        ~filtered_df['tradingsymbol'].str.contains(current_month)  # Exclude monthly options
                        ]

                    # Manually check for current month in both formats
                    valid_matches = []
                    for _, row in matches.iterrows():
                        symbol = row['tradingsymbol']
                        if len(symbol) >= len(base_pattern) + 1:  # Ensure there are at least more digits
                            # Extract potential month part
                            # For single digit month (e.g., 5 for May)
                            if len(symbol) >= len(base_pattern) + 1:
                                single_month_part = symbol[len(base_pattern):len(base_pattern)+1]
                                if single_month_part == current_month_single_digit or single_month_part == next_month_single_digit:
                                    valid_matches.append(row)
                                    continue

                            # For double digit month (e.g., 05 for May or 11 for November)
                            if len(symbol) >= len(base_pattern) + 2:
                                double_month_part = symbol[len(base_pattern):len(base_pattern)+2]
                                if double_month_part == current_month_digit or double_month_part == next_month_digit:
                                    valid_matches.append(row)
                                    continue

                    if valid_matches:
                        print(f"Found {len(valid_matches)} weekly options for {index} using manual inspection")
                        weekly_df = pd.DataFrame(valid_matches)
                        if weekly_options_df.empty:
                            weekly_options_df = weekly_df
                        else:
                            weekly_options_df = pd.concat([weekly_options_df, weekly_df])

            # Print sample weekly options for verification
            if len(weekly_options_df) > 0:
                sample_weekly = weekly_options_df['tradingsymbol'].sample(min(5, len(weekly_options_df))).tolist()
                print(f"Sample weekly options: {sample_weekly}")

                # Verify parsing of a sample
                for symbol in sample_weekly[:2]:
                    parsed = self._parse_option_upstox_symbol(symbol)
                    if parsed:
                        print(f"Parsed {symbol} as: {parsed}")
                    else:
                        print(f"Failed to parse {symbol}")
            else:
                print("No weekly options found for NIFTY/SENSEX with any pattern")

            # Combine all dataframes - futures, monthly options, weekly options, and FNO equities
            filtered_df = pd.concat([futures_df, monthly_options_df, weekly_options_df, equity_df])

            print(f"Combined {len(filtered_df)} instruments (including FNO equities)")

            # Get current stock prices to filter strikes
            stock_prices = self._get_current_market_prices()
            print(f"Fetched {len(stock_prices)} current stock prices for strike filtering")

            # Group instruments by stock symbol to process each stock separately
            parsed_instruments = []
            for _, row in filtered_df.iterrows():
                # Handle equity instruments
                if row['exchange'] == 'NSE_EQ':
                    stock_name = row['tradingsymbol']
                    if stock_name in fno_stock_names:
                        parsed_instruments.append({
                            'stock': stock_name,
                            'row': row,
                            'is_option': False,
                            'is_future': False,
                            'is_equity': True
                        })
                    continue

                # Check if it's a futures contract
                if row['tradingsymbol'].endswith('FUT'):
                    # Extract the stock symbol from futures tradingsymbol (like INFY25MAYFUT)
                    stock_match = re.match(r"([A-Z]+)\d{2}[A-Z]{3}FUT", row['tradingsymbol'])
                    if stock_match:
                        stock = stock_match.group(1)
                        parsed_instruments.append({
                            'stock': stock,
                            'row': row,
                            'is_option': False,
                            'is_future': True
                        })
                    continue

                # Parse the tradingsymbol to get instrument details for options
                parsed = self._parse_option_symbol(row['tradingsymbol'])
                if parsed:
                    parsed['row'] = row
                    parsed_instruments.append(parsed)
                else:
                    # Keep non-option instruments
                    parsed_instruments.append({
                        'stock': row['name'] if 'name' in row else row['tradingsymbol'],
                        'row': row,
                        'is_option': False
                    })

            # Group by stock
            stock_groups = {}
            for instr in parsed_instruments:
                stock = instr.get('stock')
                if stock not in stock_groups:
                    stock_groups[stock] = []
                stock_groups[stock].append(instr)

            # Process each stock to get the exact strikes we want
            final_instruments = []
            skipped_stocks = []
            for stock, instruments in stock_groups.items():
                # For futures and equities, add them directly without filtering
                futures_and_equity = [
                    instr for instr in instruments
                    if instr.get('is_future', False) or instr.get('is_equity', False)
                ]
                for item in futures_and_equity:
                    final_instruments.append(item['row'])

                # Special handling for NIFTY and SENSEX
                if stock in ["NIFTY", "SENSEX"]:
                    # Get current price for strike filtering
                    current_price = stock_prices.get(stock)
                    if not current_price:
                        skipped_stocks.append(stock)
                        print(f"Skipping {stock} options: No current price available")
                        continue  # Skip option filtering for this stock if no price is available

                    # Handle weekly options with strike filtering (similar to monthly options)
                    weekly_options = [
                        instr for instr in instruments
                        if instr.get('is_option', True) and instr.get('is_weekly', False)
                    ]

                    if weekly_options:
                        print(f"Found {len(weekly_options)} weekly options for {stock}, filtering strikes...")

                        # Filter to options with strike price
                        valid_weekly_options = [instr for instr in weekly_options if 'strike_price' in instr]

                        if valid_weekly_options:
                            # Get all unique strikes for this stock's weekly options
                            weekly_strikes = sorted(set(option['strike_price'] for option in valid_weekly_options))

                            # Find the closest strike to current price
                            closest_strike_idx = min(range(len(weekly_strikes)),
                                                     key=lambda i: abs(weekly_strikes[i] - current_price))

                            # Get 5 strikes below, the closest, and 5 strikes above
                            start_idx = max(0, closest_strike_idx - 7)
                            end_idx = min(len(weekly_strikes) - 1, closest_strike_idx + 7)

                            # Select exactly 11 strikes (5 below, 1 at/near, 5 above) if possible
                            selected_weekly_strikes = weekly_strikes[start_idx:end_idx + 1]

                            # Ensure we have at most 11 strikes
                            if len(selected_weekly_strikes) > 15:
                                mid_idx = len(selected_weekly_strikes) // 2
                                selected_weekly_strikes = selected_weekly_strikes[mid_idx - 7:mid_idx + 8]

                            print(f"{stock} weekly options - current price: {current_price}, selected strikes: {selected_weekly_strikes}")

                            # Filter instruments to only those with the selected strikes
                            for instr in weekly_options:
                                if 'strike_price' in instr and instr['strike_price'] in selected_weekly_strikes:
                                    final_instruments.append(instr['row'])
                        else:
                            # No weekly options with strike price, add all weekly options
                            for instr in weekly_options:
                                final_instruments.append(instr['row'])

                # Process monthly options for all stocks
                monthly_options = [
                    instr for instr in instruments
                    if instr.get('is_option', True) and not instr.get('is_future', False) and not instr.get('is_weekly', False)
                ]

                if not monthly_options:
                    continue

                current_price = stock_prices.get(stock)
                if not current_price:
                    skipped_stocks.append(stock)
                    print(f"Skipping {stock} monthly options: No current price available")
                    continue  # Skip option filtering for this stock if no price is available

                # Filter options based on strike prices
                if monthly_options:
                    # Filter to options with strike price
                    valid_options = [instr for instr in monthly_options if 'strike_price' in instr]

                    if valid_options:
                        # Get all unique strikes for this stock
                        all_strikes = sorted(set(option['strike_price'] for option in valid_options))

                        # Find the closest strike to current price
                        closest_strike_idx = min(range(len(all_strikes)),
                                                 key=lambda i: abs(all_strikes[i] - current_price))

                        # Get 5 strikes below, the closest, and 5 strikes above
                        start_idx = max(0, closest_strike_idx - 7)
                        end_idx = min(len(all_strikes) - 1, closest_strike_idx + 7)

                        # Select exactly 11 strikes (5 below, 1 at/near, 5 above) if possible
                        selected_strikes = all_strikes[start_idx:end_idx + 1]

                        # Ensure we have at most 11 strikes
                        if len(selected_strikes) > 15:
                            mid_idx = len(selected_strikes) // 2
                            selected_strikes = selected_strikes[mid_idx - 7:mid_idx + 8]

                        #print(f"{stock} monthly options - current price: {current_price}, selected strikes: {selected_strikes}")

                        # Filter instruments to only those with the selected strikes
                        for instr in monthly_options:
                            if 'strike_price' not in instr or instr['strike_price'] in selected_strikes:
                                final_instruments.append(instr['row'])
                    else:
                        # No options with strike price, add all options
                        for instr in monthly_options:
                            final_instruments.append(instr['row'])

            # Log skipped stocks
            if skipped_stocks:
                print(f"Skipped {len(skipped_stocks)} stocks without price data: {', '.join(skipped_stocks[:20])}")
                if len(skipped_stocks) > 20:
                    print(f"...and {len(skipped_stocks) - 20} more")

            # Convert final instruments list back to DataFrame
            if final_instruments:
                final_df = pd.DataFrame(final_instruments)
                print(f"Final filtered instruments count: {len(final_df)}")
                # List some samples to verify
                if 'tradingsymbol' in final_df.columns:
                    futures_samples = final_df[final_df['tradingsymbol'].str.endswith('FUT')]['tradingsymbol'].sample(
                        min(3, len(final_df[final_df['tradingsymbol'].str.endswith('FUT')]))
                    ).tolist() if not final_df[final_df['tradingsymbol'].str.endswith('FUT')].empty else []

                    # Get samples of weekly options for NIFTY and SENSEX - try both patterns
                    weekly_pattern = r'(NIFTY|SENSEX)(\d{3}|\d{4})\d{2}\d+(CE|PE)'
                    weekly_options = final_df[final_df['tradingsymbol'].str.match(weekly_pattern)]
                    weekly_samples = weekly_options['tradingsymbol'].sample(
                        min(3, len(weekly_options))
                    ).tolist() if not weekly_options.empty else []

                    # Get samples of monthly options
                    monthly_pattern = r'(NIFTY|SENSEX)\d{2}[A-Z]{3}\d+(CE|PE)'
                    monthly_options = final_df[
                        ~final_df['tradingsymbol'].str.endswith('FUT') &
                        final_df['tradingsymbol'].str.match(monthly_pattern)
                        ]
                    monthly_samples = monthly_options['tradingsymbol'].sample(
                        min(3, len(monthly_options))
                    ).tolist() if not monthly_options.empty else []

                    # Get samples of equity FNO stocks
                    equity_samples = final_df[final_df['exchange'] == 'NSE_EQ']['tradingsymbol'].sample(
                        min(3, len(final_df[final_df['exchange'] == 'NSE_EQ']))
                    ).tolist() if not final_df[final_df['exchange'] == 'NSE_EQ'].empty else []

            else:
                print("No instruments matched the filtering criteria")
                final_df = pd.DataFrame()

            # Create a dictionary with instrument_key as the key and relevant details as values
            instrument_dict = {}
            for _, row in final_df.iterrows():
                instrument_key = row['instrument_key']
                instrument_dict[instrument_key] = {
                    'trading_symbol': row['tradingsymbol'],
                    'name': row['name'] if 'name' in row else row['tradingsymbol'],
                    'instrument_type': row['instrument_type'],
                    'exchange': row['exchange'],
                    'expiry': row['expiry'] if 'expiry' in row else None,
                    'lot_size': int(row['lot_size']) if 'lot_size' in row else 0
                }

            return instrument_dict

        except Exception as e:
            print(f"Error fetching instrument data: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}

    def _get_current_market_prices(self) -> Dict[str, float]:
        """
        Get current market prices for stocks and indices

        Returns:
            Dict[str, float]: Dictionary with symbol as key and price as value
        """
        prices = {}
        try:
            # First try to get prices from the database
            with self.database._get_cursor() as cur:
                cur.execute("""
                    SELECT symbol, close 
                    FROM stock_data_cache 
                    WHERE interval = '1d'
                    AND close > 0
                """)
                for row in cur.fetchall():
                    symbol = row[0].replace('.NS', '')  # Remove .NS suffix
                    prices[symbol] = float(row[1]) if row[1] else 0

            # Add special handling for indices
            """
            index_prices = {
                "NIFTY": 24600,     # Default value if not found
                "BANKNIFTY": 54600,
                "FINNIFTY": 26100,
                "MIDCPNIFTY": 12627,
                "SENSEX": 81193,
                "BANKEX": 62250
            }
            
            
            # Override with defaults for indices if not in database
            for index, default_price in index_prices.items():
                if index not in prices:
                    prices[index] = default_price
            """
            # Remove any stocks with zero or negative prices
            prices = {k: v for k, v in prices.items() if v > 0}

            print(f"Loaded {len(prices)} stock/index prices for strike filtering")

        except Exception as e:
            print(f"Error fetching current market prices: {str(e)}")

        return prices

    def _get_all_weekly_expiries(self):
        """Get all weekly expiries for the current month."""
        today = datetime.now()
        year, month = today.year, today.month
        num_days = monthrange(year, month)[1]  # Get the number of days in the current month

        expiries = []
        for day in range(1, num_days + 1):
            date = datetime(year, month, day)
            if date.weekday() == 3:  # Thursday
                expiries.append(date.strftime('%Y-%m-%d'))

        return expiries

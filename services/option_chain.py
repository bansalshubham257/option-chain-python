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
            with ThreadPoolExecutor(max_workers=min(8, len(symbols))) as executor:
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
            
            result = {
                'options_orders': [],
                'futures_orders': [],
                'oi_records': []
            }
            
            prefix = f"NSE_FO:{stock_symbol}"
            suffix = "APRFUT"
    
            pattern = re.compile(rf"{prefix}\d+{suffix}")
            fut_instrument_key = next((key for key in market_quotes if pattern.match(key)), None)

            if fut_instrument_key and fut_instrument_key in market_quotes:
                fut_data = market_quotes[fut_instrument_key]
                result['oi_records'].append({
                    'symbol': stock_symbol,
                    'expiry': expiry_date,
                    'strike': 0,  # Using 0 for futures
                    'option_type': 'FU',
                    'oi': Decimal(str(fut_data.get('oi', 0))),
                    'volume': Decimal(str(fut_data.get('volume', 0))),
                    'price': Decimal(str(fut_data.get('last_price', 0))),
                    'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M")
                })
            # Process data

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

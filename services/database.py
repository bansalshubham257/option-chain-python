import json
import os
import time
from functools import lru_cache

import pandas as pd
import psycopg2
from services.custom_ta import ta
from psycopg2._psycopg import OperationalError
from psycopg2.extras import execute_batch
from decimal import Decimal
from contextlib import contextmanager
import numpy as np  # Add this import

from datetime import datetime, date
import pytz
from curl_cffi import requests
import yfinance as yf

# Memory cache for pivot point calculation tracking

class DatabaseService:

    pivot_calculation_cache = {}

    def __init__(self, max_retries=3, retry_delay=2):
        self.conn_params = {
            'dbname': os.getenv('DB_NAME', 'your_db_name'),
            'user': os.getenv('DB_USER', 'your_db_user'),
            'password': os.getenv('DB_PASSWORD', 'your_db_password'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session(impersonate="chrome110")
        print("databse init done")

    def test_connection(self):
        """Test database connection"""
        try:
            with self._get_cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
        except Exception as e:
            print(f"Database connection failed: {str(e)}")
            return False

    def _init_db_with_retry(self):
        """Initialize connection pool with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._init_db()
                return
            except OperationalError as e:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Failed to initialize database after {self.max_retries} attempts: {str(e)}")
                print(f"Database connection failed (attempt {attempt + 1}/{self.max_retries}), retrying...")
                time.sleep(self.retry_delay)

    def _init_db(self):
        """Initialize connection pool"""
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=20,
            dsn=os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/mydb')
        )

    def _verify_connection(self):
        """Verify that the connection pool works"""
        try:
            with self._get_cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("Database connection test failed")
        except Exception as e:
            self.connection_pool.closeall()
            raise RuntimeError(f"Database connection verification failed: {str(e)}")

    @contextmanager
    def _get_cursor(self):
        """Get a database cursor with automatic cleanup"""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(**self.conn_params)
                with conn.cursor() as cur:
                    yield cur
                conn.commit()
                break
            except OperationalError as e:
                if conn:
                    conn.close()
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Database connection failed after {self.max_retries} attempts: {str(e)}")
                print(f"Database connection failed (attempt {attempt + 1}/{self.max_retries}), retrying...")
                time.sleep(self.retry_delay)
            except Exception as e:
                if conn:
                    conn.rollback()
                raise
            finally:
                if conn:
                    conn.close()

    def get_instrument_details_by_key(self, instrument_key):
        """Get instrument details by instrument key."""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size,
                       instrument_type, expiry_date, strike_price, option_type
                FROM instrument_keys
                WHERE instrument_key = %s
            """, (instrument_key,))
            result = cur.fetchone()
            if result:
                return {
                    'symbol': result[0],
                    'instrument_key': result[1],
                    'exchange': result[2],
                    'tradingsymbol': result[3],
                    'lot_size': result[4],
                    'instrument_type': result[5],
                    'expiry_date': result[6],
                    'strike_price': result[7],
                    'option_type': result[8]
                }
            return None

    def get_instruments_by_keys(self, instrument_keys):
        """Get multiple instrument details by instrument keys."""
        if not instrument_keys:
            return []
            
        with self._get_cursor() as cur:
            placeholders = ','.join(['%s'] * len(instrument_keys))
            query = f"""
                SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size,
                       instrument_type, expiry_date, strike_price, option_type
                FROM instrument_keys
                WHERE instrument_key IN ({placeholders})
            """
            cur.execute(query, instrument_keys)
            results = cur.fetchall()
            
            return [
                {
                    'symbol': row[0],
                    'instrument_key': row[1],
                    'exchange': row[2],
                    'tradingsymbol': row[3],
                    'lot_size': row[4],
                    'instrument_type': row[5],
                    'expiry_date': row[6].isoformat() if row[6] else None,
                    'strike_price': float(row[7]) if row[7] else None,
                    'option_type': row[8]
                }
                for row in results
            ]

    def get_options_orders(self):
        """Get all options orders"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp
                    FROM options_orders
                """)
                results = cur.fetchall()
                return [{
                    'stock': r[0], 'strike_price': r[1], 'type': r[2],
                    'ltp': r[3], 'bid_qty': r[4], 'ask_qty': r[5],
                    'lot_size': r[6], 'timestamp': r[7].isoformat()
                } for r in results]
        except Exception as e:
            print(f"Error fetching options orders: {str(e)}")

    def get_futures_orders(self):
        """Get all futures orders"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT symbol, ltp, bid_qty, ask_qty, lot_size, timestamp
                    FROM futures_orders
                """)
                results = cur.fetchall()
                return [{
                    'stock': r[0], 'ltp': r[1], 'bid_qty': r[2],
                    'ask_qty': r[3], 'lot_size': r[4], 'timestamp': r[5].isoformat()
                } for r in results]
        except Exception as e:
            print(f"Error fetching futures orders: {str(e)}")

    def get_fno_data(self, stock, expiry, strike, option_type):
        """Get F&O data for specific option"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT display_time, oi, volume, price, strike_price, option_type
                FROM oi_volume_history
                WHERE symbol = %s AND expiry_date = %s
                  AND strike_price = %s AND option_type = %s
                ORDER BY display_time
            """, (stock, expiry, strike, option_type))

            data = [{
                'time': r[0], 'oi': float(r[1]) if r[1] else 0,
                'volume': float(r[2]) if r[2] else 0, 'price': float(r[3]) if r[3] else 0,
                'strike': str(r[4]), 'optionType': r[5]
            } for r in cur.fetchall()]

            return {"data": data}

    def get_option_data(self, stock):
        """Get all option data for a stock"""
        with self._get_cursor() as cur:
            # Get expiries
            cur.execute("""
                SELECT DISTINCT expiry_date 
                FROM oi_volume_history
                WHERE symbol = %s
                ORDER BY expiry_date
            """, (stock,))
            expiries = [r[0].strftime('%Y-%m-%d') for r in cur.fetchall()]

            # Get strikes
            cur.execute("""
                SELECT DISTINCT strike_price
                FROM oi_volume_history
                WHERE symbol = %s
                ORDER BY strike_price
            """, (stock,))
            strikes = [float(r[0]) for r in cur.fetchall()]

            # Get option chain data
            cur.execute("""
                SELECT 
                    expiry_date, strike_price, option_type,
                    json_agg(
                        json_build_object(
                            'time', display_time,
                            'oi', oi,
                            'volume', volume,
                            'price', price
                        )
                    ) as history
                FROM oi_volume_history
                WHERE symbol = %s
                GROUP BY expiry_date, strike_price, option_type
            """, (stock,))

            option_data = {}
            for expiry_date, strike_price, option_type, history in cur.fetchall():
                key = f"oi_volume_data:{stock}:{expiry_date}:{strike_price}:{option_type}"
                option_data[key] = history

            return {
                "expiries": expiries,
                "strikes": strikes,
                "data": option_data
            }

    def save_options_data(self, symbol, orders):
        """Bulk insert options orders"""
        if not orders:
            return

        with self._get_cursor() as cur:
            data = [(order['stock'], order['strike_price'], order['type'],
                     order['ltp'], order['bid_qty'], order['ask_qty'],
                     order['lot_size'], order['timestamp']) for order in orders]

            execute_batch(cur, """
                INSERT INTO options_orders 
                (symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, strike_price, option_type) DO NOTHING
            """, data, page_size=100)

    def save_futures_data(self, symbol, orders):
        """Bulk insert futures orders"""
        if not orders:
            return

        with self._get_cursor() as cur:
            data = [(order['stock'], order['ltp'], order['bid_qty'],
                     order['ask_qty'], order['lot_size'], order['timestamp']) for order in orders]

            execute_batch(cur, """
                INSERT INTO futures_orders 
                (symbol, ltp, bid_qty, ask_qty, lot_size, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """, data, page_size=100)

    def save_oi_volume_batch_feed(self, records):
        """Save OI volume data with option greeks as individual columns"""
        if not records:
            return

        # Filter and process records
        filtered_records = [
            r for r in records
            if not (isinstance(r['strike'], (int, float)) and r['strike'] < 10)
        ]

        if not filtered_records:
            return

        # Convert NumPy types to native Python types and handle None values
        processed_records = []
        for r in filtered_records:
            try:
                # Safely convert values or use defaults if None
                processed_record = (
                    str(r['symbol']),
                    str(r['expiry']) if r['expiry'] is not None else None,
                    float(r['strike']) if r['strike'] is not None else 0.0,
                    str(r['option_type']) if r['option_type'] is not None else None,
                    float(r['oi']) if r['oi'] is not None else 0.0,
                    float(r['volume']) if r['volume'] is not None else 0.0,
                    float(r['price']) if r['price'] is not None else 0.0,
                    str(r['timestamp']) if r['timestamp'] is not None else None,
                    float(r['pct_change']) if r['pct_change'] is not None else 0.0,
                    float(r['vega']) if r['vega'] is not None else 0.0,
                    float(r['theta']) if r['theta'] is not None else 0.0,
                    float(r['gamma']) if r['gamma'] is not None else 0.0,
                    float(r['delta']) if r['delta'] is not None else 0.0,
                    float(r['iv']) if r['iv'] is not None else 0.0,
                    float(r['pop']) if r['pop'] is not None else 0.0
                )
                processed_records.append(processed_record)
            except (TypeError, ValueError) as e:
                print(f"Error processing record: {r}")
                print(f"Error details: {e}")
                # Skip this record to avoid breaking the batch
                continue

        # If we have no valid records after processing, exit early
        if not processed_records:
            print("No valid records to insert after processing")
            return

        try:
            with self._get_cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO oi_volume_history (
                        symbol, expiry_date, strike_price, option_type,
                        oi, volume, price, display_time, pct_change,
                        vega, theta, gamma, delta, iv, pop
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, expiry_date, strike_price, option_type, display_time) 
                    DO NOTHING
                """, processed_records, page_size=100)
                print(f"Successfully inserted {len(processed_records)} records into oi_volume_history")
        except Exception as e:
            print(f"Database error in save_oi_volume_batch: {e}")
            import traceback
            traceback.print_exc()


    def save_oi_volume_batch(self, records):
        """Save OI volume data with option greeks as individual columns"""
        if not records:
            return

        # Filter and process records
        filtered_records = [
            r for r in records
            if not (isinstance(r['strike'], (int, float)) and r['strike'] < 10)
        ]

        if not filtered_records:
            return
        
        # Convert NumPy types to native Python types
        processed_records = [
            (
                str(r['symbol']),
                str(r['expiry']),
                float(r['strike']),  # Convert np.float64 to float
                str(r['option_type']),
                float(r['oi']),  # Convert np.float64 to float
                float(r['volume']),  # Convert np.float64 to float
                float(r['price']),  # Convert np.float64 to float
                str(r['timestamp']),
                float(r['pct_change']),
                float(r['vega']),
                float(r['theta']),
                float(r['gamma']),
                float(r['delta']),
                float(r['iv']),
                float(r['pop'])
            )
            for r in filtered_records
        ]


        
        with self._get_cursor() as cur:
            execute_batch(cur, """
                INSERT INTO oi_volume_history (
                    symbol, expiry_date, strike_price, option_type,
                    oi, volume, price, display_time, pct_change,
                    vega, theta, gamma, delta, iv, pop
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, expiry_date, strike_price, option_type, display_time) 
                DO NOTHING
            """, processed_records, page_size=100)

    def clear_old_data(self):
        """Delete previous day's data"""
        print("inside clear_old_data")
        with self._get_cursor() as cur:
            print("cur", cur)
            cur.execute("DELETE FROM options_orders")
            cur.execute("DELETE FROM futures_orders")
            cur.execute("DELETE FROM oi_volume_history")
            cur.execute("DELETE FROM buildup_results")
            cur.execute("DELETE FROM fno_analytics")
            cur.execute("DELETE FROM instrument_keys")


    def save_market_data(self, data_type, data):
        """Save market data to cache by converting dict to JSON"""
        with self._get_cursor() as cur:
            cur.execute("""
                INSERT INTO market_data_cache (data_type, data)
                VALUES (%s, %s)
                ON CONFLICT (data_type) 
                DO UPDATE SET data = EXCLUDED.data, last_updated = NOW()
            """, (data_type, json.dumps(data)))  # Convert dict to JSON string


    def get_fii_market_data(self, data_type):
        """Get cached market data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT data FROM market_data_cache
                WHERE data_type = %s
            """, (data_type,))
            result = cur.fetchone()
            return result[0] if result else None

    def get_market_data(self, data_type):
        """
        Get market data including indices, top gainers, and top losers from stock_data_cache

        Args:
            data_type: Type of market data to fetch ('indices', 'gainers', 'losers', or 'all')

        Returns:
            dict: Structured market data
        """
        print("inside get_market_data........")
        try:
            market_data = {}

            with self._get_cursor() as cur:
                # Fetch indices data (NIFTY, BANKNIFTY, etc.)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE symbol IN ('NIFTY.NS', 'BANKNIFTY.NS', 'BANKEX.NS', 'SENSEX.NS', 'FINNIFTY.NS', 'MIDCPNIFTY.NS')
                        AND interval = '1d'
                        ORDER BY symbol
                    """)

                indices = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    indices.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['indices'] = indices

                # Fetch top gainers (stocks with highest positive percent change)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE interval = '1d'
                        AND percent_change > 0
                        AND symbol NOT LIKE '%INDEX%'
                        ORDER BY percent_change DESC
                        LIMIT 10
                    """)

                gainers = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    gainers.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['gainers'] = gainers

                # Fetch top losers (stocks with highest negative percent change)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE interval = '1d'
                        AND percent_change < 0
                        AND symbol NOT LIKE '%INDEX%'
                        ORDER BY percent_change ASC
                        LIMIT 10
                    """)

                losers = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    losers.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['losers'] = losers

            return market_data

        except Exception as e:
            import traceback
            print(f"Error fetching market data: {e}")
            traceback.print_exc()
            return None

    def clear_old_market_data(self):
        """Clear market data older than 1 day"""
        with self._get_cursor() as cur:
            cur.execute("""
                DELETE FROM market_data_cache
                WHERE last_updated < NOW() - INTERVAL '1 day'
            """)

    def save_buildup_results(self, results):
        """Save buildup analysis results to the database with proper conflict handling"""
        if not results:
            return

        data = []
        timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M')

        # Process buildup data
        for result_type, items in results.items():
            if result_type in ['futures_long_buildup', 'futures_short_buildup',
                               'options_long_buildup', 'options_short_buildup']:
                for item in items:
                    data.append((
                        item['symbol'],
                        'buildup',  # analytics_type
                        result_type.replace('_buildup', ''),  # category
                        item.get('strike', 0),
                        item.get('option_type', 'FUT'),
                        item.get('price_change', 0),
                        item.get('oi_change', 0),
                        item.get('volume_change', 0),
                        item.get('absolute_oi', 0),
                        item.get('timestamp', timestamp)
                    ))

        # Process OI extremes (if present in results)
        if 'oi_gainers' in results:
            for item in results['oi_gainers']:
                data.append((
                    item['symbol'],
                    'oi_analytics',
                    'oi_gainer',
                    item.get('strike', 0),
                    item.get('type', 'FUT'),
                    None,  # price_change
                    item['oi_change'],
                    None,  # volume_change
                    item['oi'],
                    item.get('timestamp', timestamp)
                ))

        if 'oi_losers' in results:
            for item in results['oi_losers']:
                data.append((
                    item['symbol'],
                    'oi_analytics',
                    'oi_loser',
                    item.get('strike', 0),
                    item.get('type', 'FUT'),
                    None,  # price_change
                    item['oi_change'],
                    None,  # volume_change
                    item['oi'],
                    item.get('timestamp', timestamp)
                ))

        with self._get_cursor() as cur:
            # Insert into fno_analytics
            execute_batch(cur, """
                INSERT INTO fno_analytics 
                (symbol, analytics_type, category, strike, option_type,
                 price_change, oi_change, volume_change, absolute_oi, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, analytics_type, category, strike, option_type, timestamp) 
                DO UPDATE SET
                    price_change = EXCLUDED.price_change,
                    oi_change = EXCLUDED.oi_change,
                    volume_change = EXCLUDED.volume_change,
                    absolute_oi = EXCLUDED.absolute_oi
            """, data, page_size=100)

    def get_buildup_results(self, limit=20):
        """Get recent buildup results"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, result_type, category, strike, option_type,
                       price_change, oi_change, volume_change, absolute_oi, timestamp
                FROM buildup_results
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))

            results = []
            for row in cur.fetchall():
                results.append({
                    'symbol': row[0],
                    'result_type': row[1],
                    'category': row[2],
                    'strike': float(row[3]),
                    'option_type': row[4],
                    'price_change': float(row[5]),
                    'oi_change': float(row[6]),
                    'volume_change': float(row[7]),
                    'absolute_oi': int(row[8]),
                    'timestamp': row[9]
                })
            return results

    @lru_cache(maxsize=100)
    def get_anchored_pivot_data(self, ticker, interval):
        """Cache yfinance data for pivot calculations to reduce API calls"""
        try:
            stock = yf.Ticker(ticker, session=self.session)

            # Determine anchor timeframe based on current interval
            if interval in ['5m', '15m']:
                # Intraday pivots use previous day's daily data
                return stock.history(period='2d', interval='1d')
            elif interval in ['30m', '1h', '1d']:
                # Daily pivots use previous month's monthly data
                return stock.history(period='2y', interval='1d')  # Increased to 2 years
            elif interval == '1wk':
                # Weekly pivots use previous quarter's data
                return stock.history(period='6mo', interval='3mo')

            # Default case - return None
            return None
        except Exception as e:
            print(f"Error fetching anchor data for {ticker}: {str(e)}")
            return None

    def update_index_data(self, symbol, interval, data):
        """
        Update index data with only price information, no indicators calculation.
        Used for indices like NIFTY, BANKNIFTY, etc. to save processing time.
        
        Args:
            symbol: The index symbol (e.g. 'NIFTY', 'BANKNIFTY')
            interval: Data interval (e.g. '1d', '1h')
            data: DataFrame with OHLCV data
        
        Returns:
            bool: Success status
        """
        try:
            # Convert to DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                if isinstance(data, dict):
                    data = pd.DataFrame(data)
                else:
                    raise TypeError(f"Expected DataFrame or dict, got {type(data)}")

            # Make copy to avoid modifying original
            df = data.copy()

            # Ensure the index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Sort by date
            df = df.sort_index()

            # Take only the most recent valid data point
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                print(f"No valid data available for index {symbol} at interval {interval}.")
                return False

            latest_data = latest_data.apply(lambda x: self._convert_numpy_types(x))

            # Extract only the price data we need
            timestamp = latest_data.name.to_pydatetime()
            high = float(latest_data['High'])
            low = float(latest_data['Low'])
            close = float(latest_data['Close'])
            Open = float(latest_data['Open'])
            volume = float(latest_data['Volume']) if 'Volume' in latest_data else 0

            # Calculate price change and percent change
            if len(df) > 1:
                prev_close = float(df.iloc[-2]['Close'])
                price_change = close - prev_close
                percent_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
            else:
                price_change = 0
                percent_change = 0

            # Update database using a simplified query for indices
            with self._get_cursor() as cur:
                upsert_sql = """
                    INSERT INTO stock_data_cache (
                        symbol, interval, timestamp, open, high, low, close, volume,
                        price_change, percent_change
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, interval)
                    DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        open = EXCLUDED.open, 
                        high = EXCLUDED.high, 
                        low = EXCLUDED.low,
                        close = EXCLUDED.close, 
                        volume = EXCLUDED.volume,
                        price_change = EXCLUDED.price_change,
                        percent_change = EXCLUDED.percent_change,
                        last_updated = NOW()
                """

                params = (
                    symbol,
                    interval,
                    self._convert_numpy_types(timestamp),
                    self._convert_numpy_types(Open),
                    self._convert_numpy_types(high),
                    self._convert_numpy_types(low),
                    self._convert_numpy_types(close),
                    self._convert_numpy_types(volume),
                    self._convert_numpy_types(price_change),
                    self._convert_numpy_types(percent_change)
                )

                cur.execute(upsert_sql, params)

            return True
        except Exception as e:
            import traceback
            print(f"Error updating index {symbol} data: {str(e)}")
            traceback.print_exc()
            return False

    def update_stock_prices_batch(self, stock_data_list):
        """Update stock prices in bulk"""
        if not stock_data_list:
            return

        try:
            with self._get_cursor() as cur:
                # Prepare data for batch update
                records = []
                for data in stock_data_list:
                    symbol = data['symbol']
                    # Ensure symbol has .NS suffix
                    if not symbol.endswith('.NS'):
                        symbol = f"{symbol}.NS"

                    records.append((
                        symbol,
                        float(data['close']) if data['close'] is not None else 0.0,
                        float(data['price_change']) if data['price_change'] is not None else 0.0,
                        float(data['percent_change']) if data['percent_change'] is not None else 0.0,
                        data['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(data['timestamp'], datetime) else data['timestamp']
                    ))
                
                # Use execute_batch for efficient batch update
                execute_batch(cur, """
                    UPDATE stock_data_cache
                    SET close = %s, price_change = %s, percent_change = %s, last_updated = %s
                    WHERE symbol = %s
                """, [(r[1], r[2], r[3], r[4], r[0]) for r in records], page_size=100)

                rows_affected = cur.rowcount
                print(f"Updated {rows_affected} stock price records")
        except Exception as e:
            print(f"Error updating stock prices batch: {e}")
            import traceback
            traceback.print_exc()

    def get_option_instruments_by_symbol(self, symbol):
        """Get all option instrument keys for a specific symbol"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        strike_price, 
                        expiry_date as expiry, 
                        option_type, 
                        prev_close as last_close
                    FROM instrument_keys 
                    WHERE symbol = %s 
                    AND (option_type = 'CE' OR option_type = 'PE')
                    """
                , (symbol,))
                results = cur.fetchall()

                # Format the result as a list of dictionaries
                return [
                    {
                        'instrument_key': row[0],
                        'symbol': row[1],
                        'strike_price': float(row[2]) if row[2] else None,
                        'expiry': row[3].isoformat() if row[3] else None,
                        'option_type': row[4],
                        'last_close': float(row[5]) if row[5] else 0
                    }
                    for row in results
                ]
        except Exception as e:
            print(f"Error fetching option instruments: {str(e)}")
            return []

    def get_option_stock_symbols(self):
        """Get all unique stock symbols that have options available"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT symbol 
                    FROM instrument_keys 
                    ORDER BY symbol
                    """
                )
                results = cur.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            print(f"Error fetching option stock symbols: {str(e)}")
            return []

    def get_instrument_key_by_symbol_and_exchange(self, symbol, exchange):
        """Get instrument key by symbol and exchange."""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        exchange, 
                        tradingsymbol,
                        prev_close as last_close
                    FROM instrument_keys
                    WHERE symbol = %s AND exchange = %s
                    LIMIT 1
                """, (symbol, exchange))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'exchange': result[2],
                        'tradingsymbol': result[3],
                        'last_close': float(result[4]) if result[4] else 0
                    }
                return None
        except Exception as e:
            print(f"Error fetching instrument key: {str(e)}")
            return None

    def get_instrument_key_by_key(self, instrument_key):
        """Get instrument details by instrument key."""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        exchange, 
                        tradingsymbol,
                        prev_close as last_close
                    FROM instrument_keys
                    WHERE instrument_key = %s
                    LIMIT 1
                """, (instrument_key,))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'exchange': result[2],
                        'tradingsymbol': result[3],
                        'last_close': float(result[4]) if result[4] else 0
                    }
                return None
        except Exception as e:
            print(f"Error fetching instrument by key: {str(e)}")
            return None

    def get_option_instrument_key(self, symbol, expiry, strike, option_type):
        """Get option instrument key by symbol, expiry, strike and option type."""
        try:
            with self._get_cursor() as cur:
                # Convert expiry to date if it's a string
                if isinstance(expiry, str):
                    expiry_date = datetime.strptime(expiry, '%Y-%m-%d').date()
                else:
                    expiry_date = expiry

                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol,
                        strike_price,
                        expiry_date,
                        option_type,
                        prev_close as last_close
                    FROM instrument_keys
                    WHERE symbol = %s 
                    AND expiry_date = %s 
                    AND strike_price = %s 
                    AND option_type = %s
                    LIMIT 1
                """, (symbol, expiry_date, strike, option_type))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'strike_price': float(result[2]) if result[2] else 0,
                        'expiry_date': result[3].isoformat() if result[3] else None,
                        'option_type': result[4],
                        'last_close': float(result[5]) if result[5] else 0
                    }
                return None
        except Exception as e:
            print(f"Error fetching option instrument key: {str(e)}")
            return None

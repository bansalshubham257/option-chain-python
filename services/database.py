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

    def save_oi_volume_batch(self, records):
        """Save OI volume data"""
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
                str(r['timestamp'])
            )
            for r in filtered_records
        ]

        with self._get_cursor() as cur:
            execute_batch(cur, """
                INSERT INTO oi_volume_history (
                    symbol, expiry_date, strike_price, option_type,
                    oi, volume, price, display_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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


    def save_market_data(self, data_type, data):
        """Save market data to cache by converting dict to JSON"""
        with self._get_cursor() as cur:
            cur.execute("""
                INSERT INTO market_data_cache (data_type, data)
                VALUES (%s, %s)
                ON CONFLICT (data_type) 
                DO UPDATE SET data = EXCLUDED.data, last_updated = NOW()
            """, (data_type, json.dumps(data)))  # Convert dict to JSON string


    def get_market_data(self, data_type):
        """Get cached market data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT data FROM market_data_cache
                WHERE data_type = %s
            """, (data_type,))
            result = cur.fetchone()
            return result[0] if result else None

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

            # Insert into buildup_results (if needed)
            execute_batch(cur, """
                INSERT INTO buildup_results 
                (symbol, result_type, category, strike, option_type,
                 oi_change, absolute_oi, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, result_type, category, strike, option_type, timestamp)
                DO UPDATE SET
                    oi_change = EXCLUDED.oi_change,
                    absolute_oi = EXCLUDED.absolute_oi
            """, [
                (item[0], item[1], item[2], item[3], item[4], item[6], item[8], item[9])
                for item in data
            ], page_size=100)

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

    def save_52_week_data(self, data):
        """Save 52-week high/low data to database"""
        if not data:
            return
    
        timestamp = datetime.now(pytz.timezone('Asia/Kolkata'))
    
        # Convert NumPy types to native Python types
        processed_data = []
        for item in data:
            processed_data.append((
                str(item['symbol']),
                float(item['current_price']),  # Convert to native float
                float(item['week52_high']),
                float(item['week52_low']),
                float(item['pct_from_high']),
                float(item['pct_from_low']),
                int(item['days_since_high']),  # Convert to native int
                int(item['days_since_low']),
                str(item['status']),
                timestamp
            ))
    
        with self._get_cursor() as cur:
            # Clear old data
            cur.execute("DELETE FROM fiftytwo_week_extremes")
    
            # Insert new data with proper type conversion
            execute_batch(cur, """
                INSERT INTO fiftytwo_week_extremes 
                (symbol, current_price, week52_high, week52_low, 
                 pct_from_high, pct_from_low, days_since_high, 
                 days_since_low, status, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, processed_data, page_size=100)
        
    def get_52_week_data(self, limit=50):
        """Get cached 52-week high/low data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, current_price, week52_high, week52_low,
                       pct_from_high, pct_from_low, days_since_high,
                       days_since_low, status, timestamp
                FROM fiftytwo_week_extremes
                ORDER BY 
                    CASE WHEN status = 'near_high' THEN pct_from_high ELSE pct_from_low END,
                    timestamp DESC
                LIMIT %s
            """, (limit,))
    
            results = []
            for row in cur.fetchall():
                results.append({
                    'symbol': row[0],
                    'current_price': float(row[1]),
                    'week52_high': float(row[2]),
                    'week52_low': float(row[3]),
                    'pct_from_high': float(row[4]),
                    'pct_from_low': float(row[5]),
                    'days_since_high': row[6],
                    'days_since_low': row[7],
                    'status': row[8],
                    'timestamp': row[9].isoformat()
                })
            return results

    # Add these methods to your DatabaseService class

    # At the top of app.py, add this dictionary to track pivot calculation status


    @lru_cache(maxsize=100)
    def get_anchored_pivot_data(self, ticker, interval):
        """Cache yfinance data for pivot calculations to reduce API calls"""
        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)

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

    def update_stock_data(self, symbol, interval, data):
        """Update stock data with daily recalculation of pivot points and indicators"""
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

            # Calculate Heikin-Ashi (HA) values
            if len(df) > 1:
                df['HA_Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
                df['HA_Open'] = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
                df['HA_High'] = df[['High', 'HA_Open', 'HA_Close']].max(axis=1)
                df['HA_Low'] = df[['Low', 'HA_Open', 'HA_Close']].min(axis=1)

            # Ensure HA columns exist in the DataFrame
            for col in ['HA_Open', 'HA_Close', 'HA_High', 'HA_Low']:
                if col not in df.columns:
                    df[col] = None  # Add the column with default None values

            if 'Volume' in df.columns and len(df) > 0:
            # Calculate VWAP for each row
                df['VP'] = df['Close'] * df['Volume']  # Value * Price
                df['CV'] = df['Volume'].cumsum()  # Cumulative Volume
                df['CVP'] = df['VP'].cumsum()  # Cumulative Value * Price
                df['VWAP'] = df['CVP'] / df['CV'].replace(0, np.nan)  # Avoid division by zero
            else:
                df['VWAP'] = df['Close']  # Fallback if no volume

            if len(df) > 1:
                df['macd_line'], df['macd_signal'], df['macd_histogram'] = ta.MACD(df['Close'])
                try:
                    # Technical indicator calculation
                    df['adx'] = ta.ADX(df['High'], df['Low'], df['Close'])
                except Exception as e:
                    print(f"Error calculating ADX: {str(e)}")
                    df['adx'] = None
                df['adx_di_positive'] = ta.PLUS_DI(df['High'], df['Low'], df['Close'])
                df['adx_di_negative'] = ta.MINUS_DI(df['High'], df['Low'], df['Close'])
                
                # Enhanced Bollinger Bands calculation to match TradingView implementation
                # Using SMA as basis with 2.0 standard deviations by default
                """
                length = 20  # Default length 
                mult = 2.0   # Default multiplier for standard deviation
                
                # Calculate basis (middle band) using SMA
                basis = df['Close'].rolling(window=length).mean()
                
                # Calculate standard deviation
                dev = mult * df['Close'].rolling(window=length).std()
                
                # Calculate upper and lower bands
                df['upper_bollinger'] = basis + dev
                df['middle_bollinger'] = basis  # Adding middle band for completeness
                df['lower_bollinger'] = basis - dev
                
                """
                
                # Additional bands with different MA types could be added here
                # For example, EMA-based Bollinger Bands:
                # ema_basis = df['Close'].ewm(span=length, adjust=False).mean()
                # df['upper_bollinger_ema'] = ema_basis + (mult * df['Close'].rolling(window=length).std())
                # df['lower_bollinger_ema'] = ema_basis - (mult * df['Close'].rolling(window=length).std())
                
                df['parabolic_sar'] = ta.SAR(df['High'], df['Low'])
                df['rsi'] = ta.RSI(df['Close'])
                stoch_rsi_k, stoch_rsi_d = ta.STOCHRSI(df['Close'])
                df['stoch_rsi'] = stoch_rsi_k  # Use only the first output (fastk)
                df['ichimoku_base'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2
                df['ichimoku_conversion'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
                df['ichimoku_span_a'] = ((df['ichimoku_base'] + df['ichimoku_conversion']) / 2).shift(26)
                df['ichimoku_span_b'] = ((df['High'].rolling(window=52).max() + df['Low'].rolling(window=52).min()) / 2).shift(26)
                df['ichimoku_cloud_top'] = df[['ichimoku_span_a', 'ichimoku_span_b']].max(axis=1)
                df['ichimoku_cloud_bottom'] = df[['ichimoku_span_a', 'ichimoku_span_b']].min(axis=1)

                # Calculate Bollinger %B
                #df['Bollinger_B'] = (df['Close'] - df['lower_bollinger']) / (df['upper_bollinger'] - df['lower_bollinger'])


            # Calculate Supertrend
            if len(df) > 1:
                atr = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
                hl2 = (df['High'] + df['Low']) / 2
                df['Supertrend'] = hl2 - (2 * atr)

            # Calculate Accumulation/Distribution (Acc/Dist)
            if len(df) > 1:
                clv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low']).replace(0, np.nan)
                df['Acc_Dist'] = clv * df['Volume']

            # Calculate Commodity Channel Index (CCI)
            if len(df) > 1:
                df['CCI'] = ta.CCI(df['High'], df['Low'], df['Close'], timeperiod=20)

            # Calculate Chaikin Money Flow (CMF)
            if len(df) > 1:
                mfv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low']).replace(0, np.nan) * df['Volume']
                df['CMF'] = mfv.rolling(window=20).sum() / df['Volume'].rolling(window=20).sum()

            # Calculate Money Flow Index (MFI)
            if len(df) > 1:
                df['MFI'] = ta.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=14)

            # Calculate On Balance Volume (OBV)
            if len(df) > 1:
                df['On_Balance_Volume'] = ta.OBV(df['Close'], df['Volume'])

            # Calculate Williams %R
            if len(df) > 1:
                df['Williams_R'] = ta.WILLR(df['High'], df['Low'], df['Close'], timeperiod=14)

            # Calculate Bollinger Band %B
            # Replace the problematic Bollinger Bands calculation with this code:
            # Enhanced Bollinger Bands calculation to match TradingView implementation
            if len(df) > 1:
                length = 20  # Default length
                mult = 2.0   # Default multiplier for standard deviation

                # Calculate basis (middle band) using SMA
                basis = ta.SMA(df['Close'], timeperiod=length)

                # Calculate standard deviation
                dev = ta.STDDEV(df['Close'], timeperiod=length)

                # Calculate upper and lower bands
                df['upper_bollinger'] = basis + (mult * dev)
                df['middle_bollinger'] = basis  # Store middle band
                df['lower_bollinger'] = basis - (mult * dev)

                # Now calculate Bollinger %B correctly
                denominator = df['upper_bollinger'] - df['lower_bollinger']
                df['Bollinger_B'] = np.where(
                    denominator != 0,
                    (df['Close'] - df['lower_bollinger']) / denominator,
                    0.5  # Default value when upper == lower
                )

            # Calculate Intraday Intensity
            if len(df) > 1:
                df['Intraday_Intensity'] = ((2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']).replace(0, np.nan)) * df['Volume']

            # Calculate Force Index
            if len(df) > 1:
                df['Force_Index'] = df['Close'].diff() * df['Volume']

            # Calculate Schaff Trend Cycle (STC)
            if len(df) > 1:
                macd, macd_signal, _ = ta.MACD(df['Close'])
                df['STC'] = ta.STOCH(macd, macd_signal, macd_signal, fastk_period=14, slowk_period=3, slowd_period=3)[0]

            # Calculate new indicators
            if len(df) > 1:
                for period in [10, 20, 50, 100, 200]:
                    df[f'sma{period}'] = df['Close'].rolling(window=period).mean()
                    df[f'ema{period}'] = ta.EMA(df['Close'], timeperiod=period)
                    df[f'wma{period}'] = ta.WMA(df['Close'], timeperiod=period)
                    df[f'tma{period}'] = df['Close'].rolling(window=period).mean().rolling(window=period).mean()
                    df[f'rma{period}'] = df['Close'].ewm(span=period, adjust=False).mean()
                    df[f'tema{period}'] = ta.TEMA(df['Close'], timeperiod=period)
                    df[f'hma{period}'] = ta.WMA(2 * ta.WMA(df['Close'], timeperiod=period // 2) - ta.WMA(df['Close'], timeperiod=period), timeperiod=int(period**0.5))
                    df[f'vwma{period}'] = (df['Close'] * df['Volume']).rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
                    df[f'std{period}'] = df['Close'].rolling(window=period).std()

            if len(df) > 1:
                # ATR (Average True Range)
                df['ATR'] = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)

                # TRIX (Triple Exponential Average)
                df['TRIX'] = ta.TRIX(df['Close'], timeperiod=14)

                # ROC (Rate of Change)
                df['ROC'] = ta.ROC(df['Close'], timeperiod=10)

                # Keltner Channels
                df['Keltner_Middle'] = df['Close'].rolling(window=20).mean()
                df['Keltner_Upper'] = df['Keltner_Middle'] + (2 * df['ATR'])
                df['Keltner_Lower'] = df['Keltner_Middle'] - (2 * df['ATR'])

                # Donchian Channels
                df['Donchian_High'] = df['High'].rolling(window=20).max()
                df['Donchian_Low'] = df['Low'].rolling(window=20).min()

                # Chaikin Oscillator
                ad_line = ta.AD(df['High'], df['Low'], df['Close'], df['Volume'])
                df['Chaikin_Oscillator'] = ta.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'], fastperiod=3, slowperiod=10)

            # Ensure all required columns exist in the DataFrame
            # Filter out rows with NaN in critical indicators like Supertrend
            df = df.dropna(subset=['Supertrend'])

            # Ensure the index is datetime and sort by date
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            # Take only the most recent valid data point
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                print(f"No valid data available for {symbol} at interval {interval}.")
                return False

            latest_data = latest_data.apply(lambda x: self._convert_numpy_types(x))

            # Extract values for current data
            timestamp = latest_data.name.to_pydatetime()
            high = float(latest_data['High'])
            low = float(latest_data['Low'])
            close = float(latest_data['Close'])
            Open = float(latest_data['Open'])
            volume = float(latest_data['Volume']) if 'Volume' in latest_data else 0
            vwap = float(latest_data['VWAP']) if not pd.isna(latest_data['VWAP']) else close  # Use close as fallback


            if len(df) > 1:
                prev_close = float(df.iloc[-2]['Close'])
                price_change = close - prev_close
                percent_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
            else:
                price_change = None
                percent_change = None
                

            # Check if we need to recalculate pivot points
            current_date = datetime.now().date()

            # Create a cache key for this symbol/interval
            cache_key = f"{symbol}_{interval}"

            # Check if we've already calculated pivots today
            pivot_needs_update = True
            pivot = r1 = r2 = r3 = s1 = s2 = s3 = None

            if cache_key in self.pivot_calculation_cache:
                last_calc_date = self.pivot_calculation_cache[cache_key]['date']
                if last_calc_date == current_date:
                    # We already calculated pivots today, reuse them from cache
                    pivot_data = self.pivot_calculation_cache[cache_key]
                    pivot, r1, r2, r3, s1, s2, s3 = (
                        pivot_data['pivot'], pivot_data['r1'], pivot_data['r2'],
                        pivot_data['r3'], pivot_data['s1'], pivot_data['s2'], pivot_data['s3']
                    )
                    pivot_needs_update = False
            if len(df) > 1:
                df['rsi'] = ta.RSI(df['Close'])
            # Calculate new pivot points if needed
            if pivot_needs_update:
                # First check if we can get them from the database
                with self._get_cursor() as cur:
                    cur.execute("""
                        SELECT pivot, r1, r2, r3, s1, s2, s3, DATE(last_updated)
                        FROM stock_data_cache
                        WHERE symbol = %s AND interval = %s
                        ORDER BY last_updated DESC
                        LIMIT 1
                    """, (symbol, interval))

                    existing_data = cur.fetchone()

                    if existing_data and existing_data[7] == current_date:
                        # Already calculated today, reuse from database
                        pivot, r1, r2, r3, s1, s2, s3 = existing_data[0:7]
                        pivot_needs_update = False
                    else:
                        # Need to calculate new pivot points
                        try:
                            # Get anchor data efficiently using cached function
                            anchor_data = self.get_anchored_pivot_data(symbol, interval)

                            if anchor_data is not None and len(anchor_data) >= 2:
                                prev_data = anchor_data.iloc[-2]
                                prev_high = float(prev_data['High'])
                                prev_low = float(prev_data['Low'])
                                prev_close = float(prev_data['Close'])

                                # Calculate pivot points
                                pivot = (prev_high + prev_low + prev_close) / 3
                                r1 = (2 * pivot) - prev_low
                                r2 = pivot + (prev_high - prev_low)
                                r3 = prev_high + 2 * (pivot - prev_low)
                                s1 = (2 * pivot) - prev_high
                                s2 = pivot - (prev_high - prev_low)
                                s3 = prev_low - 2 * (prev_high - pivot)
                            else:
                                # Fallback: use previous data point if available
                                if len(df) >= 2:
                                    prev_data = df.iloc[-2]
                                    prev_high = float(prev_data['High'])
                                    prev_low = float(prev_data['Low'])
                                    prev_close = float(prev_data['Close'])

                                    pivot = (prev_high + prev_low + prev_close) / 3
                                    r1 = (2 * pivot) - prev_low
                                    r2 = pivot + (prev_high - prev_low)
                                    r3 = prev_high + 2 * (pivot - prev_low)
                                    s1 = (2 * pivot) - prev_high
                                    s2 = pivot - (prev_high - prev_low)
                                    s3 = prev_low - 2 * (prev_high - pivot)
                        except Exception as e:
                            print(f"Error calculating pivot points for {symbol}: {str(e)}")
                            # Try to use existing data if available
                            if existing_data:
                                pivot, r1, r2, r3, s1, s2, s3 = existing_data[0:7]
                                pivot_needs_update = False

                # If we've calculated new pivot points, update the cache
                if pivot_needs_update and pivot is not None:
                    self.pivot_calculation_cache[cache_key] = {
                        'date': current_date,
                        'pivot': pivot,
                        'r1': r1, 'r2': r2, 'r3': r3,
                        's1': s1, 's2': s2, 's3': s3
                    }

            # Convert NumPy types to native Python types for the latest data
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                return False

            # Ensure all extracted values are native Python types
            pivot = float(pivot) if pivot is not None else None
            r1 = float(r1) if r1 is not None else None
            r2 = float(r2) if r2 is not None else None
            r3 = float(r3) if r3 is not None else None
            s1 = float(s1) if s1 is not None else None
            s2 = float(s2) if s2 is not None else None
            s3 = float(s3) if s3 is not None else None

            # Safely convert all columns in `latest_data` to native Python types
            if isinstance(latest_data, pd.Series):
                latest_data = latest_data.map(
                    lambda x: float(x) if isinstance(x, (np.float64, np.float32, np.int64, np.int32)) else x
                )
            else:
                raise TypeError("Expected `latest_data` to be a pandas Series, but got: {}".format(type(latest_data)))

            # Prepare data for insertion
            required_columns = [
                'HA_Open', 'HA_Close', 'HA_High', 'HA_Low', 'Supertrend',
                'upper_bollinger', 'lower_bollinger', 'middle_bollinger',
                'ichimoku_base', 'ichimoku_conversion', 'ichimoku_span_a', 'ichimoku_span_b',
                'ichimoku_cloud_top', 'ichimoku_cloud_bottom',
                'stoch_rsi', 'parabolic_sar', 'macd_line', 'macd_signal', 'macd_histogram',
                'adx', 'adx_di_positive', 'adx_di_negative',
                'Acc_Dist', 'CCI', 'CMF', 'MFI', 'On_Balance_Volume', 'Williams_R',
                'Bollinger_B', 'Intraday_Intensity', 'Force_Index', 'STC',  # Added bollinger_b
                'sma10', 'sma20', 'sma50', 'sma100', 'sma200',
                'ema10', 'ema50', 'ema100', 'ema200',
                'wma10', 'wma50', 'wma100', 'wma200',
                'tma10', 'tma50', 'tma100', 'tma200',
                'rma10', 'rma50', 'rma100', 'rma200',
                'tema10', 'tema50', 'tema100', 'tema200',
                'hma10', 'hma50', 'hma100', 'hma200',
                'vwma10', 'vwma50', 'vwma100', 'vwma200',
                'std10', 'std50', 'std100', 'std200',
                'ATR', 'TRIX', 'ROC', 'rsi', 'VWAP',  # Added rsi, VWAP
                'Keltner_Middle', 'Keltner_Upper', 'Keltner_Lower',
                'Donchian_High', 'Donchian_Low', 'Chaikin_Oscillator'
            ]



            # Identify missing columns
            missing_columns = {col: None for col in required_columns if col not in df.columns}

            # Add missing columns in one operation
            if missing_columns:
                print("missing columns", missing_columns)
                # df = pd.concat([df, pd.DataFrame(missing_columns, index=df.index)], axis=1)


            # Update database using a single query with upsert
            with self._get_cursor() as cur:
                upsert_sql = """
                    INSERT INTO stock_data_cache (
                        symbol, interval, timestamp, open, high, low, close, volume,
                        pivot, r1, r2, r3, s1, s2, s3, price_change, percent_change, vwap,
                        ichimoku_base, ichimoku_conversion, ichimoku_span_a, ichimoku_span_b,
                        ichimoku_cloud_top, ichimoku_cloud_bottom, stoch_rsi, parabolic_sar,
                        upper_bollinger, lower_bollinger, middle_bollinger, macd_line, macd_signal, macd_histogram,
                        adx, adx_di_positive, adx_di_negative, HA_Open, HA_Close, HA_High, HA_Low,
                        Supertrend, Acc_Dist, CCI, CMF, MFI, On_Balance_Volume, Williams_R,
                        Bollinger_B, Intraday_Intensity, Force_Index, STC, sma10, sma20, sma50, sma100,
                        sma200, ema10, ema50, ema100, ema200, wma10, wma50, wma100, wma200,
                        tma10, tma50, tma100, tma200, rma10, rma50, rma100, rma200, tema10,
                        tema50, tema100, tema200, hma10, hma50, hma100, hma200, vwma10, vwma50,
                        vwma100, vwma200, std10, std50, std100, std200, ATR, TRIX, ROC,
                        Keltner_Middle, Keltner_Upper, Keltner_Lower, Donchian_High, Donchian_Low,
                        Chaikin_Oscillator,rsi
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, interval, timestamp)
                    DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume, pivot = EXCLUDED.pivot,
                        r1 = EXCLUDED.r1, r2 = EXCLUDED.r2, r3 = EXCLUDED.r3, s1 = EXCLUDED.s1,
                        s2 = EXCLUDED.s2, s3 = EXCLUDED.s3, price_change = EXCLUDED.price_change,
                        percent_change = EXCLUDED.percent_change, vwap = EXCLUDED.vwap,
                        ichimoku_base = EXCLUDED.ichimoku_base, ichimoku_conversion = EXCLUDED.ichimoku_conversion,
                        ichimoku_span_a = EXCLUDED.ichimoku_span_a, ichimoku_span_b = EXCLUDED.ichimoku_span_b,
                        ichimoku_cloud_top = EXCLUDED.ichimoku_cloud_top, ichimoku_cloud_bottom = EXCLUDED.ichimoku_cloud_bottom,
                        stoch_rsi = EXCLUDED.stoch_rsi, parabolic_sar = EXCLUDED.parabolic_sar,
                        upper_bollinger = EXCLUDED.upper_bollinger, lower_bollinger = EXCLUDED.lower_bollinger,
                        middle_bollinger = EXCLUDED.middle_bollinger, macd_line = EXCLUDED.macd_line, 
                        macd_signal = EXCLUDED.macd_signal,
                        macd_histogram = EXCLUDED.macd_histogram, adx = EXCLUDED.adx,
                        adx_di_positive = EXCLUDED.adx_di_positive, adx_di_negative = EXCLUDED.adx_di_negative,
                        HA_Open = EXCLUDED.HA_Open, HA_Close = EXCLUDED.HA_Close, ha_high = EXCLUDED.HA_High,
                        HA_Low = EXCLUDED.HA_Low, Supertrend = EXCLUDED.Supertrend, Acc_Dist = EXCLUDED.Acc_Dist,
                        CCI = EXCLUDED.CCI, CMF = EXCLUDED.CMF, MFI = EXCLUDED.MFI, On_Balance_Volume = EXCLUDED.On_Balance_Volume,
                        Williams_R = EXCLUDED.Williams_R, Bollinger_B = EXCLUDED.Bollinger_B,
                        Intraday_Intensity = EXCLUDED.Intraday_Intensity, Force_Index = EXCLUDED.Force_Index,
                        STC = EXCLUDED.STC, sma10 = EXCLUDED.sma10, sma20 = EXCLUDED.sma20, sma50 = EXCLUDED.sma50, sma100 = EXCLUDED.sma100,
                        sma200 = EXCLUDED.sma200, ema10 = EXCLUDED.ema10, ema50 = EXCLUDED.ema50, ema100 = EXCLUDED.ema100,
                        ema200 = EXCLUDED.ema200, wma10 = EXCLUDED.wma10, wma50 = EXCLUDED.wma50, wma100 = EXCLUDED.wma100,
                        wma200 = EXCLUDED.wma200, tma10 = EXCLUDED.tma10, tma50 = EXCLUDED.tma50, tma100 = EXCLUDED.tma100,
                        tma200 = EXCLUDED.tma200, rma10 = EXCLUDED.rma10, rma50 = EXCLUDED.rma50, rma100 = EXCLUDED.rma100,
                        rma200 = EXCLUDED.rma200, tema10 = EXCLUDED.tema10, tema50 = EXCLUDED.tema50, tema100 = EXCLUDED.tema100,
                        tema200 = EXCLUDED.tema200, hma10 = EXCLUDED.hma10, hma50 = EXCLUDED.hma50, hma100 = EXCLUDED.hma100,
                        hma200 = EXCLUDED.hma200, vwma10 = EXCLUDED.vwma10, vwma50 = EXCLUDED.vwma50, vwma100 = EXCLUDED.vwma100,
                        vwma200 = EXCLUDED.vwma200, std10 = EXCLUDED.std10, std50 = EXCLUDED.std50, std100 = EXCLUDED.std100,
                        std200 = EXCLUDED.std200, ATR = EXCLUDED.ATR, TRIX = EXCLUDED.TRIX, ROC = EXCLUDED.ROC,
                        Keltner_Middle = EXCLUDED.Keltner_Middle, Keltner_Upper = EXCLUDED.Keltner_Upper,
                        Keltner_Lower = EXCLUDED.Keltner_Lower, Donchian_High = EXCLUDED.Donchian_High,
                        Donchian_Low = EXCLUDED.Donchian_Low, Chaikin_Oscillator = EXCLUDED.Chaikin_Oscillator, rsi = EXCLUDED.rsi
                """

                # Convert all numpy values to native Python types
                # In your update_stock_data method, replace the params tuple construction with:

                params = (
                    symbol,                                                    # 1
                    interval,                                                  # 2
                    self._convert_numpy_types(timestamp),                      # 3
                    self._convert_numpy_types(Open),                     # 4
                    self._convert_numpy_types(high),                           # 5
                    self._convert_numpy_types(low),                            # 6
                    self._convert_numpy_types(close),                          # 7
                    self._convert_numpy_types(volume),                         # 8
                    self._convert_numpy_types(pivot),                          # 9
                    self._convert_numpy_types(r1),                            # 10
                    self._convert_numpy_types(r2),                            # 11
                    self._convert_numpy_types(r3),                            # 12
                    self._convert_numpy_types(s1),                            # 13
                    self._convert_numpy_types(s2),                            # 14
                    self._convert_numpy_types(s3),                            # 15
                    self._convert_numpy_types(price_change),                  # 16
                    self._convert_numpy_types(percent_change),                # 17
                    self._convert_numpy_types(vwap),                          # 18
                    # Ichimoku
                    self._convert_numpy_types(latest_data['ichimoku_base']),            # 19
                    self._convert_numpy_types(latest_data['ichimoku_conversion']),      # 20
                    self._convert_numpy_types(latest_data['ichimoku_span_a']),          # 21
                    self._convert_numpy_types(latest_data['ichimoku_span_b']),          # 22
                    self._convert_numpy_types(latest_data['ichimoku_cloud_top']),       # 23
                    self._convert_numpy_types(latest_data['ichimoku_cloud_bottom']),    # 24
                    # Other indicators
                    self._convert_numpy_types(latest_data['stoch_rsi']),               # 25
                    self._convert_numpy_types(latest_data['parabolic_sar']),           # 26
                    # Bollinger Bands
                    self._convert_numpy_types(latest_data['upper_bollinger']),         # 27
                    self._convert_numpy_types(latest_data['lower_bollinger']),         # 28
                    self._convert_numpy_types(latest_data['middle_bollinger']),        # 29
                    # MACD
                    self._convert_numpy_types(latest_data['macd_line']),               # 30
                    self._convert_numpy_types(latest_data['macd_signal']),             # 31
                    self._convert_numpy_types(latest_data['macd_histogram']),          # 32
                    # ADX
                    self._convert_numpy_types(latest_data['adx']),                     # 33
                    self._convert_numpy_types(latest_data['adx_di_positive']),         # 34
                    self._convert_numpy_types(latest_data['adx_di_negative']),         # 35
                    # Heikin Ashi
                    self._convert_numpy_types(latest_data['HA_Open']),                 # 36
                    self._convert_numpy_types(latest_data['HA_Close']),                # 37
                    self._convert_numpy_types(latest_data['HA_High']),                 # 38
                    self._convert_numpy_types(latest_data['HA_Low']),                  # 39
                    # Volume indicators
                    self._convert_numpy_types(latest_data['Supertrend']),              # 40
                    self._convert_numpy_types(latest_data['Acc_Dist']),                # 41
                    self._convert_numpy_types(latest_data['CCI']),                     # 42
                    self._convert_numpy_types(latest_data['CMF']),                     # 43
                    self._convert_numpy_types(latest_data['MFI']),                     # 44
                    self._convert_numpy_types(latest_data['On_Balance_Volume']),       # 45
                    self._convert_numpy_types(latest_data['Williams_R']),              # 46
                    self._convert_numpy_types(latest_data['Bollinger_B']),             # 47
                    self._convert_numpy_types(latest_data['Intraday_Intensity']),      # 48
                    self._convert_numpy_types(latest_data['Force_Index']),             # 49
                    self._convert_numpy_types(latest_data['STC']),                     # 50
                    # Moving averages
                    self._convert_numpy_types(latest_data['sma10']),
                    self._convert_numpy_types(latest_data.get('sma20', None)),# 51
                    self._convert_numpy_types(latest_data['sma50']),                   # 52
                    self._convert_numpy_types(latest_data['sma100']),                  # 53
                    self._convert_numpy_types(latest_data['sma200']),                  # 54
                    self._convert_numpy_types(latest_data['ema10']),                   # 55
                    self._convert_numpy_types(latest_data['ema50']),                   # 56
                    self._convert_numpy_types(latest_data['ema100']),                  # 57
                    self._convert_numpy_types(latest_data['ema200']),                  # 58
                    self._convert_numpy_types(latest_data['wma10']),                   # 59
                    self._convert_numpy_types(latest_data['wma50']),                   # 60
                    self._convert_numpy_types(latest_data['wma100']),                  # 61
                    self._convert_numpy_types(latest_data['wma200']),                  # 62
                    self._convert_numpy_types(latest_data['tma10']),                   # 63
                    self._convert_numpy_types(latest_data['tma50']),                   # 64
                    self._convert_numpy_types(latest_data['tma100']),                  # 65
                    self._convert_numpy_types(latest_data['tma200']),                  # 66
                    self._convert_numpy_types(latest_data['rma10']),                   # 67
                    self._convert_numpy_types(latest_data['rma50']),                   # 68
                    self._convert_numpy_types(latest_data['rma100']),                  # 69
                    self._convert_numpy_types(latest_data['rma200']),                  # 70
                    self._convert_numpy_types(latest_data['tema10']),                  # 71
                    self._convert_numpy_types(latest_data['tema50']),                  # 72
                    self._convert_numpy_types(latest_data['tema100']),                 # 73
                    self._convert_numpy_types(latest_data['tema200']),                 # 74
                    self._convert_numpy_types(latest_data['hma10']),                   # 75
                    self._convert_numpy_types(latest_data['hma50']),                   # 76
                    self._convert_numpy_types(latest_data['hma100']),                  # 77
                    self._convert_numpy_types(latest_data['hma200']),                  # 78
                    self._convert_numpy_types(latest_data['vwma10']),                  # 79
                    self._convert_numpy_types(latest_data['vwma50']),                  # 80
                    self._convert_numpy_types(latest_data['vwma100']),                 # 81
                    self._convert_numpy_types(latest_data['vwma200']),                 # 82
                    # Standard deviations
                    self._convert_numpy_types(latest_data['std10']),                   # 83
                    self._convert_numpy_types(latest_data['std50']),                   # 84
                    self._convert_numpy_types(latest_data['std100']),                  # 85
                    self._convert_numpy_types(latest_data['std200']),                  # 86
                    # Other indicators
                    self._convert_numpy_types(latest_data['ATR']),                     # 87
                    self._convert_numpy_types(latest_data['TRIX']),                    # 88
                    self._convert_numpy_types(latest_data['ROC']),                     # 89
                    # Keltner Channels
                    self._convert_numpy_types(latest_data['Keltner_Middle']),          # 90
                    self._convert_numpy_types(latest_data['Keltner_Upper']),           # 91
                    self._convert_numpy_types(latest_data['Keltner_Lower']),           # 92
                    # Donchian Channels
                    self._convert_numpy_types(latest_data['Donchian_High']),           # 93
                    self._convert_numpy_types(latest_data['Donchian_Low']),            # 94
                    # Chaikin
                    self._convert_numpy_types(latest_data['Chaikin_Oscillator']),      # 95

                    # These may be the missing parameters:
                    self._convert_numpy_types(latest_data.get('rsi', None)),           # 96  <-- RSI was missing
                )

                cur.execute(upsert_sql, params)

            return True
        except Exception as e:
            import traceback
            print(f"Error updating {symbol} data: {str(e)}")
            traceback.print_exc()
            return False
        
    def get_stock_data(self, symbol, interval):
        """Get cached stock data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT data FROM stock_data_cache
                WHERE symbol = %s AND interval = %s
            """, (symbol, interval))
            result = cur.fetchone()
            return json.loads(result[0]) if result else None


    def _convert_numpy_types(self, value):
        """Convert numpy types to native Python types"""
        if isinstance(value, (np.floating, np.float32, np.float64)):
            return float(value)
        elif isinstance(value, (np.integer, np.int32, np.int64)):
            return int(value)
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, (list, tuple)):
            return [self._convert_numpy_types(x) for x in value]
        elif isinstance(value, dict):
            return {k: self._convert_numpy_types(v) for k, v in value.items()}
        return value

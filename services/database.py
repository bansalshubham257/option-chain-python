import json
import os
import time

import psycopg2
from psycopg2._psycopg import OperationalError
from psycopg2.extras import execute_batch
from decimal import Decimal
from contextlib import contextmanager

class DatabaseService:

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
        filtered_records = [
            r for r in records
            if not (isinstance(r['strike'], (int, float)) and r['strike'] < 10)
        ]

        if not filtered_records:
            return

        with self._get_cursor() as cur:
            execute_batch(cur, """
                INSERT INTO oi_volume_history (
                    symbol, expiry_date, strike_price, option_type,
                    oi, volume, price, display_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                (r['symbol'], r['expiry'], r['strike'], r['option_type'],
                 r['oi'], r['volume'], r['price'], r['timestamp'])
                for r in filtered_records
            ], page_size=100)

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
        """Save all analytics data to fno_analytics table"""
        if not results:
            return
    
        data = []
        
        # Process futures buildup
        for item in results.get('futures_long_buildup', []):
            data.append((
                item['symbol'],
                'buildup',
                'futures_long',
                item.get('strike', 0),
                'FUT',
                item.get('price_change', 0),
                item.get('oi_change', 0),
                item.get('volume_change', 0),
                item.get('absolute_oi', 0),
                item['timestamp']
            ))
        
        for item in results.get('futures_short_buildup', []):
            data.append((
                item['symbol'],
                'buildup',
                'futures_short',
                item.get('strike', 0),
                'FUT',
                item.get('price_change', 0),
                item.get('oi_change', 0),
                item.get('volume_change', 0),
                item.get('absolute_oi', 0),
                item['timestamp']
            ))
        
        # Process options buildup
        for item in results.get('options_long_buildup', []):
            data.append((
                item['symbol'],
                'buildup',
                'options_long',
                item.get('strike', 0),
                item.get('type', 'CE'),
                item.get('price_change', 0),
                item.get('oi_change', 0),
                item.get('volume_change', 0),
                item.get('absolute_oi', 0),
                item['timestamp']
            ))
        
        for item in results.get('options_short_buildup', []):
            data.append((
                item['symbol'],
                'buildup',
                'options_short',
                item.get('strike', 0),
                item.get('type', 'PE'),
                item.get('price_change', 0),
                item.get('oi_change', 0),
                item.get('volume_change', 0),
                item.get('absolute_oi', 0),
                item['timestamp']
            ))
        
        # Process OI analytics if present
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
                    item['timestamp']
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
                    item['timestamp']
                ))
    
        if not data:
            return
    
        with self._get_cursor() as cur:
            execute_batch(cur, """
                INSERT INTO fno_analytics 
                (symbol, analytics_type, category, strike, option_type,
                 price_change, oi_change, volume_change, absolute_oi, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, analytics_type, category, strike, option_type, timestamp) 
                DO NOTHING
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

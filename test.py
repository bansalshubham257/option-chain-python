import ssl
import json
import os

import pandas as pd
import requests
import re
import pytz
from datetime import datetime

from flask import Flask
from flask_cors import CORS
from decimal import Decimal
import psycopg2
from contextlib import contextmanager

# Add these at the top of your files:
import concurrent.futures  # For ThreadPoolExecutor
from psycopg2.extras import execute_batch  # For batch database inserts
from decimal import Decimal  # If not already imported
import time  # For rate limiting sleeps

from decimal import Decimal
import json


from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

utc_now = datetime.utcnow()
ist = pytz.timezone('Asia/Kolkata')
ist_now = utc_now.astimezone(ist)
formatted_time = ist_now       

# Database connection setup
def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

@contextmanager
def db_cursor():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

# Load all instruments
try:
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    scripts = pd.read_json('https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz')

    df = pd.read_csv(url, compression='gzip')
    fno_stocks_raw = df[df['exchange'] == 'NSE_FO'][['tradingsymbol', 'lot_size']].dropna()

except Exception as e:
    print(f"❌ Error loading instruments data: {e}")
    scripts, df, fno_stocks_raw = None, None, None  # Handle failure gracefully

excluded_stocks = {"NIFTYNXT", "NIFTY", "FINNIFTY", "TATASTEEL", "IDEA", "YESBANK", "IDFCFIRSTB", "TATASTEEL", "PNB"}

fno_stocks = {re.split(r'\d', row['tradingsymbol'], 1)[0]: int(row['lot_size'])
              for _, row in fno_stocks_raw.iterrows() if row['tradingsymbol'] not in excluded_stocks}

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("23:55", "%H:%M").time()

def is_market_open():
    now = datetime.now(IST)
    return now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE

def getInstrumentKey(symbol):
    try:
        key = scripts[scripts['trading_symbol'] == symbol]['instrument_key'].values
        return key[0] if len(key) > 0 else None
    except Exception as e:
        print(f"⚠️ Error getting instrument key for {symbol}: {e}")
        return None

def getFuturesInstrumentKey(symbol):
    """Fetch the instrument key for futures contracts"""
    expiry = "FUT 27 MAR 25"
    symbol = symbol + " " + expiry
    try:
        # ✅ Filter for FUTURES contracts only
        key = scripts[(scripts['trading_symbol'] == symbol) & (scripts['instrument_type'] == "FUT")]['instrument_key'].values
        return key[0] if len(key) > 0 else None
    except Exception as e:
        print(f"⚠️ Error getting futures instrument key for {symbol}: {e}")
        return None

def fetch_market_quotes(instrument_keys):
    """Fetch market quotes with rate limiting and retry logic."""
    try:
        url = 'https://api.upstox.com/v2/market-quote/quotes'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': ','.join(instrument_keys)}

        response = requests.get(url, headers=headers, params=params)
        # Validate successful response
        if response.status_code == 200:
            data = response.json()
            return data.get('data', {})
        print(f"⚠️ API error {response.status_code}: {response.text}")
        return {}

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {str(e)}")
        return {}

def process_large_futures_orders(market_quotes, stock_symbol, lot_size):
    """Detect large futures orders from market_quotes data"""

    large_orders = []

    prefix = f"NSE_FO:{stock_symbol}"  # stock_symbol is dynamic
    suffix = "MARFUT"

    # Search for the correct futures instrument key dynamically
    pattern = re.compile(rf"{prefix}\d+{suffix}")
    fut_instrument_key = next((key for key in market_quotes if pattern.match(key)), None)

    if not fut_instrument_key:
        print(f"⚠️ No data found for {stock_symbol} futures")
        return large_orders  # ✅ No futures data found, return empty list

    # ✅ Now, `fut_instrument_key` is dynamically selected
    futures_data = market_quotes[fut_instrument_key]
    depth_data = futures_data.get('depth', {})
    top_bids = depth_data.get('buy', [])[:5]
    top_asks = depth_data.get('sell', [])[:5]

    threshold = lot_size * 28
    ltp = futures_data.get('last_price', 0)
    valid_bid = any(bid['quantity'] >= threshold for bid in top_bids)
    valid_ask = any(ask['quantity'] >= threshold for ask in top_asks)

    if (valid_bid or valid_ask) and ltp > 2:
        ist_now = datetime.utcnow().astimezone(pytz.timezone('Asia/Kolkata'))  # Keep as datetime object

        large_orders.append({
            'stock': stock_symbol,
            'ltp': ltp,
            'bid_qty': max((b['quantity'] for b in top_bids), default=0),
            'ask_qty': max((a['quantity'] for a in top_asks), default=0),
            'lot_size': lot_size,
            'timestamp': formatted_time
        })
        print("Future large_orders - ", large_orders)
    return large_orders
"""
def fetch_futures_orders(stock_symbol, expiry_date, lot_size):
    if stock_symbol in excluded_stocks:
        return None

    # Get the futures instrument key
    fut_instrument_key = getFuturesInstrumentKey(stock_symbol)

    if not fut_instrument_key:
        print(f"⚠️ No futures instrument key for {stock_symbol}")
        return None

    try:
        # Fetch market quotes for the futures instrument
        market_quotes = fetch_market_quotes([fut_instrument_key])

        if not market_quotes:
            print(f"⚠️ No market quotes found for {stock_symbol} futures")
            return None

        # Process large futures orders
        large_orders_futures = process_large_futures_orders(market_quotes, stock_symbol, lot_size)

        # Store the detected large futures orders
        if large_orders_futures:
            save_futures_data(stock_symbol, large_orders_futures)

        return large_orders_futures

    except Exception as e:
        print(f"❌ Error processing futures orders for {stock_symbol}: {e}")
        return None
"""
def convert_to_decimal(data):
    """Convert floats to Decimals in a dictionary."""
    if isinstance(data, float):
        return Decimal(str(data))
    elif isinstance(data, dict):
        return {k: convert_to_decimal(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_to_decimal(item) for item in data]
    return data


def fetch_option_chain(stock_symbol, expiry_date, lot_size):
    """Optimized version that prepares data for parallel DB writes"""
    if stock_symbol in excluded_stocks:
        return None

    # Get instrument key (cache this if possible)
    instrument_key = getInstrumentKey(stock_symbol)
    if not instrument_key:
        print(f"⚠️ No instrument key for {stock_symbol}")
        return None

    try:
        display_time = ist_now.strftime("%H:%M")
        
        # 1. Fetch option chain data
        url = 'https://api.upstox.com/v2/option/chain'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            print(f"⚠️ Option chain API failed for {stock_symbol}: {response.status_code}")
            return None

        data = response.json().get('data', [])
        if not data:
            print(f"⚠️ No option chain data for {stock_symbol}")
            return None

        # 2. Prepare market quotes request
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
        fut_instrument_key = getFuturesInstrumentKey(stock_symbol)
        if fut_instrument_key:
            instrument_keys.append(fut_instrument_key)

        if not instrument_keys:
            print(f"⚠️ No valid instrument keys for {stock_symbol}")
            return None

        # 3. Fetch market quotes (rate-limited)
        market_quotes = fetch_market_quotes(instrument_keys)
        if not market_quotes:
            print(f"⚠️ No market quotes for {stock_symbol}")
            return None

        # 4. Process data
        result = {
            'options_orders': [],
            'futures_orders': [],
            'oi_records': []
        }

        # Process futures first
        futures_orders = process_large_futures_orders(market_quotes, stock_symbol, lot_size)
        if futures_orders:
            result['futures_orders'] = futures_orders
            print("futures_orders = ", futures_orders)

        # Process options and OI data
        for key, quote_data in market_quotes.items():
            symbol = quote_data.get('symbol', '')
            
            # Skip futures data we already processed
            if key == fut_instrument_key:
                continue

            # Determine option type
            if symbol.endswith("CE"):
                option_type = "CE"
            elif symbol.endswith("PE"):
                option_type = "PE"
            else:
                continue

            # Extract strike price
            strike_match = re.search(r'(\d+)(CE|PE)$', symbol)
            if not strike_match:
                continue
                
            strike_price = int(strike_match.group(1))

            # Check for large orders
            depth = quote_data.get('depth', {})
            top_bids = depth.get('buy', [])[:5]
            top_asks = depth.get('sell', [])[:5]
            ltp = quote_data.get('last_price', 0)
            
            threshold = lot_size * 87  # Your existing threshold
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
                    'timestamp': formatted_time
                })
                print("options_orders recorded")

            # Prepare OI volume data (for batch insert)
            result['oi_records'].append({
                'symbol': stock_symbol,
                'expiry': expiry_date,
                'strike': strike_price,
                'option_type': option_type,
                'oi': Decimal(str(quote_data.get('oi', 0))),
                'volume': Decimal(str(quote_data.get('volume', 0))),
                'price': Decimal(str(ltp)),
                'timestamp': display_time
            })

        print(f"📊 {stock_symbol}: {len(result['options_orders'])} orders, {len(result['oi_records'])} OI records")
        return result

    except Exception as e:
        print(f"❌ Error in fetch_option_chain for {stock_symbol}: {str(e)}")
        return None
        
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(str(obj))  # Convert Decimal to float
        return super().default(obj)

from psycopg2.extras import execute_batch  # Add this import at the top

# Replace your existing save functions with these optimized versions:

def save_options_data(symbol: str, orders: list):
    """BULK INSERT options orders (10x faster)"""
    if not orders:
        return
        
    with db_cursor() as cur:
        # Prepare all rows in one list
        data = [(order['stock'], order['strike_price'], order['type'],
                order['ltp'], order['bid_qty'], order['ask_qty'],
                order['lot_size'], order['timestamp']) for order in orders]
        
        # Use execute_batch for optimized bulk insert
        execute_batch(cur, """
            INSERT INTO options_orders 
            (symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, strike_price, option_type) DO NOTHING
        """, data, page_size=100)  # 100 rows per batch

def save_futures_data(symbol: str, orders: list):
    """BULK INSERT futures orders"""
    if not orders:
        return
        
    with db_cursor() as cur:
        data = [(order['stock'], order['ltp'], order['bid_qty'],
               order['ask_qty'], order['lot_size'], order['timestamp']) for order in orders]
        
        execute_batch(cur, """
            INSERT INTO futures_orders 
            (symbol, ltp, bid_qty, ask_qty, lot_size, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING
        """, data, page_size=100)

def save_oi_volume_batch(records: list):
    """BULK INSERT OI data (critical for performance)"""
    if not records:
        return
        
    with db_cursor() as cur:
        data = [(r['symbol'], r['expiry'], r['strike'], r['option_type'],
                r['oi'], r['volume'], r['price'], r['timestamp']) for r in records]
        
        execute_batch(cur, """
            INSERT INTO oi_volume_history 
            (symbol, expiry_date, strike_price, option_type, oi, volume, price, display_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO UPDATE SET oi=EXCLUDED.oi, volume=EXCLUDED.volume, price=EXCLUDED.price
        """, data, page_size=100)

def clear_old_data():
    """Delete previous day's data at market open"""
    with db_cursor() as cur:
        # Clear options data older than 1 day
        cur.execute("""
        DELETE FROM options_orders 
        WHERE timestamp < CURRENT_DATE
        """)

        # Clear futures data
        cur.execute("""
        DELETE FROM futures_orders 
        WHERE timestamp < CURRENT_DATE
        """)

        cur.execute("""
        DELETE FROM oi_volume 
        WHERE timestamp < CURRENT_DATE
        """)

        print("✅ Cleared previous day's data")

import ssl
import json
import os

import pandas as pd
import requests
import re
import pytz
from datetime import datetime

from boto3.dynamodb.types import TypeSerializer
from flask import Flask
from flask_cors import CORS
from decimal import Decimal
from file_utils import atomic_json_read, atomic_json_write
import time
import random

from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

# JSON file to store detected orders
PERSISTENT_STORAGE_PATH = "/persistent/data"

JSON_FILE = os.path.join(PERSISTENT_STORAGE_PATH, "large_orders.json")
FUTURES_JSON_FILE = os.path.join(PERSISTENT_STORAGE_PATH, "futures_large_orders.json")
OI_VOLUME_JSON_FILE =  os.path.join(PERSISTENT_STORAGE_PATH, "oi_volume_data.json")

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
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()

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

# Define the rate limit (e.g., 10 requests per second)
ONE_SECOND = 1
MAX_CALLS_PER_SECOND = 5

def fetch_market_quotes(instrument_keys):
    """Fetch market quotes with rate limiting and retry logic."""
    try:
        url = 'https://api.upstox.com/v2/market-quote/quotes'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': ','.join(instrument_keys)}

        response = requests.get(url, headers=headers, params=params)
    except requests.RequestException as e:
        print(f"❌ Request failed: {e}")
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
        ist_now = datetime.utcnow().astimezone(pytz.timezone('Asia/Kolkata')).strftime("%H:%M:%S")

        large_orders.append({
            'stock': stock_symbol,
            'ltp': ltp,
            'bid_qty': max((b['quantity'] for b in top_bids), default=0),
            'ask_qty': max((a['quantity'] for a in top_asks), default=0),
            'lot_size': lot_size,
            'timestamp': ist_now
        })
        print("Future large_orders - ", large_orders)
    return large_orders

def store_large_options_orders(large_orders_options):
    """Load existing options orders, avoid duplicates, and store new ones."""
    if not large_orders_options:
        print("ℹ️ No new large options orders detected. Skipping storage.")
        return  # Skip if no new orders

    try:
        # Step 1: Load Existing Data
        existing_orders = atomic_json_read(JSON_FILE) or []
        existing_stocks = {o['stock'] for o in existing_orders}

        # Step 2: Append Only New Unique Orders
        new_orders = [o for o in large_orders_options
                      if o['stock'] not in existing_stocks]

        if new_orders:
            atomic_json_write(JSON_FILE, existing_orders + new_orders)
            print(f"✅ Stored {len(new_orders)} new options orders")

    except Exception as e:
        print(f"❌ Error storing options: {e}")


def store_large_futures_orders(large_orders_futures):
    """Thread-safe storage of futures orders with atomic file operations"""
    if not large_orders_futures:
        return

    try:
        # Load existing orders atomically
        existing_orders = atomic_json_read(FUTURES_JSON_FILE) or []
        existing_stocks = {o['stock'] for o in existing_orders}

        # Filter only new unique orders
        new_orders = [
            order for order in large_orders_futures
            if order['stock'] not in existing_stocks
        ]

        if new_orders:
            # Store combined data atomically
            atomic_json_write(FUTURES_JSON_FILE, existing_orders + new_orders)
            print(f"✅ Stored {len(new_orders)} new futures orders (Total: {len(existing_orders) + len(new_orders)})")
        else:
            print("ℹ️ No new unique futures orders to store")

    except Exception as e:
        print(f"❌ Error storing futures orders: {e}")

def fetch_futures_orders(stock_symbol, expiry_date, lot_size, table):
    """Fetch and process large futures orders."""
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
            store_large_futures_orders(large_orders_futures)

        return large_orders_futures

    except Exception as e:
        print(f"❌ Error processing futures orders for {stock_symbol}: {e}")
        return None

serializer = TypeSerializer()

def convert_to_decimal(data):
    """Convert floats to Decimals in a dictionary."""
    if isinstance(data, float):
        return Decimal(str(data))
    elif isinstance(data, dict):
        return {k: convert_to_decimal(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_to_decimal(item) for item in data]
    return data


def fetch_option_chain(stock_symbol, expiry_date, lot_size, table):
    """Fetch and process large options orders."""
    if stock_symbol in excluded_stocks:
        return None

    # Get the options instrument key
    instrument_key = getInstrumentKey(stock_symbol)
    if not instrument_key:
        print(f"⚠️ No instrument key for {stock_symbol}")
        return None

    try:
        # Fetch the option chain data
        url = 'https://api.upstox.com/v2/option/chain'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            return None

        data = response.json().get('data', [])
        if not data:
            return None

        # Process the option chain data
        spot_price = data[0].get('underlying_spot_price', 0)
        strikes = sorted(set(option['strike_price'] for option in data))
        closest_strikes = [s for s in strikes if s <= spot_price][-3:] + [s for s in strikes if s >= spot_price][:3]

        # Fetch market quotes for the closest strikes
        instrument_keys = []
        for option in data:
            if option['strike_price'] in closest_strikes:
                if option['call_options'].get('instrument_key'):
                    instrument_keys.append(option['call_options']['instrument_key'])
                if option['put_options'].get('instrument_key'):
                    instrument_keys.append(option['put_options']['instrument_key'])

        market_quotes = fetch_market_quotes(instrument_keys)

        # Process large options orders
        large_orders = []
        batch_items = []  # Store items for batch write

        for key, data in market_quotes.items():
            symbol = data.get('symbol', '')
            option_type = "CE" if symbol.endswith("CE") else "PE" if symbol.endswith("PE") else None
            if not option_type:
                continue

            strike_price_match = re.search(r'(\d+)$', symbol[:-2])
            if not strike_price_match:
                continue

            strike_price = int(strike_price_match.group(1))
            depth_data = data.get('depth', {})
            top_bids = depth_data.get('buy', [])[:5]
            top_asks = depth_data.get('sell', [])[:5]

            threshold = lot_size * 87
            ltp = data.get('last_price', 0)
            valid_bid = any(bid['quantity'] >= threshold for bid in top_bids)
            valid_ask = any(ask['quantity'] >= threshold for ask in top_asks)

            if (valid_bid or valid_ask) and ltp > 2:
                utc_now = datetime.utcnow()
                ist = pytz.timezone('Asia/Kolkata')
                ist_now = utc_now.astimezone(ist)
                formatted_time = ist_now.strftime("%H:%M:%S")

                large_orders.append({
                    'stock': stock_symbol,
                    'strike_price': strike_price,
                    'type': option_type,
                    'ltp': data.get('last_price'),
                    'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                    'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                    'lot_size': lot_size,
                    'timestamp': formatted_time
                })
                print("Option large_orders - ", large_orders)
                store_large_options_orders(large_orders)

                # Prepare data for DynamoDB batch write
        oi = Decimal(str(data.get('oi', 0)))  # Convert float to Decimal
        volume = Decimal(str(data.get('volume', 0)))  # Convert float to Decimal
        price = Decimal(str(data.get('last_price', 0)))  # Convert float to Decimal
        oi_volume_key = f"oi_volume_data:{stock_symbol}:{expiry_date}:{strike_price}:{option_type}"
        timestamp = datetime.now().strftime("%H:%M")
        new_entry = {"time": timestamp, "oi": oi, "volume": volume, "price": price}

        # Fetch existing data (if available)
        existing_data = load_oi_volume_data(oi_volume_key)
        existing_data.append(new_entry)

        # Save updated data
        save_oi_volume_data(oi_volume_key, existing_data)

        return large_orders

    except Exception as e:
        print(f"❌ Error processing options orders for {stock_symbol}: {e}")
        return None


def save_oi_volume_data(data_key, data):
    """Save OI volume data to JSON file"""
    try:
        existing_data = atomic_json_read(OI_VOLUME_JSON_FILE) or {}
        existing_data[data_key] = data
        atomic_json_write(OI_VOLUME_JSON_FILE, existing_data)
    except Exception as e:
        print(f"Error saving OI data: {e}")

def load_oi_volume_data(data_key):
    """Load specific OI volume data from JSON file"""
    try:
        if os.path.exists(OI_VOLUME_JSON_FILE):
            with open(OI_VOLUME_JSON_FILE, 'r') as f:
                data = json.load(f)
                return data.get(data_key, [])
        return []
    except Exception as e:
        print(f"Error loading OI volume data: {e}")
        return []

def clear_old_data_files():
    """Delete all data files at market open"""
    try:
        files_to_clear = [
            JSON_FILE,           # options large orders
            FUTURES_JSON_FILE,   # futures large orders
            OI_VOLUME_JSON_FILE  # OI volume data
        ]

        for filepath in files_to_clear:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"✅ Cleared old data: {os.path.basename(filepath)}")

    except Exception as e:
        print(f"❌ Error clearing old data: {e}")

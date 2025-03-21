import ssl
import json
import os
import time
from decimal import Decimal

import pandas as pd
import redis
import requests
import re
import pytz
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

# JSON file to store detected orders
JSON_FILE = "/tmp/large_orders.json"
FUTURES_JSON_FILE = "/tmp/futures_large_orders.json"

#REDIS_HOST = os.getenv("REDIS_HOST")
#REDIS_PORT = os.getenv("REDIS_PORT")
#REDIS_USER = os.getenv("REDIS_USER")
#REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Construct the Redis URL dynamically
#REDIS_URL = f"redis://{REDIS_USER}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"

#redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

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
    try:
        # ✅ Filter for FUTURES contracts only
        key = scripts[(scripts['asset_symbol'] == symbol) & (scripts['instrument_type'] == "FUT")]['instrument_key'].values
        return key[0] if len(key) > 0 else None
    except Exception as e:
        print(f"⚠️ Error getting futures instrument key for {symbol}: {e}")
        return None
def fetch_market_quotes(instrument_keys):
    try:
        url = 'https://api.upstox.com/v2/market-quote/quotes'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': ','.join(instrument_keys)}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json().get('data', {})
        else:
            print(f"❌ API error ({response.status_code}): {response.text}")
            return {}

    except requests.RequestException as e:
        print(f"❌ Request failed: {e}")
        return {}

def process_large_futures_orders(market_quotes, stock_symbol, lot_size, fut_instrument_key):
    """Detect large futures orders from market_quotes data"""
    large_orders = []

    if fut_instrument_key not in market_quotes:
        return large_orders  # ✅ No futures data found, return empty list

    futures_data = market_quotes[fut_instrument_key]
    depth_data = futures_data.get('depth', {})
    top_bids = depth_data.get('buy', [])[:5]
    top_asks = depth_data.get('sell', [])[:5]

    threshold = lot_size * 14
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

    return large_orders

def store_large_futures_orders(large_orders_futures):
    """Load existing futures orders from JSON, append new ones, and store back"""
    if not large_orders_futures:
        print("ℹ️ No new large futures orders detected. Skipping storage.")
        return  # ✅ If no new large orders, do nothing

    try:
        # ✅ Step 1: Load Existing Data
        existing_orders = []
        if os.path.exists(FUTURES_JSON_FILE):
            with open(FUTURES_JSON_FILE, "r") as file:
                existing_orders = json.load(file)

        # ✅ Step 2: Append New Large Orders
        existing_orders.extend(large_orders_futures)

        # ✅ Step 3: Save Updated Data
        with open(FUTURES_JSON_FILE, "w") as file:
            json.dump(existing_orders, file, indent=4)

        print(f"✅ Stored {len(large_orders_futures)} new futures orders. Total: {len(existing_orders)}")

    except Exception as e:
        print(f"❌ Error storing futures orders: {e}")

def fetch_option_chain(stock_symbol, expiry_date, lot_size, table):
    if stock_symbol in excluded_stocks:
        return None
    instrument_key = getInstrumentKey(stock_symbol)
    fut_instrument_key = getFuturesInstrumentKey(stock_symbol)

    if not instrument_key:
        print(f"⚠️ No instrument key for {stock_symbol}")
        return None

    if not fut_instrument_key:
        print(f"⚠️ No Fut instrument key key for {stock_symbol}")
        return None

    try:
        url = 'https://api.upstox.com/v2/option/chain'
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            return None

        data = response.json().get('data', [])
        if not data:
            return None

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

        instrument_keys.append(fut_instrument_key)
        market_quotes = fetch_market_quotes(instrument_keys)

        large_orders_futures = process_large_futures_orders(market_quotes, stock_symbol, lot_size, getFuturesInstrumentKeyMarket(stock_symbol, expiry_date))

        if large_orders_futures:
            store_large_futures_orders(large_orders_futures)

        large_orders = []

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
                utc_now = datetime.utcnow()  # Get current UTC time
                ist = pytz.timezone('Asia/Kolkata')  # Define IST timezone
                ist_now = utc_now.astimezone(ist)  # Convert UTC to IST
                formatted_time = ist_now.strftime("%H:%M:%S")  # Format to HH:MM:SS

                large_orders.append({
                    'stock': stock_symbol,
                    'strike_price': strike_price,
                    'type': option_type,
                    'ltp': data.get('last_price'),
                    'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                    'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                    'lot_size' : lot_size,
                    'timestamp' : formatted_time
                })
                print("large_orders - ", large_orders)

            #oi = Decimal(str(data.get('oi', 0)))  # Convert float to Decimal
            #volume = Decimal(str(data.get('volume', 0)))  # Convert float to Decimal
            #price = Decimal(str(data.get('last_price', 0)))  # Convert float to Decimal
            #oi_volume_key = f"oi_volume_data:{stock_symbol}:{expiry_date}:{strike_price}:{option_type}"  # Changed key
            # Create timestamped data entry
            #timestamp = datetime.now().strftime("%H:%M")  # Store only time for intraday tracking
            #new_entry = {"time": timestamp, "oi": oi, "volume": volume, "price": price}

            # Fetch existing data (if available)
            #response = table.get_item(Key={"oi_volume_key": oi_volume_key})
            #existing_data = response.get("Item", {}).get("data", [])
            #existing_data.append(new_entry)

            #def convert_floats(obj):
            #    if isinstance(obj, float):
            #        return Decimal(str(obj))  # Convert float to Decimal
            #    elif isinstance(obj, list):
            #        return [convert_floats(i) for i in obj]  # Convert list elements
            #    elif isinstance(obj, dict):
            #        return {k: convert_floats(v) for k, v in obj.items()}  # Convert dict elements
            #    return obj

            #existing_data = convert_floats(existing_data)  # Ensure all numbers are Decimal

            # Store updated list in DynamoDB
            #table.put_item(Item={"oi_volume_key": oi_volume_key, "data": existing_data})

        return large_orders
    except requests.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None
        
def getFuturesInstrumentKeyMarket(symbol, expiry_date):
    """Convert expiry date format and generate futures instrument key."""
    try:
        # ✅ Convert expiry_date "2025-03-27" → "27MAR"
        expiry_str = datetime.strptime(expiry_date, "%Y-%m-%d").strftime("%d%b").upper()

        # ✅ Construct the futures instrument key
        futures_key = f"NSE_FO:{symbol}{expiry_str}FUT"

        return futures_key
    except Exception as e:
        print(f"⚠️ Error getting futures instrument key for {symbol}: {e}")
        return None
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

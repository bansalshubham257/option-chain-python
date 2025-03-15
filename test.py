import ssl
import json
import os
import pandas as pd
import requests
import re
import pytz
from datetime import datetime
from flask import Flask, jsonify
from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

ssl._create_default_https_context = ssl._create_unverified_context

# JSON file to store detected orders
JSON_FILE = "/tmp/large_orders.json"

# Load all instruments
url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
scripts = pd.read_json('https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz')

df = pd.read_csv(url, compression='gzip')
fno_stocks_raw = df[df['exchange'] == 'NSE_FO'][['tradingsymbol', 'lot_size']].dropna()

excluded_stocks = {"NIFTYNXT", "NIFTY", "FINNIFTY", "YESBANK", "PNB", "IDEA", "IDFCFIRSTB", "TATASTEEL"}

fno_stocks = {re.split(r'\d', row['tradingsymbol'], 1)[0]: int(row['lot_size'])
              for _, row in fno_stocks_raw.iterrows() if row['tradingsymbol'] not in excluded_stocks}

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("18:30", "%H:%M").time()

def is_market_open():
    now = datetime.now(IST)
    return now.weekday() < 6 and MARKET_OPEN <= now.time() <= MARKET_CLOSE

def getInstrumentKey(symbol):
    key = scripts[scripts['trading_symbol'] == symbol]['instrument_key'].values
    return key[0] if len(key) > 0 else None

def fetch_market_quotes(instrument_keys):
    url = 'https://api.upstox.com/v2/market-quote/quotes'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    params = {'instrument_key': ','.join(instrument_keys)}
    response = requests.get(url, headers=headers, params=params)
    return response.json().get('data', {}) if response.status_code == 200 else {}

def fetch_option_chain(stock_symbol, expiry_date, lot_size):
    if stock_symbol in excluded_stocks:
        return None
    instrument_key = getInstrumentKey(stock_symbol)
    if not instrument_key:
        return None

    url = 'https://api.upstox.com/v2/option/chain'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    params = {'instrument_key': instrument_key, 'expiry_date': expiry_date}

    import requests

def fetch_option_chain(stock_symbol, expiry_date, lot_size):
    instrument_key = getInstrumentKey(stock_symbol)
    if not instrument_key:
        print(f"⚠️ No instrument key for {stock_symbol}")
        return None

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

    market_quotes = fetch_market_quotes(instrument_keys)
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

        threshold = lot_size * 80
        valid_bid = any(bid['quantity'] >= threshold for bid in top_bids)
        valid_ask = any(ask['quantity'] >= threshold for ask in top_asks)
         
        if valid_bid or valid_ask:
            timestamp = datetime.now().strftime("%H:%M:%S") # Add detection time
            large_orders.append({
                'stock': stock_symbol,
                'strike_price': strike_price,
                'type': option_type,
                'ltp': data.get('last_price'),
                'bid_qty': max((b['quantity'] for b in top_bids), default=0),
                'ask_qty': max((a['quantity'] for a in top_asks), default=0),
                'lot_size' : lot_size,
                'timestamp' : timestamp  
            })

    return large_orders

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

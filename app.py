import os
from flask import Flask, request, jsonify,session, request
from flask_cors import CORS
import ssl
import json
import threading
import time
from datetime import datetime, timedelta
import pytz
import requests
from flask_socketio import SocketIO, emit
from threading import Thread
import yfinance as yf
import uuid

# Add these at the top of your files:
import concurrent.futures  # For ThreadPoolExecutor
from psycopg2.extras import execute_batch  # For batch database inserts
from decimal import Decimal  # If not already imported

from utils import fetch_all_nse_stocks, analyze_stock

from test import (is_market_open, fno_stocks, clear_old_data, fetch_option_chain,
                  db_cursor,save_options_data, save_futures_data, save_oi_volume_batch )

import gzip
import pandas as pd
from io import BytesIO
import re
from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

EXPIRY_DATE = "2025-04-24"
MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()
BASE_URL = "https://assets.upstox.com/market-quote/instruments/exchange"
UPSTOX_API_KEY = ACCESS_TOKEN
UPSTOX_BASE_URL = "https://api.upstox.com"

# 📌 API Routes
@app.route("/stocks", methods=["GET"])
def get_all_stocks():
    return jsonify(fetch_all_nse_stocks())

@app.route("/analyze", methods=["GET"])
def analyze():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "Stock symbol is required"}), 400
    return jsonify(analyze_stock(symbol))

@app.route('/get-orders', methods=['GET'])
def get_orders():
    if not is_market_open():
        return jsonify({'status': 'Market is closed'})
    try:
        with db_cursor() as cur:
            cur.execute("""
            SELECT symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp
            FROM options_orders
            """)
            results = cur.fetchall()
            orders = [{
                'stock': r[0],
                'strike_price': r[1],
                'type': r[2],
                'ltp': r[3],
                'bid_qty': r[4],
                'ask_qty': r[5],
                'lot_size': r[6],
                'timestamp': r[7].isoformat()
            } for r in results]
            return jsonify(orders)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/get-futures-orders', methods=['GET'])
def get_futures_orders():
    if not is_market_open():
        return jsonify({'status': 'Market is closed'})
    try:
        with db_cursor() as cur:
            cur.execute("""
            SELECT symbol, ltp, bid_qty, ask_qty, lot_size, timestamp
            FROM futures_orders
            """)
            results = cur.fetchall()
            orders = [{
                'stock': r[0],
                'ltp': r[1],
                'bid_qty': r[2],
                'ask_qty': r[3],
                'lot_size': r[4],
                'timestamp': r[5].isoformat()
            } for r in results]
            return jsonify(orders)
    except Exception as e:
        return jsonify({"error": str(e)})

IST = pytz.timezone("Asia/Kolkata")


def is_market_closed():
    """ Check if the market is closed """
    now = datetime.now(IST).time()
    return now >= MARKET_CLOSE

def fetch_and_store_orders():
    """Optimized version with parallel DB writes and API rate control"""
    if is_market_closed():
        print("⏸️ Market is closed")
        return

    total_start = time.time()
    processed_stocks = 0
    db_time_total = 0
    api_time_total = 0

    # Rate limiting control (200 requests/minute max)
    MIN_REQUEST_INTERVAL = 0.3  # 60/200 = 0.3s between requests
    last_api_call = 0

    for stock, lot_size in fno_stocks.items():
        try:
            # Rate limiting
            elapsed = time.time() - last_api_call
            if elapsed < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - elapsed)

            print(f"\n🔍 Processing {stock}...")
            api_start = time.time()
            last_api_call = time.time()  # Update immediately after sleep

            # Get data (single-threaded for API safety)
            result = fetch_option_chain(stock, EXPIRY_DATE, lot_size)
            api_time = time.time() - api_start
            api_time_total += api_time

            if not result:
                continue

            # Parallel database writes
            db_start = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # Submit all DB operations in parallel
                futures = [
                    executor.submit(save_options_data, stock, result['options_orders']),
                    executor.submit(save_futures_data, stock, result['futures_orders']),
                    executor.submit(save_oi_volume_batch, result['oi_records'])
                ]
                concurrent.futures.wait(futures)
                
                # Check for exceptions
                for future in futures:
                    if future.exception():
                        raise future.exception()

            db_time = time.time() - db_start
            db_time_total += db_time
            processed_stocks += 1

            print(f"✅ Saved {stock} | API: {api_time:.1f}s | DB: {db_time:.1f}s")

        except Exception as e:
            print(f"❌ Failed {stock}: {type(e).__name__} - {str(e)}")
            continue

    # Performance summary
    total_time = time.time() - total_start
    print(f"\n🏁 Completed {processed_stocks}/{len(fno_stocks)} stocks")
    print(f"⏱️  Total: {total_time:.1f}s | API: {api_time_total:.1f}s ({api_time_total/processed_stocks:.1f}s/stock) | DB: {db_time_total:.1f}s ({db_time_total/processed_stocks:.1f}s/stock)")
    print(f"🚀 DB speedup: {(db_time_total/processed_stocks):.1f}s/stock (vs ~4s before)")

def run_script():
    last_clear_date = None

    while True:
        now = datetime.now(IST)

        # Check if we need to clear old data (once per day at market open)
        if (now.weekday() < 5 and
                MARKET_OPEN <= now.time() <= MARKET_CLOSE and
                (last_clear_date is None or last_clear_date != now.date())):

            try:
                clear_old_data()
                last_clear_date = now.date()
                print("🔄 Cleared all previous day's data")
            except Exception as e:
                print(f"⚠️ Failed to clear old data: {e}")

        # Normal processing during market hours
        if now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE:
            try:
                fetch_and_store_orders()
                time.sleep(30)  # Run every 30 seconds
            except Exception as e:
                print(f"⚠️ Script error: {e}")
                time.sleep(60)
        else:
            time.sleep(300)  # 5 min sleep outside market hours

@app.route('/get_fno_stocks', methods=['GET'])
def get_fno_stocks():
    """API to return the list of F&O stocks"""
    return jsonify(fno_stocks)

@app.route('/get_fno_data', methods=['GET'])
def get_fno_data():
    try:
        stock = request.args.get('stock')
        expiry = request.args.get('expiry')
        strike = request.args.get('strike')
        option_type = request.args.get('option_type')

        with db_cursor() as cur:
            cur.execute("""
            SELECT display_time, oi, volume, price, strike_price, option_type
            FROM oi_volume_history
            WHERE symbol = %s AND expiry_date = %s
              AND strike_price = %s AND option_type = %s
            ORDER BY display_time
            """, (stock, expiry, strike, option_type))

            data = [{
                'time': r[0],
                'oi': float(r[1]) if r[1] else 0,
                'volume': float(r[2]) if r[2] else 0,
                'price': float(r[3]) if r[3] else 0,
                'strike': str(r[4]),  # Ensure strike price is returned
                'optionType': r[5]  # Ensure option type (CE/PE) is included
            } for r in cur.fetchall()]

            return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_option_data', methods=['GET'])
def get_option_data():
    stock = request.args.get('stock')
    if not stock:
        return jsonify({"error": "Stock symbol is required"}), 400

    try:
        with db_cursor() as cur:
            # Get all unique expiries for the stock
            cur.execute("""
            SELECT DISTINCT expiry_date 
            FROM oi_volume_history
            WHERE symbol = %s
            ORDER BY expiry_date
            """, (stock,))
            expiries = [r[0].strftime('%Y-%m-%d') for r in cur.fetchall()]

            # Get all strike prices for the stock
            cur.execute("""
            SELECT DISTINCT strike_price
            FROM oi_volume_history
            WHERE symbol = %s
            ORDER BY strike_price
            """, (stock,))
            strikes = [float(r[0]) for r in cur.fetchall()]

            # Get all option chains data
            cur.execute("""
            SELECT 
                expiry_date,
                strike_price,
                option_type,
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

            return jsonify({
                "expiries": expiries,
                "strikes": strikes,
                "data": option_data
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/fii-dii', methods=['GET'])
def get_fii_dii_data():
    year_month = request.args.get('year_month')  # e.g., "2025-03"
    request_type = request.args.get('request_type')
    if not year_month:
        return jsonify({"error": "year_month parameter is required"}), 400

    # Fetch FII/DII data from the external API
    api_url = f"https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data?request_type={request_type}&year_month={year_month}"
    response = requests.get(api_url)

    if response.status_code == 200:
        return jsonify(response.json())
    else:
        return jsonify({"error": "Failed to fetch data from the external API"}), 500

symbol_to_instrument = {}

def fetch_csv_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
            return pd.read_csv(f)
    return None

def parse_option_symbol(symbol):
    match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)", symbol)
    if match:
        return {
            "stock": match.group(1),
            "expiry": match.group(2),
            "strike_price": int(match.group(3)),
            "option_type": match.group(4)
        }
    return None

@app.route("/fetch_stocks", methods=["GET"])
def get_nse_bse_stocks():
    global symbol_to_instrument
    df = fetch_csv_data(f"{BASE_URL}/complete.csv.gz")
    if df is not None:
        df.columns = df.columns.str.lower()
        if "exchange" in df.columns and "tradingsymbol" in df.columns and "lot_size" in df.columns and "instrument_key" in df.columns:
            # Get NSE F&O stocks
            nse_fno_stocks_raw = df[df['exchange'] == 'NSE_FO'][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()
            # Get BSE F&O stocks (only for SENSEX and BANKEX)
            bse_fno_stocks_raw = df[(df['exchange'] == 'BSE_FO') &
                                    (df['tradingsymbol'].str.contains('SENSEX|BANKEX', regex=True))][['tradingsymbol', 'lot_size', 'instrument_key']].dropna()

            # Combine both NSE and BSE F&O data
            fno_stocks_raw = pd.concat([nse_fno_stocks_raw, bse_fno_stocks_raw])

            stock_data = {}
            symbol_to_instrument = {}

            for _, row in fno_stocks_raw.iterrows():
                parsed = parse_option_symbol(row['tradingsymbol'])
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
                    symbol_to_instrument[row['tradingsymbol']] = row['instrument_key']

            # Fetch and store stock instrument keys for NSE equities
            fno_stocks_list = df[df['exchange'] == 'NSE_EQ'][['tradingsymbol', 'instrument_key']].dropna()
            for _, row in fno_stocks_list.iterrows():
                stock_name = row['tradingsymbol']
                symbol_to_instrument[stock_name] = row['instrument_key']
                if stock_name in stock_data:
                    stock_data[stock_name]["instrument_key"] = row['instrument_key']

            index_instrument_keys = {
                "NIFTY": "NSE_INDEX|Nifty 50",
                "BANKNIFTY": "NSE_INDEX|Nifty Bank",
                "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
                "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
                "SENSEX": "BSE_INDEX|SENSEX",
                "BANKEX": "BSE_INDEX|BANKEX"
            }
            for index, key in index_instrument_keys.items():
                if index in stock_data:
                    stock_data[index]["instrument_key"] = key
                    symbol_to_instrument[index] = key  # Add to mapping as well

            for stock in stock_data:
                stock_data[stock]["expiries"] = sorted(stock_data[stock]["expiries"])

            return jsonify(stock_data)
        else:
            return jsonify({"error": "Required columns not found in the dataset"})
    return jsonify({"error": "Failed to fetch stocks"})

@app.route("/fetch_price", methods=["GET"])
def get_price():
    instrument_keys = []
    instrument_key = request.args.get("instrument_key")

    if not instrument_key:
        return jsonify({"error": "Instrument key is required"})

    trading_symbol = next((sym for sym, key in symbol_to_instrument.items() if key == instrument_key), None)
    if not trading_symbol:
        return jsonify({"error": "Trading symbol not found for the given instrument key"})

    parsed_data = parse_option_symbol(trading_symbol)
    if not parsed_data:
        return jsonify({"error": "Failed to parse option symbol"})

    stock_name = parsed_data["stock"]
    stock_instrument_key = symbol_to_instrument.get(stock_name)
    instrument_keys.append(instrument_key)
    instrument_keys.append(stock_instrument_key)

    url = f"{UPSTOX_BASE_URL}/v2/market-quote/quotes"
    headers = {"Authorization": f"Bearer {UPSTOX_API_KEY}"}
    params = {'instrument_key': ','.join(instrument_keys)}

    response = requests.get(url, headers=headers, params=params)
    data = response.json().get("data", {})

    # Determine exchange prefix based on instrument key
    if "BSE_FO" in instrument_key:
        option_key = f"BSE_FO:{trading_symbol}"
    else:
        option_key = f"NSE_FO:{trading_symbol}"

    option_price = data.get(option_key, {}).get("last_price", "N/A")

    # Handle both NSE and BSE indices and equities
    index_symbols = ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"]
    if stock_name in index_symbols:
        stock_price = data.get(stock_instrument_key.replace('|', ':'), {}).get("last_price", "N/A")
    else:
        # Check if it's a BSE equity (you might need to adjust this based on your data)
        if "BSE_EQ" in stock_instrument_key:
            stock_price = data.get(f"BSE_EQ:{stock_name}", {}).get("last_price", "N/A")
        else:
            stock_price = data.get(f"NSE_EQ:{stock_name}", {}).get("last_price", "N/A")

    return jsonify({
        "option_price": option_price,
        "stock_price": stock_price
    })

@app.route("/fetch_bulk_prices", methods=["GET"])
def get_bulk_prices():
    instrument_keys = request.args.get("instrument_keys", "").split(',')
    if not instrument_keys:
        return jsonify({"error": "Instrument keys are required"})

    # Get all instrument keys including underlying stocks
    all_keys = []
    stock_keys = set()

    for instrument_key in instrument_keys:
        trading_symbol = next((sym for sym, key in symbol_to_instrument.items() if key == instrument_key), None)
        if trading_symbol:
            parsed_data = parse_option_symbol(trading_symbol)
            if parsed_data:
                stock_name = parsed_data["stock"]
                stock_key = symbol_to_instrument.get(stock_name)
                if stock_key:
                    stock_keys.add(stock_key)

    # Prepare the API request
    all_keys.extend(instrument_keys)
    all_keys.extend(stock_keys)

    url = f"{UPSTOX_BASE_URL}/v2/market-quote/quotes"
    headers = {"Authorization": f"Bearer {UPSTOX_API_KEY}"}
    params = {'instrument_key': ','.join(all_keys)}

    try:
        response = requests.get(url, headers=headers, params=params)
        data = response.json().get("data", {})

        # Process the response
        result = {}
        index_symbols = ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"]

        for instrument_key in instrument_keys:
            trading_symbol = next((sym for sym, key in symbol_to_instrument.items() if key == instrument_key), None)
            if trading_symbol:
                parsed_data = parse_option_symbol(trading_symbol)
                if parsed_data:
                    stock_name = parsed_data["stock"]

                    # Get option price - determine exchange
                    if "BSE_FO" in instrument_key:
                        option_key = f"BSE_FO:{trading_symbol}"
                    else:
                        option_key = f"NSE_FO:{trading_symbol}"

                    option_data = data.get(option_key, {})
                    option_price = option_data.get("last_price", 0)

                    # Get stock price
                    if stock_name in index_symbols:
                        stock_key = symbol_to_instrument.get(stock_name).replace('|', ':')
                        stock_price = data.get(stock_key, {}).get("last_price", 0)
                    else:
                        # Check if it's a BSE equity
                        if "BSE_EQ" in symbol_to_instrument.get(stock_name, ""):
                            stock_price = data.get(f"BSE_EQ:{stock_name}", {}).get("last_price", 0)
                        else:
                            stock_price = data.get(f"NSE_EQ:{stock_name}", {}).get("last_price", 0)

                    result[instrument_key] = {
                        "option_price": float(option_price) if option_price else 0,
                        "stock_price": float(stock_price) if stock_price else 0
                    }

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})




# Run Flask
if __name__ == "__main__":
    print(f"🛠️ Starting with BACKGROUND_WORKER={os.getenv('BACKGROUND_WORKER')}")

    # Clear old data on worker startup if needed
    if os.getenv('BACKGROUND_WORKER', 'false').lower() == 'true':
        now = datetime.now(IST)
        if now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE:
            try:
                from test import clear_old_data_files
                clear_old_data_files()
            except Exception as e:
                print(f"⚠️ Startup cleanup failed: {e}")

        print("🔵 Starting background worker ONLY")
        run_script()
    else:
        print("🌍 Starting web service ONLY")
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port)

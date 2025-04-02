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

import math
from ta.momentum import RSIIndicator, StochasticOscillator, TSIIndicator
from ta.trend import MACD, EMAIndicator, SMAIndicator, WMAIndicator, ADXIndicator, IchimokuIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import VolumeWeightedAveragePrice

import numpy as np

from utils import fetch_all_nse_stocks, analyze_stock

from test import (is_market_open, fno_stocks, clear_old_data, fetch_option_chain,
                  db_cursor,save_options_data, save_futures_data, save_oi_volume_batch )

import gzip
import pandas as pd
from io import BytesIO
import re
from config import ACCESS_TOKEN  # Store securely

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com", "https://aitradinglab.blogspot.com", "https://www.aitradinglab.in", "https://bansalshubham257.github.io"]}})
ssl._create_default_https_context = ssl._create_unverified_context

EXPIRY_DATE = "2025-04-24"
MARKET_OPEN = datetime.strptime("09:15", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()
BASE_URL = "https://assets.upstox.com/market-quote/instruments/exchange"
UPSTOX_API_KEY = ACCESS_TOKEN
UPSTOX_BASE_URL = "https://api.upstox.com"
symbol_to_instrument = {}

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
def get_upstox_fno_stocks():
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

INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
    {"name": "FinNifty", "symbol": "^NIFTY_FIN_SERVICE.NS", "color": "#2ca02c"},
    {"name": "Midcap Nifty", "symbol": "^NSEMDCP50", "color": "#d62728"},
    {"name": "Sensex", "symbol": "^BSESN", "color": "#9467bd"}
]

NIFTY_50_STOCKS = [
    'RELIANCE.NS', 'HDFCBANK.NS', 'TCS.NS', 'BHARTIARTL.NS', 'ICICIBANK.NS', 'SBIN.NS','INFY.NS', 'BAJFINANCE.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'LT.NS', 'HCLTECH.NS', 'KOTAKBANK.NS', 'SUNPHARMA.NS', 'MARUTI.NS', 'NTPC.NS',
    'AXISBANK.NS', 'POWERGRID.NS', 'ONGC.NS', 'TITAN.NS', 'WIPRO.NS', 'BAJAJFINSV.NS', 'M&M.NS', 'ULTRACEMCO.NS',
    'ADANIENT.NS', 'JSWSTEEL.NS', 'ADANIPORTS.NS', 'TATAMOTORS.NS', 'COALINDIA.NS', 'ASIANPAINT.NS', 'BEL.NS', 'BAJAJ-AUTO.NS',
    'NESTLEIND.NS', 'ZOMATO.NS', 'TATASTEEL.NS', 'TRENT.NS', 'GRASIM.NS', 'SBILIFE.NS', 'HINDALCO.NS', 'HDFCLIFE.NS',
    'EICHERMOT.NS', 'JIOFIN.NS', 'TECHM.NS', 'DRREDDY.NS', 'APOLLOHOSP.NS', 'CIPLA.NS', 'TATACONSUM.NS',
    'HEROMOTOCO.NS', 'INDUSINDBK.NS'
]

@app.route('/api/global-market-data', methods=['GET'])
def get_global_market_data():
    try:
        # Get all data
        indices = get_global_indices()
        crypto = get_top_crypto()
        commodities = get_commodities()

        # Combine all data
        response = {
            'timestamp': datetime.now().isoformat(),
            'indices': indices,
            'cryptocurrencies': crypto,
            'commodities': commodities
        }

        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_global_indices():
    """Fetch top global stock indices using yfinance"""
    indices = {
        '^GSPC': 'S&P 500',
        '^DJI': 'Dow Jones',
        '^IXIC': 'NASDAQ',
        '^FTSE': 'FTSE 100',
        '^N225': 'Nikkei 225',
        '^HSI': 'Hang Seng',
        '^GDAXI': 'DAX',
        '^FCHI': 'CAC 40'
    }

    results = []
    for symbol, name in indices.items():
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d')

            if not data.empty:
                last_close = data['Close'].iloc[-1]
                prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                change = last_close - prev_close
                percent_change = (change / prev_close) * 100

                results.append({
                    'symbol': symbol,
                    'name': name,
                    'price': round(last_close, 2),
                    'change': round(change, 2),
                    'percent_change': round(percent_change, 2)
                })
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")

    # Sort by absolute percent change (most movement first)
    results.sort(key=lambda x: abs(x['percent_change']), reverse=True)
    return results

def get_top_crypto(limit=5):
    """Get top cryptocurrencies using yfinance"""
    cryptos = {
        'BTC-USD': 'Bitcoin',
        'ETH-USD': 'Ethereum',
        'BNB-USD': 'Binance Coin',
        'SOL-USD': 'Solana',
        'XRP-USD': 'XRP',
        'ADA-USD': 'Cardano',
        'DOGE-USD': 'Dogecoin',
        'DOT-USD': 'Polkadot',
        'SHIB-USD': 'Shiba Inu',
        'AVAX-USD': 'Avalanche'
    }

    results = []
    for symbol, name in list(cryptos.items())[:limit]:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d')

            if not data.empty:
                last_close = data['Close'].iloc[-1]
                prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                change = last_close - prev_close
                percent_change = (change / prev_close) * 100

                # Get market cap if available (yfinance doesn't always provide this)
                market_cap = ticker.info.get('marketCap', None)

                results.append({
                    'name': name,
                    'symbol': symbol.replace('-USD', ''),
                    'price': round(last_close, 2),
                    'market_cap': round(market_cap, 2) if market_cap else None,
                    'percent_change_24h': round(percent_change, 2)
                })
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")

    return results

def get_commodities():
    """Get major commodities prices using yfinance"""
    commodities = {
        'GC=F': 'Gold',
        'SI=F': 'Silver',
        'CL=F': 'Crude Oil',
        'NG=F': 'Natural Gas',
        'HG=F': 'Copper',
        'ZC=F': 'Corn',
        'ZS=F': 'Soybeans',
        'KE=F': 'Coffee'
    }

    results = []
    for symbol, name in commodities.items():
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d')

            if not data.empty:
                last_close = data['Close'].iloc[-1]
                prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                change = last_close - prev_close
                percent_change = (change / prev_close) * 100

                results.append({
                    'name': name,
                    'symbol': symbol,
                    'price': round(last_close, 2),
                    'change': round(change, 2),
                    'percent_change': round(percent_change, 2)
                })
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")

    return results



def get_correct_previous_close(symbol):
    """Get yesterday's close price using daily data"""
    try:
        ticker = yf.Ticker(symbol)
        # Get 2 days of daily data to ensure we get yesterday's close
        hist = ticker.history(period="3d", interval="1d")
        if len(hist) >= 2:
            return hist['Close'].iloc[-2]  # Yesterday's close
        return None
    except Exception as e:
        print(f"Error getting previous close for {symbol}: {str(e)}")
        return None

def get_current_price(symbol):
    """Get current price from intraday data"""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return hist['Close'].iloc[-1]  # Latest price
        return None
    except Exception as e:
        print(f"Error getting current price for {symbol}: {str(e)}")
        return None

def get_yfinance_indices():
    """Get index data with proper previous close calculation"""
    indices_data = []
    for index in INDIAN_INDICES:
        try:
            current_price = get_current_price(index["symbol"])
            prev_close = get_correct_previous_close(index["symbol"])

            if current_price is not None and prev_close is not None:
                change = round(current_price - prev_close, 2)
                change_percent = round((change / prev_close) * 100, 2)

                indices_data.append({
                    "name": index["name"],
                    "current_price": current_price,
                    "change": change,
                    "change_percent": change_percent,
                    "prev_close": prev_close,
                    "color": index["color"],
                    "status_color": "#2ecc71" if change >= 0 else "#e74c3c"
                })
        except Exception as e:
            print(f"Error processing {index['name']}: {str(e)}")
            continue

    return indices_data

def get_yfinance_top_movers():
    """Calculate top movers with proper previous close"""
    changes = []
    for symbol in NIFTY_50_STOCKS:
        try:
            prev_close = get_correct_previous_close(symbol)
            current_price = get_current_price(symbol)

            if prev_close is not None and current_price is not None:
                change = round(current_price - prev_close, 2)
                pct = round((change/prev_close)*100, 2)
                changes.append({
                    "symbol": symbol.replace(".NS", ""),
                    "lastPrice": current_price,
                    "change": change,
                    "pChange": pct
                })
        except:
            continue

    changes.sort(key=lambda x: x["pChange"], reverse=True)
    return changes[:5], changes[-5:][::-1]

@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    ist = pytz.timezone('Asia/Kolkata')
    update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

    try:
        indices = get_yfinance_indices()
        gainers, losers = get_yfinance_top_movers()

        return jsonify({
            "success": True,
            "indices": indices,
            "top_gainers": gainers,
            "top_losers": losers,
            "last_updated": update_time
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "last_updated": update_time
        }), 500

CACHE_TIMEOUT = timedelta(minutes=15)
FNO_STOCKS = [
    'RELIANCE.NS', 'TATASTEEL.NS', 'HDFCBANK.NS', 'ICICIBANK.NS',
    'INFY.NS', 'BHARTIARTL.NS', 'HINDUNILVR.NS', 'KOTAKBANK.NS',
    'TCS.NS', 'WIPRO.NS', 'HCLTECH.NS', 'SBIN.NS', 'AXISBANK.NS',
    'ITC.NS', 'ONGC.NS', 'NTPC.NS', 'POWERGRID.NS', 'ULTRACEMCO.NS'
]

class AdvancedStockScanner:
    def __init__(self):
        self.cache = {}
        self.math_operations = {
            '+': lambda a, b: a + b,
            '-': lambda a, b: a - b,
            '*': lambda a, b: a * b,
            '/': lambda a, b: a / b if b != 0 else float('nan'),
            'min': lambda a, b: min(a, b),
            'max': lambda a, b: max(a, b),
            'abs': lambda a: abs(a),
            'round': lambda a: round(a),
            'ceil': lambda a: math.ceil(a),
            'floor': lambda a: math.floor(a)
        }

    def _get_stock_data(self, symbol, interval='1d'):
        """Get 1 year of stock data with caching"""
        cache_key = f"{symbol}_{interval}"
        now = datetime.now()

        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if now - timestamp < CACHE_TIMEOUT:
                return data.copy()

        try:
            data = yf.Ticker(symbol).history(period='1y', interval=interval)
            if not data.empty:
                data['Symbol'] = symbol  # Add symbol to dataframe
                self.cache[cache_key] = (data.copy(), now)
            return data
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def _convert_to_serializable(self, value):
        """Convert numpy types to native Python types for JSON serialization"""
        if isinstance(value, (np.integer)):
            return int(value)
        if isinstance(value, (np.floating)):
            return float(value)
        if isinstance(value, (np.ndarray)):
            return value.tolist()
        return value

    def calculate_technical_indicators(self, df, interval):
        """Calculate all technical indicators with serializable output"""
        if df.empty or len(df) < 2:
            return {}

        try:
            results = {
                'Symbol': df['Symbol'].iloc[-1],
                'Close': self._convert_to_serializable(df['Close'].iloc[-1]),
                'Open': self._convert_to_serializable(df['Open'].iloc[-1]),
                'High': self._convert_to_serializable(df['High'].iloc[-1]),
                'Low': self._convert_to_serializable(df['Low'].iloc[-1]),
                'Volume': self._convert_to_serializable(df['Volume'].iloc[-1]),
                '%Change': self._convert_to_serializable(
                    (df['Close'].iloc[-1] - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100
                )
            }

            # VWAP
            vwap = VolumeWeightedAveragePrice(
                high=df['High'],
                low=df['Low'],
                close=df['Close'],
                volume=df['Volume'],
                window=14
            )
            results['VWAP'] = self._convert_to_serializable(vwap.volume_weighted_average_price().iloc[-1])

            # Pivot points (from daily data)
            if interval.endswith('m'):
                daily_data = self._get_stock_data(df['Symbol'].iloc[0], '1d')
                if not daily_data.empty:
                    pivot = (daily_data['High'].iloc[-1] + daily_data['Low'].iloc[-1] + daily_data['Close'].iloc[-1]) / 3
                    diff = daily_data['High'].iloc[-1] - daily_data['Low'].iloc[-1]
                    results.update({
                        'Pivot': self._convert_to_serializable(pivot),
                        'R1': self._convert_to_serializable(2 * pivot - daily_data['Low'].iloc[-1]),
                        'R2': self._convert_to_serializable(pivot + diff),
                        'R3': self._convert_to_serializable(pivot + 2 * diff),
                        'S1': self._convert_to_serializable(2 * pivot - daily_data['High'].iloc[-1]),
                        'S2': self._convert_to_serializable(pivot - diff),
                        'S3': self._convert_to_serializable(pivot - 2 * diff)
                    })
            else:
                pivot = (df['High'].iloc[-1] + df['Low'].iloc[-1] + df['Close'].iloc[-1]) / 3
                diff = df['High'].iloc[-1] - df['Low'].iloc[-1]
                results.update({
                    'Pivot': self._convert_to_serializable(pivot),
                    'R1': self._convert_to_serializable(2 * pivot - df['Low'].iloc[-1]),
                    'R2': self._convert_to_serializable(pivot + diff),
                    'R3': self._convert_to_serializable(pivot + 2 * diff),
                    'S1': self._convert_to_serializable(2 * pivot - df['High'].iloc[-1]),
                    'S2': self._convert_to_serializable(pivot - diff),
                    'S3': self._convert_to_serializable(pivot - 2 * diff)
                })

            # Moving Averages
            results['SMA_5'] = self._convert_to_serializable(
                SMAIndicator(close=df['Close'], window=5).sma_indicator().iloc[-1])
            results['SMA_10'] = self._convert_to_serializable(
                SMAIndicator(close=df['Close'], window=10).sma_indicator().iloc[-1])
            results['SMA_20'] = self._convert_to_serializable(
                SMAIndicator(close=df['Close'], window=20).sma_indicator().iloc[-1])
            results['SMA_50'] = self._convert_to_serializable(
                SMAIndicator(close=df['Close'], window=50).sma_indicator().iloc[-1])
            results['SMA_200'] = self._convert_to_serializable(
                SMAIndicator(close=df['Close'], window=200).sma_indicator().iloc[-1])

            results['EMA_5'] = self._convert_to_serializable(
                EMAIndicator(close=df['Close'], window=5).ema_indicator().iloc[-1])
            results['EMA_10'] = self._convert_to_serializable(
                EMAIndicator(close=df['Close'], window=10).ema_indicator().iloc[-1])
            results['EMA_20'] = self._convert_to_serializable(
                EMAIndicator(close=df['Close'], window=20).ema_indicator().iloc[-1])
            results['EMA_50'] = self._convert_to_serializable(
                EMAIndicator(close=df['Close'], window=50).ema_indicator().iloc[-1])
            results['EMA_200'] = self._convert_to_serializable(
                EMAIndicator(close=df['Close'], window=200).ema_indicator().iloc[-1])

            results['WMA_20'] = self._convert_to_serializable(
                WMAIndicator(close=df['Close'], window=20).wma().iloc[-1])

            # Oscillators
            if len(df) >= 14:
                rsi = RSIIndicator(close=df['Close'], window=14)
                results['RSI'] = self._convert_to_serializable(rsi.rsi().iloc[-1])

                stoch = StochasticOscillator(
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'],
                    window=14,
                    smooth_window=3
                )
                results['Stoch_%K'] = self._convert_to_serializable(stoch.stoch().iloc[-1])
                results['Stoch_%D'] = self._convert_to_serializable(stoch.stoch_signal().iloc[-1])

                tsi = TSIIndicator(close=df['Close'], window_slow=25, window_fast=13)
                results['TSI'] = self._convert_to_serializable(tsi.tsi().iloc[-1])

            # MACD
            if len(df) >= 26:
                macd = MACD(close=df['Close'])
                results['MACD'] = self._convert_to_serializable(macd.macd().iloc[-1])
                results['MACD_Signal'] = self._convert_to_serializable(macd.macd_signal().iloc[-1])
                results['MACD_Hist'] = self._convert_to_serializable(macd.macd_diff().iloc[-1])

            # ADX
            if len(df) >= 14:
                adx = ADXIndicator(
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'],
                    window=14
                )
                results['ADX'] = self._convert_to_serializable(adx.adx().iloc[-1])
                results['ADX_Pos'] = self._convert_to_serializable(adx.adx_pos().iloc[-1])
                results['ADX_Neg'] = self._convert_to_serializable(adx.adx_neg().iloc[-1])

            # Ichimoku Cloud
            if len(df) >= 52:
                ichi = IchimokuIndicator(
                    high=df['High'],
                    low=df['Low'],
                    window1=9,
                    window2=26,
                    window3=52
                )
                results['Ichimoku_Conversion'] = self._convert_to_serializable(ichi.ichimoku_conversion_line().iloc[-1])
                results['Ichimoku_Base'] = self._convert_to_serializable(ichi.ichimoku_base_line().iloc[-1])
                results['Ichimoku_A'] = self._convert_to_serializable(ichi.ichimoku_a().iloc[-1])
                results['Ichimoku_B'] = self._convert_to_serializable(ichi.ichimoku_b().iloc[-1])

            # Bollinger Bands
            bb = BollingerBands(close=df['Close'], window=20, window_dev=2)
            results['BB_Upper'] = self._convert_to_serializable(bb.bollinger_hband().iloc[-1])
            results['BB_Middle'] = self._convert_to_serializable(bb.bollinger_mavg().iloc[-1])
            results['BB_Lower'] = self._convert_to_serializable(bb.bollinger_lband().iloc[-1])

            # ATR
            atr = AverageTrueRange(
                high=df['High'],
                low=df['Low'],
                close=df['Close'],
                window=14
            )
            results['ATR'] = self._convert_to_serializable(atr.average_true_range().iloc[-1])

            return results

        except Exception as e:
            print(f"Error calculating indicators: {str(e)}")
            return {}

    def evaluate_expression(self, expr, indicators):
        """Evaluate a mathematical expression"""
        try:
            if isinstance(expr, (int, float)):
                return expr
            if isinstance(expr, str) and expr in indicators:
                return indicators.get(expr, 0)
            if isinstance(expr, dict):
                if expr['type'] == 'value':
                    return expr['value']
                if expr['type'] == 'field':
                    return indicators.get(expr['field'], 0)
                if expr['type'] == 'operation':
                    op = expr['operation']
                    operands = [self.evaluate_expression(o, indicators) for o in expr['operands']]
                    if op in self.math_operations:
                        return self.math_operations[op](*operands)
            return 0
        except:
            return 0


    def scan_stocks(self, conditions):
        """Scan all F&O stocks with advanced conditions"""
        results = []

        for symbol in FNO_STOCKS:
            try:
                metrics = {'Symbol': symbol}
                valid = True

                # Get all unique intervals needed
                intervals = set()
                for condition in conditions:
                    intervals.add(condition.get('left_interval', '1d'))
                    if condition.get('right_type') == 'field':
                        intervals.add(condition.get('right_interval', '1d'))

                # Fetch data for all required intervals
                data = {interval: self._get_stock_data(symbol, interval) for interval in intervals}

                for condition in conditions:
                    # Process left side
                    left_interval = condition.get('left_interval', '1d')
                    left_df = data.get(left_interval, pd.DataFrame())
                    left_indicators = self.calculate_technical_indicators(left_df, left_interval)

                    # Process right side based on type
                    if condition.get('right_type') == 'value':
                        right_value = float(condition.get('right_value', 0))
                        right_indicators = {'value': right_value}
                    elif condition.get('right_type') == 'field':
                        right_interval = condition.get('right_interval', '1d')
                        right_df = data.get(right_interval, pd.DataFrame())
                        right_indicators = self.calculate_technical_indicators(right_df, right_interval)
                    elif condition.get('right_type') == 'operation':
                        # Handle math operations
                        op = condition.get('operation', {}).get('operation')
                        operands = condition.get('operation', {}).get('operands', [])
                        try:
                            right_value = self.math_operations.get(op, lambda *x: 0)(*[float(o) for o in operands])
                            right_indicators = {'operation_result': right_value}
                        except Exception as e:
                            print(f"Math operation failed: {e}")
                            valid = False
                            break

                    if not self.evaluate_condition(condition, left_indicators, right_indicators):
                        valid = False
                        break

                if valid:
                    default_data = self._get_stock_data(symbol, '1d')
                    metrics.update(self.calculate_technical_indicators(default_data, '1d'))
                    results.append(metrics)

            except Exception as e:
                print(f"Error scanning {symbol}: {str(e)}")
                continue

        return results

    def evaluate_condition(self, condition, left_indicators, right_indicators=None):
        """Evaluate a single condition with math operations"""
        if right_indicators is None:
            right_indicators = {}

        try:
            left_field = condition.get('left_field')
            if not left_field:
                return False

            left = left_indicators.get(left_field, 0)

            right_type = condition.get('right_type')
            if right_type == 'value':
                right = float(condition.get('right_value', 0))
            elif right_type == 'field':
                right_field = condition.get('right_field')
                if not right_field:
                    return False
                right = right_indicators.get(right_field, 0)
            else:
                right = 0

            op = condition.get('operator')
            if op == '>': return left > right
            if op == '<': return left < right
            if op == '==': return abs(left - right) < 0.0001
            if op == '>=': return left >= right
            if op == '<=': return left <= right
            if op == 'crossed_above': return left > right
            if op == 'crossed_below': return left < right

            return False
        except Exception as e:
            print(f"Error evaluating condition: {e}")
            return False

# Initialize scanner
scanner = AdvancedStockScanner()

@app.route('/api/scan', methods=['POST'])
def scan():
    try:
        conditions = request.json.get('conditions', [])
        results = scanner.scan_stocks(conditions)
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/indicators', methods=['GET'])
def get_indicators():
    return jsonify({
        'success': True,
        'categories': [
            {
                'name': 'Price',
                'indicators': ['Close', 'Open', 'High', 'Low', 'Volume', '%Change']
            },
            {
                'name': 'Pivot Points',
                'indicators': ['Pivot', 'R1', 'R2', 'R3', 'S1', 'S2', 'S3']
            },
            {
                'name': 'Moving Averages',
                'indicators': ['SMA_5', 'SMA_10', 'SMA_20', 'SMA_50', 'SMA_200',
                               'EMA_5', 'EMA_10', 'EMA_20', 'EMA_50', 'EMA_200',
                               'WMA_20']
            },
            {
                'name': 'Oscillators',
                'indicators': ['RSI', 'Stoch_%K', 'Stoch_%D', 'TSI', 'MACD',
                               'MACD_Signal', 'MACD_Hist']
            },
            {
                'name': 'Trend',
                'indicators': ['ADX', 'ADX_Pos', 'ADX_Neg', 'Ichimoku_Conversion',
                               'Ichimoku_Base', 'Ichimoku_A', 'Ichimoku_B']
            },
            {
                'name': 'Volatility',
                'indicators': ['BB_Upper', 'BB_Middle', 'BB_Lower', 'ATR']
            },
            {
                'name': 'Volume',
                'indicators': ['VWAP']
            }
        ],
        'intervals': ['1m', '5m', '15m', '30m', '1h', '1d'],
        'operations': ['+', '-', '*', '/', 'min', 'max', 'abs', 'round', 'ceil', 'floor']
    })

@app.route('/api/fno-stocks', methods=['GET'])
def get_fno_stocks():
    stocks_list = [
        f"{stock.strip().upper()}.NS"  # Clean and format
        for stock in fno_stocks.keys()
        if stock and str(stock).strip()  # Drop empty/None/whitespace
    ]
    unique_stocks = sorted(list(set(stocks_list)))
    return jsonify({
        'success': True,
        'stocks': unique_stocks
    })

@app.route('/api/ipos')
def get_ipos():
    try:
        # Fetch data from NiftyTrader
        response = requests.get('https://webapi.niftytrader.in/webapi/Other/ipo-company-list')
        response.raise_for_status()  # Raise error if request fails

        data = response.json()
        all_ipos = data['resultData']

        return jsonify(all_ipos)  # Return ALL IPOs (not just current)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

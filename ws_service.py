from datetime import datetime
import pytz
import os
import yfinance as yf

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import gzip
import pandas as pd
from io import BytesIO
import ssl
import re
from config import ACCESS_TOKEN  # Store securely

ssl._create_default_https_context = ssl._create_unverified_context

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com", "https://aitradinglab.blogspot.com", "https://www.aitradinglab.in/", "https://bansalshubham257.github.io"]}})

BASE_URL = "https://assets.upstox.com/market-quote/instruments/exchange"
UPSTOX_API_KEY = ACCESS_TOKEN
UPSTOX_BASE_URL = "https://api.upstox.com"

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
def get_all_stocks():
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
    {"name": "FinNifty", "symbol": "^NSEFIN", "color": "#2ca02c"},
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

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

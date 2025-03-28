from flask import Flask, jsonify
from datetime import datetime
import pytz
import os
import requests
from flask_cors import CORS
import time
import yfinance as yf

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})

INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
    {"name": "Nifty IT", "symbol": "^CNXIT", "color": "#2ca02c"},
    {"name": "Sensex", "symbol": "^BSESN", "color": "#d62728"}
]

NIFTY_50_STOCKS = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'INFY.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS'
]

@app.route('/api/global-market-data', methods=['GET'])
def get_market_data():
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
        hist = ticker.history(period="2d", interval="1d")
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

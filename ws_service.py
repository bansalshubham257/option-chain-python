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

def get_nse_session():
    """Create a session with proper headers for NSE"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    })
    try:
        session.get("https://www.nseindia.com", timeout=5)
        time.sleep(2)
        return session
    except:
        return None

def get_nse_data():
    """Try to get data from NSE with fallback to yfinance"""
    session = get_nse_session()
    if not session:
        return get_yfinance_data()

    try:
        # Get indices
        indices_data = []
        for index in INDIAN_INDICES:
            try:
                url = f"https://www.nseindia.com/api/equity-stockIndices?index={index['name'].upper().replace(' ', '%20')}"
                data = session.get(url, timeout=5).json()["data"][0]
                indices_data.append({
                    "name": index["name"],
                    "current_price": data["lastPrice"],
                    "change": round(data["lastPrice"] - data["previousClose"], 2),
                    "change_percent": round(data["pChange"], 2),
                    "prev_close": data["previousClose"],
                    "color": index["color"],
                    "status_color": "#2ecc71" if data["pChange"] >= 0 else "#e74c3c"
                })
            except:
                # Fallback to yfinance if NSE fails
                ticker = yf.Ticker(index["symbol"])
                hist = ticker.history(period="1d", interval="1m")
                if len(hist) > 1:
                    current = hist["Close"].iloc[-1]
                    prev = hist["Close"].iloc[-2]
                    change = round(current - prev, 2)
                    pct = round((change/prev)*100, 2)
                    indices_data.append({
                        "name": index["name"],
                        "current_price": current,
                        "change": change,
                        "change_percent": pct,
                        "prev_close": prev,
                        "color": index["color"],
                        "status_color": "#2ecc71" if change >= 0 else "#e74c3c"
                    })

        # Get gainers/losers
        try:
            gainers = session.get(
                "https://www.nseindia.com/api/live-analysis-variations?index=gainers",
                timeout=5
            ).json()["NIFTY"]["data"][:5]

            losers = session.get(
                "https://www.nseindia.com/api/live-analysis-variations?index=losers",
                timeout=5
            ).json()["NIFTY"]["data"][:5]
        except:
            # Fallback to yfinance calculation
            gainers, losers = get_yfinance_top_movers()

        return {
            "source": "nse",
            "indices": indices_data,
            "top_gainers": gainers,
            "top_losers": losers
        }
    except Exception as e:
        print(f"NSE failed, falling back to yfinance: {str(e)}")
        return get_yfinance_data()

def get_yfinance_data():
    """Fallback to yfinance when NSE fails"""
    indices_data = []
    for index in INDIAN_INDICES:
        try:
            ticker = yf.Ticker(index["symbol"])
            hist = ticker.history(period="1d", interval="1m")
            if len(hist) > 1:
                current = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[-2]
                change = round(current - prev, 2)
                pct = round((change/prev)*100, 2)
                indices_data.append({
                    "name": index["name"],
                    "current_price": current,
                    "change": change,
                    "change_percent": pct,
                    "prev_close": prev,
                    "color": index["color"],
                    "status_color": "#2ecc71" if change >= 0 else "#e74c3c"
                })
        except:
            continue

    gainers, losers = get_yfinance_top_movers()

    return {
        "source": "yfinance",
        "indices": indices_data,
        "top_gainers": gainers,
        "top_losers": losers
    }

def get_yfinance_top_movers():
    """Calculate top movers from yfinance"""
    changes = []
    for symbol in NIFTY_50_STOCKS:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d", interval="1m")
            if len(hist) > 1:
                current = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[0]
                change = round(current - prev, 2)
                pct = round((change/prev)*100, 2)
                changes.append({
                    "symbol": symbol.replace(".NS", ""),
                    "lastPrice": current,
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
        data = get_nse_data()
        return jsonify({
            "success": True,
            "source": data["source"],
            "indices": data["indices"],
            "top_gainers": data["top_gainers"],
            "top_losers": data["top_losers"],
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

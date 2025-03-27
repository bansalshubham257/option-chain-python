from flask import Flask, jsonify
from datetime import datetime
import pytz
import os
import requests
from flask_cors import CORS
import time

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})

INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "NIFTY 50", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "NIFTY BANK", "color": "#ff7f0e"},
    {"name": "Nifty IT", "symbol": "NIFTY IT", "color": "#2ca02c"},
    {"name": "Sensex", "symbol": "SENSEX", "color": "#d62728"},
    {"name": "Nifty Midcap 50", "symbol": "NIFTY MIDCAP 50", "color": "#9467bd"},
    {"name": "Nifty Smallcap 50", "symbol": "NIFTY SMLCAP 50", "color": "#8c564b"}
]

def initialize_nse_session():
    """Initialize session with required cookies and headers"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/"
    })
    
    # First request to get cookies
    session.get("https://www.nseindia.com", timeout=10)
    time.sleep(2)  # Important delay
    return session

def fetch_nse_data(session):
    """Fetch all market data from NSE"""
    base_url = "https://www.nseindia.com/api/"
    
    # 1. Fetch indices data
    indices_data = []
    for index in INDIAN_INDICES:
        try:
            url = f"{base_url}equity-stockIndices?index={index['symbol'].upper().replace(' ', '%20')}"
            response = session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()["data"][0]
            
            indices_data.append({
                "name": index["name"],
                "symbol": index["symbol"],
                "current_price": data["lastPrice"],
                "change": round(data["lastPrice"] - data["previousClose"], 2),
                "change_percent": round(data["pChange"], 2),
                "prev_close": data["previousClose"],
                "color": index["color"],
                "status_color": "#2ecc71" if data["pChange"] >= 0 else "#e74c3c"
            })
        except Exception as e:
            print(f"Error fetching {index['name']}: {str(e)}")
            continue
    
    # 2. Fetch gainers and losers
    try:
        gainers = session.get(
            f"{base_url}live-analysis-variations?index=gainers",
            timeout=10
        ).json()["NIFTY"]["data"][:5]
        
        losers = session.get(
            f"{base_url}live-analysis-variations?index=losers", 
            timeout=10
        ).json()["NIFTY"]["data"][:5]
        
        return {
            "indices": indices_data,
            "top_gainers": gainers,
            "top_losers": losers
        }
    except Exception as e:
        print(f"Error fetching gainers/losers: {str(e)}")
        return None

@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    ist = pytz.timezone('Asia/Kolkata')
    update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # Initialize new session for each request (NSE requires this)
        session = initialize_nse_session()
        data = fetch_nse_data(session)
        
        if data and data["indices"]:
            return jsonify({
                "success": True,
                "indices": data["indices"],
                "top_gainers": data["top_gainers"],
                "top_losers": data["top_losers"],
                "last_updated": update_time
            })
        raise Exception("Failed to fetch data from NSE")
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "last_updated": update_time
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

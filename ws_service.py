from flask import Flask, jsonify
from datetime import datetime
import pytz
import os
import requests
from flask_cors import CORS
import time

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})

# NSE Index Symbols
INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "NIFTY 50", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "NIFTY BANK", "color": "#ff7f0e"},
    {"name": "Nifty IT", "symbol": "NIFTY IT", "color": "#2ca02c"},
    {"name": "Sensex", "symbol": "SENSEX", "color": "#d62728"},
    {"name": "Nifty Midcap 50", "symbol": "NIFTY MIDCAP 50", "color": "#9467bd"},
    {"name": "Nifty Smallcap 50", "symbol": "NIFTY SMLCAP 50", "color": "#8c564b"}
]

# Create a session with NSE
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
})

def get_nse_data():
    """Fetch all required data from NSE in one go"""
    try:
        # Initialize session (important for NSE)
        session.get("https://www.nseindia.com", timeout=5)
        time.sleep(1)  # Be gentle with NSE
        
        # Fetch indices data
        indices_data = []
        for index in INDIAN_INDICES:
            url = f"https://www.nseindia.com/api/equity-stockIndices?index={index['symbol'].upper().replace(' ', '%20')}"
            response = session.get(url, timeout=5).json()
            
            if "data" in response and len(response["data"]) > 0:
                data = response["data"][0]
                prev_close = data["previousClose"]
                last_price = data["lastPrice"]
                change = round(last_price - prev_close, 2)
                change_percent = round((change / prev_close) * 100, 2)
                
                indices_data.append({
                    "name": index["name"],
                    "symbol": index["symbol"],
                    "current_price": last_price,
                    "change": change,
                    "change_percent": change_percent,
                    "prev_close": prev_close,
                    "color": index["color"],
                    "status_color": "#2ecc71" if change >= 0 else "#e74c3c"
                })
        
        # Fetch top gainers and losers
        gainers = session.get(
            "https://www.nseindia.com/api/live-analysis-variations?index=gainers",
            timeout=5
        ).json()["NIFTY"]["data"][:5]
        
        losers = session.get(
            "https://www.nseindia.com/api/live-analysis-variations?index=losers",
            timeout=5
        ).json()["NIFTY"]["data"][:5]
        
        return {
            "indices": indices_data,
            "top_gainers": gainers,
            "top_losers": losers
        }
        
    except Exception as e:
        print(f"Error fetching NSE data: {str(e)}")
        return None

@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    ist = pytz.timezone('Asia/Kolkata')
    update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        data = get_nse_data()
        if data:
            return jsonify({
                "success": True,
                "indices": data["indices"],
                "top_gainers": data["top_gainers"],
                "top_losers": data["top_losers"],
                "last_updated": update_time
            })
        else:
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

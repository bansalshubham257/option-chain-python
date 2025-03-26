from flask import Flask, jsonify
import yfinance as yf
import pandas as pd
from flask_caching import Cache
from datetime import datetime

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# Configuration
CACHE_TIMEOUT = 60  # seconds - respects API rate limits

# Indian indices with their display names and Yahoo Finance symbols
INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
    {"name": "Nifty IT", "symbol": "^CNXIT", "color": "#2ca02c"},
    {"name": "Sensex", "symbol": "^BSESN", "color": "#d62728"},
    {"name": "Nifty Midcap 50", "symbol": "^NSEMDCP50", "color": "#9467bd"},
    {"name": "Nifty Smallcap 50", "symbol": "^NSESC50", "color": "#8c564b"}
]

@app.route('/api/indices', methods=['GET'])
@cache.cached(timeout=CACHE_TIMEOUT)
def get_indices_data():
    try:
        indices_data = []
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for index in INDIAN_INDICES:
            ticker = yf.Ticker(index["symbol"])
            hist = ticker.history(period='1d', interval='1m')
            
            if not hist.empty:
                current_price = round(hist['Close'].iloc[-1], 2)
                prev_close = round(hist['Close'].iloc[-2] if len(hist) > 1 else current_price, 2)
                change = round(current_price - prev_close, 2)
                change_percent = round((change / prev_close) * 100, 2)
                
                # Determine color based on performance
                status_color = "#2ecc71" if change >= 0 else "#e74c3c"
                
                indices_data.append({
                    "name": index["name"],
                    "symbol": index["symbol"],
                    "current_price": current_price,
                    "change": change,
                    "change_percent": change_percent,
                    "prev_close": prev_close,
                    "color": index["color"],
                    "status_color": status_color,
                    "last_updated": update_time
                })
        
        return jsonify({
            "success": True,
            "data": indices_data,
            "last_updated": update_time
        })
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }), 500

if __name__ == '__main__':
    app.run(debug=True)

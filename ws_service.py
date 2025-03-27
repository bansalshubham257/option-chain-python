from flask import Flask, jsonify
import yfinance as yf
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

INDIAN_INDICES = [
    {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
    {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
    {"name": "Nifty IT", "symbol": "^CNXIT", "color": "#2ca02c"},
    {"name": "Sensex", "symbol": "^BSESN", "color": "#d62728"},
    {"name": "Nifty Midcap 50", "symbol": "^NSEMDCP50", "color": "#9467bd"},
    {"name": "Nifty Smallcap 50", "symbol": "^NSESC50", "color": "#8c564b"}
]

def get_reliable_previous_close(symbol):
    """Get the proper previous close price from the last trading day"""
    ticker = yf.Ticker(symbol)
    
    # Get data for last 5 days to ensure we get previous trading day
    hist = ticker.history(period='5d')
    
    if len(hist) < 2:
        return None
    
    # Get the most recent trading day's close (current day if market is open)
    current_close = hist['Close'].iloc[-1]
    
    # Find the previous trading day's close
    for i in range(2, len(hist)+1):
        prev_close = hist['Close'].iloc[-i]
        if prev_close != current_close:
            return prev_close
    
    return None

@app.route('/api/indices', methods=['GET'])
def get_indices_data():
    try:
        indices_data = []
        ist = pytz.timezone('Asia/Kolkata')
        update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        
        for index in INDIAN_INDICES:
            ticker = yf.Ticker(index["symbol"])
            
            # Get current price (1m interval for intraday)
            current_data = ticker.history(period='1d', interval='1m')
            
            if not current_data.empty:
                current_price = round(current_data['Close'].iloc[-1], 2)
                prev_close = round(get_reliable_previous_close(index["symbol"]), 2)
                
                if prev_close is not None:
                    change = round(current_price - prev_close, 2)
                    change_percent = round((change / prev_close) * 100, 2)
                    
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
            "last_updated": datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
        }), 500

if __name__ == '__main__':
    app.run(debug=True)

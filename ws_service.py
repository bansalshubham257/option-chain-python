from flask import Flask, jsonify
import yfinance as yf
from datetime import datetime
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

@app.route('/api/indices', methods=['GET'])
def get_indices_data():
    try:
        indices_data = []
        ist = pytz.timezone('Asia/Kolkata')
        update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

        for index in INDIAN_INDICES:
            ticker = yf.Ticker(index["symbol"])
            
            # Fetch the last 5 days of historical data to get the official previous close
            hist = ticker.history(period='5d')

            # Fetch intraday data to get the latest price
            current_data = ticker.history(period='1d', interval='1m')

            if not hist.empty and not current_data.empty:
                current_price = round(current_data['Close'].iloc[-1], 2)  # Latest available price
                
                prev_close = round(hist['Close'].iloc[-2], 2)  # Official previous close from the last trading session
                
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
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

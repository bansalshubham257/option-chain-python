import os
from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
import yfinance as yf
from threading import Thread
import time
from datetime import datetime
import pytz

# Initialize Flask app for WebSocket service only
ws_app = Flask(__name__)
socketio = SocketIO(ws_app, 
                  cors_allowed_origins=["https://swingtradingwithme.blogspot.com"],
                  async_mode='eventlet',
                  engineio_logger=True,
                  logger=True)

IST = pytz.timezone("Asia/Kolkata")

# Mapping of NSE indices
NSE_INDICES = {
    '^NSEI': 'NIFTY 50',
    '^NSEBANK': 'NIFTY BANK', 
    '^NSEIT': 'NIFTY IT',
    '^NSEAUTO': 'NIFTY AUTO',
    '^NFINNIFTY': 'NIFTY FINANCIAL SERVICES'
}

def fetch_live_indices():
    """Fetch live indices data using yfinance"""
    try:
        tickers = yf.Tickers(list(NSE_INDICES.keys()))
        data = {}
        
        for symbol, name in NSE_INDICES.items():
            try:
                ticker = tickers.tickers[symbol]
                hist = ticker.history(period='1d', interval='1m')
                
                if not hist.empty:
                    last_data = hist.iloc[-1]
                    prev_close = ticker.fast_info['previousClose']
                    change = last_data['Close'] - prev_close
                    pchange = (change / prev_close) * 100
                    
                    data[name] = {
                        'symbol': symbol,
                        'last': round(last_data['Close'], 2),
                        'change': round(change, 2),
                        'pChange': round(pchange, 2),
                        'high': round(last_data['High'], 2),
                        'low': round(last_data['Low'], 2),
                        'timestamp': datetime.now(IST).isoformat()
                    }
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                continue
                
        return data
    except Exception as e:
        print(f"Error fetching live indices: {str(e)}")
        return None

def live_data_thread():
    """Background thread that emits live data"""
    while True:
        try:
            data = fetch_live_indices()
            if data:
                socketio.emit('indices_update', data)
            time.sleep(5)  # Update every 5 seconds
        except Exception as e:
            print(f"Error in live data thread: {str(e)}")
            time.sleep(10)

# WebSocket event handlers
@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect') 
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")

# Health check endpoint
@ws_app.route('/health')
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == "__main__":
    # Start the background thread
    Thread(target=live_data_thread, daemon=True).start()
    
    # Run the WebSocket service
    print("🚀 Starting WebSocket service on port 8000")
    socketio.run(ws_app, host="0.0.0.0", port=8000)

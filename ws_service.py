import os
from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from threading import Thread, Lock
import time
from datetime import datetime
import pytz
import yfinance as yf
from flask_cors import CORS

# Initialize Flask app
app = Flask(__name__)

# Configure CORS
CORS(app, resources={
    r"/health": {"origins": ["https://swingtradingwithme.blogspot.com"]},
    r"/socket.io/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}
})

# Initialize SocketIO
socketio = SocketIO(app,
                  cors_allowed_origins=["https://swingtradingwithme.blogspot.com"],
                  async_mode='eventlet',
                  logger=True,
                  engineio_logger=True,
                  ping_timeout=60,
                  ping_interval=25)

# Timezone setup
IST = pytz.timezone("Asia/Kolkata")

# Validated NSE indices mapping
NSE_INDICES = {
    '^NSEI': 'NIFTY 50',
    '^NSEBANK': 'NIFTY BANK',
    'NIFTY_MIDCAP_100.NS': 'NIFTY MIDCAP 100',
    'NIFTY_IT.NS': 'NIFTY IT',
    'NIFTY_AUTO.NS': 'NIFTY AUTO',
    'NIFTY_FIN_SERVICE.NS': 'NIFTY FINANCIAL SERVICES'
}

# Thread-safe data storage
latest_data = {}
data_lock = Lock()

def fetch_index_data(ticker, symbol, name):
    """Fetch data for a single index with robust error handling"""
    try:
        data = ticker.history(period='1d', interval='1m')
        if data.empty:
            data = ticker.history(period='1d')  # Fallback to daily data
        
        if not data.empty:
            last = data.iloc[-1]
            prev_close = ticker.fast_info.get('previousClose', last.Close)
            
            return {
                'symbol': symbol,
                'last': round(last.Close, 2),
                'change': round(last.Close - prev_close, 2),
                'pChange': round((last.Close - prev_close) / prev_close * 100, 2),
                'high': round(data.High.max(), 2),
                'low': round(data.Low.min(), 2),
                'volume': int(data.Volume.sum()),
                'timestamp': datetime.now(IST).isoformat()
            }
    except Exception as e:
        print(f"Error fetching {name}: {str(e)}")
    return None

def fetch_all_indices():
    """Fetch all indices data with parallel processing"""
    global latest_data
    results = {}
    
    try:
        tickers = yf.Tickers(list(NSE_INDICES.keys()))
        
        for symbol, name in NSE_INDICES.items():
            ticker = tickers.tickers[symbol]
            if data := fetch_index_data(ticker, symbol, name):
                results[name] = data
                
        with data_lock:
            latest_data = results.copy()
            
    except Exception as e:
        print(f"Critical error in fetch_all_indices: {str(e)}")
    
    return results or None

def background_data_emitter():
    """Background thread that emits data periodically"""
    while True:
        try:
            start_time = time.time()
            data = fetch_all_indices()
            
            if data:
                socketio.emit('indices_update', data)
                print(f"Emitted data for {len(data)} indices")
            
            # Maintain 5-second interval accounting for processing time
            elapsed = time.time() - start_time
            sleep_time = max(0, 5 - elapsed)
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"Error in emitter thread: {str(e)}")
            time.sleep(10)

@app.route('/health')
def health_check():
    """Health check endpoint with CORS"""
    with data_lock:
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now(IST).isoformat(),
            'indices_available': list(latest_data.keys()),
            'service': 'market-indices-ws'
        })

@socketio.on('connect')
def handle_connect():
    """Handle new WebSocket connections"""
    print(f"Client connected: {request.sid}")
    with data_lock:
        emit('connection_response', {
            'status': 'connected',
            'data': latest_data
        })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnections"""
    print(f"Client disconnected: {request.sid}")

if __name__ == '__main__':
    # Start background thread
    Thread(target=background_data_emitter, daemon=True).start()
    
    # Run the app
    print("🚀 Starting WebSocket service on port 8000")
    socketio.run(app, host='0.0.0.0', port=8000)

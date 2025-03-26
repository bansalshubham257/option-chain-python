from flask import Flask
from flask_socketio import SocketIO
import yfinance as yf
from threading import Thread
import time
import pytz

# Initialize Flask app
app = Flask(__name__)
socketio = SocketIO(app,
                  cors_allowed_origins=["*"],
                  async_mode='eventlet',
                  logger=True,
                  engineio_logger=True)

IST = pytz.timezone("Asia/Kolkata")

# NSE indices to track
NSE_INDICES = {
    '^NSEI': 'NIFTY 50',
    '^NSEBANK': 'NIFTY BANK',
    '^NSEIT': 'NIFTY IT',
    '^NSEAUTO': 'NIFTY AUTO',
    '^NFINNIFTY': 'NIFTY FINANCIAL SERVICES'
}

def fetch_live_indices():
    """Fetch live market data"""
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
                        'timestamp': time.time()
                    }
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                continue
                
        return data
    except Exception as e:
        print(f"Error fetching indices: {str(e)}")
        return None

def emit_live_data():
    """Background thread to emit data"""
    while True:
        try:
            data = fetch_live_indices()
            if data:
                socketio.emit('indices_update', data)
            time.sleep(5)
        except Exception as e:
            print(f"Error in emit thread: {str(e)}")
            time.sleep(10)

# WebSocket events
@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")

# Health check endpoint
@app.route('/')
def health_check():
    return {'status': 'running', 'service': 'market-indices-ws'}

if __name__ == '__main__':
    # Start background thread
    Thread(target=emit_live_data, daemon=True).start()
    
    # Run the app
    socketio.run(app, host='0.0.0.0', port=8000)

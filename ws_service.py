import os
import yfinance as yf
from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from threading import Thread
import time
from datetime import datetime
import pytz

# Initialize Flask app
app = Flask(__name__)

# Configure CORS to allow all origins for development
# In production, replace with specific origins
CORS(app, resources={
    r"/*": {"origins": "*"}
})

# Initialize SocketIO with more permissive CORS
socketio = SocketIO(app, 
    cors_allowed_origins="*",
    async_mode='threading',
    ping_timeout=10,
    ping_interval=5
)

# Indian Stock Market Indices
INDICES = {
    '^NSEI': 'Nifty 50',
    '^NSEBANK': 'Nifty Bank',
    '^BSESN': 'Sensex',
    'NIFTY_MIDCAP_100.NS': 'Nifty Midcap 100',
    'NIFTY_IT.NS': 'Nifty IT',
    'NIFTY_FIN_SERVICE.NS': 'Nifty Financial Services'
}

def fetch_index_data(ticker_symbol, name):
    """Fetch live data for a single index"""
    try:
        # Fetch live ticker data
        ticker = yf.Ticker(ticker_symbol)
        
        # Get latest price information
        hist = ticker.history(period='1d')
        
        if not hist.empty:
            last_close = hist['Close'].iloc[-1]
            previous_close = ticker.info.get('previousClose', last_close)
            
            # Calculate changes
            change = last_close - previous_close
            percent_change = (change / previous_close) * 100
            
            return {
                'symbol': name,
                'last': round(last_close, 2),
                'change': round(change, 2),
                'pChange': round(percent_change, 2),
                'high': round(hist['High'].max(), 2),
                'low': round(hist['Low'].min(), 2),
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
            }
    except Exception as e:
        print(f"Error fetching {name}: {str(e)}")
    return None

def background_data_fetcher():
    """Background thread to fetch and broadcast market data"""
    while True:
        try:
            market_data = {}
            
            # Fetch data for all indices
            for symbol, name in INDICES.items():
                index_data = fetch_index_data(symbol, name)
                if index_data:
                    market_data[name] = index_data
            
            # Broadcast data via WebSocket
            if market_data:
                socketio.emit('market_update', market_data)
                print(f"Broadcasted data for {len(market_data)} indices")
            
            # Wait for 5 seconds before next fetch
            time.sleep(5)
        
        except Exception as e:
            print(f"Error in background data fetcher: {str(e)}")
            time.sleep(10)

@app.route('/health')
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'market-data-server',
        'indices': list(INDICES.values())
    })

if __name__ == '__main__':
    # Start background data fetching thread
    import threading
    data_thread = threading.Thread(target=background_data_fetcher, daemon=True)
    data_thread.start()
    
    # Run the Flask-SocketIO app
    socketio.run(app, host='0.0.0.0', port=8000, debug=True)

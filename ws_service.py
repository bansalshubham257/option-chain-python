import os
import yfinance as yf
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import threading
import time
from datetime import datetime
import pytz

# Initialize Flask app
app = Flask(__name__)

# Configure CORS to allow all origins
CORS(app, resources={
    r"/*": {"origins": "*"}
})

# Initialize SocketIO
socketio = SocketIO(app, 
    cors_allowed_origins="*",
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

# Track connected clients
connected_clients = set()

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
        'indices': list(INDICES.values()),
        'connected_clients': len(connected_clients)
    })

@socketio.on('connect')
def handle_connect():
    """Handle new client connections"""
    # Use request.sid from flask-socketio
    client_sid = request.sid
    connected_clients.add(client_sid)
    print(f"Client connected: {client_sid}")
    emit('connection_ack', {
        'status': 'connected', 
        'sid': client_sid,
        'message': 'Successfully connected to market data server'
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnections"""
    client_sid = request.sid
    connected_clients.discard(client_sid)
    print(f"Client disconnected: {client_sid}")

if __name__ == '__main__':
    # Start background data fetching thread
    data_thread = threading.Thread(target=background_data_fetcher, daemon=True)
    data_thread.start()
    
    # Run the Flask-SocketIO app
    socketio.run(
        app, 
        host='0.0.0.0', 
        port=int(os.environ.get('PORT', 8000)), 
        debug=True
    )

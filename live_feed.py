import asyncio
import json
import ssl
import websockets
import requests
from google.protobuf.json_format import MessageToDict
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time

from config import Config
from services.database import DatabaseService

import MarketDataFeed_pb2

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": [
    "https://swingtradingwithme.blogspot.com",
    "https://aitradinglab.blogspot.com",
    "https://www.aitradinglab.in",
    "https://bansalshubham257.github.io",
    "http://localhost:63342"
]}}, supports_credentials=True)

# Shared data structure
market_data = {}
active_subscription = []

db_service = DatabaseService()

def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    access_token = Config.ACCESS_TOKEN
    print("Access Token:", access_token)  # Log the access token
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    response = requests.get(url=url, headers=headers)
    print("Authorization Response:", response.json())  # Log the response
    return response.json()

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = MarketDataFeed_pb2.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

async def websocket_worker():
    """Main WebSocket connection handler."""
    global market_data, active_subscription

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            auth = get_market_data_feed_authorize_v3()
            uri = auth["data"]["authorized_redirect_uri"]

            async with websockets.connect(uri, ssl=ssl_context) as websocket:
                print("WebSocket connected")

                while True:
                    # Wait for subscription updates
                    if active_subscription:
                        print("Updating subscription:", active_subscription)
                        await websocket.send(json.dumps({
                            "guid": str(time.time()),
                            "method": "sub",
                            "data": {
                                "mode": "full",
                                "instrumentKeys": active_subscription
                            }
                        }).encode('utf-8'))
                        active_subscription = []  # Clear the subscription update flag

                    # Receive and process market data
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        decoded = decode_protobuf(message)
                        data = MessageToDict(decoded)
                        print("decoded data:", data)  # Log the decoded data
                        for instrument, feed in data.get("feeds", {}).items():
                            full_feed = feed.get("fullFeed", {})
                            market_ff = full_feed.get("marketFF", {})
                            index_ff = full_feed.get("indexFF", {})

                            ltpc = market_ff.get("ltpc", index_ff.get("ltpc", {}))

                            # Get correct OI value
                            oi = market_ff.get("oi", 0)

                            # Get volume from marketOHLC if available (preferring daily data)
                            volume = 0
                            market_ohlc = market_ff.get("marketOHLC", {}).get("ohlc", [])
                            if market_ohlc:
                                # Try to get volume from daily interval first
                                for ohlc_data in market_ohlc:
                                    if ohlc_data.get("interval") == "1d":
                                        volume = int(ohlc_data.get("vol", 0))
                                        break

                                # If daily volume not found, use any available volume
                                if volume == 0 and market_ohlc:
                                    volume = int(market_ohlc[0].get("vol", 0))

                            # If still no volume, try vtt field
                            if volume == 0:
                                volume = int(market_ff.get("vtt", 0))

                            # Get bid and ask quantities
                            bid_ask_quote = market_ff.get("marketLevel", {}).get("bidAskQuote", [{}])
                            bidQ = bid_ask_quote[0].get("bidQ", 0) if bid_ask_quote else 0
                            askQ = bid_ask_quote[0].get("askQ", 0) if bid_ask_quote else 0

                            market_data[instrument] = {
                                "ltp": ltpc.get("ltp"),
                                "volume": volume,
                                "oi": oi,
                                "bidQ": bidQ,
                                "askQ": askQ
                            }
                    except asyncio.TimeoutError:
                        # No data received, continue listening
                        pass

        except Exception as e:
            print(f"Connection error: {e}, reconnecting in 5 seconds...")
            await asyncio.sleep(5)

@app.route('/api/market_data', methods=['GET'])
def get_market_data():
    """Fetch and return market data for requested instruments."""
    global active_subscription

    requested_keys = request.args.getlist('keys')
    print("Requested Keys:", requested_keys)  # Log the requested keys
    if not requested_keys:
        return jsonify({"error": "No keys specified"}), 400

    # Update the active subscription
    active_subscription = requested_keys

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 10  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for all requested keys is available
        if all(key in market_data for key in requested_keys):
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    result = {key: market_data.get(key, {}) for key in requested_keys}
    return jsonify(result)

@app.route('/api/live_indices', methods=['GET'])
def get_live_indices():
    """Fetch live data for static indices."""
    global active_subscription

    # Static instrument keys for indices
    static_indices = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
        "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
        "SENSEX": "BSE_INDEX|SENSEX",
        "BANKEX": "BSE_INDEX|BANKEX"
    }

    # Update the active subscription with static keys
    active_subscription = list(static_indices.values())

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 10  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for all static keys is available
        if all(key in market_data for key in active_subscription):
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Prepare the response with live data for indices
    indices = []
    for index, key in static_indices.items():
        data = market_data.get(key, {})
        ltp = data.get("ltp", 0) or 0  # Ensure ltp is not None

        # Fetch prev_close from the database
        instrument = db_service.get_instrument_key_by_key(key)
        prev_close = instrument.get('last_close', 0) if instrument else 0

        # Calculate price change and percentage change
        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        indices.append({
            "symbol": index,
            "close": ltp,
            "price_change": price_change,
            "percent_change": percent_change
        })

    return jsonify({"indices": indices})


@app.route('/api/latest_price', methods=['GET'])
def get_latest_price():
    """Fetch the latest price for a given instrument key."""
    global active_subscription

    # Get the instrument key from the request
    instrument_key = request.args.get('instrument_key')
    if not instrument_key:
        return jsonify({"error": "Instrument key is required"}), 400

    # Fetch the last close price from the database
    try:
        instrument = db_service.get_instrument_key_by_key(instrument_key)
        if not instrument:
            return jsonify({"error": f"No instrument found for key: {instrument_key}"}), 404
        last_close = instrument.get('last_close', 0)
    except Exception as e:
        return jsonify({"error": f"Error fetching last close: {str(e)}"}), 500

    # Update the active subscription
    active_subscription = [instrument_key]

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 10  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for the requested key is available
        if instrument_key in market_data:
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Fetch the latest data for the instrument key
    data = market_data.get(instrument_key, {})
    if not data:
        return jsonify({"error": "No data available for the given instrument key"}), 404

    # Prepare the response
    ltp = data.get("ltp", 0)
    price_change = ltp - last_close
    percent_change = (price_change / last_close * 100) if last_close != 0 else 0

    response = {
        "ltp": ltp,
        "last_close": last_close,
        "price_change": price_change,
        "percent_change": percent_change
    }
    return jsonify(response)

@app.route('/api/latest-option-price', methods=['GET'])
def fetch_latest_option_price():
    """Fetch the latest price for a specific option, strike, and option type."""
    global active_subscription

    # Get parameters from the request
    symbol = request.args.get('symbol')
    expiry = request.args.get('expiry')
    strike = request.args.get('strike')
    option_type = request.args.get('optionType')

    if not symbol or not expiry or not strike or not option_type:
        return jsonify({"error": "Symbol, expiry, strike, and optionType are required"}), 400

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, option_type)
        if not instrument:
            return jsonify({"error": "Instrument key not found for the given parameters"}), 404

        instrument_key = instrument['instrument_key']
        prev_close = instrument.get('last_close', 0)

        # Update the active subscription
        active_subscription = [instrument_key]

        # Wait for the WebSocket to process the subscription and fetch data
        timeout = 10  # Maximum wait time in seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if data for the requested key is available
            if instrument_key in market_data:
                break
            time.sleep(0.5)  # Wait for 500ms before checking again

        # Fetch the latest data for the instrument key
        data = market_data.get(instrument_key, {})
        if not data:
            return jsonify({"error": "No data available for the given instrument key"}), 404

        # Prepare the response
        ltp = data.get("ltp", 0)
        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        response = {
            "price": ltp,
            "prev_close": prev_close,
            "price_change": price_change,
            "percent_change": percent_change,
            "timestamp": time.time()
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": f"Error fetching latest price: {str(e)}"}), 500


@app.route('/api/live_oi_volume', methods=['GET'])
def fetch_live_oi_volume():
    """Fetch live OI and volume for a specific option."""
    global active_subscription

    # Get parameters from the request
    symbol = request.args.get('symbol')
    expiry = request.args.get('expiry')
    strike = request.args.get('strike')
    option_type = request.args.get('optionType')

    if not symbol or not expiry or not strike or not option_type:
        return jsonify({"error": "Symbol, expiry, strike, and optionType are required"}), 400

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, option_type)
        if not instrument:
            return jsonify({"error": "Instrument key not found for the given parameters"}), 404

        instrument_key = instrument['instrument_key']

        # Update the active subscription
        active_subscription = [instrument_key]

        # Wait for the WebSocket to process the subscription and fetch data
        timeout = 10  # Maximum wait time in seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if data for the requested key is available
            if instrument_key in market_data:
                break
            time.sleep(0.5)  # Wait for 500ms before checking again

        # Fetch the latest data for the instrument key
        data = market_data.get(instrument_key, {})
        if not data:
            return jsonify({"error": "No data available for the given instrument key"}), 404

        # Prepare the response with correct OI and volume values
        response = {
            "oi": data.get("oi", 0),  # Using the correct OI field
            "volume": data.get("volume", 0),  # Using the correct volume field
            "timestamp": time.time()
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": f"Error fetching live OI and volume: {str(e)}"}), 500

@app.route('/api/instruments', methods=['GET'])
def get_instrument_key():
    """Fetch the instrument key for a specific symbol and exchange."""
    symbol = request.args.get('symbol')
    exchange = request.args.get('exchange')
    if not symbol or not exchange:
        return jsonify({"error": "Symbol and exchange parameters are required"}), 400

    try:
        # Fetch the instrument key for the given symbol and exchange from the database
        instrument = db_service.get_instrument_key_by_symbol_and_exchange(symbol, exchange)
        if not instrument:
            return jsonify({"error": f"No instrument key found for symbol: {symbol} and exchange: {exchange}"}), 404

        return jsonify({
            'symbol': instrument['symbol'],
            'instrument_key': instrument['instrument_key'],
            'exchange': instrument['exchange'],
            'tradingsymbol': instrument['tradingsymbol']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/option_instruments', methods=['GET'])
def get_option_instruments():
    """Get all option instrument keys for a specific stock."""
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({"error": "Symbol parameter is required"}), 400

    try:
        # Fetch all option instruments for the given symbol from the database
        instruments = db_service.get_option_instruments_by_symbol(symbol)
        if not instruments or len(instruments) == 0:
            return jsonify({"error": f"No option instruments found for symbol: {symbol}"}), 404

        # Group instruments by expiry and strike
        grouped_instruments = {}
        for instrument in instruments:
            expiry = instrument.get('expiry', '')
            strike = instrument.get('strike_price', '')
            option_type = instrument.get('option_type', '')

            if expiry not in grouped_instruments:
                grouped_instruments[expiry] = {}

            if strike not in grouped_instruments[expiry]:
                grouped_instruments[expiry][strike] = {}

            grouped_instruments[expiry][strike][option_type] = instrument.get('instrument_key', '')

        return jsonify({
            'symbol': symbol,
            'instruments': grouped_instruments
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/multiple_oi_volume', methods=['GET'])
def fetch_multiple_oi_volume():
    """Fetch OI and volume for multiple instrument keys at once."""
    global active_subscription

    # Get instrument keys as a comma-separated list
    instrument_keys_str = request.args.get('instrument_keys')
    if not instrument_keys_str:
        return jsonify({"error": "instrument_keys parameter is required"}), 400

    # Split into a list
    instrument_keys = instrument_keys_str.split(',')

    # Update the active subscription
    active_subscription = instrument_keys

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 10  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for all requested keys is available
        if all(key in market_data for key in instrument_keys):
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Prepare the response with OI and volume data for all instruments
    result = {}
    for key in instrument_keys:
        data = market_data.get(key, {})
        result[key] = {
            "oi": data.get("oi", 0),
            "volume": data.get("volume", 0),
            "ltp": data.get("ltp", 0)
        }

    return jsonify(result)

@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    """Fetch all available stock symbols for which options are available."""
    try:
        # Get unique symbols from the database that have option instruments
        stocks = db_service.get_option_stock_symbols()
        if not stocks:
            return jsonify({"error": "No stocks found with option instruments"}), 404

        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def start_websocket():
    """Start WebSocket in background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_worker())

def start_flask():
    """Start Flask server."""
    app.run(debug=True, threaded=True, port=5000)

if __name__ == '__main__':
    threading.Thread(target=start_websocket, daemon=True).start()
    start_flask()

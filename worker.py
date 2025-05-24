import asyncio
import json
import ssl
import websockets
import requests
import os
from google.protobuf.json_format import MessageToDict
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import threading
import time
import uvicorn
from typing import List, Dict, Any, Optional

from config import Config
from services.database import DatabaseService
import MarketDataFeed_pb2

# FastAPI app setup
app = FastAPI()

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://swingtradingwithme.blogspot.com",
        "https://aitradinglab.blogspot.com",
        "https://www.aitradinglab.in",
        "https://bansalshubham257.github.io",
        "http://localhost:63342"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared data structure
market_data: Dict[str, Dict[str, Any]] = {}
active_subscription: List[str] = []

db_service = DatabaseService()

def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    access_token = Config.ACCESS_TOKEN
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    response = requests.get(url=url, headers=headers)
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
                    if active_subscription:
                        await websocket.send(json.dumps({
                            "guid": str(time.time()),
                            "method": "sub",
                            "data": {
                                "mode": "full",
                                "instrumentKeys": active_subscription
                            }
                        }).encode('utf-8'))
                        active_subscription = []

                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        decoded = decode_protobuf(message)
                        data = MessageToDict(decoded)
                        for instrument, feed in data.get("feeds", {}).items():
                            full_feed = feed.get("fullFeed", {})
                            market_ff = full_feed.get("marketFF", {})
                            index_ff = full_feed.get("indexFF", {})

                            ltpc = market_ff.get("ltpc", index_ff.get("ltpc", {}))
                            oi = market_ff.get("oi", 0)

                            volume = 0
                            market_ohlc = market_ff.get("marketOHLC", {}).get("ohlc", [])
                            if market_ohlc:
                                for ohlc_data in market_ohlc:
                                    if ohlc_data.get("interval") == "1d":
                                        volume = int(ohlc_data.get("vol", 0))
                                        break
                                if volume == 0 and market_ohlc:
                                    volume = int(market_ohlc[0].get("vol", 0))
                            if volume == 0:
                                volume = int(market_ff.get("vtt", 0))

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
                        pass

        except Exception as e:
            print(f"Connection error: {e}, reconnecting in 5 seconds...")
            await asyncio.sleep(5)

@app.get("/api/market_data")
async def get_market_data(request: Request):
    """Fetch and return market data for requested instruments."""
    global active_subscription

    requested_keys = request.query_params.getlist('keys')
    if not requested_keys:
        raise HTTPException(status_code=400, detail="No keys specified")

    active_subscription = requested_keys

    timeout = 10
    start_time = time.time()
    while time.time() - start_time < timeout:
        if all(key in market_data for key in requested_keys):
            break
        time.sleep(0.5)

    return {key: market_data.get(key, {}) for key in requested_keys}

@app.get("/api/live_indices")
async def get_live_indices():
    """Fetch live data for static indices."""
    global active_subscription

    static_indices = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
        "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
        "SENSEX": "BSE_INDEX|SENSEX",
        "BANKEX": "BSE_INDEX|BANKEX"
    }

    active_subscription = list(static_indices.values())

    timeout = 10
    start_time = time.time()
    while time.time() - start_time < timeout:
        if all(key in market_data for key in active_subscription):
            break
        time.sleep(0.5)

    indices = []
    for index, key in static_indices.items():
        data = market_data.get(key, {})
        ltp = data.get("ltp", 0) or 0

        instrument = db_service.get_instrument_key_by_key(key)
        prev_close = instrument.get('last_close', 0) if instrument else 0

        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        indices.append({
            "symbol": index,
            "close": ltp,
            "price_change": price_change,
            "percent_change": percent_change
        })

    return {"indices": indices}

@app.get('/api/latest_price')
async def get_latest_price(instrument_key: str):
    """Fetch the latest price for a given instrument key."""
    global active_subscription

    if not instrument_key:
        raise HTTPException(status_code=400, detail="Instrument key is required")

    # Fetch the last close price from the database
    try:
        instrument = db_service.get_instrument_key_by_key(instrument_key)
        if not instrument:
            raise HTTPException(status_code=404, detail=f"No instrument found for key: {instrument_key}")
        last_close = instrument.get('last_close', 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching last close: {str(e)}")

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
        raise HTTPException(status_code=404, detail="No data available for the given instrument key")

    # Prepare the response
    ltp = data.get("ltp", 0)
    price_change = ltp - last_close
    percent_change = (price_change / last_close * 100) if last_close != 0 else 0

    return {
        "ltp": ltp,
        "last_close": last_close,
        "price_change": price_change,
        "percent_change": percent_change
    }

@app.get('/api/latest-option-price')
async def fetch_latest_option_price(symbol: str, expiry: str, strike: str, optionType: str):
    """Fetch the latest price for a specific option, strike, and option type."""
    global active_subscription

    if not symbol or not expiry or not strike or not optionType:
        raise HTTPException(status_code=400, detail="Symbol, expiry, strike, and optionType are required")

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, optionType)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument key not found for the given parameters")

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
            raise HTTPException(status_code=404, detail="No data available for the given instrument key")

        # Prepare the response
        ltp = data.get("ltp", 0)
        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        return {
            "price": ltp,
            "prev_close": prev_close,
            "price_change": price_change,
            "percent_change": percent_change,
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest price: {str(e)}")

@app.get('/api/live_oi_volume')
async def fetch_live_oi_volume(symbol: str, expiry: str, strike: str, optionType: str):
    """Fetch live OI and volume for a specific option."""
    global active_subscription

    if not symbol or not expiry or not strike or not optionType:
        raise HTTPException(status_code=400, detail="Symbol, expiry, strike, and optionType are required")

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, optionType)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument key not found for the given parameters")

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
            raise HTTPException(status_code=404, detail="No data available for the given instrument key")

        # Prepare the response with correct OI and volume values
        return {
            "oi": data.get("oi", 0),
            "volume": data.get("volume", 0),
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching live OI and volume: {str(e)}")

@app.get('/api/instruments')
async def get_instrument_key(symbol: str, exchange: str):
    """Fetch the instrument key for a specific symbol and exchange."""
    if not symbol or not exchange:
        raise HTTPException(status_code=400, detail="Symbol and exchange parameters are required")

    try:
        # Fetch the instrument key for the given symbol and exchange from the database
        instrument = db_service.get_instrument_key_by_symbol_and_exchange(symbol, exchange)
        if not instrument:
            raise HTTPException(status_code=404, 
                              detail=f"No instrument key found for symbol: {symbol} and exchange: {exchange}")

        return {
            'symbol': instrument['symbol'],
            'instrument_key': instrument['instrument_key'],
            'exchange': instrument['exchange'],
            'tradingsymbol': instrument['tradingsymbol']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/api/option_instruments')
async def get_option_instruments(symbol: str):
    """Get all option instrument keys for a specific stock."""
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol parameter is required")

    try:
        # Fetch all option instruments for the given symbol from the database
        instruments = db_service.get_option_instruments_by_symbol(symbol)
        if not instruments or len(instruments) == 0:
            raise HTTPException(status_code=404, detail=f"No option instruments found for symbol: {symbol}")

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

        return {
            'symbol': symbol,
            'instruments': grouped_instruments
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/api/multiple_oi_volume')
async def fetch_multiple_oi_volume(instrument_keys: str):
    """Fetch OI and volume for multiple instrument keys at once."""
    global active_subscription

    if not instrument_keys:
        raise HTTPException(status_code=400, detail="instrument_keys parameter is required")

    # Split into a list
    instrument_keys_list = instrument_keys.split(',')

    # Update the active subscription
    active_subscription = instrument_keys_list

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 10  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for all requested keys is available
        if all(key in market_data for key in instrument_keys_list):
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Prepare the response with OI and volume data for all instruments
    result = {}
    for key in instrument_keys_list:
        data = market_data.get(key, {})
        result[key] = {
            "oi": data.get("oi", 0),
            "volume": data.get("volume", 0),
            "ltp": data.get("ltp", 0)
        }

    return result

@app.get("/api/stocks")
async def get_stocks():
    """Fetch all available stock symbols for which options are available."""
    try:
        stocks = db_service.get_option_stock_symbols()
        if not stocks:
            raise HTTPException(status_code=404, detail="No stocks found with option instruments")
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def run_websocket():
    asyncio.run(websocket_worker())

def start_services():
    # Start WebSocket in a separate thread
    threading.Thread(target=run_websocket, daemon=True).start()

    # Start FastAPI
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv('PORT', 8000)))

if __name__ == '__main__':
    start_services()

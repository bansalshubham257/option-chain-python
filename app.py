from flask import Flask, request, jsonify
from fetch_data import get_instrument_key, get_expiry_dates, get_option_chain
from flask_cors import CORS
import os

app = Flask(__name__)

# Allow only your Blogger site
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})

@app.route("/expiry_dates", methods=["GET", "OPTIONS"])
def expiry_dates():
    if request.method == "OPTIONS":
        return _build_cors_preflight_response()

    symbol = request.args.get("symbol", "").upper()
    instrument_key = get_instrument_key(symbol)

    if not instrument_key:
        return _build_actual_response({"error": "Invalid stock symbol"}, 400)

    expiry_dates = get_expiry_dates(instrument_key)
    return _build_actual_response({"expiry_dates": expiry_dates}) if expiry_dates else _build_actual_response({"error": "No expiry dates found"}, 404)

@app.route("/option_chain", methods=["GET", "OPTIONS"])
def option_chain():
    if request.method == "OPTIONS":
        return _build_cors_preflight_response()

    symbol = request.args.get("symbol", "").upper()
    expiry_date = request.args.get("expiry_date", "")

    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return _build_actual_response({"error": "Invalid stock symbol"}, 400)

    options_data = get_option_chain(instrument_key, expiry_date)
    if not options_data:
        return _build_actual_response({"error": "No option chain data found"}, 404)

    formatted_data = [
        {
            "strike": option["strike_price"],
            "call_oi": option["call_options"]["market_data"].get("oi", 0),
            "put_oi": option["put_options"]["market_data"].get("oi", 0),
        }
        for option in options_data
    ]

    return _build_actual_response({"options": formatted_data})

# Function to handle CORS preflight requests
def _build_cors_preflight_response():
    response = jsonify({"message": "CORS preflight success"})
    response.headers.add("Access-Control-Allow-Origin", "https://swingtradingwithme.blogspot.com")
    response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type")
    return response

# Function to add CORS headers to actual responses
def _build_actual_response(response_data, status_code=200):
    response = jsonify(response_data)
    response.headers.add("Access-Control-Allow-Origin", "https://swingtradingwithme.blogspot.com")
    return response

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)

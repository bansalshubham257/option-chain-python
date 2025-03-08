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

            # CALL SIDE
            "call_oi": option["call_options"]["market_data"].get("oi", 0),
            "call_oi_change": option["call_options"]["market_data"].get("oi_change", 0),
            "call_volume": option["call_options"]["market_data"].get("volume", 0),
            "call_iv": option["call_options"]["market_data"].get("iv", 0),
            "call_ltp": option["call_options"]["market_data"].get("ltp", 0),
            "call_change": option["call_options"]["market_data"].get("ltp_change", 0),
            "call_bid_qty": option["call_options"]["market_data"].get("bid_qty", 0),
            "call_bid": option["call_options"]["market_data"].get("bid", 0),
            "call_ask": option["call_options"]["market_data"].get("ask", 0),
            "call_ask_qty": option["call_options"]["market_data"].get("ask_qty", 0),

            # PUT SIDE
            "put_bid_qty": option["put_options"]["market_data"].get("bid_qty", 0),
            "put_bid": option["put_options"]["market_data"].get("bid", 0),
            "put_ask": option["put_options"]["market_data"].get("ask", 0),
            "put_ask_qty": option["put_options"]["market_data"].get("ask_qty", 0),
            "put_change": option["put_options"]["market_data"].get("ltp_change", 0),
            "put_ltp": option["put_options"]["market_data"].get("ltp", 0),
            "put_iv": option["put_options"]["market_data"].get("iv", 0),
            "put_volume": option["put_options"]["market_data"].get("volume", 0),
            "put_oi_change": option["put_options"]["market_data"].get("oi_change", 0),
            "put_oi": option["put_options"]["market_data"].get("oi", 0),
        }
        for option in options_data
    ]

    return _build_actual_response({"options": formatted_data})

@app.route("/option_details", methods=["GET"])
def option_details():
    symbol = request.args.get("symbol", "").upper()
    expiry_date = request.args.get("expiry_date", "")
    strike_price = request.args.get("strike", "")
    option_type = request.args.get("option_type", "").upper()  # CE or PE

    if not symbol or not expiry_date or not strike_price or not option_type:
        return jsonify({"error": "Missing required parameters"}), 400

    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return jsonify({"error": "Invalid stock symbol"}), 400

    options_data = get_option_chain(instrument_key, expiry_date)
    if not options_data:
        return jsonify({"error": "No option chain data found"}), 404

    # Find the matching strike price
    for option in options_data:
        if option["strike_price"] == float(strike_price):
            if option_type == "CE":
                return jsonify({
                    "volume": option["call_options"]["market_data"].get("volume", 0),
                    "cmp": option["call_options"]["market_data"].get("ltp", 0)
                })
            elif option_type == "PE":
                return jsonify({
                    "volume": option["put_options"]["market_data"].get("volume", 0),
                    "cmp": option["put_options"]["market_data"].get("ltp", 0)
                })
    
    return jsonify({"error": "Strike price not found"}), 404


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

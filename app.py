from flask import Flask, request, jsonify
from fetch_data import get_instrument_key, get_expiry_dates, get_option_chain
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)  # Allow all origins (can be restricted to specific domains)

@app.route("/expiry_dates", methods=["GET"])
def expiry_dates():
    symbol = request.args.get("symbol", "").upper()
    instrument_key = get_instrument_key(symbol)

    if not instrument_key:
        return jsonify({"error": "Invalid stock symbol"}), 400

    expiry_dates = get_expiry_dates(instrument_key)
    return jsonify({"expiry_dates": expiry_dates}) if expiry_dates else jsonify({"error": "No expiry dates found"}), 404

@app.route("/option_chain", methods=["GET"])
def option_chain():
    symbol = request.args.get("symbol", "").upper()
    expiry_date = request.args.get("expiry_date", "")

    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return jsonify({"error": "Invalid stock symbol"}), 400

    options_data = get_option_chain(instrument_key, expiry_date)
    if not options_data:
        return jsonify({"error": "No option chain data found"}), 404

    formatted_data = [
        {
            "strike": option["strike_price"],
            "call_oi": option["call_options"]["market_data"].get("oi", 0),
            "put_oi": option["put_options"]["market_data"].get("oi", 0),
        }
        for option in options_data
    ]

    return jsonify({"options": formatted_data})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)

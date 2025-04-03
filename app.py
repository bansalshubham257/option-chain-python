import logging
from functools import lru_cache

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import pytz
from datetime import datetime
import threading
import time
from services.market_data import MarketDataService
from services.option_chain import OptionChainService
from services.stock_analysis import StockAnalysisService
from services.database import DatabaseService

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": [
    "https://swingtradingwithme.blogspot.com",
    "https://aitradinglab.blogspot.com",
    "https://www.aitradinglab.in",
    "https://bansalshubham257.github.io"
]}})

# Initialize services
market_data_service = MarketDataService()
option_chain_service = OptionChainService()
stock_analysis_service = StockAnalysisService()
database_service = DatabaseService()

# API Routes
@app.route("/stocks", methods=["GET"])
def get_all_stocks():
    return jsonify(stock_analysis_service.fetch_all_nse_stocks())

@app.route("/analyze", methods=["GET"])
def analyze():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "Stock symbol is required"}), 400
    return jsonify(stock_analysis_service.analyze_stock(symbol))

@app.route('/get-orders', methods=['GET'])
def get_orders():
    if not market_data_service.is_market_open():
        return jsonify({'status': 'Market is closed'})
    return jsonify(database_service.get_options_orders())

@app.route('/get-futures-orders', methods=['GET'])
def get_futures_orders():
    if not market_data_service.is_market_open():
        return jsonify({'status': 'Market is closed'})
    return jsonify(database_service.get_futures_orders())

@app.route('/get_fno_stocks', methods=['GET'])
def get_upstox_fno_stocks():
    return jsonify(option_chain_service.get_fno_stocks())

@app.route('/get_fno_data', methods=['GET'])
def get_fno_data():
    stock = request.args.get('stock')
    expiry = request.args.get('expiry')
    strike = request.args.get('strike')
    option_type = request.args.get('option_type')
    return jsonify(database_service.get_fno_data(stock, expiry, strike, option_type))

@app.route('/get_option_data', methods=['GET'])
def get_option_data():
    stock = request.args.get('stock')
    if not stock:
        return jsonify({"error": "Stock symbol is required"}), 400
    return jsonify(database_service.get_option_data(stock))

@app.route('/api/fii-dii', methods=['GET'])
def get_fii_dii_data():
    year_month = request.args.get('year_month')
    request_type = request.args.get('request_type')
    return jsonify(market_data_service.get_fii_dii_data(year_month, request_type))

@app.route("/fetch_stocks", methods=["GET"])
def get_nse_bse_stocks():
    return jsonify(option_chain_service.fetch_stocks())

@app.route("/fetch_price", methods=["GET"])
def get_price():
    instrument_key = request.args.get("instrument_key")
    return jsonify(option_chain_service.fetch_price(instrument_key))

@app.route("/fetch_bulk_prices", methods=["GET"])
def get_bulk_prices():
    instrument_keys = request.args.get("instrument_keys", "").split(',')
    return jsonify(option_chain_service.fetch_bulk_prices(instrument_keys))

@app.route('/api/global-market-data', methods=['GET'])
def get_global_market_data():
    return jsonify(market_data_service.get_global_market_data())

@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    return jsonify(market_data_service.get_indian_market_data())

@app.route('/api/ipos')
def get_ipos():
    return jsonify(market_data_service.get_ipos())

def run_script():
    """Background worker for market hours processing"""
    option_chain_service.run_market_processing()

if __name__ == "__main__":
    if os.getenv('BACKGROUND_WORKER', 'false').lower() == 'true':
        print("Starting background worker ONLY")
        run_script()
    else:
        print("Starting web service ONLY")
        port = int(os.environ.get("PORT", 10000))
        db = DatabaseService()
        if db.test_connection():
            print("✅ Database connection successful")
    
            # Test basic query
            with db._get_cursor() as cur:
                cur.execute("SELECT current_database()")
                print(f"Connected to database: {cur.fetchone()[0]}")
        else:
            print("❌ Database connection failed")
        app.run(host="0.0.0.0", port=port)

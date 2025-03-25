import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import ssl
import json
import threading
import time
from datetime import datetime, timedelta
import pytz
import boto3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import fetch_all_nse_stocks, analyze_stock
from file_utils import atomic_json_read
from test import (is_market_open, fno_stocks, clear_old_data_files, fetch_option_chain,
                  fetch_futures_orders, JSON_FILE, FUTURES_JSON_FILE, OI_VOLUME_JSON_FILE)

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)  # Change to your region
table = dynamodb.Table('oi_volume_data')  # Replace with your actual table name

EXPIRY_DATE = "2025-03-27"
MARKET_OPEN = datetime.strptime("09:10", "%H:%M").time()
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()

# 📌 API Routes
@app.route("/stocks", methods=["GET"])
def get_all_stocks():
    return jsonify(fetch_all_nse_stocks())

@app.route("/analyze", methods=["GET"])
def analyze():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "Stock symbol is required"}), 400
    return jsonify(analyze_stock(symbol))

@app.route('/get-orders', methods=['GET'])
def get_orders():
    if not is_market_open():
        return jsonify({'status': 'Market is closed'})
    try:
        data = atomic_json_read(JSON_FILE)
        return jsonify(data if data else [])
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/get-futures-orders', methods=['GET'])
def get_futures_orders():
    """API to fetch large futures orders"""
    if not is_market_open():
        return jsonify({'status': 'Market is closed'})
    try:
        if os.path.exists(FUTURES_JSON_FILE):
            with open(FUTURES_JSON_FILE, 'r') as file:
                data = json.load(file)
                return jsonify(data)  # ✅ Return futures large orders
        else:
            return jsonify([])  # ✅ Return empty list if file doesn't exist
    except Exception as e:
        return jsonify({"error": str(e)})

IST = pytz.timezone("Asia/Kolkata")


def is_market_closed():
    """ Check if the market is closed """
    now = datetime.now(IST).time()
    return now >= MARKET_CLOSE

def fetch_and_store_orders():
    if is_market_closed():
        return

    with ThreadPoolExecutor(max_workers=3) as executor:  # Increased workers
        # Process futures and options in parallel
        futures = []

        # Submit futures tasks
        futures.extend(
            executor.submit(fetch_futures_orders, stock, EXPIRY_DATE, lot_size, None)
            for stock, lot_size in fno_stocks.items()
        )

        # Submit options tasks
        futures.extend(
            executor.submit(fetch_option_chain, stock, EXPIRY_DATE, lot_size, None)
            for stock, lot_size in fno_stocks.items()
        )

        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    print(f"✅ Processed orders for a stock")
            except Exception as e:
                print(f"❌ Error processing orders: {e}")

def run_script():
    last_clear_date = None

    while True:
        now = datetime.now(IST)

        # Check if we need to clear old data (once per day at market open)
        if (now.weekday() < 5 and
                MARKET_OPEN <= now.time() <= MARKET_CLOSE and
                (last_clear_date is None or last_clear_date != now.date())):

            try:
                clear_old_data_files()
                last_clear_date = now.date()
                print("🔄 Cleared all previous day's data")
            except Exception as e:
                print(f"⚠️ Failed to clear old data: {e}")

        # Normal processing during market hours
        if now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE:
            try:
                fetch_and_store_orders()
                time.sleep(30)  # Run every 30 seconds
            except Exception as e:
                print(f"⚠️ Script error: {e}")
                time.sleep(60)
        else:
            time.sleep(300)  # 5 min sleep outside market hours

@app.route('/get_fno_stocks', methods=['GET'])
def get_fno_stocks():
    """API to return the list of F&O stocks"""
    return jsonify(fno_stocks)

@app.route('/get_fno_data', methods=['GET'])
def get_fno_data():
    try:
        stock = request.args.get('stock')
        expiry = request.args.get('expiry')
        strike = request.args.get('strike')
        option_type = request.args.get('option_type')

        key = f"oi_volume_data:{stock}:{expiry}:{strike}:{option_type}"
        data = atomic_json_read(OI_VOLUME_JSON_FILE) or {}
        return jsonify({"data": data.get(key, [])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_option_data', methods=['GET'])
def get_option_data():
    stock = request.args.get('stock')
    if not stock:
        return jsonify({"error": "Stock symbol is required"}), 400

    try:
        # Fetch stored strike prices and expiries from Redis
        response = table.scan(
            FilterExpression="begins_with(oi_volume_key, :stock)",
            ExpressionAttributeValues={":stock": f"oi_volume_data:{stock}:"}
        )

        if "Items" not in response or not response["Items"]:
            return jsonify({"error": "Option data not found for this stock"}), 404

        # Format response properly
        option_data = {item["oi_volume_key"]: item["data"] for item in response["Items"]}

        return jsonify({"data": option_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/fii-dii', methods=['GET'])
def get_fii_dii_data():
    year_month = request.args.get('year_month')  # e.g., "2025-03"
    request_type = request.args.get('request_type')
    if not year_month:
        return jsonify({"error": "year_month parameter is required"}), 400

    # Fetch FII/DII data from the external API
    api_url = f"https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data?request_type={request_type}&year_month={year_month}"
    response = requests.get(api_url)

    if response.status_code == 200:
        return jsonify(response.json())
    else:
        return jsonify({"error": "Failed to fetch data from the external API"}), 500

NSE_BASE_URL = "https://www.nseindia.com/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest"
}

# Route to fetch sector heatmap data
@app.route('/api/heatmap', methods=['GET'])
def get_heatmap():
    sector = request.args.get('sector')  # Get sector from query parameters
    if not sector:
        return jsonify({"error": "Sector parameter is required"}), 400

    try:
        # Fetch data from NSE API
        url = f"{NSE_BASE_URL}/heatmap-symbols?type=Sectoral%20Indices&indices={sector}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

# Route to fetch sectoral indices heatmap data
@app.route('/api/sectoral-indices', methods=['GET'])
def get_sectoral_indices():
    try:
        # Fetch data from NSE API
        url = f"{NSE_BASE_URL}/heatmap-index?type=Sectoral%20Indices"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

# Run Flask
if __name__ == "__main__":
    print(f"🛠️ Starting with BACKGROUND_WORKER={os.getenv('BACKGROUND_WORKER')}")

    # Clear old data on worker startup if needed
    if os.getenv('BACKGROUND_WORKER', 'false').lower() == 'true':
        now = datetime.now(IST)
        if now.weekday() < 5 and MARKET_OPEN <= now.time() <= MARKET_CLOSE:
            try:
                from test import clear_old_data_files
                clear_old_data_files()
            except Exception as e:
                print(f"⚠️ Startup cleanup failed: {e}")

        print("🔵 Starting background worker ONLY")
        run_script()
    else:
        print("🌍 Starting web service ONLY")
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port)

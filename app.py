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
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import fetch_all_nse_stocks, analyze_stock
from test import is_market_open, fno_stocks, fetch_option_chain, JSON_FILE, FUTURES_JSON_FILE

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

# 📌 5️⃣ API Routes
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
        if os.path.exists(JSON_FILE):
            with open(JSON_FILE, 'r') as file:
                data = json.load(file)
                return jsonify(data)
        else:
            return jsonify([])  # Return empty list if file is missing
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
MARKET_CLOSE = datetime.strptime("15:30", "%H:%M").time()

def is_market_closed():
    """ Check if the market is closed """
    now = datetime.now(IST).time()
    return now >= MARKET_CLOSE

def fetch_and_store_orders():
    """ Fetch option chain data and store it in Redis instead of a file """

    # 🔹 Get today's date

    # 🔹 Check if market is open
    if is_market_closed():
        print("Market is closed. Skipping script execution.")
        return  # ❌ Do not update orders when market is close

    # ✅ Step 1: Load existing data, handle "Market is closed" JSON
    all_orders = []
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as file:
            try:
                data = json.load(file)
                if isinstance(data, list):
                    all_orders = data  # Load existing orders if format is correct
                elif isinstance(data, dict) and "status" in data:
                    print("ℹ️ Market was closed previously. Starting fresh.")
                    all_orders = []  # Reset when market opens
            except json.JSONDecodeError:
                print("⚠️ JSON file is corrupted. Resetting data.")
                all_orders = []  # Reset in case of corruption

    # ✅ Step 2: Fetch new large orders in parallel
    new_orders = []
    print("🔍 Fetching new orders...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_option_chain, stock, EXPIRY_DATE, lot_size, table): stock for stock, lot_size in fno_stocks.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                new_orders.extend(result)

    # ✅ Step 3: Create a dictionary for quick lookup and replacement
    orders_dict = {(order["stock"], order["strike_price"], order["type"]): order for order in all_orders}
    for order in new_orders:
        key = (order["stock"], order["strike_price"], order["type"])
        orders_dict[key] = order  # If exists, it replaces old; otherwise, it appends

    # ✅ Step 5: Convert back to list and save
    updated_orders = list(orders_dict.values())
    with open(JSON_FILE, 'w') as file:
        json.dump(updated_orders, file)

    print(f"✅ Orders before update: {len(all_orders)}, Orders after update: {len(updated_orders)}, New/Replaced Orders: {len(updated_orders) - len(all_orders)}")

last_run_time = 0
CACHE_DURATION = 30  # Cache data for 30 seconds

@app.route('/run-script', methods=['GET'])
def run_script():
    #clear_old_data()  # Clear old data if market reopens
    global last_run_time
    """ Trigger script asynchronously to avoid Render timeout """
    if not is_market_open():
        return jsonify({
            'status': 'Market is closed',
        })

    current_time = time.time()
    if current_time - last_run_time < CACHE_DURATION:
        return jsonify({'status': 'Using cached result. Try again later.'}), 200

    # Update last run time
    last_run_time = current_time

    thread = threading.Thread(target=fetch_and_store_orders)
    thread.start()  # Start script in background

    return jsonify({'status': 'Script is running in the background'}), 202

@app.route('/get_fno_stocks', methods=['GET'])
def get_fno_stocks():
    """API to return the list of F&O stocks"""
    return jsonify(fno_stocks)

@app.route('/get_fno_data', methods=['GET'])
def get_fno_data():
    try:
        # Fetch stored F&O stocks from Redis
        stock = request.args.get('stock')
        expiry = request.args.get('expiry')
        strike = request.args.get('strike')
        option_type = request.args.get('option_type')  # 'CE' or 'PE'

        if not (stock and expiry and strike and option_type):
            return jsonify({"error": "Missing parameters"}), 400

        key = f"oi_volume_data:{stock}:{expiry}:{strike}:{option_type}"
        response = table.get_item(Key={"oi_volume_key": key})
        fno_data = response.get("Item", {}).get("data")

        if not fno_data:
            return jsonify({"error": "F&O stock data not found"}), 404

        return jsonify({"data": fno_data})  # Wrap list inside a dictionary
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

MARKET_OPEN_TIME = time.strftime("%H:%M", time.strptime("09:15", "%H:%M"))
CLEAR_DATA_START_TIME = time.strftime("%H:%M", time.strptime("08:15", "%H:%M"))

def clear_old_data():
    """Delete previous day’s data when the market reopens."""
    now = datetime.now()
    current_time = now.strftime("%H:%M")  # ✅ Convert time to string format HH:MM

    # ✅ Check if we are between 8:15 - 9:15 AM
    if CLEAR_DATA_START_TIME <= current_time <= MARKET_OPEN_TIME:
        print("🧹 Clearing old market data...")

        # ✅ Get yesterday's date
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # ✅ Scan and delete all items with yesterday's date
            scan = table.scan()
            with table.batch_writer() as batch:
                for item in scan.get("Items", []):
                    if yesterday in item.get("oi_volume_key", ""):
                        batch.delete_item(Key={"oi_volume_key": item["oi_volume_key"]})

            print("✅ Old market data cleared successfully.")
        except Exception as e:
            print(f"❌ Error clearing old data: {e}")
    else:
        print("⏳ Market not open yet, skipping data clearing.")

# Run Flask
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)

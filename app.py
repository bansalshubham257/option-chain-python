import os
import ta  # Technical Indicators
from flask import Flask, request, jsonify
from flask_cors import CORS
import ssl
import yfinance as yf
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.embed import components
import pandas as pd
from bokeh.layouts import column
import json
import threading
import time
from datetime import datetime
import pytz
import redis
import numpy as np
import boto3
from collections import Counter

from test import is_market_open, fno_stocks, fetch_option_chain, JSON_FILE

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

# Fetch values from Render environment variables
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USER = os.getenv("REDIS_USER")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)  # Change to your region
table = dynamodb.Table('oi_volume_data')  # Replace with your actual table name

# Construct the Redis URL dynamically
REDIS_URL = f"redis://{REDIS_USER}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

LOCAL_CSV_FILE = "nse_stocks.csv"
EXPIRY_DATE = "2025-03-27"

def fetch_all_nse_stocks():
    """Read NSE-listed stocks from a locally saved CSV file"""
    try:
        df = pd.read_csv(LOCAL_CSV_FILE)
        return df["SYMBOL"].tolist()
    except Exception as e:
        print(f"Error reading NSE stocks file: {e}")
        return []

# 📌 3️⃣ Fetch Live Stock Data from NSE & Merge with Historical Data
def get_stock_data(symbol):
    """
    Fetch stock data from Yahoo Finance and compute technical indicators.
    """
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="300d")  # Fetch last 100 days to ensure indicators work

        if df.empty:
            return None

        # Rename columns to match previous structure
        df = df.reset_index()
        df = df.rename(columns={
            "Date": "timestamp",
            "Close": "last_price",
            "High": "day_high",
            "Low": "day_low",
            "Volume": "volume"
        })

        # 📊 Calculate Technical Indicators
        df["SMA_50"] = df["last_price"].rolling(window=50).mean()
        df["SMA_200"] = df["last_price"].rolling(window=200).mean()
        df["EMA_50"] = df["last_price"].ewm(span=50, adjust=False).mean()
        df["EMA_200"] = df["last_price"].ewm(span=200, adjust=False).mean()
        df["RSI"] = ta.momentum.RSIIndicator(df["last_price"], window=14).rsi()
        df["MACD"] = ta.trend.MACD(df["last_price"]).macd()

        # 📌 Bollinger Bands
        bb = ta.volatility.BollingerBands(df["last_price"], window=20, window_dev=2)
        df["upper_band"] = bb.bollinger_hband()
        df["lower_band"] = bb.bollinger_lband()

        # Check for breakout
        df["bollinger_signal"] = df.apply(lambda row: "Bullish Breakout" if row["last_price"] > row["upper_band"]
        else ("Bearish Breakdown" if row["last_price"] < row["lower_band"]
              else "No breakout"), axis=1)

        # 📌 Pivot Point Calculation
        df["pivot"] = (df["day_high"] + df["day_low"] + df["last_price"]) / 3
        df["above_pivot"] = df["last_price"] > df["pivot"]

        df["cumulative_vp"] = (df["last_price"] * df["volume"]).cumsum()
        df["cumulative_volume"] = df["volume"].cumsum()
        df["VWAP"] = df["cumulative_vp"] / df["cumulative_volume"]

        # Drop temp cumulative columns
        df.drop(columns=["cumulative_vp", "cumulative_volume"], inplace=True)

        return df # Ensure only valid rows are returned

    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return None

def calculate_fibonacci_levels(df, lookback=75):
    """Calculate Fibonacci retracement levels based on recent swing high and low."""
    recent_high = df["day_high"].tail(lookback).max()
    recent_low = df["day_low"].tail(lookback).min()

    levels = {
        "0.236": recent_high - (0.236 * (recent_high - recent_low)),
        "0.382": recent_high - (0.382 * (recent_high - recent_low)),
        "0.5": recent_high - (0.5 * (recent_high - recent_low)),
        "0.618": recent_high - (0.618 * (recent_high - recent_low)),
        "0.786": recent_high - (0.786 * (recent_high - recent_low)),
    }

    return levels, recent_high, recent_low

def get_fibonacci_analysis(latest_price, fib_levels):
    """Analyze Fibonacci support/resistance and predict price movements."""
    details = []

    # Sorting Fibonacci levels for easy comparison
    sorted_levels = sorted(fib_levels.items(), key=lambda x: x[1], reverse=True)

    current_position = None
    next_resistance = None
    next_support = None

    for i, (level, price) in enumerate(sorted_levels):
        if latest_price > price:
            current_position = f"Above {level} ({price:.2f})"
            next_support = f"{sorted_levels[i][0]} ({sorted_levels[i][1]:.2f})" if i < len(sorted_levels) - 1 else "Recent Low"
            break
        else:
            next_resistance = f"{level} ({price:.2f})"

    # If no resistance found, assume recent high
    if not next_resistance:
        next_resistance = f"Recent High ({max(fib_levels.values()):.2f})"

    details.append(f"Current Position: {current_position}")
    details.append(f"Next Support Level: {next_support}")
    details.append(f"Next Resistance Level: {next_resistance}")

    return details
# 📌 2️⃣ Analyze Stock Signals

def analyze_52_week_levels(df, latest_price):
    """Check if stock is near its 52-week high or low and give predictions."""
    high_52 = df["day_high"].max()  # 52-week high
    low_52 = df["day_low"].min()  # 52-week low

    threshold = 0.02  # 2% threshold to consider "near"

    position = None
    recommendation = None

    if latest_price >= high_52 * (1 - threshold):  # Near 52-week high
        position = f"Near 52-Week High ({high_52:.2f})"
        recommendation = "⚠️ Caution: The stock is near its yearly high. Consider booking profits or waiting for a pullback."
    elif latest_price <= low_52 * (1 + threshold):  # Near 52-week low
        position = f"Near 52-Week Low ({low_52:.2f})"
        recommendation = "✅ Opportunity: The stock is near its yearly low. Watch for potential reversal and accumulation."

    return position, recommendation

def calculate_supertrend(df, atr_period=10, factor=3):
    """
    Calculate the Supertrend indicator without TA-Lib.

    Args:
        df (pd.DataFrame): DataFrame containing 'day_high', 'day_low', and 'day_close' columns.
        atr_period (int): ATR period (default: 10).
        factor (float): ATR multiplier (default: 3).

    Returns:
        str: 'Bullish' or 'Bearish' trend.
    """

    # Ensure columns exist
    if not {'day_high', 'day_low', 'Close'}.issubset(df.columns):
        raise ValueError("Missing required columns: 'day_high', 'day_low', 'Close'.")

    # Compute ATR using pandas-ta
    df['ATR'] = ta.volatility.average_true_range(df['day_high'], df['day_low'], df['Close'], window=atr_period)

    # Compute upper & lower bands
    df['UpperBand'] = ((df['day_high'] + df['day_low']) / 2) + (factor * df['ATR'])
    df['LowerBand'] = ((df['day_high'] + df['day_low']) / 2) - (factor * df['ATR'])

    # Initialize Supertrend
    df['Supertrend'] = np.nan
    df['Trend'] = np.nan

    # Start Supertrend calculation
    for i in range(1, len(df)):
        prev_supertrend = df.iloc[i - 1]['Supertrend']

        if df.iloc[i - 1]['Close'] > prev_supertrend:
            df.iloc[i, df.columns.get_loc('Supertrend')] = df.iloc[i]['LowerBand'] \
                if df.iloc[i]['Close'] > df.iloc[i]['LowerBand'] else prev_supertrend
        else:
            df.iloc[i, df.columns.get_loc('Supertrend')] = df.iloc[i]['UpperBand'] \
                if df.iloc[i]['Close'] < df.iloc[i]['UpperBand'] else prev_supertrend

        # Assign trend direction
        df.iloc[i, df.columns.get_loc('Trend')] = "Bullish" if df.iloc[i]['Close'] > df.iloc[i]['Supertrend'] else "Bearish"

    return df['Trend'].iloc[-1]  # Return latest trend


def find_support_resistance(df):
    """Detect Major & Minor Support & Resistance Levels."""
    high = df['day_high'].iloc[-2]  # Previous day's High
    low = df['day_low'].iloc[-2]  # Previous day's Low
    close = df['Close'].iloc[-2]  # Previous day's Close

    # Calculate Pivot Point
    pivot = (high + low + close) / 3

    # Calculate Support & Resistance Levels
    r1 = (2 * pivot) - low
    r2 = pivot + (high - low)
    s1 = (2 * pivot) - high
    s2 = pivot - (high - low)

    current_price = df['Close'].iloc[-1]

    if current_price > pivot:
        summary = f"Price ({current_price}) is above Pivot ({pivot:.2f}). If it breaks {r1:.2f}, it may touch {r2:.2f}."
    elif current_price < pivot:
        summary = f"Price ({current_price}) is below Pivot ({pivot:.2f}). If it breaks {s1:.2f}, it may drop to {s2:.2f}."
    else:
        summary = f"Price ({current_price}) is at the Pivot ({pivot:.2f}), indicating a neutral zone."


    # Add Labels for Major/Minor Support & Resistance
    pivots = [
        ("Current Price", df['Close'].iloc[-1]),
        ("(1D time Frame) Pivot Point", round(pivot, 2)),
        ("Pivot Resistance 1 (R1)", round(r1, 2)),
        ("Pivot Resistance 2 (R2)", round(r2, 2)),
        ("Pivot Support 1 (S1)", round(s1, 2)),
        ("Pivot Support 2 (S2)", round(s2, 2)),
        ("summary", summary)
    ]

    return pivots

def detect_trendline_breakout(df):
    """Detect if price breaks above resistance or below support trendline."""

    # Get the latest closing price
    latest_price = df['Close'].iloc[-1]

    # Assume we have already detected the trendline levels
    support_trendline = df['day_low'].rolling(window=50).min().iloc[-1]  # Approximate support
    resistance_trendline = df['day_high'].rolling(window=50).max().iloc[-1]  # Approximate resistance

    # Check for breakout or breakdown
    if latest_price > resistance_trendline:
        return "Bullish Breakout"
    elif latest_price < support_trendline:
        return "Bearish Breakdown"
    else:
        return "No Breakout detected"

def calculate_donchian_channels(df, period=20):
    """
    Calculate Donchian Channels (Upper, Lower, Middle) and return a user-friendly summary.

    Args:
        df (pd.DataFrame): DataFrame with 'High' and 'Low' columns.
        period (int): Lookback period (default: 20).

    Returns:
        dict: Donchian Channel values and an easy-to-understand summary
    """
    df['Upper'] = df['day_high'].rolling(window=period).max()
    df['Lower'] = df['day_low'].rolling(window=period).min()
    df['Middle'] = (df['Upper'] + df['Lower']) / 2

    upper = round(df['Upper'].iloc[-1], 2)
    lower = round(df['Lower'].iloc[-1], 2)
    middle = round(df['Middle'].iloc[-1], 2)
    current_price = round(df['Close'].iloc[-1], 2)

    # Generate Summary for User
    if current_price > upper:
        trend = "Breakout above the Donchian Upper Band! Possible strong bullish momentum."
    elif current_price < lower:
        trend = "Breakdown below the Donchian Lower Band! Possible strong bearish momentum."
    else:
        trend = f"Price is currently between the Donchian range ({lower} - {upper}). Potential consolidation."

    summary = (
        f"Donchian Channel Levels:\n"
        f"🔹 Upper Band: {upper}\n"
        f"🔸 Middle Band: {middle}\n"
        f"🔹 Lower Band: {lower}\n"
        f"📌 Current Price: {current_price}\n"
        f"📢 Trend Analysis: {trend}"
    )

    donchain = [
        ("summary", summary)
    ]

    return donchain


def analyze_stock(symbol):
    df = get_stock_data(symbol)
    df.rename(columns={"last_price": "Close"}, inplace=True)

    if df is None or df.empty:
        return {"error": "Stock data not found"}

    df["SMA_50"] = df["Close"].rolling(window=50).mean()
    df["SMA_200"] = df["Close"].rolling(window=200).mean()
    df["BB_middle"] = df["Close"].rolling(window=20).mean()
    df["BB_std"] = df["Close"].rolling(window=20).std()
    df["BB_upper"] = df["BB_middle"] + (df["BB_std"] * 2)
    df["BB_lower"] = df["BB_middle"] - (df["BB_std"] * 2)
    df["EMA_12"] = df["Close"].ewm(span=12, adjust=False).mean()
    df["EMA_26"] = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["EMA_12"] - df["EMA_26"]
    df["Signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Histogram"] = df["MACD"] - df["Signal_Line"]

    if len(df) == 0:
        return {"error": "No data available for the stock"}

    fib_levels, fib_high, fib_low = calculate_fibonacci_levels(df)

    latest_price = df.iloc[-1]["day_low"]  # Using Low for better accuracy

    supertrend_data = calculate_supertrend(df)
    pivots = find_support_resistance(df)
    trendline_status = detect_trendline_breakout(df)

    latest = df.iloc[-1]
    bullish_signals = []
    bearish_signals = []
    active_indicators = {}

    # 🔵 Compare price with 50% Fibonacci Level
    if latest_price > fib_levels["0.5"]:
        bullish_signals.append("Above 50% Fibonacci Level (Bullish)")
        active_indicators["Fibonacci"] = True
    else:
        bearish_signals.append("Below 50% Fibonacci Level (Bearish)")
        active_indicators["Fibonacci"] = True

    # 🔵 Additional Fibonacci Analysis (Support/Resistance)
    fibonacci_details = get_fibonacci_analysis(latest_price, fib_levels)

    # ✅ 52-Week High/Low Analysis
    position_52w, recommendation_52w = analyze_52_week_levels(df, latest_price)

    donchian_data = calculate_donchian_channels(df)

    # ✅ Combine all additional indicators into one dictionary

    additional_details = {
        "Supertrend": supertrend_data,
        "Trendline_Breakout": trendline_status,
        "Support_Resistance": pivots, # Show last 5 key levels
        "Donchian_Channels": donchian_data  # ✅ Added Donchian Channels
    }

    if fibonacci_details:
        additional_details["Fibonacci Analysis"] = fibonacci_details  # 🟢 Key as section title

    if position_52w:
        additional_details["52-Week High/Low"] = [
            f"📍 Position: {position_52w}",
            f"📝 Recommendation: {recommendation_52w}"
        ]

    if latest["RSI"] > 70:
        bearish_signals.append("Overbought (RSI > 70)")
        active_indicators["RSI"] = True
    elif latest["RSI"] < 30:
        bullish_signals.append("Oversold (RSI < 30)")
        active_indicators["RSI"] = True

    if latest["MACD"] > 0:
        bullish_signals.append("MACD Bullish Crossover")
        active_indicators["MACD"] = True
    else:
        bearish_signals.append("MACD Bearish Crossover")
        active_indicators["MACD"] = True

    if latest["SMA_50"] > latest["SMA_200"]:
        bullish_signals.append("Golden Cross (SMA50 > SMA200)")
        active_indicators["SMA_50"] = True
        active_indicators["SMA_200"] = True
    else:
        bearish_signals.append("Death Cross (SMA50 < SMA200)")
        active_indicators["SMA_50"] = True
        active_indicators["SMA_200"] = True

    if latest["above_pivot"]:
        bullish_signals.append("Above Pivot")
        active_indicators["Pivot"] = True
    else:
        bearish_signals.append("Below Pivot")
        active_indicators["Pivot"] = True

    if latest["bollinger_signal"] == "Bullish Breakout":
        bullish_signals.append("Bollinger Band Breakout")
        active_indicators["BB_upper"] = True
        active_indicators["BB_lower"] = True
    elif latest["bollinger_signal"] == "Bearish Breakdown":
        bearish_signals.append("Bollinger Band Breakdown")
        active_indicators["BB_upper"] = True
        active_indicators["BB_lower"] = True

    if latest["Close"] > latest["VWAP"]:
        bullish_signals.append("Above VWAP")
        active_indicators["VWAP"] = True
    else:
        bearish_signals.append("Below VWAP")
        active_indicators["VWAP"] = True

    script, div = generate_chart(df, active_indicators)

    return {
        "symbol": symbol,
        "bullish_signals": bullish_signals,
        "bearish_signals": bearish_signals,
        "verdict": "Bullish" if len(bullish_signals) > len(bearish_signals) else "Bearish",
        "script": script,
        "div": div,
        "additional_details": additional_details # New Section for Support/Resistance
    }

def generate_chart(df, active_indicators):
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)

    source = ColumnDataSource(df)

    p = figure(x_axis_type="datetime", title="Stock Chart", width=1000, height=500)
    p_rsi = figure(x_axis_type="datetime", title="RSI", width=1000, height=200, x_range=p.x_range)
    p_macd = figure(x_axis_type="datetime", title="MACD", width=1000, height=200, x_range=p.x_range)

    inc = df["Close"] > df["Open"]
    dec = df["Open"] > df["Close"]

    p.segment(df["timestamp"], df["day_high"], df["timestamp"], df["day_low"], color="black")
    p.vbar(df["timestamp"][inc], width=12*60*60*1000, top=df["Close"][inc], bottom=df["Open"][inc], fill_color="green", line_color="black")
    p.vbar(df["timestamp"][dec], width=12*60*60*1000, top=df["Open"][dec], bottom=df["Close"][dec], fill_color="red", line_color="black")

    if active_indicators.get("SMA_50"):
        p.line(df["timestamp"], df["SMA_50"], legend_label="SMA 50", line_width=2, color="blue")
    if active_indicators.get("SMA_200"):
        p.line(df["timestamp"], df["SMA_200"], legend_label="SMA 200", line_width=2, color="red")
    if active_indicators.get("BB_upper"):
        p.line(df["timestamp"], df["BB_upper"], legend_label="Bollinger Upper", line_width=1.5, color="purple", line_dash="dashed")
    if active_indicators.get("BB_lower"):
        p.line(df["timestamp"], df["BB_lower"], legend_label="Bollinger Lower", line_width=1.5, color="purple", line_dash="dashed")
    if active_indicators.get("VWAP"):
        p.line(df["timestamp"], df["VWAP"], legend_label="VWAP", line_width=2, color="orange", line_dash="dotted")
    if active_indicators.get("Pivot"):
        pivot_value = df["pivot"].iloc[-1]  # Get the latest pivot value
        p.line([df["timestamp"].min(), df["timestamp"].max()], [pivot_value, pivot_value],
               legend_label="Pivot", line_width=2, color="brown", line_dash="solid")

    if "RSI" in df.columns:
        p_rsi.line(df["timestamp"], df["RSI"], legend_label="RSI", line_width=2, color="green")
    if "MACD" in df.columns and "Signal_Line" in df.columns:
        p_macd.line(df["timestamp"], df["MACD"], legend_label="MACD", line_width=2, color="cyan")
        p_macd.line(df["timestamp"], df["Signal_Line"], legend_label="Signal Line", line_width=2, color="red")

    p.legend.location = "top_left"
    script, div = components(column(p, p_rsi, p_macd))
    return script, div


def get_oi_volume_analysis(stock_symbol, expiry_date, strike_price, option_type):
    """
    Fetch OI and Volume analysis from Redis.
    """
    print("before oi_volume_key")
    oi_volume_key = f"oi_volume_data:{stock_symbol}:{expiry_date}:{strike_price}:{option_type}"  # Changed key
    print(oi_volume_key)
    data = redis_client.get(oi_volume_key)
    if data:
        try:
            data = json.loads(data)  # ✅ Convert JSON string to Python list/dict
        except json.JSONDecodeError:
            return {"error": "Corrupted JSON data in Redis"}  # Handle decode error
    else:
        data = []  # If no data found, return empty list

    return {"data": data}

@app.route('/get_oi_volume_analysis', methods=['GET'])
def get_analysis():
    try:
        stock_symbol = request.args.get('stock')
        expiry_date = request.args.get('expiry')
        strike_price = request.args.get('strike')
        option_type = request.args.get('option_type')
    except Exception as e:
        print(f"Error parsing query parameters: {e}")
    if not all([stock_symbol, expiry_date, strike_price, option_type]):
        return jsonify({"error": "Missing parameters"}), 400
    data = get_oi_volume_analysis(stock_symbol, expiry_date, strike_price, option_type)
    # Ensure data is a dictionary before returning
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid data format returned from analysis function"}), 500
    return jsonify(data)

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

IST = pytz.timezone("Asia/Kolkata")
MARKET_CLOSE = datetime.strptime("20:30", "%H:%M").time()

def is_market_closed():
    """ Check if the market is closed """
    now = datetime.now(IST).time()
    return now >= MARKET_CLOSE

def fetch_and_store_orders():
    """ Fetch option chain data and store it in Redis instead of a file """

    # 🔹 Get today's dat

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

    # ✅ Step 2: Fetch new large orders

    # 🔹 Step 2: Fetch new large orders
    new_orders = []
    print("🔍 Fetching new orders...")
    for stock, lot_size in fno_stocks.items():
        result = fetch_option_chain(stock, EXPIRY_DATE, lot_size, table)
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

# Run Flask
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)

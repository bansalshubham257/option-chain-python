import os
from io import StringIO
import requests
import ta  # Technical Indicators
from flask import Flask, request, jsonify
from flask_cors import CORS
import ssl
import yfinance as yf
from scipy.signal import argrelextrema
import numpy as np
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.embed import components
import pandas as pd
from bokeh.layouts import column
import json
import threading
import time

from test import is_market_open, fno_stocks, fetch_option_chain, JSON_FILE

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

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

        df["consolidation_breakout"] = detect_consolidation_breakout(df)
        df["cup_handle"] = detect_cup_handle(df)
        df["head_shoulders"] = detect_head_shoulders(df)
        df["double_top_bottom"] = detect_double_top_bottom(df)

        return df # Ensure only valid rows are returned

    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return None


# ✅ Consolidation Breakout Detection
def detect_consolidation_breakout(df, window=20, threshold=2):
    """Detects if the stock is breaking out from a period of consolidation."""
    rolling_std = df["last_price"].rolling(window=window).std()
    breakout_signal = (rolling_std < threshold) & (df["last_price"] > df["upper_band"])
    return breakout_signal.map({True: "Breakout", False: "No Breakout"})

# ✅ Cup and Handle Pattern Detection
def detect_cup_handle(df):
    """Detects Cup and Handle pattern based on moving averages and price movement."""
    df["slope_50"] = df["SMA_50"].diff()
    is_cup_handle = (df["slope_50"] < 0).rolling(window=30).sum() > 20  # Identifying a rounded bottom
    return is_cup_handle.map({True: "Cup & Handle", False: "No Pattern"})

# ✅ Head and Shoulders Detection
def detect_head_shoulders(df):
    """Detects Head and Shoulders pattern based on peaks and troughs."""
    df["local_max"] = df["last_price"].rolling(window=5, center=True).max()
    df["local_min"] = df["last_price"].rolling(window=5, center=True).min()

    peaks = argrelextrema(df["last_price"].values, np.greater, order=5)[0]
    troughs = argrelextrema(df["last_price"].values, np.less, order=5)[0]

    if len(peaks) >= 3 and len(troughs) >= 2:
        return "Head & Shoulders"
    return "No Pattern"

# ✅ Double Top & Double Bottom Detection
def detect_double_top_bottom(df):
    """Detects Double Top and Double Bottom patterns."""
    peaks = argrelextrema(df["last_price"].values, np.greater, order=5)[0]
    troughs = argrelextrema(df["last_price"].values, np.less, order=5)[0]

    if len(peaks) >= 2 and abs(df.iloc[peaks[-1]]["last_price"] - df.iloc[peaks[-2]]["last_price"]) < 0.02 * df.iloc[peaks[-1]]["last_price"]:
        return "Double Top"

    if len(troughs) >= 2 and abs(df.iloc[troughs[-1]]["last_price"] - df.iloc[troughs[-2]]["last_price"]) < 0.02 * df.iloc[troughs[-1]]["last_price"]:
        return "Double Bottom"

    return "No Pattern"

# 📌 2️⃣ Analyze Stock Signals

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

    latest = df.iloc[-1]
    bullish_signals = []
    bearish_signals = []
    active_indicators = {}

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
        "div": div
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
        
def fetch_and_store_orders():
    """ Fetch option chain data in a separate thread to prevent timeout """
    if not is_market_open():
        print("Market is closed. Skipping script execution.")
        return
    
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as file:
            try:
                all_orders = json.load(file)
            except json.JSONDecodeError:
                all_orders = []  # In case of file corruption
    else:
        all_orders = []

    # ✅ Step 2: Fetch new large orders
    new_orders = []

    for stock, lot_size in fno_stocks.items():
        result = fetch_option_chain(stock, EXPIRY_DATE, lot_size)
        if result:
            new_orders.extend(result)

    all_orders.extend(new_orders)
    
    # Save to JSON file
    with open(JSON_FILE, 'w') as file:
        json.dump(all_orders, file)
    
    print(f"✅ Orders after update: {len(all_orders)} (New Orders Added: {len(all_orders) - len(all_orders)})")

last_run_time = 0
CACHE_DURATION = 30  # Cache data for 30 seconds

@app.route('/run-script', methods=['GET'])
def run_script():
    global last_run_time
    """ Trigger script asynchronously to avoid Render timeout """
    if not is_market_open():
        return jsonify({'status': 'Market is closed'})

    current_time = time.time()
    if current_time - last_run_time < CACHE_DURATION:
        return jsonify({'status': 'Using cached result. Try again later.'}), 200

    # Update last run time
    last_run_time = current_time
    
    thread = threading.Thread(target=fetch_and_store_orders)
    thread.start()  # Start script in background

    return jsonify({'status': 'Script is running in the background'}), 202


# Run Flask
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)


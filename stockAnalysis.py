from io import StringIO
import requests
import pandas as pd
import ta  # Technical Indicators
from flask import Flask, request, jsonify
from flask_cors import CORS
import ssl
import yfinance as yf

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://swingtradingwithme.blogspot.com"]}})
ssl._create_default_https_context = ssl._create_unverified_context

NSE_CSV_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

# 📌 1️⃣ Fetch All NSE Stocks Dynamically
def fetch_all_nse_stocks():
    """Fetch all NSE-listed stocks dynamically from NSE CSV"""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(NSE_CSV_URL, headers=headers)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        return df["SYMBOL"].tolist()
    except Exception as e:
        print(f"Error fetching NSE stocks: {e}")
        return []

# 📌 3️⃣ Fetch Live Stock Data from NSE & Merge with Historical Data
def get_stock_data(symbol):
    """
    Fetch stock data from Yahoo Finance and compute technical indicators.
    """
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="100d")  # Fetch last 100 days to ensure indicators work

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

# 📌 2️⃣ Analyze Stock Signals
def analyze_stock(symbol):
    #symbol = symbol.replace(".NS", "")  # Ensure compatibility
    df = get_stock_data(symbol)

    if df is None or df.empty:
        return {"error": "Stock data not found"}

    latest = df.iloc[-1]  # Get latest data row
    bullish_signals = []
    bearish_signals = []


    if latest["RSI"] > 70:
        bearish_signals.append("Overbought (RSI > 70)")
    elif latest["RSI"] < 30:
        bullish_signals.append("Oversold (RSI < 30)")

    if latest["MACD"] > 0:
        bullish_signals.append("MACD Bullish Crossover")
    else:
        bearish_signals.append("MACD Bearish Crossover")

    if latest["SMA_50"] > latest["SMA_200"]:
        bullish_signals.append("Golden Cross (SMA50 > SMA200)")
    else:
        bearish_signals.append("Death Cross (SMA50 < SMA200)")

    if latest["above_pivot"]:
        bullish_signals.append("Above Pivot")
    else:
        bearish_signals.append("Below Pivot")

    if latest["bollinger_signal"] == "Bullish Breakout":
        bullish_signals.append("Bollinger Band Breakout")
    elif latest["bollinger_signal"] == "Bearish Breakdown":
        bearish_signals.append("Bollinger Band Breakdown")

    if latest["last_price"] > latest["VWAP"]:
        bullish_signals.append("Above VWAP")
    else:
        bearish_signals.append("Below VWAP")

    return {
        "symbol": symbol,
        "bullish_signals": bullish_signals,
        "bearish_signals": bearish_signals,
        "verdict": "Bullish" if len(bullish_signals) > len(bearish_signals) else "Bearish"
    }

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

# Run Flask
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render provides PORT, default to 10000
    app.run(host="0.0.0.0", port=port)

import yfinance as yf
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.embed import components
import pandas as pd
from bokeh.layouts import column
import ta
import numpy as np


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

LOCAL_CSV_FILE = "nse_stocks.csv"

def fetch_all_nse_stocks():
    """Read NSE-listed stocks from a locally saved CSV file"""
    try:
        df = pd.read_csv(LOCAL_CSV_FILE)
        return df["SYMBOL"].tolist()
    except Exception as e:
        print(f"Error reading NSE stocks file: {e}")
        return []


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

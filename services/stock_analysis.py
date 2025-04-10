import yfinance as yf
import pandas as pd
import numpy as np
import ta
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.embed import components
from bokeh.layouts import column
from decimal import Decimal

class StockAnalysisService:
    def __init__(self):
        self.LOCAL_CSV_FILE = "nse_stocks.csv"
        print("stock analysis init done")

    def fetch_all_nse_stocks(self):
        """Read NSE-listed stocks from local CSV"""
        try:
            df = pd.read_csv(self.LOCAL_CSV_FILE)
            return df["SYMBOL"].tolist()
        except Exception as e:
            print(f"Error reading NSE stocks file: {e}")
            return []

    def analyze_stock(self, symbol):
        """Analyze a stock and return technical indicators"""
        df = self._get_stock_data(symbol)
        if df is None or df.empty:
            return {"error": "Stock data not found"}

        df.rename(columns={"last_price": "Close"}, inplace=True)

        # Calculate indicators
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

        # Fibonacci levels
        fib_levels, fib_high, fib_low = self._calculate_fibonacci_levels(df)
        latest_price = df.iloc[-1]["day_low"]

        # Other indicators
        supertrend_data = self._calculate_supertrend(df)
        pivots = self._find_support_resistance(df)
        trendline_status = self._detect_trendline_breakout(df)
        donchian_data = self._calculate_donchian_channels(df)

        # 52-week analysis
        position_52w, recommendation_52w = self._analyze_52_week_levels(df, latest_price)

        # Fibonacci analysis
        fibonacci_details = self._get_fibonacci_analysis(latest_price, fib_levels)

        # Generate signals
        latest = df.iloc[-1]
        bullish_signals = []
        bearish_signals = []
        active_indicators = {}

        # Price vs Fibonacci levels
        if latest_price > fib_levels["0.5"]:
            bullish_signals.append("Above 50% Fibonacci Level (Bullish)")
            active_indicators["Fibonacci"] = True
        else:
            bearish_signals.append("Below 50% Fibonacci Level (Bearish)")
            active_indicators["Fibonacci"] = True

        # RSI signals
        if latest["RSI"] > 70:
            bearish_signals.append("Overbought (RSI > 70)")
            active_indicators["RSI"] = True
        elif latest["RSI"] < 30:
            bullish_signals.append("Oversold (RSI < 30)")
            active_indicators["RSI"] = True

        # MACD signals
        if latest["MACD"] > 0:
            bullish_signals.append("MACD Bullish Crossover")
            active_indicators["MACD"] = True
        else:
            bearish_signals.append("MACD Bearish Crossover")
            active_indicators["MACD"] = True

        # SMA signals
        if latest["SMA_50"] > latest["SMA_200"]:
            bullish_signals.append("Golden Cross (SMA50 > SMA200)")
            active_indicators["SMA_50"] = True
            active_indicators["SMA_200"] = True
        else:
            bearish_signals.append("Death Cross (SMA50 < SMA200)")
            active_indicators["SMA_50"] = True
            active_indicators["SMA_200"] = True

        # Pivot signals
        if latest["above_pivot"]:
            bullish_signals.append("Above Pivot")
            active_indicators["Pivot"] = True
        else:
            bearish_signals.append("Below Pivot")
            active_indicators["Pivot"] = True

        # Bollinger Bands signals
        if latest["bollinger_signal"] == "Bullish Breakout":
            bullish_signals.append("Bollinger Band Breakout")
            active_indicators["BB_upper"] = True
            active_indicators["BB_lower"] = True
        elif latest["bollinger_signal"] == "Bearish Breakdown":
            bearish_signals.append("Bollinger Band Breakdown")
            active_indicators["BB_upper"] = True
            active_indicators["BB_lower"] = True

        # VWAP signals
        if latest["Close"] > latest["VWAP"]:
            bullish_signals.append("Above VWAP")
            active_indicators["VWAP"] = True
        else:
            bearish_signals.append("Below VWAP")
            active_indicators["VWAP"] = True

        # Generate chart
        script, div = self._generate_chart(df, active_indicators)

        # Prepare response
        additional_details = {
            "Supertrend": supertrend_data,
            "Trendline_Breakout": trendline_status,
            "Support_Resistance": pivots,
            "Donchian_Channels": donchian_data,
            "Fibonacci Analysis": fibonacci_details
        }

        if position_52w:
            additional_details["52-Week High/Low"] = [
                f"Position: {position_52w}",
                f"Recommendation: {recommendation_52w}"
            ]

        return {
            "symbol": symbol,
            "bullish_signals": bullish_signals,
            "bearish_signals": bearish_signals,
            "verdict": "Bullish" if len(bullish_signals) > len(bearish_signals) else "Bearish",
            "script": script,
            "div": div,
            "additional_details": additional_details
        }

    def _get_stock_data(self, symbol):
        """Fetch stock data from Yahoo Finance"""
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period="300d")

            if df.empty:
                return None

            # Rename columns
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "timestamp",
                "Close": "last_price",
                "High": "day_high",
                "Low": "day_low",
                "Volume": "volume"
            })

            # Calculate indicators
            df["SMA_50"] = df["last_price"].rolling(window=50).mean()
            df["SMA_200"] = df["last_price"].rolling(window=200).mean()
            df["EMA_50"] = df["last_price"].ewm(span=50, adjust=False).mean()
            df["EMA_200"] = df["last_price"].ewm(span=200, adjust=False).mean()
            df["RSI"] = ta.momentum.RSIIndicator(df["last_price"], window=14).rsi()
            df["MACD"] = ta.trend.MACD(df["last_price"]).macd()

            # Bollinger Bands
            bb = ta.volatility.BollingerBands(df["last_price"], window=20, window_dev=2)
            df["upper_band"] = bb.bollinger_hband()
            df["lower_band"] = bb.bollinger_lband()
            df["bollinger_signal"] = df.apply(
                lambda row: "Bullish Breakout" if row["last_price"] > row["upper_band"]
                else ("Bearish Breakdown" if row["last_price"] < row["lower_band"]
                      else "No breakout"), axis=1)

            # Pivot Point
            df["pivot"] = (df["day_high"] + df["day_low"] + df["last_price"]) / 3
            df["above_pivot"] = df["last_price"] > df["pivot"]

            # VWAP
            df["cumulative_vp"] = (df["last_price"] * df["volume"]).cumsum()
            df["cumulative_volume"] = df["volume"].cumsum()
            df["VWAP"] = df["cumulative_vp"] / df["cumulative_volume"]
            df.drop(columns=["cumulative_vp", "cumulative_volume"], inplace=True)

            return df

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None

    def _calculate_fibonacci_levels(self, df, lookback=75):
        """Calculate Fibonacci retracement levels"""
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

    def _get_fibonacci_analysis(self, latest_price, fib_levels):
        """Analyze Fibonacci support/resistance"""
        details = []
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

        if not next_resistance:
            next_resistance = f"Recent High ({max(fib_levels.values()):.2f})"

        details.append(f"Current Position: {current_position}")
        details.append(f"Next Support Level: {next_support}")
        details.append(f"Next Resistance Level: {next_resistance}")

        return details

    def _analyze_52_week_levels(self, df, latest_price):
        """Check if stock is near 52-week high/low"""
        high_52 = df["day_high"].max()
        low_52 = df["day_low"].min()
        threshold = 0.02  # 2% threshold

        position = None
        recommendation = None

        if latest_price >= high_52 * (1 - threshold):
            position = f"Near 52-Week High ({high_52:.2f})"
            recommendation = "Caution: The stock is near its yearly high. Consider booking profits."
        elif latest_price <= low_52 * (1 + threshold):
            position = f"Near 52-Week Low ({low_52:.2f})"
            recommendation = "Opportunity: The stock is near its yearly low. Watch for potential reversal."

        return position, recommendation

    def _calculate_supertrend(self, df, atr_period=10, factor=3):
        """Calculate Supertrend indicator"""
        if not {'day_high', 'day_low', 'Close'}.issubset(df.columns):
            raise ValueError("Missing required columns")

        df['ATR'] = ta.volatility.average_true_range(df['day_high'], df['day_low'], df['Close'], window=atr_period)
        df['UpperBand'] = ((df['day_high'] + df['day_low']) / 2) + (factor * df['ATR'])
        df['LowerBand'] = ((df['day_high'] + df['day_low']) / 2) - (factor * df['ATR'])

        df['Supertrend'] = np.nan
        df['Trend'] = np.nan

        for i in range(1, len(df)):
            prev_supertrend = df.iloc[i - 1]['Supertrend']

            if df.iloc[i - 1]['Close'] > prev_supertrend:
                df.iloc[i, df.columns.get_loc('Supertrend')] = df.iloc[i]['LowerBand'] \
                    if df.iloc[i]['Close'] > df.iloc[i]['LowerBand'] else prev_supertrend
            else:
                df.iloc[i, df.columns.get_loc('Supertrend')] = df.iloc[i]['UpperBand'] \
                    if df.iloc[i]['Close'] < df.iloc[i]['UpperBand'] else prev_supertrend

            df.iloc[i, df.columns.get_loc('Trend')] = "Bullish" if df.iloc[i]['Close'] > df.iloc[i]['Supertrend'] else "Bearish"

        return df['Trend'].iloc[-1]

    def _find_support_resistance(self, df):
        """Detect support/resistance levels"""
        high = df['day_high'].iloc[-2]
        low = df['day_low'].iloc[-2]
        close = df['Close'].iloc[-2]

        pivot = (high + low + close) / 3
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

    def _detect_trendline_breakout(self, df):
        """Detect trendline breakout"""
        latest_price = df['Close'].iloc[-1]
        support_trendline = df['day_low'].rolling(window=50).min().iloc[-1]
        resistance_trendline = df['day_high'].rolling(window=50).max().iloc[-1]

        if latest_price > resistance_trendline:
            return "Bullish Breakout"
        elif latest_price < support_trendline:
            return "Bearish Breakdown"
        else:
            return "No Breakout detected"

    def _calculate_donchian_channels(self, df, period=20):
        """Calculate Donchian Channels"""
        df['Upper'] = df['day_high'].rolling(window=period).max()
        df['Lower'] = df['day_low'].rolling(window=period).min()
        df['Middle'] = (df['Upper'] + df['Lower']) / 2

        upper = round(df['Upper'].iloc[-1], 2)
        lower = round(df['Lower'].iloc[-1], 2)
        middle = round(df['Middle'].iloc[-1], 2)
        current_price = round(df['Close'].iloc[-1], 2)

        if current_price > upper:
            trend = "Breakout above the Donchian Upper Band! Possible strong bullish momentum."
        elif current_price < lower:
            trend = "Breakdown below the Donchian Lower Band! Possible strong bearish momentum."
        else:
            trend = f"Price is currently between the Donchian range ({lower} - {upper}). Potential consolidation."

        summary = (
            f"Donchian Channel Levels:\n"
            f"Upper Band: {upper}\n"
            f"Middle Band: {middle}\n"
            f"Lower Band: {lower}\n"
            f"Current Price: {current_price}\n"
            f"Trend Analysis: {trend}"
        )

        return [("summary", summary)]

    def _generate_chart(self, df, active_indicators):
        """Generate Bokeh chart with technical indicators"""
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        source = ColumnDataSource(df)

        p = figure(x_axis_type="datetime", title="Stock Chart", width=1000, height=500)
        p_rsi = figure(x_axis_type="datetime", title="RSI", width=1000, height=200, x_range=p.x_range)
        p_macd = figure(x_axis_type="datetime", title="MACD", width=1000, height=200, x_range=p.x_range)

        inc = df["Close"] > df["Open"]
        dec = df["Open"] > df["Close"]

        p.segment(df["timestamp"], df["day_high"], df["timestamp"], df["day_low"], color="black")
        p.vbar(df["timestamp"][inc], width=12*60*60*1000, top=df["Close"][inc], bottom=df["Open"][inc],
               fill_color="green", line_color="black")
        p.vbar(df["timestamp"][dec], width=12*60*60*1000, top=df["Open"][dec], bottom=df["Close"][dec],
               fill_color="red", line_color="black")

        if active_indicators.get("SMA_50"):
            p.line(df["timestamp"], df["SMA_50"], legend_label="SMA 50", line_width=2, color="blue")
        if active_indicators.get("SMA_200"):
            p.line(df["timestamp"], df["SMA_200"], legend_label="SMA 200", line_width=2, color="red")
        if active_indicators.get("BB_upper"):
            p.line(df["timestamp"], df["BB_upper"], legend_label="Bollinger Upper", line_width=1.5,
                   color="purple", line_dash="dashed")
        if active_indicators.get("BB_lower"):
            p.line(df["timestamp"], df["BB_lower"], legend_label="Bollinger Lower", line_width=1.5,
                   color="purple", line_dash="dashed")
        if active_indicators.get("VWAP"):
            p.line(df["timestamp"], df["VWAP"], legend_label="VWAP", line_width=2,
                   color="orange", line_dash="dotted")
        if active_indicators.get("Pivot"):
            pivot_value = df["pivot"].iloc[-1]
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

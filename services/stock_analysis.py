import yfinance as yf
import pandas as pd
import numpy as np
import ta
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.embed import components
from bokeh.layouts import column
from decimal import Decimal
import pytz
from datetime import datetime, timedelta
from services.database import DatabaseService  # Import DatabaseService
import concurrent.futures
import time

class StockAnalysisService:
    def __init__(self):
        self.LOCAL_CSV_FILE = "nse_stocks.csv"
        self.database_service = DatabaseService()  # Initialize database_service
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

    def get_52_week_extremes(self, threshold=0.05):
        """Get stocks near 52-week highs or lows within threshold percentage"""
        try:
            nse_stocks = self.fetch_all_nse_stocks()
            if not nse_stocks:
                return {"error": "No stocks found"}

            results = []
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)

            for symbol in nse_stocks:
                try:
                    if not symbol or len(symbol) < 2:
                        continue

                    full_symbol = f"{symbol}.NS"
                    stock = yf.Ticker(full_symbol)
                    hist = stock.history(period="1y")

                    if hist.empty or len(hist) < 5:  # Minimum data points
                        continue

                    # Robust timezone handling
                    if hist.index.tz is None:
                        # If naive, localize to UTC (Yahoo Finance default)
                        hist.index = hist.index.tz_localize('UTC')
                    # Convert to IST regardless of original timezone
                    hist.index = hist.index.tz_convert(ist)

                    high_52 = hist["High"].max()
                    low_52 = hist["Low"].min()
                    current = hist["Close"].iloc[-1]

                    pct_from_high = (high_52 - current) / high_52
                    pct_from_low = (current - low_52) / low_52

                    # Get timezone-aware timestamps for highs and lows
                    high_date = hist["High"].idxmax()
                    low_date = hist["Low"].idxmin()

                    # Calculate days since high/low
                    days_since_high = (now - high_date).days
                    days_since_low = (now - low_date).days

                    # Near 52-week high
                    if pct_from_high <= threshold:
                        results.append({
                            "symbol": symbol,
                            "current_price": round(current, 2),
                            "week52_high": round(high_52, 2),
                            "week52_low": round(low_52, 2),
                            "pct_from_high": round(pct_from_high * 100, 2),
                            "pct_from_low": round(pct_from_low * 100, 2),
                            "days_since_high": days_since_high,
                            "days_since_low": days_since_low,
                            "status": "near_high"
                        })

                    # Near 52-week low
                    if pct_from_low <= threshold:
                        results.append({
                            "symbol": symbol,
                            "current_price": round(current, 2),
                            "week52_high": round(high_52, 2),
                            "week52_low": round(low_52, 2),
                            "pct_from_high": round(pct_from_high * 100, 2),
                            "pct_from_low": round(pct_from_low * 100, 2),
                            "days_since_high": days_since_high,
                            "days_since_low": days_since_low,
                            "status": "near_low"
                        })

                except Exception as e:
                    print(f"Error processing {symbol}: {str(e)}")
                    continue

            return results

        except Exception as e:
            print(f"Error in get_52_week_extremes: {str(e)}")
            return []

    def fetch_and_store_financials(self):
        """Fetch and store quarterly financial data for all NSE stocks with optimized batch processing."""
        nse_stocks = self.fetch_all_nse_stocks()
        if not nse_stocks:
            print("No stocks found to fetch financials.")
            return

        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        print(f"Starting financial data collection for {len(nse_stocks)} stocks at {now.strftime('%Y-%m-%d %H:%M:%S')}")

        # Configuration for batch processing
        BATCH_SIZE = 50  # Number of stocks in each batch
        MAX_WORKERS = 8  # Maximum number of parallel workers
        MAX_RETRIES = 3  # Maximum number of retries for a failed request
        BASE_DELAY = 1.0  # Base delay between API calls in seconds
        
        # Split stocks into batches
        batches = [nse_stocks[i:i + BATCH_SIZE] for i in range(0, len(nse_stocks), BATCH_SIZE)]
        total_batches = len(batches)
        
        print(f"Processing {total_batches} batches with {BATCH_SIZE} stocks per batch, using {MAX_WORKERS} workers")
        
        results_by_batch = []
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Track API rate limiting
        rate_limit_delay = BASE_DELAY
        consecutive_failures = 0
        
        # Process each batch with adaptive rate limiting
        for batch_idx, batch in enumerate(batches):
            batch_start_time = time.time()
            print(f"Processing batch {batch_idx+1}/{total_batches} with {len(batch)} stocks...")
            
            batch_results = []
            batch_failures = 0
            
            # Function to fetch financials with retries and adaptive delay
            def fetch_financials_with_retry(symbol, retry_count=0):
                nonlocal consecutive_failures, rate_limit_delay
                
                try:
                    # Apply adaptive delay to avoid rate limiting
                    time.sleep(rate_limit_delay)
                    
                    full_symbol = f"{symbol}.NS"
                    stock = yf.Ticker(full_symbol)
                    
                    # Get next earnings date from calendar if available
                    next_earnings_date = None
                    try:
                        # Properly handle different types of calendar data
                        if hasattr(stock, 'calendar'):
                            # Check if calendar is a DataFrame
                            if hasattr(stock.calendar, 'empty') and not stock.calendar.empty:
                                if 'Earnings Date' in stock.calendar.columns:
                                    # Handle case where 'Earnings Date' contains a list of dates
                                    earnings_date_value = stock.calendar['Earnings Date'].iloc[0]
                                    if isinstance(earnings_date_value, list) or isinstance(earnings_date_value, tuple):
                                        next_earnings_date = earnings_date_value[0]
                                    else:
                                        next_earnings_date = earnings_date_value
                            # Handle case where calendar is a dict (newer versions of yfinance)
                            elif isinstance(stock.calendar, dict) and stock.calendar:
                                # In dictionary format, the key might be 'Earnings Date' or similar
                                earnings_key = next((k for k in stock.calendar.keys() 
                                                   if k.lower().startswith('earnings')), None)
                                if earnings_key and stock.calendar[earnings_key]:
                                    earnings_date_value = stock.calendar[earnings_key]
                                    if isinstance(earnings_date_value, list) or isinstance(earnings_date_value, tuple):
                                        next_earnings_date = earnings_date_value[0]
                                    else:
                                        next_earnings_date = earnings_date_value
                        
                        # Convert date to datetime object if needed
                        if isinstance(next_earnings_date, pd.Timestamp):
                            next_earnings_date = next_earnings_date.to_pydatetime()
                        elif hasattr(next_earnings_date, 'date') and callable(getattr(next_earnings_date, 'date')):
                            # Handle date objects that have a date() method
                            next_earnings_date = datetime.combine(next_earnings_date.date(), datetime.min.time())
                        elif isinstance(next_earnings_date, (str, int, float)):
                            # Try to parse strings or convert numbers
                            try:
                                next_earnings_date = pd.to_datetime(next_earnings_date).to_pydatetime()
                            except:
                                next_earnings_date = None
                    except Exception as e:
                        print(f"Error extracting earnings date for {symbol}: {e}")
                        next_earnings_date = None
                        # Continue processing despite earnings date extraction failure
                    
                    # Attempt to fetch quarterly financials - try multiple report types
                    financials_data = None
                    for data_type in ['quarterly_financials', 'quarterly_income_stmt', 'quarterly_balance_sheet']:
                        try:
                            data = getattr(stock, data_type, None)
                            if data is not None and not data.empty:
                                financials_data = data
                                break
                        except Exception:
                            continue
                    
                    # Process fetched data if available
                    if financials_data is not None and not financials_data.empty:
                        # Process financial data
                        financials = []
                        for quarter in financials_data.columns:
                            quarter_date = None
                            if isinstance(quarter, pd.Timestamp):
                                quarter_date = quarter.to_pydatetime()
                            else:
                                try:
                                    quarter_date = pd.to_datetime(quarter).to_pydatetime()
                                except:
                                    continue  # Skip this quarter if date can't be parsed
                            
                            if quarter_date.tzinfo is None:
                                quarter_date = pytz.utc.localize(quarter_date)
                            
                            # Calculate type based on the current time
                            type = "current" if (now - quarter_date).days <= 90 else "previous"
                            
                            # Safely access rows using `.get` method
                            revenue = None
                            for rev_field in ['Total Revenue', 'Revenue', 'Net Revenue', 'Gross Revenue']:
                                if rev_field in financials_data.index:
                                    revenue = financials_data.loc[rev_field, quarter]
                                    break
                            
                            # Handle other financial metrics similarly with failover fields
                            net_income = None
                            for ni_field in ['Net Income', 'Net Income Common Stockholders', 'Net Income From Continuing Operations']:
                                if ni_field in financials_data.index:
                                    net_income = financials_data.loc[ni_field, quarter]
                                    break
                                    
                            operating_income = None
                            for oi_field in ['Operating Income', 'EBIT', 'Operating Profit']:
                                if oi_field in financials_data.index:
                                    operating_income = financials_data.loc[oi_field, quarter]
                                    break
                                    
                            ebitda = None
                            if 'EBITDA' in financials_data.index:
                                ebitda = financials_data.loc['EBITDA', quarter]
                            
                            financials.append({
                                "quarter_ending": quarter_date,
                                "revenue": revenue,
                                "net_income": net_income,
                                "operating_income": operating_income,
                                "ebitda": ebitda,
                                "type": type
                            })
                        
                        # Reset rate limit delay on success and consecutive failures
                        consecutive_failures = 0
                        rate_limit_delay = max(BASE_DELAY, rate_limit_delay * 0.9)  # Gradually reduce delay on success
                        
                        return {"symbol": symbol, "financials": financials, "next_earnings_date": next_earnings_date}
                    else:
                        # Still return earnings date if we found it, even without financials
                        if next_earnings_date:
                            consecutive_failures = 0  # Not a complete failure
                            return {"symbol": symbol, "financials": [], "next_earnings_date": next_earnings_date}
                        
                        # No usable data found
                        if retry_count < MAX_RETRIES:
                            # Increment consecutive failures and retry with backoff
                            consecutive_failures += 1
                            retry_delay = BASE_DELAY * (2 ** retry_count)
                            time.sleep(retry_delay)
                            return fetch_financials_with_retry(symbol, retry_count + 1)
                        else:
                            return None
                            
                except Exception as e:
                    # On exception, increase consecutive failures and retry with backoff
                    consecutive_failures += 1
                    
                    # Adjust rate limit delay based on consecutive failures
                    if consecutive_failures > 3:
                        rate_limit_delay = min(5.0, rate_limit_delay * 1.5)  # Increase delay if facing many failures
                    
                    if retry_count < MAX_RETRIES:
                        retry_delay = BASE_DELAY * (2 ** retry_count) 
                        time.sleep(retry_delay)
                        return fetch_financials_with_retry(symbol, retry_count + 1)
                    else:
                        print(f"Error processing {symbol} after {MAX_RETRIES} retries: {e}")
                        return None
            
            # Process current batch with parallel workers
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all stocks in the batch
                future_to_symbol = {executor.submit(fetch_financials_with_retry, symbol): symbol for symbol in batch}
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        if result:
                            if result["financials"]:
                                success_count += 1
                                batch_results.append(result)
                            else:
                                skipped_count += 1
                                # Still add to results if we have an earnings date
                                if result.get("next_earnings_date"):
                                    batch_results.append(result)
                        else:
                            failed_count += 1
                            batch_failures += 1
                    except Exception as e:
                        print(f"Exception processing {symbol}: {e}")
                        failed_count += 1
                        batch_failures += 1
            
            # Save batch results to database in a single batch operation
            if batch_results:
                self.database_service.save_quarterly_financials_batch(batch_results)
                results_by_batch.append(batch_results)
            
            # Calculate batch stats
            batch_time = time.time() - batch_start_time
            batch_success_rate = (len(batch) - batch_failures) / len(batch) * 100
            
            print(f"Batch {batch_idx+1}/{total_batches} completed in {batch_time:.2f}s with {batch_success_rate:.1f}% success rate")
            print(f"Progress: {success_count} successful, {skipped_count} skipped, {failed_count} failed")
            
            # Adaptive batch delay based on success rate
            if batch_success_rate < 70:
                # If success rate is low, add a cooldown period
                cooldown = min(30, 5 * (1 + (70 - batch_success_rate) / 10))
                print(f"Low success rate detected. Cooling down for {cooldown:.1f}s before next batch")
                time.sleep(cooldown)
        
        total_processed = success_count + skipped_count
        print(f"Financial data collection completed: {success_count} stocks with financials, "
              f"{skipped_count} stocks with only earnings date, {failed_count} failed stocks.")
        print(f"Success rate: {(total_processed / len(nse_stocks)) * 100:.1f}%")
        
        # Return overall statistics
        return {
            "total_stocks": len(nse_stocks),
            "success_count": success_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "completion_time": (datetime.now(ist) - now).total_seconds()
        }

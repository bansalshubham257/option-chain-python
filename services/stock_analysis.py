import pandas as pd
import numpy as np
import ta
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, Legend, LabelSet
from bokeh.embed import components
from bokeh.layouts import column, row
from bokeh.palettes import Category10
from decimal import Decimal
import pytz
from datetime import datetime, timedelta
from services.database import DatabaseService  # Import DatabaseService
from services.option_chain import OptionChainService
import concurrent.futures
import time
import requests
import random
from curl_cffi import requests
import yfinance as yf


class StockAnalysisService:
    def __init__(self):
        self.option_chain_service = OptionChainService()
        self.LOCAL_CSV_FILE = "nse_stocks.csv"
        self.database_service = DatabaseService()  # Initialize database_service
        self.session = requests.Session(impersonate="chrome110")
    

    def fetch_all_nse_stocks(self):
        """Read NSE-listed stocks from local CSV"""
        try:
            df = pd.read_csv(self.LOCAL_CSV_FILE)
            return df["SYMBOL"].tolist()
        except Exception as e:
            print(f"Error reading NSE stocks file: {e}")
            return []

    def analyze_stock(self, symbol):
        """Analyze a stock and return technical indicators using DB data"""
        df = self._get_stock_data_from_db(symbol)
        if df is None or df.empty:
            return {"error": "Stock data not found in database"}

        # Get latest price and pivot points from data
        latest_data = df.iloc[-1]
        latest_price = latest_data["close"]

        # Use pivot levels from data
        pivot = latest_data["pivot"]
        r1 = latest_data["r1"]
        r2 = latest_data["r2"]
        r3 = latest_data["r3"]
        s1 = latest_data["s1"]
        s2 = latest_data["s2"]
        s3 = latest_data["s3"]

        # Get fundamental data if available
        fundamental_data = self._get_fundamental_data_from_db(symbol)

        # Get scanner data if available
        scanner_data = self._get_scanner_results(symbol)

        # Other indicators
        supertrend_data = self._get_supertrend_from_db(df)
        pivots = self._get_pivots_from_db(df)
        trendline_status = self._detect_trendline_breakout_from_db(df)
        donchian_data = self._get_donchian_from_db(df)
        
        # 52-week analysis
        position_52w, recommendation_52w = self._analyze_52_week_levels(df, latest_price)

        # Analyze price in relation to pivot levels
        pivot_analysis = self._get_pivot_analysis(latest_price, pivot, r1, r2, r3, s1, s2, s3)

        # Generate signals
        latest = df.iloc[-1]
        bullish_signals = []
        bearish_signals = []
        active_indicators = {}

        # Price vs Pivot levels
        if latest_price > pivot:
            bullish_signals.append("Above Pivot Level (Bullish)")
            active_indicators["Pivot"] = True
            if latest_price > r1:
                bullish_signals.append("Above R1 Resistance (Strong Bullish)")
                if latest_price > r2:
                    bullish_signals.append("Above R2 Resistance (Very Bullish)")
        else:
            bearish_signals.append("Below Pivot Level (Bearish)")
            active_indicators["Pivot"] = True
            if latest_price < s1:
                bearish_signals.append("Below S1 Support (Strong Bearish)")
                if latest_price < s2:
                    bearish_signals.append("Below S2 Support (Very Bearish)")

        # RSI signals
        if latest["rsi"] > 70:
            bearish_signals.append("Overbought (RSI > 70)")
            active_indicators["RSI"] = True
        elif latest["rsi"] < 30:
            bullish_signals.append("Oversold (RSI < 30)")
            active_indicators["RSI"] = True

        # MACD signals
        if latest["macd_line"] > latest["macd_signal"]:
            bullish_signals.append("MACD Bullish Crossover")
            active_indicators["MACD"] = True
        else:
            bearish_signals.append("MACD Bearish Crossover")
            active_indicators["MACD"] = True

        # SMA signals
        if latest["sma50"] > latest["sma200"]:
            bullish_signals.append("Golden Cross (SMA50 > SMA200)")
            active_indicators["SMA_50"] = True
            active_indicators["SMA_200"] = True
        else:
            bearish_signals.append("Death Cross (SMA50 < SMA200)")
            active_indicators["SMA_50"] = True
            active_indicators["SMA_200"] = True

        # Pivot signals - using pivot columns from DB
        if latest["close"] > latest["pivot"]:
            bullish_signals.append("Above Pivot")
            active_indicators["Pivot"] = True
        else:
            bearish_signals.append("Below Pivot")
            active_indicators["Pivot"] = True

        # Bollinger Bands signals
        if latest["close"] > latest["upper_bollinger"]:
            bullish_signals.append("Bollinger Band Breakout")
            active_indicators["BB_upper"] = True
            active_indicators["BB_lower"] = True
        elif latest["close"] < latest["lower_bollinger"]:
            bearish_signals.append("Bollinger Band Breakdown")
            active_indicators["BB_upper"] = True
            active_indicators["BB_lower"] = True

        # VWAP signals
        if "vwap" in latest and latest["vwap"] is not None:
            if latest["close"] > latest["vwap"]:
                bullish_signals.append("Above VWAP")
                active_indicators["VWAP"] = True
            else:
                bearish_signals.append("Below VWAP")
                active_indicators["VWAP"] = True

        # Generate chart
        script, div = self._generate_chart_from_db(df, active_indicators)

        # Prepare response with additional details
        additional_details = {
            "Supertrend": supertrend_data,
            "Trendline_Breakout": trendline_status,
            "Support_Resistance": pivots,
            "Donchian_Channels": donchian_data,
            "Pivot_Analysis": pivot_analysis
        }

        # Add scanner results if available
        if scanner_data:
            additional_details["Scanner_Signals"] = scanner_data

        # Add fundamental data
        if fundamental_data:
            additional_details["Fundamentals"] = fundamental_data

        if position_52w:
            additional_details["52-Week High/Low"] = [
                f"Position: {position_52w}",
                f"Recommendation: {recommendation_52w}"
            ]

        return {
            "symbol": symbol,
            "current_price": latest_price,
            "price_change": latest["price_change"],
            "percentage_change": latest["percentage_change"],
            "bullish_signals": bullish_signals,
            "bearish_signals": bearish_signals,
            "verdict": "Bullish" if len(bullish_signals) > len(bearish_signals) else "Bearish",
            "script": script,
            "div": div,
            "additional_details": additional_details
        }

    def _get_stock_data_from_db(self, symbol):
        """Fetch stock data from database instead of yfinance"""
        try:
            with self.database_service._get_cursor() as cur:
                # Query database for stock data with all necessary indicators
                cur.execute("""
                    SELECT 
                        timestamp, open, high, low, close, volume, vwap,
                        pivot, r1, r2, r3, s1, s2, s3,
                        sma20, sma50, sma100, sma200, 
                        ema50, ema200,
                        rsi, macd_line, macd_signal, macd_histogram,
                        upper_bollinger, lower_bollinger, middle_bollinger,
                        Supertrend,
                        Donchian_High, Donchian_Low,
                        adx, adx_di_positive, adx_di_negative,
                        cci, mfi, williams_r, force_index,
                        atr, trix, roc, price_change, percent_change
                    FROM stock_data_cache
                    WHERE symbol = %s AND interval = '1d'
                    ORDER BY timestamp DESC
                    LIMIT 300
                """, (symbol,))
                
                rows = cur.fetchall()
                if not rows:
                    return None

                # Convert to DataFrame
                columns = [
                    "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "pivot", "r1", "r2", "r3", "s1", "s2", "s3",
                    "sma20", "sma50", "sma100", "sma200",
                    "ema50", "ema200",
                    "rsi", "macd_line", "macd_signal", "macd_histogram",
                    "upper_bollinger", "lower_bollinger", "middle_bollinger",
                    "Supertrend",
                    "Donchian_High", "Donchian_Low",
                    "adx", "adx_di_positive", "adx_di_negative",
                    "cci", "mfi", "williams_r", "force_index",
                    "atr", "trix", "roc", "price_change", "percentage_change"
                ]

                df = pd.DataFrame(rows, columns=columns)
                df.set_index("timestamp", inplace=True)
                df = df.sort_index()

                # Convert all values to appropriate types
                numeric_cols = df.columns.difference(["timestamp"])
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                return df

        except Exception as e:
            print(f"Error fetching stock data from database for {symbol}: {str(e)}")
            return None

    def _get_fundamental_data_from_db(self, symbol_with_ns):
        """Fetch fundamental data from database for a given stock"""
        try:
            symbol = symbol_with_ns.replace(".NS", "")
            with self.database_service._get_cursor() as cur:
                # First, try to get next earnings date if available
                earnings_date = None
                cur.execute("""
                    SELECT next_earnings_date
                    FROM stock_earnings
                    WHERE symbol = %s
                """, (symbol,))
                
                earnings_result = cur.fetchone()
                if earnings_result and earnings_result[0]:
                    earnings_date = earnings_result[0]
                
                # Check if stock_industry_info table exists before querying it

                industry_info = {}
                cur.execute("""
                        SELECT industry, sector
                        FROM stock_data_cache
                        WHERE symbol = %s
                    """, (symbol_with_ns,))

                industry_result = cur.fetchone()
                
                if industry_result:
                    industry_info = {
                        "Industry": industry_result[0],
                        "Sector": industry_result[1]
                    }

                fundamental_metrics = {}
                if cur.fetchone()[0]:  # Columns exist
                    # Get key fundamental metrics
                    cur.execute("""
                        SELECT pe_ratio, market_cap, dividend_yield, dividend_rate, total_debt
                        FROM stock_data_cache
                        WHERE symbol = %s AND interval = '1d'
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (symbol_with_ns,))
                    
                    metrics_result = cur.fetchone()
                    if metrics_result:
                        # Format market cap and total debt in more readable format (B for billions, M for millions)
                        market_cap = metrics_result[1]
                        if market_cap:
                            if market_cap >= 1_000_000_000:
                                market_cap_formatted = f"{market_cap/1_000_000_000:.2f}B"
                            else:
                                market_cap_formatted = f"{market_cap/1_000_000:.2f}M"
                        else:
                            market_cap_formatted = "N/A"
                            
                        total_debt = metrics_result[4]
                        if total_debt:
                            if total_debt >= 1_000_000_000:
                                total_debt_formatted = f"{total_debt/1_000_000_000:.2f}B"
                            else:
                                total_debt_formatted = f"{total_debt/1_000_000:.2f}M"
                        else:
                            total_debt_formatted = "N/A"

                        fundamental_metrics = {
                            "P/E Ratio": round(metrics_result[0], 2) if metrics_result[0] else "N/A",
                            "Market Cap": market_cap_formatted,
                            "Dividend Yield": f"{metrics_result[2]:.2f}%" if metrics_result[2] else "N/A",
                            "Dividend Rate": metrics_result[3] if metrics_result[3] else "N/A",
                            "Total Debt": total_debt_formatted
                        }

                # Get quarterly financials data
                cur.execute("""
                    SELECT quarter_ending, revenue, net_income, operating_income, ebitda, type
                    FROM quarterly_financials
                    WHERE symbol = %s
                    ORDER BY quarter_ending DESC
                    LIMIT 4
                """, (symbol,))

                rows = cur.fetchall()
                if not rows:
                    return None

                # Format the quarterly financial data
                financials = []
                for row in rows:
                    qtr_data = {
                        "quarter_ending": row[0].strftime('%Y-%m-%d') if row[0] else None,
                        "revenue": float(row[1]) if row[1] is not None else None,
                        "net_income": float(row[2]) if row[2] is not None else None,
                        "operating_income": float(row[3]) if row[3] is not None else None,
                        "ebitda": float(row[4]) if row[4] is not None else None,
                        "type": row[5]
                    }
                    financials.append(qtr_data)

                # Calculate growth metrics if we have at least 2 quarters
                growth_metrics = {}
                if len(financials) >= 2:
                    current = financials[0]
                    previous = financials[1]

                    if current["revenue"] and previous["revenue"] and previous["revenue"] != 0:
                        revenue_growth = ((current["revenue"] - previous["revenue"]) / previous["revenue"]) * 100
                        growth_metrics["Revenue_Growth(QoQ)"] = f"{revenue_growth:.2f}%"

                    if current["net_income"] and previous["net_income"] and previous["net_income"] != 0 and previous["net_income"] > 0:
                        income_growth = ((current["net_income"] - previous["net_income"]) / abs(previous["net_income"])) * 100
                        growth_metrics["Net_Income_Growth(QoQ)"] = f"{income_growth:.2f}%"

                # Generate financial charts if we have data
                financials_chart_script, financials_chart_div = self._generate_quarterly_financials_chart(financials)

                result = {
                    "Next_Earnings": earnings_date.strftime("%Y-%m-%d") if hasattr(earnings_date, "strftime") else str(earnings_date) if earnings_date else "Not available",
                    "Quarterly_Financials": financials,
                    "Growth_Metrics": growth_metrics,
                    "Financials_Chart": {
                        "script": financials_chart_script,
                        "div": financials_chart_div
                    }
                }
                
                # Add industry info if available
                if industry_info:
                    result["Company_Info"] = industry_info
                    
                # Add fundamental metrics if available
                if fundamental_metrics:
                    result["Key_Metrics"] = fundamental_metrics
                
                return result
                
        except Exception as e:
            print(f"Error fetching fundamental data for {symbol}: {str(e)}")
            return None

    def _get_scanner_results(self, symbol):
        """Fetch scanner results for a stock from the scanner_results table"""
        try:
            # Remove .NS suffix if present for query
            
            with self.database_service._get_cursor() as cur:
                # First, check if we have any recent scanner results
                cur.execute("""
                    SELECT scan_date 
                    FROM scanner_results 
                    WHERE stock_name = %s 
                    ORDER BY scan_date DESC 
                    LIMIT 1
                """, (symbol,))
                
                date_result = cur.fetchone()
                if not date_result:
                    return None  # No scanner results found
                
                latest_date = date_result[0]
                
                # Get all column names for construction of dynamic query
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'scanner_results' 
                    AND column_name != 'id' 
                    AND column_name != 'stock_name' 
                    AND column_name != 'scan_date'
                    AND data_type = 'boolean'
                """)
                
                columns = [col[0] for col in cur.fetchall()]
                
                # Construct a dynamic query to fetch all TRUE boolean columns
                condition_parts = []
                for col in columns:
                    condition_parts.append(f"{col} = TRUE")
                
                conditions = " OR ".join(condition_parts)
                
                # Fetch the latest scanner results where any condition is TRUE
                query = f"""
                    SELECT * 
                    FROM scanner_results 
                    WHERE stock_name = %s 
                    AND scan_date = %s
                    AND ({conditions})
                """
                
                cur.execute(query, (symbol, latest_date))
                result = cur.fetchone()
                
                if not result:
                    return None  # No TRUE signals found
                
                # Get the column names
                col_names = [desc[0] for desc in cur.description]
                
                # Create a dictionary from column names and values
                row_dict = dict(zip(col_names, result))
                
                # Extract only the TRUE conditions (excluding id, stock_name, scan_date)
                active_signals = []
                for col, value in row_dict.items():
                    if col not in ['id', 'stock_name', 'scan_date'] and value is True:
                        # Format the column name for better readability
                        readable_name = col.replace('_', ' ').title()
                        active_signals.append(readable_name)
                
                # Group signals into categories
                signal_categories = self._categorize_scanner_signals(active_signals)
                
                return signal_categories
                
        except Exception as e:
            print(f"Error fetching scanner results for {symbol}: {str(e)}")
            return None

    def _categorize_scanner_signals(self, signals):
        """Categorize scanner signals for better presentation"""
        categories = {
            "Price Action": [],
            "Moving Averages": [],
            "RSI Signals": [],
            "MACD Signals": [],
            "Bollinger Bands": [],
            "Chart Patterns": [],
            "Candlestick Patterns": [],
            "Momentum": [],
            "Oscillators": [],
            "Breakouts": [],
            "Other": []
        }
        
        for signal in signals:
            signal_lower = signal.lower()
            
            # Categorize the signal based on keywords
            if any(x in signal_lower for x in ['sma', 'ema', 'cross']):
                categories["Moving Averages"].append(signal)
            elif 'rsi' in signal_lower:
                categories["RSI Signals"].append(signal)
            elif 'macd' in signal_lower:
                categories["MACD Signals"].append(signal)
            elif 'bollinger' in signal_lower:
                categories["Bollinger Bands"].append(signal)
            elif any(x in signal_lower for x in ['pattern', 'head', 'shoulder', 'triangle', 'wedge', 'rectangle']):
                categories["Chart Patterns"].append(signal)
            elif any(x in signal_lower for x in ['candlestick', 'doji', 'hammer', 'engulf', 'marubozu', 'star']):
                categories["Candlestick Patterns"].append(signal)
            elif any(x in signal_lower for x in ['momentum', 'increasing', 'decreasing']):
                categories["Momentum"].append(signal)
            elif any(x in signal_lower for x in ['cci', 'mfi', 'williams', 'roc', 'stochastic']):
                categories["Oscillators"].append(signal)
            elif any(x in signal_lower for x in ['breakout', 'breakdown']):
                categories["Breakouts"].append(signal)
            elif any(x in signal_lower for x in ['behavior', 'opening', 'high', 'low', 'week']):
                categories["Price Action"].append(signal)
            else:
                categories["Other"].append(signal)
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}

    def _generate_quarterly_financials_chart(self, financials):
        """Generate a bar chart for quarterly financial data"""
        if not financials or len(financials) < 2:
            return "", ""
        
        try:
            # Convert financials to DataFrame
            df = pd.DataFrame(financials)
            
            # Reverse to show oldest to newest quarters
            df = df.iloc[::-1]
            
            # Extract quarter names (e.g., Q1 2023) from dates
            df['quarter_name'] = df['quarter_ending'].apply(lambda x: 
                f"Q{(pd.to_datetime(x).month - 1) // 3 + 1} {pd.to_datetime(x).year}" if x else "")
            
            # Add pre-formatted text columns for labels
            df['revenue_formatted'] = df['revenue'].apply(lambda x: f"{x/1000000:.1f}M" if x and not np.isnan(x) else "")
            df['net_income_formatted'] = df['net_income'].apply(lambda x: f"{x/1000000:.1f}M" if x and not np.isnan(x) else "")
            
            # Create source for the chart
            source = ColumnDataSource(df)
            
            # Create figure for revenue
            p_revenue = figure(
                x_range=df['quarter_name'].tolist(),
                height=300,
                width=600,
                title="Quarterly Revenue",
                toolbar_location="right",
                tools="pan,box_zoom,reset,save",
            )
            
            # Add revenue bars
            revenue_renderer = p_revenue.vbar(
                x='quarter_name',
                top='revenue',
                width=0.6,
                source=source,
                fill_color=Category10[10][0],
                line_color=Category10[10][0],
                fill_alpha=0.7,
                hover_alpha=1.0,
            )
            
            # Add tooltips for revenue
            hover_revenue = HoverTool(
                tooltips=[
                    ("Quarter", "@quarter_name"),
                    ("Revenue", "@revenue{$0.00 a}"),
                ],
                renderers=[revenue_renderer],
                mode='mouse'
            )
            p_revenue.add_tools(hover_revenue)
            
            # Set axis labels
            p_revenue.xaxis.axis_label = "Quarter"
            p_revenue.yaxis.axis_label = "Revenue ($)"
            
            # Add value labels above bars - use pre-formatted text
            revenue_labels = LabelSet(
                x='quarter_name',
                y='revenue',
                text='revenue_formatted',
                level='glyph',
                x_offset=-20,
                y_offset=8,
                source=source,
                text_font_size="8pt",
                text_color="black",
                text_font_style="bold",
                text_baseline="middle",
                text_align="center"
            )
            p_revenue.add_layout(revenue_labels)
            
            # Create figure for net income
            p_income = figure(
                x_range=df['quarter_name'].tolist(),
                height=300,
                width=600,
                title="Quarterly Net Income",
                toolbar_location="right",
                tools="pan,box_zoom,reset,save",
            )
            
            # Add net income bars
            income_renderer = p_income.vbar(
                x='quarter_name',
                top='net_income',
                width=0.6,
                source=source,
                fill_color=Category10[10][1],
                line_color=Category10[10][1],
                fill_alpha=0.7,
                hover_alpha=1.0,
            )
            
            # Add tooltips for net income
            hover_income = HoverTool(
                tooltips=[
                    ("Quarter", "@quarter_name"),
                    ("Net Income", "@net_income{$0.00 a}"),
                ],
                renderers=[income_renderer],
                mode='mouse'
            )
            p_income.add_tools(hover_income)
            
            # Set axis labels
            p_income.xaxis.axis_label = "Quarter"
            p_income.yaxis.axis_label = "Net Income ($)"
            
            # Add value labels above bars - use pre-formatted text
            income_labels = LabelSet(
                x='quarter_name',
                y='net_income',
                text='net_income_formatted',
                level='glyph',
                x_offset=-20,
                y_offset=8,
                source=source,
                text_font_size="8pt",
                text_color="black",
                text_font_style="bold",
                text_baseline="middle",
                text_align="center"
            )
            p_income.add_layout(income_labels)
            
            # Style the charts
            for p in [p_revenue, p_income]:
                p.xgrid.grid_line_color = None
                p.y_range.start = 0
                p.title.text_font_size = '12pt'
                p.title.text_font_style = 'bold'
                p.xaxis.major_label_orientation = 0.7
            
            # Combine plots into a column layout
            layout = column(p_revenue, p_income)
            
            # Generate the components
            script, div = components(layout)
            
            return script, div
            
        except Exception as e:
            print(f"Error generating quarterly financials chart: {e}")
            return "", ""

    def _analyze_adx(self, df):
        """Analyze ADX (Average Directional Index) data"""
        if "adx" not in df.columns or "adx_di_positive" not in df.columns or "adx_di_negative" not in df.columns:
            return None
        
        latest = df.iloc[-1]
        adx_value = latest["adx"]
        plus_di = latest["adx_di_positive"]
        minus_di = latest["adx_di_negative"]
        
        # Skip analysis if any of the current values are None
        if adx_value is None or plus_di is None or minus_di is None:
            return None
        
        # Get previous values for comparison
        prev = df.iloc[-2] if len(df) > 1 else None
        prev_adx = prev["adx"] if prev is not None else None
        prev_plus_di = prev["adx_di_positive"] if prev is not None else None
        prev_minus_di = prev["adx_di_negative"] if prev is not None else None
        
        # Initialize analysis result
        result = {}
        
        # Current ADX values
        result["Current ADX"] = round(adx_value, 2)
        result["+DI"] = round(plus_di, 2) 
        result["-DI"] = round(minus_di, 2)
        
        # Trend strength interpretation
        if adx_value < 20:
            result["Trend Strength"] = "Weak or absent trend"
        elif adx_value < 30:
            result["Trend Strength"] = "Moderate trend"
        elif adx_value < 50:
            result["Trend Strength"] = "Strong trend"
        else:
            result["Trend Strength"] = "Very strong trend"
        
        # Trend direction
        if plus_di > minus_di:
            result["Trend Direction"] = "Bullish"
        else:
            result["Trend Direction"] = "Bearish"
        
        # Trend momentum (based on ADX changes)
        if prev_adx is not None:
            result["ADX Change"] = round(adx_value - prev_adx, 2)
            
            if adx_value > prev_adx:
                result["Trend Momentum"] = "Strengthening"
            elif adx_value < prev_adx:
                result["Trend Momentum"] = "Weakening"
            else:
                result["Trend Momentum"] = "Stable"
        
        # DI crossover check - safely handle None values
        if prev_plus_di is not None and prev_minus_di is not None:
            if plus_di > minus_di and prev_plus_di <= prev_minus_di:
                result["Recent Signal"] = "Bullish crossover (+DI crossed above -DI)"
            elif plus_di < minus_di and prev_plus_di >= prev_minus_di:
                result["Recent Signal"] = "Bearish crossover (-DI crossed above +DI)"
        
        # Trading recommendation based on ADX analysis - add safe checks
        if adx_value >= 25:  # Strong trend environment
            if plus_di > minus_di:
                if prev_plus_di is not None and prev_adx is not None and plus_di > prev_plus_di and adx_value > prev_adx:
                    result["Recommendation"] = "Consider long positions (strong bullish trend)"
                else:
                    result["Recommendation"] = "Hold long positions, monitor for weakening"
            else:
                if prev_minus_di is not None and prev_adx is not None and minus_di > prev_minus_di and adx_value > prev_adx:
                    result["Recommendation"] = "Consider short positions (strong bearish trend)"
                else:
                    result["Recommendation"] = "Hold short positions, monitor for weakening"
        else:  # Weak trend environment
            result["Recommendation"] = "Ranging market - consider range-based strategies instead of trend following"
        
        return result

    def _generate_chart_from_db(self, df, active_indicators):
        """Generate Bokeh chart from database data"""
        """
        source = ColumnDataSource(df.reset_index())

        p = figure(x_axis_type="datetime", title="Stock Chart", width=1000, height=500)
        p_rsi = figure(x_axis_type="datetime", title="RSI", width=1000, height=200, x_range=p.x_range)
        p_macd = figure(x_axis_type="datetime", title="MACD", width=1000, height=200, x_range=p.x_range)

        # Plot candlesticks - Fix column names to match the DataFrame
        inc = df["close"] > df["open"]
        dec = df["open"] > df["close"]

        # Fixed segment method to use correct x0 and x1 parameters instead of x
        p.segment(source=source, x0='timestamp', y0='low', x1='timestamp', y1='high', color="black")
        
        p.vbar(x=df.index[inc], width=12*60*60*1000, top=df["close"][inc], bottom=df["open"][inc],
               fill_color="green", line_color="black")
        p.vbar(x=df.index[dec], width=12*60*60*1000, top=df["open"][dec], bottom=df["close"][dec],
               fill_color="red", line_color="black")

        # Add indicators
        if active_indicators.get("SMA_50"):
            p.line(source=source, x='timestamp', y='sma50', legend_label="SMA 50", line_width=2, color="blue")
        if active_indicators.get("SMA_200"):
            p.line(source=source, x='timestamp', y='sma200', legend_label="SMA 200", line_width=2, color="red")
        if active_indicators.get("BB_upper"):
            p.line(source=source, x='timestamp', y='upper_bollinger', legend_label="Bollinger Upper", line_width=1.5,
                   color="purple", line_dash="dashed")
        if active_indicators.get("BB_lower"):
            p.line(source=source, x='timestamp', y='lower_bollinger', legend_label="Bollinger Lower", line_width=1.5,
                   color="purple", line_dash="dashed")
        if active_indicators.get("VWAP"):
            p.line(source=source, x='timestamp', y='vwap', legend_label="VWAP", line_width=2,
                   color="orange", line_dash="dotted")
        if active_indicators.get("Pivot"):
            pivot_value = df["pivot"].iloc[-1]
            p.line([df.index.min(), df.index.max()], [pivot_value, pivot_value],
                   legend_label="Pivot", line_width=2, color="brown", line_dash="solid")

        # Add RSI and MACD
        if "rsi" in df.columns:
            p_rsi.line(source=source, x='timestamp', y='rsi', legend_label="RSI", line_width=2, color="green")

        if all(col in df.columns for col in ["macd_line", "macd_signal"]):
            p_macd.line(source=source, x='timestamp', y='macd_line', legend_label="MACD", line_width=2, color="cyan")
            p_macd.line(source=source, x='timestamp', y='macd_signal', legend_label="Signal Line", line_width=2, color="red")

        p.legend.location = "top_left"
        script, div = components(column(p, p_rsi, p_macd))
        return script, div
        """
        return "",""

    def _get_supertrend_from_db(self, df):
        """Get supertrend data from database"""
        # Extract supertrend data from DataFrame
        latest_supertrend = df["Supertrend"].iloc[-1]
        if latest_supertrend > df["close"].iloc[-1]:
            return "Bearish"
        else:
            return "Bullish"

    def _get_pivots_from_db(self, df):
        """Get pivot data from database"""
        latest = df.iloc[-1]
        pivot = latest["pivot"]
        r1 = latest["r1"]
        r2 = latest["r2"]
        s1 = latest["s1"]
        s2 = latest["s2"]
        current_price = latest["close"]

        if current_price > pivot:
            summary = f"Price ({current_price:.2f}) is above Pivot ({pivot:.2f}). If it breaks {r1:.2f}, it may touch {r2:.2f}."
        elif current_price < pivot:
            summary = f"Price ({current_price:.2f}) is below Pivot ({pivot:.2f}). If it breaks {s1:.2f}, it may drop to {s2:.2f}."
        else:
            summary = f"Price ({current_price:.2f}) is at the Pivot ({pivot:.2f}), indicating a neutral zone."

        pivots = [
            ("Current Price", current_price),
            ("(1D time Frame) Pivot Point", round(pivot, 2)),
            ("Pivot Resistance 1 (R1)", round(r1, 2)),
            ("Pivot Resistance 2 (R2)", round(r2, 2)),
            ("Pivot Support 1 (S1)", round(s1, 2)),
            ("Pivot Support 2 (S2)", round(s2, 2)),
            ("summary", summary)
        ]

        return pivots

    def _detect_trendline_breakout_from_db(self, df):
        """Detect trendline breakout using database data"""
        latest_price = df['close'].iloc[-1]
        support_trendline = df['Donchian_Low'].rolling(window=20).min().iloc[-1]
        resistance_trendline = df['Donchian_High'].rolling(window=20).max().iloc[-1]

        if latest_price > resistance_trendline:
            return "Bullish Breakout"
        elif latest_price < support_trendline:
            return "Bearish Breakdown"
        else:
            return "No Breakout detected"

    def _get_pivot_analysis(self, current_price, pivot, r1, r2, r3, s1, s2, s3):
        """Analyze price in relation to pivot levels"""
        analysis = []

        # Format values for display
        current_price = float(current_price)
        pivot = float(pivot) if pivot is not None else None
        r1 = float(r1) if r1 is not None else None
        r2 = float(r2) if r2 is not None else None
        r3 = float(r3) if r3 is not None else None
        s1 = float(s1) if s1 is not None else None
        s2 = float(s2) if s2 is not None else None
        s3 = float(s3) if s3 is not None else None

        # Check if pivot values are available
        if pivot is None:
            return ["Pivot data not available."]

        # Determine current position relative to pivot levels
        if current_price > r3:
            analysis.append(f"Price is above R3 ({r3:.2f}) - Extremely bullish, potential reversal zone.")
        elif current_price > r2:
            analysis.append(f"Price is above R2 ({r2:.2f}) - Strongly bullish.")
            analysis.append(f"Next resistance at R3: {r3:.2f} (+{((r3/current_price)-1)*100:.2f}%)")
        elif current_price > r1:
            analysis.append(f"Price is above R1 ({r1:.2f}) - Bullish.")
            analysis.append(f"Next resistance at R2: {r2:.2f} (+{((r2/current_price)-1)*100:.2f}%)")
        elif current_price > pivot:
            analysis.append(f"Price is above pivot ({pivot:.2f}) - Mildly bullish.")
            analysis.append(f"Next resistance at R1: {r1:.2f} (+{((r1/current_price)-1)*100:.2f}%)")
        elif current_price < s3:
            analysis.append(f"Price is below S3 ({s3:.2f}) - Extremely bearish, potential reversal zone.")
        elif current_price < s2:
            analysis.append(f"Price is below S2 ({s2:.2f}) - Strongly bearish.")
            analysis.append(f"Next support at S3: {s3:.2f} ({((s3/current_price)-1)*100:.2f}%)")
        elif current_price < s1:
            analysis.append(f"Price is below S1 ({s1:.2f}) - Bearish.")
            analysis.append(f"Next support at S2: {s2:.2f} ({((s2/current_price)-1)*100:.2f}%)")
        else:  # Between pivot and S1
            analysis.append(f"Price is between pivot ({pivot:.2f}) and S1 ({s1:.2f}) - Mildly bearish.")
            analysis.append(f"Resistance at pivot: {pivot:.2f} (+{((pivot/current_price)-1)*100:.2f}%)")
            analysis.append(f"Support at S1: {s1:.2f} ({((s1/current_price)-1)*100:.2f}%)")

        # Add trading recommendation based on pivot analysis
        if current_price > pivot:
            if current_price < r1:
                analysis.append("Strategy: Look for buying opportunities with tight stop below pivot.")
            elif current_price < r2:
                analysis.append("Strategy: Consider trailing stops to protect profits.")
            else:
                analysis.append("Strategy: Watch for signs of resistance or reversal patterns.")
        else:
            if current_price > s1:
                analysis.append("Strategy: Watch for potential bounce at S1 or breakdown below it.")
            elif current_price > s2:
                analysis.append("Strategy: Look for selling opportunities with stop above pivot.")
            else:
                analysis.append("Strategy: Watch for signs of support or reversal patterns.")

        return analysis

    def _analyze_52_week_levels(self, df, latest_price):
        """Analyze price in relation to 52-week high and low"""
        try:
            # Calculate 52-week high and low
            year_data = df.tail(252)  # Approximately 252 trading days in a year
            if year_data.empty:
                return None, None

            high_52w = year_data['high'].max()
            low_52w = year_data['low'].min()

            # Calculate distances in percentage
            pct_from_high = ((high_52w - latest_price) / high_52w) * 100
            pct_from_low = ((latest_price - low_52w) / low_52w) * 100

            # Determine position and recommendation
            if pct_from_high <= 3:
                position = f"Close to 52-week high (within {pct_from_high:.2f}%)"
                recommendation = "Consider profit taking, strong momentum but potential resistance"
            elif pct_from_low <= 3:
                position = f"Close to 52-week low (within {pct_from_low:.2f}%)"
                recommendation = "Potential value zone, but verify support before buying"
            elif pct_from_high <= 10:
                position = f"Near 52-week high ({pct_from_high:.2f}% below)"
                recommendation = "Uptrend likely intact, watch for consolidation"
            elif pct_from_low <= 10:
                position = f"Near 52-week low ({pct_from_low:.2f}% above)"
                recommendation = "Monitor for trend reversal signs before entering"
            else:
                middle_point = (high_52w + low_52w) / 2
                if latest_price > middle_point:
                    position = f"In upper half of 52-week range ({pct_from_high:.2f}% below high, {pct_from_low:.2f}% above low)"
                    recommendation = "Moderately bullish zone"
                else:
                    position = f"In lower half of 52-week range ({pct_from_high:.2f}% below high, {pct_from_low:.2f}% above low)"
                    recommendation = "Moderately bearish zone"

            return position, recommendation
        except Exception as e:
            print(f"Error analyzing 52-week levels: {str(e)}")
            return None, None

    def _get_donchian_from_db(self, df):
        """Get Donchian channels from database"""
        latest = df.iloc[-1]
        upper = round(latest['Donchian_High'], 2)
        lower = round(latest['Donchian_Low'], 2)
        middle = round((upper + lower) / 2, 2)
        current_price = round(latest['close'], 2)

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

        # Fixed segment method to use correct x0 and x1 parameters instead of x
        p.segment(x0=df["timestamp"], y0=df["day_low"], x1=df["timestamp"], y1=df["day_high"], color="black")
        
        p.vbar(x=df["timestamp"][inc], width=12*60*60*1000, top=df["Close"][inc], bottom=df["Open"][inc],
               fill_color="green", line_color="black")
        p.vbar(x=df["timestamp"][dec], width=12*60*60*1000, top=df["Open"][dec], bottom=df["Close"][dec],
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
        """Get stocks near 52-week highs or lows within threshold percentage, using data from stock_data_cache."""
        try:
            # Get data directly from database instead of making API calls
            with self.database_service._get_cursor() as cur:
                query = """
                    SELECT 
                        symbol, close as current_price, week52_high, week52_low, 
                        pct_from_week52_high, pct_from_week52_low, days_since_week52_high, days_since_week52_low, week52_status
                    FROM stock_data_cache
                    WHERE interval = '1d'
                    AND week52_high IS NOT NULL
                    AND week52_low IS NOT NULL
                    AND (
                        (pct_from_high <= %s AND pct_from_high > 0) OR 
                        (pct_from_low <= %s AND pct_from_low > 0)
                    )
                """
                cur.execute(query, (threshold * 100, threshold * 100))
                
                results = []
                for row in cur.fetchall():
                    symbol = row[0].replace(".NS", "") if row[0] else ""
                    status = row[8] or ('near_high' if row[4] <= threshold * 100 else 'near_low')
                    
                    results.append({
                        "symbol": symbol,
                        "current_price": round(float(row[1]), 2) if row[1] else 0,
                        "week52_high": round(float(row[2]), 2) if row[2] else 0,
                        "week52_low": round(float(row[3]), 2) if row[3] else 0,
                        "pct_from_high": round(float(row[4]), 2) if row[4] else 0,
                        "pct_from_low": round(float(row[5]), 2) if row[5] else 0,
                        "days_since_high": int(row[6]) if row[6] else 0,
                        "days_since_low": int(row[7]) if row[7] else 0,
                        "status": status
                    })
                
                return results
        
        except Exception as e:
            print(f"Error fetching 52-week data from database: {str(e)}")
            return []

    def fetch_and_store_financials(self):
        """Fetch and store quarterly financial data for F&O stocks with optimized batch processing."""
        # Use F&O stocks instead of all NSE stocks
        fno_stocks = self.option_chain_service.get_fno_stocks_with_symbols()
        # Remove the .NS suffix for consistency in processing
        fno_symbols = [stock.replace('.NS', '') for stock in fno_stocks]
        
        if not fno_symbols:
            print("No F&O stocks found to fetch financials.")
            return

        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        print(f"Starting financial data collection for {len(fno_symbols)} F&O stocks at {now.strftime('%Y-%m-%d %H:%M:%S')}")

        # Configuration for batch processing
        BATCH_SIZE = 30  # Number of stocks in each batch for yfinance download
        MAX_WORKERS = 8   # Maximum number of parallel workers for ticker-specific methods
        MAX_RETRIES = 2   # Maximum number of retries for a failed request
        BASE_DELAY = 1.0  # Base delay between API calls in seconds
        
        # Split stocks into batches
        batches = [fno_symbols[i:i + BATCH_SIZE] for i in range(0, len(fno_symbols), BATCH_SIZE)]
        total_batches = len(batches)
        
        print(f"Processing {total_batches} batches with {BATCH_SIZE} stocks per batch")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Track API rate limiting
        rate_limit_delay = BASE_DELAY
        consecutive_failures = 0
        
        # Process each batch with adaptive rate limiting
        for batch_idx, batch_symbols in enumerate(batches):
            batch_start_time = time.time()
            print(f"Processing batch {batch_idx+1}/{total_batches} with {len(batch_symbols)} stocks...")
            
            # Add .NS suffix back for API call
            batch_tickers = [f"{symbol}.NS" for symbol in batch_symbols]
            ticker_str = " ".join(batch_tickers)
            
            try:
                
                print(f"Downloading batch data for {len(batch_tickers)} stocks")
                
                # Fetch price data for all stocks in batch in one API call
                batch_data = yf.download(
                    tickers=ticker_str,
                    period="2d",       # Just need recent data for current metrics
                    interval="1d",     # Daily data
                    group_by='ticker', # Group by ticker
                    progress=False,    # Disable progress bar
                    session=self.session    # Use our custom session
                )
                
                # Pre-fetch financials data for the entire batch in one call if possible
                try:
                    # Try to get financials data for multiple tickers at once by creating a batch of Ticker objects
                    # This reduces individual network calls for financials
                    print(f"Pre-fetching financial data for batch {batch_idx+1}")
                    multi_ticker = yf.Tickers(ticker_str, session=self.session)
                    
                    # Store these pre-fetched tickers for later use
                    ticker_objects = {symbol: multi_ticker.tickers[f"{symbol}.NS"] for symbol in batch_symbols}
                except Exception as e:
                    print(f"Error pre-fetching tickers batch: {e}")
                    # Fallback - we'll create individual tickers as needed
                    ticker_objects = {}
                
                # Process results for this batch
                batch_results = []
                batch_failures = 0
                
                # Function to process each stock in the batch (for data that must be fetched individually)
                def process_stock_financials(symbol, retry_count=0):
                    nonlocal consecutive_failures, rate_limit_delay
                    
                    try:
                        # Apply minimal delay to avoid rate limiting
                        time.sleep(max(0.1, rate_limit_delay / MAX_WORKERS))
                        
                        full_symbol = f"{symbol}.NS"
                        
                        # Extract useful metrics from batch data when possible
                        stock_data = None
                        extracted_metrics = {
                            "current_price": None,
                            "price_change": None,
                            "volume": None,
                            "pct_change": None
                        }
                        
                        try:
                            if full_symbol in batch_data.columns.levels[0]:
                                stock_data = batch_data[full_symbol]
                                
                                # Actually use the batch data we've retrieved!
                                if not stock_data.empty:
                                    latest_data = stock_data.iloc[-1]
                                    
                                    # Extract price information
                                    if 'Close' in latest_data:
                                        extracted_metrics["current_price"] = float(latest_data['Close'])
                                    
                                    # Extract volume information
                                    if 'Volume' in latest_data:
                                        extracted_metrics["volume"] = int(latest_data['Volume']) 
                                    
                                    # Calculate price change if we have enough data
                                    if len(stock_data) > 1 and 'Close' in stock_data:
                                        prev_close = float(stock_data['Close'].iloc[-2])
                                        current_close = float(stock_data['Close'].iloc[-1])
                                        price_change = current_close - prev_close
                                        pct_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
                                        
                                        extracted_metrics["price_change"] = round(price_change, 2)
                                        extracted_metrics["pct_change"] = round(pct_change, 2)
                                    
                                    print(f"Successfully extracted batch data for {symbol}: price={extracted_metrics['current_price']}")
                        except Exception as e:
                            print(f"Warning: Error processing batch data for {symbol}: {e}")
                        
                        # Use pre-fetched ticker object if available, otherwise create a new one
                        if symbol in ticker_objects and ticker_objects[symbol] is not None:
                            stock = ticker_objects[symbol]
                        else:
                            print(f"API called individually for {symbol} ****************")
                            # Only create individual Ticker object if we don't have it from the batch
                            session = self._get_session()
                            stock = yf.Ticker(full_symbol, session=self.session)
                        
                        # Get next earnings date from calendar if available
                        next_earnings_date = None
                        company_info = {}
                        fundamental_metrics = {}
                        
                        try:
                            # Get company info and metrics first - these are typically accessible from batch data
                            info = stock.info if hasattr(stock, 'info') else {}
                            if info and isinstance(info, dict):
                                # Extract industry and sector info
                                company_info = {
                                    "industry": info.get("industry"),
                                    "sector": info.get("sector"),
                                    "long_name": info.get("longName")
                                }
                                
                                # Extract fundamental metrics - use extracted_metrics values when available
                                fundamental_metrics = {
                                    "pe_ratio": info.get("trailingPE"),
                                    "market_cap": info.get("marketCap"),
                                    "dividend_yield": info.get("dividendYield") * 100 if info.get("dividendYield") else None,
                                    "dividend_rate": info.get("dividendRate"),
                                    "total_debt": info.get("totalDebt"),
                                    
                                    # Add data from batch download
                                    "current_price": extracted_metrics["current_price"] or info.get("currentPrice"),
                                    "price_change": extracted_metrics["price_change"],
                                    "pct_change": extracted_metrics["pct_change"],
                                    "volume": extracted_metrics["volume"] or info.get("volume")
                                }
                                
                                # Clean up None values for better DB storage
                                fundamental_metrics = {k: v for k, v in fundamental_metrics.items() if v is not None}
                                
                                # Try to get earnings date from info if available
                                if "earningsTimestamp" in info and info["earningsTimestamp"]:
                                    try:
                                        next_earnings_date = pd.to_datetime(info["earningsTimestamp"]).to_pydatetime()
                                    except:
                                        pass
                        except Exception as e:
                            print(f"Warning: Error extracting basic info for {symbol}: {e}")
                        
                        # Only try calendar data if we don't have earnings date yet
                        if not next_earnings_date:
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
                                print(f"Warning: Error extracting earnings date for {symbol}: {e}")
                        
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
                        financials = []
                        if financials_data is not None and not financials_data.empty:
                            # Process financial data
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
                        
                        # Return available data regardless of whether financials were found
                        result = {
                            "symbol": symbol, 
                            "financials": financials, 
                            "next_earnings_date": next_earnings_date,
                            "company_info": company_info,
                            "fundamental_metrics": fundamental_metrics
                        }
                        
                        # Consider this successful if we have any useful data
                        if financials or next_earnings_date or company_info or fundamental_metrics:
                            return result
                        
                        # If we have no data at all and haven't exhausted retries, try again
                        if retry_count < MAX_RETRIES:
                            consecutive_failures += 1
                            retry_delay = BASE_DELAY * (2 ** retry_count)
                            time.sleep(retry_delay)
                            return process_stock_financials(symbol, retry_count + 1)
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
                            return process_stock_financials(symbol, retry_count + 1)
                        else:
                            print(f"Error processing {symbol} after {MAX_RETRIES} retries: {e}")
                            return None
                
                # Process batch symbols with parallel workers for ticker-specific data
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Submit all stocks in the batch
                    future_to_symbol = {executor.submit(process_stock_financials, symbol): symbol for symbol in batch_symbols}
                    
                    # Process results as they complete
                    for future in concurrent.futures.as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            result = future.result()
                            if result:
                                has_financials = bool(result["financials"])
                                if has_financials:
                                    success_count += 1
                                else:
                                    skipped_count += 1
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
                
                # Calculate batch stats
                batch_time = time.time() - batch_start_time
                batch_success_rate = (len(batch_symbols) - batch_failures) / len(batch_symbols) * 100
                
                print(f"Batch {batch_idx+1}/{total_batches} completed in {batch_time:.2f}s with {batch_success_rate:.1f}% success rate")
                print(f"Progress: {success_count} successful, {skipped_count} skipped, {failed_count} failed")
                
                # Adaptive batch delay based on success rate
                if batch_success_rate < 70:
                    # If success rate is low, add a cooldown period
                    cooldown = min(30, 5 * (1 + (70 - batch_success_rate) / 10))
                    print(f"Low success rate detected. Cooling down for {cooldown:.1f}s before next batch")
                    time.sleep(cooldown)
                else:
                    # Small delay between batches to avoid overwhelming API
                    time.sleep(2)
                    
            except Exception as e:
                print(f"Error processing batch {batch_idx+1}: {e}")
                # Add longer cooldown on batch-level exception
                time.sleep(10)
        
        total_processed = success_count + skipped_count
        print(f"Financial data collection completed: {success_count} stocks with financials, "
              f"{skipped_count} stocks with only earnings date, {failed_count} failed stocks.")
        print(f"Success rate: {(total_processed / len(fno_symbols)) * 100:.1f}%")
        
        # Return overall statistics
        return {
            "total_stocks": len(fno_symbols),
            "success_count": success_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "completion_time": (datetime.now(ist) - now).total_seconds()
        }


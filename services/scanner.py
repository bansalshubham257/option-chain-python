import json
import logging
import sys
import os
import time

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from psycopg2.extras import execute_batch
from scipy.stats import linregress
from curl_cffi import requests
import yfinance as yf

# Add correct imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.database import DatabaseService
from services.option_chain import OptionChainService

class ScannerService:
    def __init__(self, database_service, option_chain_service):
        self.database_service = database_service
        self.option_chain_service = option_chain_service
        self.session = requests.Session(impersonate="chrome110")

    def save_scanner(self, name, conditions, stock_type, logic="AND"):
        """Save a scanner configuration to the database"""
        try:
            with self.database_service._get_cursor() as cur:
                cur.execute("""
                    INSERT INTO saved_scanners (name, conditions, stock_type, created_at, logic)
                    VALUES (%s, %s, %s, NOW(), %s)
                    ON CONFLICT (name) DO UPDATE
                    SET conditions = EXCLUDED.conditions,
                        stock_type = EXCLUDED.stock_type,
                        logic = EXCLUDED.logic,
                        created_at = NOW()
                    RETURNING id
                """, (name, json.dumps(conditions), stock_type, logic))

                scanner_id = cur.fetchone()[0]
                return {"status": "success", "id": scanner_id}

        except Exception as e:
            logging.error(f"Error saving scanner: {str(e)}")
            raise

    def load_scanners(self):
        """Load all saved scanners from the database"""
        try:
            with self.database_service._get_cursor() as cur:
                cur.execute("SELECT id, name, conditions, stock_type, logic FROM saved_scanners ORDER BY created_at DESC")
                scanners = [{
                    'id': row[0],
                    'name': row[1],
                    'conditions': row[2],
                    'stock_type': row[3],
                    'logic': row[4] or 'AND'  # Default to AND if NULL
                } for row in cur.fetchall()]

                return {"status": "success", "data": scanners}

        except Exception as e:
            logging.error(f"Error loading scanners: {str(e)}")
            raise

    def delete_scanner(self, scanner_id):
        """Delete a saved scanner by ID"""
        try:
            with self.database_service._get_cursor() as cur:
                cur.execute("DELETE FROM saved_scanners WHERE id = %s", (scanner_id,))
                return {"status": "success"}

        except Exception as e:
            logging.error(f"Error deleting scanner: {str(e)}")
            raise

    def query_stocks_with_sql(self, conditions, stock_type, default_interval='1d', logic='AND'):
        """Run scanner query supporting mixed intervals"""
        try:
            # Get appropriate stocks based on type
            if stock_type == 'fno':
                all_stocks = self.option_chain_service.get_fno_stocks_with_symbols()
            else:  # nifty50
                all_stocks = [
                    'ADANIENT.NS', 'ADANIPORTS.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
                    'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BHARTIARTL.NS', 'BPCL.NS',
                    'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
                    'EICHERMOT.NS', 'GRASIM.NS', 'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
                    'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 'ITC.NS',
                    'INDUSINDBK.NS', 'INFY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LT.NS',
                    'M&M.NS', 'MARUTI.NS', 'NTPC.NS', 'NESTLEIND.NS', 'ONGC.NS',
                    'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 'SUNPHARMA.NS',
                    'TCS.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS', 'TECHM.NS',
                    'TITAN.NS', 'ULTRACEMCO.NS', 'UPL.NS', 'WIPRO.NS', 'HDFC.NS'
                ]

            # Analyze conditions to collect required intervals and indicators
            required_intervals = {}  # Map of interval -> list of required symbols

            # Always include all stocks for the default interval
            required_intervals[default_interval] = all_stocks

            # Extract interval-specific requirements from conditions
            for condition in conditions:
                # Get interval from left indicator
                left_interval = condition.get('leftInterval', default_interval)
                if left_interval not in required_intervals:
                    required_intervals[left_interval] = list(all_stocks)

                # Get interval from right indicator
                right_interval = condition.get('rightInterval', default_interval)
                if right_interval not in required_intervals:
                    required_intervals[right_interval] = list(all_stocks)

            # Build SQL query parts for each interval
            cte_parts = []
            query_params = []

            # Create a CTE for each required interval
            for idx, (interval, stocks) in enumerate(required_intervals.items()):
                placeholders = ','.join(['%s'] * len(stocks))
                cte_parts.append(f"""
                    data_{idx} AS (
                        SELECT
                            symbol, interval, timestamp, open, high, low, close, volume,
                        pivot, r1, r2, r3, s1, s2, s3, price_change, percent_change, vwap,
                        pe_ratio, beta,
                        ichimoku_base, ichimoku_conversion, ichimoku_span_a, ichimoku_span_b,
                        ichimoku_cloud_top, ichimoku_cloud_bottom, stoch_rsi, parabolic_sar,
                        upper_bollinger, lower_bollinger, middle_bollinger, macd_line, macd_signal, macd_histogram,
                        adx, adx_di_positive, adx_di_negative, HA_Open, HA_Close, HA_High, HA_Low,
                        Supertrend, Acc_Dist, CCI, CMF, MFI, On_Balance_Volume, Williams_R,
                        Bollinger_B, Intraday_Intensity, Force_Index, STC, sma10, sma20, sma50, sma100,
                        sma200, ema10, ema50, ema100, ema200, wma10, wma50, wma100, wma200,
                        tma10, tma50, tma100, tma200, rma10, rma50, rma100, rma200, tema10,
                        tema50, tema100, tema200, hma10, hma50, hma100, hma200, vwma10, vwma50,
                        vwma100, vwma200, std10, std50, std100, std200, ATR, TRIX, ROC,
                        Keltner_Middle, Keltner_Upper, Keltner_Lower, Donchian_High, Donchian_Low,
                        Chaikin_Oscillator, rsi, true_range, aroon_up, aroon_down, aroon_osc,
                        buyer_initiated_trades, buyer_initiated_quantity, buyer_initiated_avg_qty,
                        seller_initiated_trades, seller_initiated_quantity, seller_initiated_avg_qty,
                        buyer_seller_ratio, buyer_seller_quantity_ratio, buyer_initiated_vwap, seller_initiated_vwap,
                        wave_trend, wave_trend_trigger, wave_trend_momentum,
                        book_value, debt_to_equity, dividend_rate, dividend_yield,
                        earnings_growth, earnings_quarterly_growth, ebitda_margins,
                        enterprise_to_ebitda, enterprise_to_revenue, enterprise_value,
                        eps_trailing_twelve_months, five_year_avg_dividend_yield, float_shares,
                        forward_eps, forward_pe, free_cashflow, gross_margins, gross_profits,
                        industry, market_cap, operating_cashflow, operating_margins,
                        payout_ratio, price_to_book, profit_margins, return_on_assets,
                        return_on_equity, revenue_growth, revenue_per_share, sector,
                        total_cash, total_cash_per_share, total_debt, total_revenue,
                        trailing_eps, trailing_pe, trailing_peg_ratio,
                            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                        FROM stock_data_cache
                        WHERE symbol IN ({placeholders})
                        AND interval = %s
                    )
                """)
                query_params.extend(stocks)
                query_params.append(interval)

            # Build SQL conditions comparing values from appropriate interval CTEs
            sql_conditions = []
            for condition in conditions:
                try:
                    # Get indicator names and values
                    left_indicator = condition.get('leftIndicator')
                    right_indicator = condition.get('rightIndicator')

                    # Get numeric value if provided
                    number_value = condition.get('numberValue')

                    # Get the SQL operator
                    sql_op = {
                        '>': '>',
                        '<': '<',
                        '=': '=',
                        '!=': '!='
                    }.get(condition.get('operator'))

                    if not sql_op:
                        print(f"Unsupported operator: {condition.get('operator')}")
                        continue

                    # Get intervals (use default if not specified)
                    left_interval = condition.get('leftInterval', default_interval)
                    right_interval = condition.get('rightInterval', default_interval)

                    # Find CTE index for each interval
                    left_idx = list(required_intervals.keys()).index(left_interval)
                    right_idx = list(required_intervals.keys()).index(right_interval)

                    # Process left indicator
                    if left_indicator == "number" and number_value is not None:
                        # Use the number value directly
                        left_sql = str(number_value)
                    elif isinstance(left_indicator, (int, float)):
                        # It's a numeric literal
                        left_sql = str(left_indicator)
                    elif isinstance(left_indicator, str) and left_indicator.replace('.', '', 1).isdigit():
                        # It's a string that represents a number
                        left_sql = left_indicator
                    else:
                        # It's a column reference
                        left_sql = f"d{left_idx}.{left_indicator}"

                    # Process right indicator
                    if right_indicator == "number" and number_value is not None:
                        # Use the number value directly
                        right_sql = str(number_value)
                    elif isinstance(right_indicator, (int, float)):
                        # It's a numeric literal
                        right_sql = str(right_indicator)
                    elif isinstance(right_indicator, str) and right_indicator.replace('.', '', 1).isdigit():
                        # It's a string that represents a number
                        right_sql = right_indicator
                    else:
                        # It's a column reference
                        right_sql = f"d{right_idx}.{right_indicator}"

                    sql_conditions.append(f"{left_sql} {sql_op} {right_sql}")

                except Exception as e:
                    print(f"Error building condition: {str(e)}")
                    continue

            # Combine all CTEs
            cte_sql = ", ".join(cte_parts)

            # Build the main query
            # First joining all interval tables
            from_clause = f"data_0 d0"
            join_clauses = []

            # Add joins for all other intervals
            for idx in range(1, len(required_intervals)):
                join_clauses.append(f"JOIN data_{idx} d{idx} ON d0.symbol = d{idx}.symbol AND d{idx}.rn = 1")

            # Build WHERE clause with the conditions
            where_clause = "d0.rn = 1"  # Always filter to the latest data point
            if sql_conditions:
                logic_op = f" {logic} "
                where_clause += f" AND ({logic_op.join(sql_conditions)})"

            # Get the main interval index (default interval) for change calculation
            main_idx = list(required_intervals.keys()).index(default_interval)

            # Final query
            query = f"""
                WITH {cte_sql}
                SELECT
                    d0.symbol,
                    d{main_idx}.close as lastPrice,
                    d{main_idx}.volume as volume,
                    d{main_idx}.price_change as change,
                    d{main_idx}.percent_change as pChange,
                    d{main_idx}.pe_ratio,
                    d{main_idx}.market_cap,
                    d{main_idx}.dividend_yield,
                    d{main_idx}.price_to_book,
                    d{main_idx}.beta
                FROM {from_clause}
                {' '.join(join_clauses)}
                WHERE {where_clause}
            """

            with self.database_service._get_cursor() as cur:
                cur.execute(query, query_params)

                results = []
                for row in cur.fetchall():
                    try:
                        symbol = row[0]
                        last_price = float(row[1]) if row[1] is not None else 0
                        volume = float(row[2]) if row[2] is not None else 0
                        price_change = float(row[3]) if row[3] is not None else 0
                        percent_change = float(row[4]) if row[4] is not None else 0

                        results.append({
                            'symbol': symbol.replace('.NS', ''),
                            'lastPrice': last_price,
                            'volume': volume,
                            'change': price_change,
                            'pChange': percent_change,
                            'pe_ratio': float(row[5]) if row[5] is not None else None,
                            'market_cap': float(row[6]) if row[6] is not None else None,
                            'dividend_yield': float(row[7]) if row[7] is not None else None,
                            'price_to_book': float(row[8]) if row[8] is not None else None,
                            'beta': float(row[9]) if row[9] is not None else None
                        })
                    except Exception as e:
                        print(f"Error processing result for {row[0]}: {str(e)}")

                return results

        except Exception as e:
            print(f"SQL scanner error: {str(e)}")
            return []

    def get_scanner_results(self, stock_name=None, scan_date=None):
        """Fetch scanner results with enriched price data in optimal time."""
        try:
            # Use a single database connection for all operations
            with self.database_service._get_cursor() as cur:
                # 1. Fetch scanner results with optional filtering
                query = """
                    SELECT sr.*, 
                           sdc.close AS last_price,
                           sdc.price_change,
                           sdc.percent_change
                    FROM scanner_results sr
                    LEFT JOIN LATERAL (
                        SELECT close, price_change, percent_change
                        FROM stock_data_cache
                        WHERE symbol = sr.stock_name 
                        AND interval = '1d'
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) sdc ON true
                    WHERE 1=1
                """

                params = []
                if stock_name:
                    query += " AND sr.stock_name = %s"
                    params.append(stock_name)
                if scan_date:
                    query += " AND sr.scan_date = %s"
                    params.append(scan_date)

                # Execute the combined query
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]

                # Clean up stock names and handle NULL values
                enriched_results = []
                for result in results:
                    result['stock_name'] = result['stock_name'].replace('.NS', '')
                    if result['last_price'] is None:
                        result['price_change'] = None
                        result['percent_change'] = None
                    enriched_results.append(result)

                return enriched_results

        except Exception as e:
            logging.error(f"Error fetching scanner results: {str(e)}")
            raise

    def calculate_narrow_range_scans(self, stock, data, results):
        """Calculate Narrow Range (NR4, NR7) and breakout/breakdown scans."""
        if len(data) < 7:  # Ensure enough data for NR7 calculation
            return

        try:
            # Calculate NR4
            nr4_data = data.iloc[-4:]
            nr4_range = nr4_data['High'].max() - nr4_data['Low'].min()
            if nr4_range == (nr4_data['High'] - nr4_data['Low']).min():
                results.append((stock, "nr4", True))

            # Calculate NR7
            nr7_data = data.iloc[-7:]
            nr7_range = nr7_data['High'].max() - nr7_data['Low'].min()
            if nr7_range == (nr7_data['High'] - nr7_data['Low']).min():
                results.append((stock, "nr7", True))

            # Check for NR4 breakout/breakdown
            if nr4_range == (nr4_data['High'] - nr4_data['Low']).min():
                last_close = data['Close'].iloc[-1]
                if last_close > nr4_data['High'].max():
                    results.append((stock, "nr4_breakout", True))
                elif last_close < nr4_data['Low'].min():
                    results.append((stock, "nr4_breakdown", True))

            # Check for NR7 breakout/breakdown
            if nr7_range == (nr7_data['High'] - nr7_data['Low']).min():
                last_close = data['Close'].iloc[-1]
                if last_close > nr7_data['High'].max():
                    results.append((stock, "nr7_breakout", True))
                elif last_close < nr7_data['Low'].min():
                    results.append((stock, "nr7_breakdown", True))

        except Exception as e:
            print(f"Error calculating Narrow Range scans for {stock}: {e}")

    def calculate_supertrend_scans(self, stock, data, results, period=7, multiplier=3):
        """Calculate Supertrend scans for daily and weekly data."""
        if len(data) < period:  # Ensure enough data for Supertrend calculation
            return

        try:
            # Calculate Supertrend
            high = data['High']
            low = data['Low']
            close = data['Close']
            atr = (high - low).rolling(window=period).mean()
            upper_band = ((high + low) / 2) + (multiplier * atr)
            lower_band = ((high + low) / 2) - (multiplier * atr)

            supertrend = pd.Series(index=data.index)
            supertrend.iloc[0] = lower_band.iloc[0] if close.iloc[0] > lower_band.iloc[0] else upper_band.iloc[0]

            for i in range(1, len(data)):
                if close.iloc[i] > supertrend.iloc[i - 1]:
                    supertrend.iloc[i] = max(lower_band.iloc[i], supertrend.iloc[i - 1])
                else:
                    supertrend.iloc[i] = min(upper_band.iloc[i], supertrend.iloc[i - 1])

            # Check for Supertrend signal change
            if len(supertrend) > 1:
                if close.iloc[-2] < supertrend.iloc[-2] and close.iloc[-1] > supertrend.iloc[-1]:
                    results.append((stock, "supertrend_signal_changed_to_buy", True))
                elif close.iloc[-2] > supertrend.iloc[-2] and close.iloc[-1] < supertrend.iloc[-1]:
                    results.append((stock, "supertrend_signal_changed_to_sell", True))

            # Check for price nearing Supertrend support/resistance
            if abs(close.iloc[-1] - supertrend.iloc[-1]) / supertrend.iloc[-1] < 0.005:
                if close.iloc[-1] > supertrend.iloc[-1]:
                    results.append((stock, "price_nearing_supertrend_support", True))
                else:
                    results.append((stock, "price_nearing_supertrend_resistance", True))

        except Exception as e:
            print(f"Error calculating Supertrend scans for {stock}: {e}")

    def calculate_weekly_supertrend_scans(self, stock, weekly_data, results, period=7, multiplier=3):
        """Calculate Supertrend scans for weekly close data."""
        if len(weekly_data) < period:  # Ensure enough data for Supertrend calculation
            return

        try:
            # Calculate Supertrend
            high = weekly_data['High']
            low = weekly_data['Low']
            close = weekly_data['Close']
            atr = (high - low).rolling(window=period).mean()
            upper_band = ((high + low) / 2) + (multiplier * atr)
            lower_band = ((high + low) / 2) - (multiplier * atr)

            supertrend = pd.Series(index=weekly_data.index)
            supertrend.iloc[0] = lower_band.iloc[0] if close.iloc[0] > lower_band.iloc[0] else upper_band.iloc[0]

            for i in range(1, len(weekly_data)):
                if close.iloc[i] > supertrend.iloc[i - 1]:
                    supertrend.iloc[i] = max(lower_band.iloc[i], supertrend.iloc[i - 1])
                else:
                    supertrend.iloc[i] = min(upper_band.iloc[i], supertrend.iloc[i - 1])

            # Check for Supertrend signal change
            if len(supertrend) > 1:
                if close.iloc[-2] < supertrend.iloc[-2] and close.iloc[-1] > supertrend.iloc[-1]:
                    results.append((stock, "weekly_supertrend_signal_changed_to_buy", True))
                elif close.iloc[-2] > supertrend.iloc[-2] and close.iloc[-1] < supertrend.iloc[-1]:
                    results.append((stock, "weekly_supertrend_signal_changed_to_sell", True))

            # Check for price nearing Supertrend support/resistance
            if abs(close.iloc[-1] - supertrend.iloc[-1]) / supertrend.iloc[-1] < 0.005:
                if close.iloc[-1] > supertrend.iloc[-1]:
                    results.append((stock, "price_nearing_weekly_supertrend_support", True))
                else:
                    results.append((stock, "price_nearing_weekly_supertrend_resistance", True))

        except Exception as e:
            print(f"Error calculating weekly Supertrend scans for {stock}: {e}")


    def scan_futures_oi_changes(self):
        """Scan for high increases or decreases in futures open interest."""
        results = []
        try:
            # Define thresholds for significant OI changes (in percentage)
            high_increase_threshold = 10
            high_decrease_threshold = -10

            with self.database_service._get_cursor() as cur:
                # Query for significant OI changes in futures from the fno_analytics table
                # Fix the timestamp comparison by casting timestamp to timestamp type
                cur.execute("""
                    SELECT 
                        symbol, 
                        oi_change, 
                        absolute_oi,
                        timestamp
                    FROM fno_analytics
                    WHERE 
                        analytics_type = 'buildup' 
                        AND category IN ('options_long', 'options_short')
                        AND option_type = 'FUT'
                        AND CAST(timestamp AS timestamp) >= NOW() - INTERVAL '24 hours'
                    ORDER BY 
                        ABS(oi_change) DESC
                """)

                futures_data = cur.fetchall()

                for row in futures_data:
                    symbol, oi_change, absolute_oi, timestamp = row

                    # Remove .NS suffix if present for storage format consistency
                    stock_name = symbol.replace('.NS', '')

                    # Check for high increase in OI
                    if oi_change >= high_increase_threshold:
                        results.append((stock_name, "futures_high_oi_increase", True))
                        print(f"✅ HIGH OI INCREASE: {stock_name} with {oi_change:.2f}% change")

                    # Check for high decrease in OI
                    elif oi_change <= high_decrease_threshold:
                        results.append((stock_name, "futures_high_oi_decrease", True))
                        print(f"⬇️ HIGH OI DECREASE: {stock_name} with {oi_change:.2f}% change")

                print(f"Found {len(results)} stocks with significant futures OI changes")
                return results

        except Exception as e:
            print(f"Error in scan_futures_oi_changes: {e}")
            return []

    def scan_options_oi_changes(self, condition):
        """Scan for options with significant OI changes"""
        try:
            threshold = float(condition.get('value', 100))
            timeframe = condition.get('timeframe', '1d')
            direction = condition.get('direction', 'above')
            option_type = condition.get('option_type', 'both').upper()
            
            interval = '2 hours' if timeframe == '1d' else '30 minutes'
            
            # Convert timestamp string to proper timestamp value
            with self.database._get_cursor() as cur:
                query = """
                    SELECT symbol, strike_price, option_type, oi, price, 
                           CAST(display_time AS timestamp) as timestamp
                    FROM oi_volume_history
                    WHERE option_type != 'FU'
                          AND CAST(display_time AS timestamp) >= NOW() - INTERVAL %s
                """
                
                params = [interval]
                
                # Filter by option type if specified
                if option_type != 'BOTH':
                    query += " AND option_type = %s"
                    params.append(option_type)
                
                query += " ORDER BY symbol, strike_price, option_type, CAST(display_time AS timestamp)"
                
                cur.execute(query, params)
                
                data = cur.fetchall()
                
                # Process the data to find OI changes
                result_symbols = set()
                
                options_data = {}
                for row in data:
                    symbol, strike, opt_type, oi, price, timestamp = row
                    key = f"{symbol}_{strike}_{opt_type}"
                    
                    if key not in options_data:
                        options_data[key] = []
                        
                    options_data[key].append({
                        'symbol': symbol,
                        'strike': strike,
                        'type': opt_type,
                        'oi': float(oi) if oi else 0,
                        'price': float(price) if price else 0,
                        'timestamp': timestamp
                    })
                
                # Calculate OI changes
                results = []
                for key, data_points in options_data.items():
                    if len(data_points) >= 2:
                        # Sort by timestamp to ensure proper order
                        sorted_data = sorted(data_points, key=lambda x: x['timestamp'])
                        
                        # Compare oldest and newest data points
                        oldest = sorted_data[0]
                        newest = sorted_data[-1]
                        
                        initial_oi = oldest['oi']
                        current_oi = newest['oi']
                        
                        # Skip if initial OI is too low to be meaningful
                        if initial_oi < 100:
                            continue
                        
                        # Calculate percentage change
                        if initial_oi > 0:  # Avoid division by zero
                            pct_change = ((current_oi - initial_oi) / initial_oi) * 100
                            
                            # Apply threshold filter
                            if (direction == 'above' and pct_change >= threshold) or \
                               (direction == 'below' and pct_change <= -threshold):
                                result_symbols.add(newest['symbol'])
                                results.append({
                                    'symbol': newest['symbol'],
                                    'strike': newest['strike'],
                                    'option_type': newest['type'],
                                    'initial_oi': initial_oi,
                                    'current_oi': current_oi,
                                    'pct_change': pct_change,
                                    'price': newest['price']
                                })
                
                return list(result_symbols)
        
        except Exception as e:
            print(f"Error in scan_options_oi_changes: {e}")
            return []

    def is_double_top(self, data, window=20, similarity_threshold=0.01, min_peak_distance=3):
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values

        # Find all peaks
        peaks = []
        for i in range(1, len(highs)-1):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                peaks.append((i, highs[i]))

        if len(peaks) < 2:
            return False

        # Find two distinct peaks with a trough between them
        valid_pairs = []
        for i in range(len(peaks)):
            for j in range(i+1, len(peaks)):
                idx1, val1 = peaks[i]
                idx2, val2 = peaks[j]
                if abs(idx2 - idx1) < min_peak_distance:
                    continue

                # Check price similarity
                if abs(val1 - val2)/max(val1, val2) > similarity_threshold:
                    continue

                # Find the lowest point between them
                trough = min(lows[idx1:idx2+1])
                valid_pairs.append((idx1, idx2, val1, trough))

        if not valid_pairs:
            return False

        # Sort by most recent pattern
        valid_pairs.sort(key=lambda x: x[1], reverse=True)

        # Check if price has broken the neckline (trough)
        latest_close = data['Close'].iloc[-1]  # Get scalar value
        return latest_close < valid_pairs[0][3]

    def is_double_bottom(self, data, window=20, similarity_threshold=0.01, min_trough_distance=3):
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values

        # Find all troughs
        troughs = []
        for i in range(1, len(lows)-1):
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                troughs.append((i, lows[i]))

        if len(troughs) < 2:
            return False

        # Find two distinct troughs with a peak between them
        valid_pairs = []
        for i in range(len(troughs)):
            for j in range(i+1, len(troughs)):
                idx1, val1 = troughs[i]
                idx2, val2 = troughs[j]
                if abs(idx2 - idx1) < min_trough_distance:
                    continue

                # Check price similarity
                if abs(val1 - val2)/max(val1, val2) > similarity_threshold:
                    continue

                # Find the highest point between them
                peak = max(highs[idx1:idx2+1])
                valid_pairs.append((idx1, idx2, val1, peak))

        if not valid_pairs:
            return False

        # Sort by most recent pattern
        valid_pairs.sort(key=lambda x: x[1], reverse=True)

        # Check if price has broken the neckline (peak)
        latest_close = data['Close'].iloc[-1]  # Get scalar value
        return latest_close > valid_pairs[0][3]


    def is_head_and_shoulders(self, data, window=30, similarity_threshold=0.03):
        """
        Detect Head and Shoulders pattern.
        Characteristics:
        1. Three peaks - middle peak (head) is highest, two shoulders on either side
        2. Neckline formed by connecting the troughs between peaks
        3. Price breaks below the neckline
        """
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values

        # Find all peaks
        peaks = []
        for i in range(1, len(highs)-1):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                peaks.append((i, highs[i]))

        if len(peaks) < 3:
            return False

        # Sort peaks by height and take top 3
        peaks.sort(key=lambda x: x[1], reverse=True)
        if len(peaks) > 3:
            peaks = peaks[:3]
        peaks.sort(key=lambda x: x[0])  # Sort by position

        # Check if middle peak is significantly higher than others
        head_val = peaks[1][1]
        shoulder1_val = peaks[0][1]
        shoulder2_val = peaks[2][1]

        if not (head_val > shoulder1_val * (1 + similarity_threshold) and
                head_val > shoulder2_val * (1 + similarity_threshold)):
            return False

        # Find troughs between peaks
        trough1_idx = trough2_idx = 0
        trough1_val = trough2_val = float('inf')

        for i in range(peaks[0][0], peaks[1][0] + 1):
            if lows[i] < trough1_val:
                trough1_val = lows[i]
                trough1_idx = i

        for i in range(peaks[1][0], peaks[2][0] + 1):
            if lows[i] < trough2_val:
                trough2_val = lows[i]
                trough2_idx = i

        # Neckline is the line connecting the two troughs
        # Check if price has broken below the neckline
        latest_close = data.iloc[-1]['Close']
        return latest_close < min(trough1_val, trough2_val)

    def is_inverse_head_and_shoulders(self, data, window=30, similarity_threshold=0.03):
        """
        Detect Inverse Head and Shoulders pattern.
        Characteristics:
        1. Three troughs - middle trough (head) is lowest, two shoulders on either side
        2. Neckline formed by connecting the peaks between troughs
        3. Price breaks above the neckline
        """
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values

        # Find all troughs
        troughs = []
        for i in range(1, len(lows)-1):
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                troughs.append((i, lows[i]))

        if len(troughs) < 3:
            return False

        # Sort troughs by depth and take bottom 3
        troughs.sort(key=lambda x: x[1])
        if len(troughs) > 3:
            troughs = troughs[:3]
        troughs.sort(key=lambda x: x[0])  # Sort by position

        # Check if middle trough is significantly lower than others
        head_val = troughs[1][1]
        shoulder1_val = troughs[0][1]
        shoulder2_val = troughs[2][1]

        if not (head_val < shoulder1_val * (1 - similarity_threshold) and
                head_val < shoulder2_val * (1 - similarity_threshold)):
            return False

        # Find peaks between troughs
        peak1_idx = peak2_idx = 0
        peak1_val = peak2_val = float('-inf')

        for i in range(troughs[0][0], troughs[1][0] + 1):
            if highs[i] > peak1_val:
                peak1_val = highs[i]
                peak1_idx = i

        for i in range(troughs[1][0], troughs[2][0] + 1):
            if highs[i] > peak2_val:
                peak2_val = highs[i]
                peak2_idx = i

        # Neckline is the line connecting the two peaks
        # Check if price has broken above the neckline
        latest_close = data.iloc[-1]['Close']
        return latest_close > max(peak1_val, peak2_val)


    def is_rising_wedge(self, data, window=20, trend_threshold=0.05):
        """
        Detect Rising Wedge (typically bearish pattern)
        Characteristics:
        1. Higher highs and higher lows (converging trendlines)
        2. Slope of upper trendline is steeper than lower
        3. Breakout typically downward
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        # Fit trendlines
        x = np.arange(len(highs))
        high_slope, _, _, _, _ = linregress(x, highs)
        low_slope, _, _, _, _ = linregress(x, lows)
    
        # Check if both trendlines are rising but converging
        if high_slope <= 0 or low_slope <= 0:
            return False
        if high_slope <= low_slope:
            return False
    
        # Check if recent price broke below lower trendline
        latest_close = data.iloc[-1]['Close']
        predicted_low = low_slope * (len(highs)-1) + lows[0]
        return latest_close < predicted_low
    
    def is_falling_wedge(self, data, window=20, trend_threshold=0.05):
        """
        Detect Falling Wedge (typically bullish pattern)
        Characteristics:
        1. Lower highs and lower lows (converging trendlines)
        2. Slope of lower trendline is steeper than upper
        3. Breakout typically upward
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        # Fit trendlines
        x = np.arange(len(highs))
        high_slope, _, _, _, _ = linregress(x, highs)
        low_slope, _, _, _, _ = linregress(x, lows)
    
        # Check if both trendlines are falling but converging
        if high_slope >= 0 or low_slope >= 0:
            return False
        if abs(low_slope) <= abs(high_slope):
            return False
    
        # Check if recent price broke above upper trendline
        latest_close = data.iloc[-1]['Close']
        predicted_high = high_slope * (len(highs)-1) + highs[0]
        return latest_close > predicted_high
    
    def is_triangle(self, data, window=20, breakout_threshold=0.01):
        """
        Detect Symmetrical Triangle (continuation pattern)
        Characteristics:
        1. Converging trendlines with similar slopes
        2. Breakout can be in either direction
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        # Fit trendlines
        x = np.arange(len(highs))
        high_slope, _, _, _, _ = linregress(x, highs)
        low_slope, _, _, _, _ = linregress(x, lows)
    
        # Check for convergence
        if high_slope >= 0 or low_slope <= 0:
            return False
    
        # Check if slopes are roughly symmetrical
        if abs(abs(high_slope) - abs(low_slope)) > 0.5 * min(abs(high_slope), abs(low_slope)):
            return False
    
        # Check for breakout
        latest_close = data.iloc[-1]['Close']
        predicted_high = high_slope * (len(highs)-1) + highs[0]
        predicted_low = low_slope * (len(lows)-1) + lows[0]
    
        if latest_close > predicted_high * (1 + breakout_threshold):
            return "bullish"
        elif latest_close < predicted_low * (1 - breakout_threshold):
            return "bearish"
        return False
    
    def is_pennant(self, data, window=15, consolidation_window=5, breakout_threshold=0.02):
        """
        Detect Pennant (continuation pattern)
        Characteristics:
        1. Small symmetrical triangle after sharp move (flag pole)
        2. Breakout in same direction as prior move
        """
        if len(data) < window:
            return False
    
        # Check for preceding strong move (flag pole)
        pole_data = data.iloc[-window:-consolidation_window]
        pole_move = (pole_data['Close'].iloc[-1] - pole_data['Close'].iloc[0]) / pole_data['Close'].iloc[0]
    
        if abs(pole_move) < 0.05:  # At least 5% move
            return False
    
        # Check for small triangle in consolidation period
        triangle_result = self.is_triangle(data.tail(consolidation_window))
        if not triangle_result:
            return False
    
        # Check breakout direction matches pole direction
        latest_close = data.iloc[-1]['Close']
        if pole_move > 0 and triangle_result == "bullish":
            return "bullish"
        elif pole_move < 0 and triangle_result == "bearish":
            return "bearish"
        return False
    
    def is_rectangle(self, data, window=20, threshold=0.01):
        """
        Detect Rectangle pattern (consolidation)
        Characteristics:
        1. Parallel support and resistance levels
        2. Breakout can be in either direction
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        avg_high = np.mean(highs)
        avg_low = np.mean(lows)
    
        # Check if highs and lows are within tight range
        high_std = np.std(highs) / avg_high
        low_std = np.std(lows) / avg_low
    
        if high_std > threshold or low_std > threshold:
            return False
    
        # Check for breakout
        latest_close = data.iloc[-1]['Close']
        if latest_close > avg_high * (1 + threshold):
            return "bullish"
        elif latest_close < avg_low * (1 - threshold):
            return "bearish"
        return False
    
    def is_flag(self, data, window=20, consolidation_window=5, angle_threshold=30):
        """
        Detect Flag pattern (continuation)
        Characteristics:
        1. Sharp move followed by small channel moving against trend
        2. Breakout in same direction as initial move
        """
        if len(data) < window:
            return False
    
        # Check for preceding strong move (pole)
        pole_data = data.iloc[-window:-consolidation_window]
        pole_move = (pole_data['Close'].iloc[-1] - pole_data['Close'].iloc[0]) / pole_data['Close'].iloc[0]
    
        if abs(pole_move) < 0.05:  # At least 5% move
            return False
    
        # Check for small counter-trend channel
        flag_data = data.tail(consolidation_window)
        highs = flag_data['High'].values
        lows = flag_data['Low'].values
    
        x = np.arange(len(highs))
        high_slope, _, _, _, _ = linregress(x, highs)
        low_slope, _, _, _, _ = linregress(x, lows)
    
        # Convert slope to angle (degrees)
        high_angle = np.degrees(np.arctan(high_slope))
        low_angle = np.degrees(np.arctan(low_slope))
    
        # Check if channel is roughly parallel and angled against pole
        if abs(high_angle - low_angle) > angle_threshold:
            return False
    
        if (pole_move > 0 and (high_angle > -angle_threshold or low_angle > -angle_threshold)) or \
                (pole_move < 0 and (high_angle < angle_threshold or low_angle < angle_threshold)):
            return False
    
        # Check breakout direction matches pole direction
        latest_close = data.iloc[-1]['Close']
        upper_bound = high_slope * (len(highs)-1) + highs[0]
        lower_bound = low_slope * (len(lows)-1) + lows[0]
    
        if pole_move > 0 and latest_close > upper_bound:
            return "bullish"
        elif pole_move < 0 and latest_close < lower_bound:
            return "bearish"
        return False
    
    def is_channel(self, data, window=20, parallel_threshold=5):
        """
        Detect Price Channel (parallel trendlines)
        Characteristics:
        1. Parallel support and resistance trendlines
        2. Can be rising, falling or horizontal
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        x = np.arange(len(highs))
        high_slope, _, _, _, _ = linregress(x, highs)
        low_slope, _, _, _, _ = linregress(x, lows)
    
        # Convert slopes to angles
        high_angle = np.degrees(np.arctan(high_slope))
        low_angle = np.degrees(np.arctan(low_slope))
    
        # Check if trendlines are roughly parallel
        if abs(high_angle - low_angle) > parallel_threshold:
            return False
    
        # Determine channel direction
        avg_slope = (high_slope + low_slope) / 2
        if avg_slope > 0.005:
            return "rising"
        elif avg_slope < -0.005:
            return "falling"
        else:
            return "horizontal"
    
    def is_triple_top(self, data, window=30, similarity_threshold=0.01):
        """
        Detect Triple Top pattern (bearish reversal)
        Characteristics:
        1. Three peaks at similar price levels
        2. Two troughs between them
        3. Break below support level
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        # Find all peaks
        peaks = []
        for i in range(1, len(highs)-1):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                peaks.append((i, highs[i]))
    
        if len(peaks) < 3:
            return False
    
        # Get three highest peaks
        peaks.sort(key=lambda x: x[1], reverse=True)
        top_peaks = peaks[:3]
        top_peaks.sort(key=lambda x: x[0])  # Sort by position
    
        # Check price similarity
        max_peak = max(p[1] for p in top_peaks)
        min_peak = min(p[1] for p in top_peaks)
        if (max_peak - min_peak) / max_peak > similarity_threshold:
            return False
    
        # Find troughs between peaks
        trough1 = min(lows[top_peaks[0][0]:top_peaks[1][0]+1])
        trough2 = min(lows[top_peaks[1][0]:top_peaks[2][0]+1])
        support_level = min(trough1, trough2)
    
        # Check breakout
        latest_close = data.iloc[-1]['Close']
        return latest_close < support_level
    
    def is_triple_bottom(self, data, window=30, similarity_threshold=0.01):
        """
        Detect Triple Bottom pattern (bullish reversal)
        Characteristics:
        1. Three troughs at similar price levels
        2. Two peaks between them
        3. Break above resistance level
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
    
        # Find all troughs
        troughs = []
        for i in range(1, len(lows)-1):
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                troughs.append((i, lows[i]))
    
        if len(troughs) < 3:
            return False
    
        # Get three lowest troughs
        troughs.sort(key=lambda x: x[1])
        bottom_troughs = troughs[:3]
        bottom_troughs.sort(key=lambda x: x[0])  # Sort by position
    
        # Check price similarity
        max_trough = max(p[1] for p in bottom_troughs)
        min_trough = min(p[1] for p in bottom_troughs)
        if (max_trough - min_trough) / max_trough > similarity_threshold:
            return False
    
        # Find peaks between troughs
        peak1 = max(highs[bottom_troughs[0][0]:bottom_troughs[1][0]+1])
        peak2 = max(highs[bottom_troughs[1][0]:bottom_troughs[2][0]+1])
        resistance_level = max(peak1, peak2)
    
        # Check breakout
        latest_close = data.iloc[-1]['Close']
        return latest_close > resistance_level

    def is_cup_and_handle(self, data, window=40, handle_window=10, depth_threshold=0.3):
        """
        Detect Cup and Handle pattern (bullish)
        Characteristics:
        1. U-shaped cup formation
        2. Smaller downward handle
        3. Breakout above handle resistance
        """
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
        closes = recent_data['Close'].values

        # Split data into cup and handle portions
        cup_data = recent_data.iloc[:-handle_window]
        handle_data = recent_data.iloc[-handle_window:]

        # Find left and right highs (rim of cup)
        left_high = cup_data['High'].iloc[0]
        right_high = cup_data['High'].iloc[-1]

        # Cup should return to similar price level
        if abs(left_high - right_high) / left_high > 0.05:
            return False

        # Find lowest point in cup (depth)
        cup_low = cup_data['Low'].min()
        cup_depth = (left_high - cup_low) / left_high

        # Cup should have significant depth
        if cup_depth < depth_threshold:
            return False

        # Handle should be downward sloping and smaller than cup
        handle_highs = handle_data['High'].values
        handle_lows = handle_data['Low'].values

        x = np.arange(len(handle_highs))
        handle_slope, _, _, _, _ = linregress(x, handle_highs)

        if handle_slope >= 0:
            return False

        # Check breakout above handle resistance
        handle_resistance = handle_highs.max()
        latest_close = data.iloc[-1]['Close']
        return latest_close > handle_resistance

    def is_rounding_bottom(self, data, window=30, smoothness=0.9):
        """
        Detect Rounding Bottom pattern (bullish reversal)
        Characteristics:
        1. Gradual U-shaped bottom formation
        2. Breakout above resistance
        """
        if len(data) < window:
            return False

        recent_data = data.tail(window)
        closes = recent_data['Close'].values

        # Fit quadratic curve (U-shape)
        x = np.arange(len(closes))
        coeffs = np.polyfit(x, closes, 2)

        # Check if curve is U-shaped (positive quadratic coefficient)
        if coeffs[0] <= 0:
            return False

        # Check how well data fits the curve
        predicted = np.polyval(coeffs, x)
        r_squared = 1 - (np.sum((closes - predicted)**2) / np.sum((closes - np.mean(closes))**2))

        if r_squared < smoothness:
            return False

        # Resistance is the starting level
        resistance = closes[0]
        latest_close = data.iloc[-1]['Close']
        return latest_close > resistance
    
    def is_diamond(self, data, window=30, expansion_threshold=0.5):
        """
        Detect Diamond pattern (reversal)
        Characteristics:
        1. Initial expansion then contraction of price range
        2. Breakout can be in either direction
        """
        if len(data) < window:
            return False
    
        recent_data = data.tail(window)
        highs = recent_data['High'].values
        lows = recent_data['Low'].values
        ranges = highs - lows
    
        # Split data into first and second halves
        half = len(ranges) // 2
        first_half = ranges[:half]
        second_half = ranges[half:]
    
        # Check if first half shows expansion and second half shows contraction
        if np.mean(first_half[-3:]) < np.mean(first_half[:3]) or \
                np.mean(second_half[-3:]) > np.mean(second_half[:3]):
            return False
    
        # Check breakout direction
        latest_close = data.iloc[-1]['Close']
        support = min(lows[half:])
        resistance = max(highs[half:])
    
        if latest_close > resistance:
            return "bullish"
        elif latest_close < support:
            return "bearish"
        return False

    def run_hourly_scanner(self):
        """
        Run the scanner to calculate breakouts/breakdowns and behavior scans with optimized batch processing.
        Uses batched API calls to reduce the number of requests to Yahoo Finance.
        """
        try:
            # Get F&O stocks to scan
            fno_stocks = self.option_chain_service.get_fno_stocks_with_symbols()
            print(f"Processing {len(fno_stocks)} stocks for breakouts/breakdowns")
            
            # Define timeframes for breakouts/breakdowns
            timeframes = {
                "previous_day": {"days": 2},  # Include today + previous day
                "one_week": {"days": 7},
                "one_month": {"days": 30},
                "fifty_two_week": {"days": 365},
                "all_time": {"days": None},  # Use all available data
            }
            
            # Configuration for batch processing
            BATCH_SIZE = 30  # Number of stocks in each batch
            total_stocks = len(fno_stocks)
            batches = [fno_stocks[i:i + BATCH_SIZE] for i in range(0, total_stocks, BATCH_SIZE)]
            total_batches = len(batches)
            
            # Prepare results
            results = []
            total_processed = 0
            
            print(f"Processing {total_batches} batches with {BATCH_SIZE} stocks per batch")
            
            # Process each batch of stocks
            for batch_idx, batch_stocks in enumerate(batches):
                batch_start_time = time.time()
                print(f"Processing batch {batch_idx+1}/{total_batches} with {len(batch_stocks)} stocks...")
                
                try:
                    # Fetch data for all stocks in the batch at once
                    ticker_str = " ".join(batch_stocks)
                    print(f"Downloading batch daily data for {len(batch_stocks)} stocks")
                    
                    # Fetch daily data for the batch
                    batch_daily_data = yf.download(
                        tickers=ticker_str,
                        period="max",       # Get maximum available history
                        interval="1d",      # Daily data
                        group_by='ticker',  # Group by ticker
                        progress=False,     # Disable progress bar
                        auto_adjust=True,    # Adjust for splits and dividends
                        session=self.session
                    )
                    
                    # Fetch weekly data for the batch
                    print(f"Downloading batch weekly data for {len(batch_stocks)} stocks")
                    batch_weekly_data = yf.download(
                        tickers=ticker_str,
                        period="max",       # Get maximum available history
                        interval="1wk",     # Weekly data
                        group_by='ticker',  # Group by ticker
                        progress=False,     # Disable progress bar
                        auto_adjust=True,    # Adjust for splits and dividends
                        session=self.session
                    )
                    
                    # Process each stock in the batch
                    for stock in batch_stocks:
                        try:
                            print(f"Processing data for stock: {stock}")
                            
                            # Extract this stock's data from the batch download
                            if stock in batch_daily_data.columns.levels[0]:
                                data = batch_daily_data[stock].copy()
                            else:
                                print(f"No daily data available for {stock}")
                                continue
                                
                            if stock in batch_weekly_data.columns.levels[0]:
                                weekly_data = batch_weekly_data[stock].copy()
                            else:
                                print(f"No weekly data available for {stock}")
                                continue
                            
                            # Ensure the data has all required columns
                            required_columns = ["Open", "High", "Low", "Close", "Volume"]
                            if not all(col in data.columns for col in required_columns):
                                missing = [col for col in required_columns if col not in data.columns]
                                print(f"Missing columns for {stock}: {missing}")
                                continue
                                
                            if not all(col in weekly_data.columns for col in required_columns):
                                missing = [col for col in required_columns if col not in weekly_data.columns]
                                print(f"Missing weekly columns for {stock}: {missing}")
                                continue
                            
                            # Ensure numeric columns are properly converted
                            for col in required_columns:
                                data[col] = pd.to_numeric(data[col], errors="coerce")
                                weekly_data[col] = pd.to_numeric(weekly_data[col], errors="coerce")
                            
                            # Drop rows with invalid numeric values
                            data = data.dropna(subset=["High", "Low", "Close"])
                            weekly_data = weekly_data.dropna(subset=["High", "Low", "Close"])
                            
                            if len(data) == 0:
                                print(f"No valid data after cleaning for {stock}")
                                continue
                            
                            if len(weekly_data) == 0:
                                print(f"No valid weekly data after cleaning for {stock}")
                                continue
                            
                            # Get the latest close price for comparison
                            last_close = data["Close"].iloc[-1]
                            total_processed += 1
                            
                            # Calculate breakouts/breakdowns for each timeframe
                            for name, params in timeframes.items():
                                try:
                                    days = params["days"]
                                    
                                    if name == "previous_day":
                                        # For previous day, we want specifically the previous trading day
                                        if len(data) >= 2:
                                            # Get only the previous day's data
                                            subset = data.iloc[-2:-1]
                                            high = subset["High"].max()
                                            low = subset["Low"].min()
                                        else:
                                            print(f"Not enough data for previous day {stock}")
                                            continue
                                    elif days is None:
                                        # Use all available data for "all_time"
                                        subset = data
                                        high = subset["High"].max()
                                        low = subset["Low"].min()
                                    else:
                                        # For other periods, calculate proper date range
                                        end_date = data.index[-1]
                                        start_date = end_date - pd.Timedelta(days=days)
                                        subset = data[data.index >= start_date]
                                        
                                        if subset.empty:
                                            print(f"No data in timeframe for {stock} {name}")
                                            continue
                                        
                                        high = subset["High"].max()
                                        low = subset["Low"].min()
                                    
                                    # Debug info - always show the SAME last close (current price)
                                    print(f"{stock} {name}: Close={last_close:.2f}, High={high:.2f}, Low={low:.2f}")
                                    
                                    # Check for breakout/breakdown
                                    if last_close > high and not pd.isna(high):
                                        print(f"✅ BREAKOUT: {stock} on {name} timeframe")
                                        results.append((stock, f"{name}_breakout", True))
                                    elif last_close < low and not pd.isna(low):
                                        print(f"⬇️ BREAKDOWN: {stock} on {name} timeframe")
                                        results.append((stock, f"{name}_breakdown", False))
                                        
                                except Exception as e:
                                    print(f"Error processing {name} timeframe for {stock}: {e}")
                            
                            # Add SRS and ARS scanner logic
                            try:
                                srs = self.calculate_srs(stock, data)
                                ars = self.calculate_ars(stock, data)

                                # Ensure SRS and ARS are pandas Series
                                if not srs.empty and not ars.empty:
                                    # New scan logic
                                    if srs.iloc[-1] > 0 and ars.iloc[-1] > 0:
                                        results.append((stock, "srs_ars_above_zero", True))
                                    elif srs.iloc[-1] < 0 and ars.iloc[-1] < 0:
                                        results.append((stock, "srs_ars_below_zero", False))
                                    if srs.iloc[-2] < 0 and srs.iloc[-1] > 0:
                                        results.append((stock, "srs_crossing_above_zero", True))
                                    if srs.iloc[-2] > 0 and srs.iloc[-1] < 0:
                                        results.append((stock, "srs_crossing_below_zero", False))
                                    if ars.iloc[-2] > 0 and ars.iloc[-1] < 0:
                                        results.append((stock, "ars_crossing_below_zero", False))
                                    if ars.iloc[-2] < 0 and ars.iloc[-1] > 0:
                                        results.append((stock, "ars_crossing_above_zero", True))
                                    if srs.iloc[-1] > 0 and ars.iloc[-1] > ars.iloc[-2]:
                                        results.append((stock, "both_above_zero_and_ars_rising", True))
                                    if ars.iloc[-1] > 0 and srs.iloc[-1] > srs.iloc[-2]:
                                        results.append((stock, "both_above_zero_and_srs_rising", True))
                            except Exception as e:
                                print(f"Error calculating SRS/ARS for {stock}: {e}")

                            # Add 1-day behavior scans
                            results.extend(self.calculate_1_day_behaviors(stock, data))

                            # Add 2-day behavior scans
                            results.extend(self.calculate_2_day_behaviors(stock, data))

                            # Add 3-day behavior scans
                            results.extend(self.calculate_3_day_behaviors(stock, data))

                            # Add absolute return scans
                            results.extend(self.calculate_absolute_returns(stock, data))

                            # Add candlestick pattern scans
                            results.extend(self.calculate_candlestick_scans(stock, data))

                            # Add momentum score scans
                            results.extend(self.calculate_momentum_scans(stock, data))

                            # Add SMA scans
                            self.calculate_sma_scans(stock, data, results)
                            self.calculate_weekly_sma_scans(stock, weekly_data, results)

                            # Add EMA scans
                            self.calculate_ema_scans(stock, data, results)
                            self.calculate_weekly_ema_scans(stock, weekly_data, results)

                            # Add Stochastic scans
                            self.calculate_stochastic_scans(stock, data, results)

                            # Add Parabolic SAR scans
                            self.calculate_parabolic_sar_scans(stock, data, results)
                            self.calculate_weekly_parabolic_sar_scans(stock, weekly_data, results)

                            # Add CCI scans
                            self.calculate_cci_scans(stock, data, results)

                            # Add RSI scans
                            self.calculate_rsi_scans(stock, data, results)
                            self.calculate_weekly_rsi_scans(stock, weekly_data, results)

                            # Add MFI scans
                            self.calculate_mfi_scans(stock, data, results)

                            # Add Williams %R scans
                            self.calculate_williams_r_scans(stock, data, results)

                            # Add ROC scans
                            self.calculate_roc_scans(stock, data, results)

                            # Add MACD scans
                            self.calculate_macd_scans(stock, data, results)

                            # Add ADX scans
                            self.calculate_adx_scans(stock, data, results)

                            # Add ATR scans
                            self.calculate_atr_scans(stock, data, results)

                            # Add Bollinger Band scans
                            self.calculate_bollinger_band_scans(stock, data, results)

                            # Add Narrow Range scans
                            self.calculate_narrow_range_scans(stock, data, results)

                            # Add Supertrend scans
                            self.calculate_supertrend_scans(stock, data, results)
                            self.calculate_weekly_supertrend_scans(stock, weekly_data, results, period=7, multiplier=3)
                            
                            # Add chart pattern detection
                            if self.is_double_top(data):
                                results.append((stock, "pattern_double_top", True))

                            if self.is_double_bottom(data):
                                results.append((stock, "pattern_double_bottom", True))

                            if self.is_head_and_shoulders(data):
                                results.append((stock, "pattern_head_and_shoulders", True))

                            if self.is_inverse_head_and_shoulders(data):
                                results.append((stock, "pattern_inverse_head_and_shoulders", True))

                            wedge_result = self.is_rising_wedge(data)
                            if wedge_result:
                                results.append((stock, "pattern_rising_wedge", True))

                            wedge_result = self.is_falling_wedge(data)
                            if wedge_result:
                                results.append((stock, "pattern_falling_wedge", True))

                            triangle_result = self.is_triangle(data)
                            if triangle_result == "bullish":
                                results.append((stock, "pattern_triangle_bullish", True))
                            elif triangle_result == "bearish":
                                results.append((stock, "pattern_triangle_bearish", True))

                            pennant_result = self.is_pennant(data)
                            if pennant_result == "bullish":
                                results.append((stock, "pattern_pennant_bullish", True))
                            elif pennant_result == "bearish":
                                results.append((stock, "pattern_pennant_bearish", True))

                            rectangle_result = self.is_rectangle(data)
                            if rectangle_result == "bullish":
                                results.append((stock, "pattern_rectangle_bullish", True))
                            elif rectangle_result == "bearish":
                                results.append((stock, "pattern_rectangle_bearish", True))

                            flag_result = self.is_flag(data)
                            if flag_result == "bullish":
                                results.append((stock, "pattern_flag_bullish", True))
                            elif flag_result == "bearish":
                                results.append((stock, "pattern_flag_bearish", True))

                            channel_result = self.is_channel(data)
                            if channel_result:
                                results.append((stock, f"pattern_channel_{channel_result}", True))

                            if self.is_triple_top(data):
                                results.append((stock, "pattern_triple_top", True))
                            if self.is_triple_bottom(data):
                                results.append((stock, "pattern_triple_bottom", True))

                            if self.is_cup_and_handle(data):
                                results.append((stock, "pattern_cup_and_handle", True))

                            if self.is_rounding_bottom(data):
                                results.append((stock, "pattern_rounding_bottom", True))

                            diamond_result = self.is_diamond(data)
                            if diamond_result == "bullish":
                                results.append((stock, "pattern_diamond_bullish", True))
                            elif diamond_result == "bearish":
                                results.append((stock, "pattern_diamond_bearish", True))

                        except Exception as e:
                            print(f"Error processing stock {stock}: {e}")
                    
                    # Calculate batch processing statistics
                    batch_time = time.time() - batch_start_time
                    batch_success_rate = len([s for s in batch_stocks if s in batch_daily_data.columns.levels[0]]) / len(batch_stocks) * 100
                    
                    print(f"Batch {batch_idx+1}/{total_batches} completed in {batch_time:.2f}s with {batch_success_rate:.1f}% success rate")
                    print(f"Processed {total_processed}/{total_stocks} stocks so far")
                    
                    # Adaptive delay between batches based on success rate
                    if batch_success_rate < 70:
                        delay = min(10, 5 * (1 + (70 - batch_success_rate) / 20))
                        print(f"Low success rate detected. Cooling down for {delay:.1f}s before next batch")
                        time.sleep(delay)
                    else:
                        time.sleep(2)  # Small delay between successful batches
                        
                except Exception as e:
                    print(f"Error processing batch {batch_idx+1}: {e}")
                    time.sleep(10)  # Longer delay after batch-level exception
            
            # Add futures open interest scan
            futures_oi_results = self.scan_futures_oi_changes()
            if futures_oi_results:
                results.extend(futures_oi_results)
                
            # Add options open interest scan - provide a default condition
            options_condition = {
                'value': 100,  # Default threshold percentage
                'timeframe': '1d',  # Default timeframe
                'direction': 'above',  # Default direction
                'option_type': 'both'  # Default option type
            }
            options_oi_results = self.scan_options_oi_changes(options_condition)
            if options_oi_results:
                results.extend(options_oi_results)

            success_rate = (total_processed / total_stocks) * 100 if total_stocks > 0 else 0
            print(f"Scanner completed: processed {total_processed}/{total_stocks} stocks ({success_rate:.1f}%), found {len(results)} signals")

            # Save results to the database
            if results:
                self.save_scanner_results(results)
                print(f"Saved {len(results)} hourly scanner results successfully.")
            else:
                print("No signals found to save")

        except Exception as e:
            print(f"Error in hourly scanner: {e}")
    
    def calculate_srs(self, stock, data):
        """Calculate Static Relative Strength (SRS) for a stock."""
        # Ensure data is a pandas DataFrame
        if data.empty or "Close" not in data:
            return pd.Series(dtype=float)

        # Calculate SRS as a Series
        return data["Close"] - data["Close"].mean()

    def calculate_ars(self, stock, data):
        """Calculate Adaptive Relative Strength (ARS) for a stock."""
        # Ensure data is a pandas DataFrame
        if data.empty or "Close" not in data:
            return pd.Series(dtype=float)

        # Calculate ARS as a Series
        rolling_mean = data["Close"].rolling(window=14).mean()
        return data["Close"] - rolling_mean

    def calculate_roc_scans(self, stock, data, results):
        """Calculate Rate of Change (ROC) scans."""
        if len(data) < 14:  # Ensure enough data for ROC calculation
            return

        try:
            # Calculate ROC
            roc = ((data['Close'] - data['Close'].shift(14)) / data['Close'].shift(14)) * 100

            # Add ROC-based scans
            if roc.iloc[-1] > 0:
                results.append((stock, "roc_trending_up", True))
            if roc.iloc[-1] < 0:
                results.append((stock, "roc_trending_down", True))
            if len(roc) > 1 and roc.iloc[-1] > roc.iloc[-2]:
                results.append((stock, "roc_increasing", True))
            if len(roc) > 1 and roc.iloc[-1] < roc.iloc[-2]:
                results.append((stock, "roc_decreasing", True))

        except Exception as e:
            print(f"Error calculating ROC scans for {stock}: {e}")

    def calculate_macd_scans(self, stock, data, results):
        """Calculate MACD-based scans."""
        if len(data) < 26:  # Ensure enough data for MACD calculation
            return

        try:
            # Calculate MACD
            ema12 = data['Close'].ewm(span=12, adjust=False).mean()
            ema26 = data['Close'].ewm(span=26, adjust=False).mean()
            macd = ema12 - ema26
            signal = macd.ewm(span=9, adjust=False).mean()

            # Add MACD-based scans
            if macd.iloc[-1] > signal.iloc[-1]:
                results.append((stock, "macd_crossing_signal_line_above", True))
            if macd.iloc[-1] < signal.iloc[-1]:
                results.append((stock, "macd_crossing_signal_line_below", True))
            if macd.iloc[-1] > 0:
                results.append((stock, "macd_above_zero", True))
            if macd.iloc[-1] < 0:
                results.append((stock, "macd_below_zero", True))
            if len(macd) > 1 and macd.iloc[-2] < signal.iloc[-2] and macd.iloc[-1] > signal.iloc[-1]:
                results.append((stock, "macd_crossed_signal_line_from_below", True))
            if len(macd) > 1 and macd.iloc[-2] > signal.iloc[-2] and macd.iloc[-1] < signal.iloc[-1]:
                results.append((stock, "macd_crossed_signal_line_from_above", True))

        except Exception as e:
            print(f"Error calculating MACD scans for {stock}: {e}")

    def calculate_cci_scans(self, stock, data, results):
        """Calculate Commodity Channel Index (CCI) scans."""
        if len(data) < 20:  # Ensure enough data for CCI calculation
            return

        try:
            # Calculate CCI
            typical_price = (data['High'] + data['Low'] + data['Close']) / 3
            sma = typical_price.rolling(window=20).mean()
            # Replace the incorrect `mad()` with manual mean absolute deviation calculation
            mean_deviation = typical_price.rolling(window=20).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
            cci = (typical_price - sma) / (0.015 * mean_deviation)
    
            # Add CCI-based scans
            if cci.iloc[-1] > 100:
                results.append((stock, "cci_overbought_zone", True))
            if cci.iloc[-1] < -100:
                results.append((stock, "cci_oversold_zone", True))
            if len(cci) > 1 and cci.iloc[-2] <= 100 < cci.iloc[-1]:
                results.append((stock, "cci_crossed_above_100", True))
            if len(cci) > 1 and cci.iloc[-2] >= -100 > cci.iloc[-1]:
                results.append((stock, "cci_crossed_below_100", True))
            if cci.iloc[-1] > 0:
                results.append((stock, "cci_bullish", True))
            if cci.iloc[-1] < 0:
                results.append((stock, "cci_bearish", True))
            if len(cci) > 1 and cci.iloc[-1] > cci.iloc[-2]:
                results.append((stock, "cci_trending_up", True))
            if len(cci) > 1 and cci.iloc[-1] < cci.iloc[-2]:
                results.append((stock, "cci_trending_down", True))
    
        except Exception as e:
            print(f"Error calculating CCI scans for {stock}: {e}")

    def calculate_rsi_scans(self, stock, data, results):
        """Calculate RSI-based scans."""
        if len(data) < 14:  # Ensure enough data for RSI calculation
            return

        try:
            # Calculate RSI
            delta = data['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))

            # Add RSI-based scans
            if rsi.iloc[-1] > 70:
                results.append((stock, "rsi_overbought_zone", True))
            if rsi.iloc[-1] < 30:
                results.append((stock, "rsi_oversold_zone", True))
            if len(rsi) > 1 and rsi.iloc[-2] <= 70 < rsi.iloc[-1]:
                results.append((stock, "rsi_crossed_above_70", True))
            if len(rsi) > 1 and rsi.iloc[-2] >= 30 > rsi.iloc[-1]:
                results.append((stock, "rsi_crossed_below_30", True))
            if rsi.iloc[-1] > 50:
                results.append((stock, "rsi_bullish", True))
            if rsi.iloc[-1] < 50:
                results.append((stock, "rsi_bearish", True))
            if len(rsi) > 1 and rsi.iloc[-1] > rsi.iloc[-2]:
                results.append((stock, "rsi_trending_up", True))
            if len(rsi) > 1 and rsi.iloc[-1] < rsi.iloc[-2]:
                results.append((stock, "rsi_trending_down", True))

        except Exception as e:
            print(f"Error calculating RSI scans for {stock}: {e}")

    def calculate_weekly_rsi_scans(self, stock, weekly_data, results):
        """Calculate weekly RSI-based scans."""
        if len(weekly_data) < 14:  # Ensure enough data for RSI calculation
            return

        try:
            # Calculate RSI
            delta = weekly_data['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))

            # Add RSI-based scans
            if rsi.iloc[-1] > 70:
                results.append((stock, "rsi_weekly_overbought_zone", True))
            if rsi.iloc[-1] < 30:
                results.append((stock, "rsi_weekly_oversold_zone", True))
            if len(rsi) > 1 and rsi.iloc[-2] <= 70 < rsi.iloc[-1]:
                results.append((stock, "rsi_weekly_crossed_above_70", True))
            if len(rsi) > 1 and rsi.iloc[-2] >= 30 > rsi.iloc[-1]:
                results.append((stock, "rsi_weekly_crossed_below_30", True))
            if rsi.iloc[-1] > 50:
                results.append((stock, "rsi_weekly_bullish", True))
            if rsi.iloc[-1] < 50:
                results.append((stock, "rsi_weekly_bearish", True))
            if len(rsi) > 1 and rsi.iloc[-1] > rsi.iloc[-2]:
                results.append((stock, "rsi_weekly_trending_up", True))
            if len(rsi) > 1 and rsi.iloc[-1] < rsi.iloc[-2]:
                results.append((stock, "rsi_weekly_trending_down", True))

        except Exception as e:
            print(f"Error calculating weekly RSI scans for {stock}: {e}")

    def calculate_mfi_scans(self, stock, data, results):
        """Calculate Money Flow Index (MFI) scans."""
        if len(data) < 14:  # Ensure enough data for MFI calculation
            return

        try:
            # Calculate MFI
            typical_price = (data['High'] + data['Low'] + data['Close']) / 3
            money_flow = typical_price * data['Volume']
            positive_flow = money_flow.where(data['Close'] > data['Close'].shift(1), 0).rolling(window=14).sum()
            negative_flow = money_flow.where(data['Close'] <= data['Close'].shift(1), 0).rolling(window=14).sum()
            mfi = 100 - (100 / (1 + (positive_flow / negative_flow)))

            # Add MFI-based scans
            if mfi.iloc[-1] > 80:
                results.append((stock, "mfi_above_80", True))
            if mfi.iloc[-1] < 20:
                results.append((stock, "mfi_below_20", True))
            if len(mfi) > 1 and mfi.iloc[-2] <= 80 < mfi.iloc[-1]:
                results.append((stock, "mfi_crossed_80_from_below", True))
            if len(mfi) > 1 and mfi.iloc[-2] >= 20 > mfi.iloc[-1]:
                results.append((stock, "mfi_crossed_20_from_above", True))
            if mfi.iloc[-1] > 50:
                results.append((stock, "mfi_bullish", True))
            if mfi.iloc[-1] < 50:
                results.append((stock, "mfi_bearish", True))
            if len(mfi) > 1 and mfi.iloc[-1] > mfi.iloc[-2]:
                results.append((stock, "mfi_trending_up", True))
            if len(mfi) > 1 and mfi.iloc[-1] < mfi.iloc[-2]:
                results.append((stock, "mfi_trending_down", True))

        except Exception as e:
            print(f"Error calculating MFI scans for {stock}: {e}")

    def calculate_williams_r_scans(self, stock, data, results):
        """Calculate Williams %R scans."""
        if len(data) < 14:  # Ensure enough data for Williams %R calculation
            return

        try:
            # Calculate Williams %R
            highest_high = data['High'].rolling(window=14).max()
            lowest_low = data['Low'].rolling(window=14).min()
            williams_r = -100 * (highest_high - data['Close']) / (highest_high - lowest_low)

            # Add Williams %R-based scans
            if williams_r.iloc[-1] > -20:  # Updated
                results.append((stock, "williams_r_above_20", True))  # Updated
            if williams_r.iloc[-1] < -80:  # Updated
                results.append((stock, "williams_r_below_80", True))  # Updated
            if len(williams_r) > 1 and williams_r.iloc[-2] <= -20 < williams_r.iloc[-1]:  # Updated
                results.append((stock, "williams_r_crossed_20_from_below", True))  # Updated
            if len(williams_r) > 1 and williams_r.iloc[-2] >= -80 > williams_r.iloc[-1]:  # Updated
                results.append((stock, "williams_r_crossed_80_from_above", True))  # Updated
            if williams_r.iloc[-1] > -50:
                results.append((stock, "williams_r_bullish", True))
            if williams_r.iloc[-1] < -50:
                results.append((stock, "williams_r_bearish", True))
            if len(williams_r) > 1 and williams_r.iloc[-1] > williams_r.iloc[-2]:
                results.append((stock, "williams_r_trending_up", True))
            if len(williams_r) > 1 and williams_r.iloc[-1] < williams_r.iloc[-2]:
                results.append((stock, "williams_r_trending_down", True))

        except Exception as e:
            print(f"Error calculating Williams %R scans for {stock}: {e}")

    def calculate_adx_scans(self, stock, data, results):
        """Calculate ADX-based scans."""
        if len(data) < 14:  # Ensure enough data for ADX calculation
            return

        try:
            # Calculate ADX, +DI, and -DI
            high = data['High']
            low = data['Low']
            close = data['Close']

            plus_dm = high.diff()
            minus_dm = low.diff()

            plus_dm[plus_dm < 0] = 0
            minus_dm[minus_dm > 0] = 0

            tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
            atr = tr.rolling(window=14).mean()

            plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
            minus_di = 100 * (abs(minus_dm).rolling(window=14).mean() / atr)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
            adx = dx.rolling(window=14).mean()

            # Add ADX-based scans
            if adx.iloc[-1] > 25 and adx.iloc[-2] <= 25:
                results.append((stock, "adx_crossing_25_from_below", True))
            if adx.iloc[-1] < 25 and adx.iloc[-2] >= 25:
                results.append((stock, "adx_crossing_25_from_above", True))
            if adx.iloc[-1] > 40 and adx.iloc[-2] <= 40:
                results.append((stock, "adx_crossing_40_from_below", True))
            if adx.iloc[-1] < 40 and adx.iloc[-2] >= 40:
                results.append((stock, "adx_crossing_40_from_above", True))
            if adx.iloc[-1] > 10 and adx.iloc[-2] <= 10:
                results.append((stock, "adx_crossing_10_from_below", True))
            if adx.iloc[-1] < 10 and adx.iloc[-2] >= 10:
                results.append((stock, "adx_crossing_10_from_above", True))

            # Add +DI and -DI crossing scans
            if plus_di.iloc[-1] > minus_di.iloc[-1] and plus_di.iloc[-2] <= minus_di.iloc[-2]:
                results.append((stock, "plus_di_crossing_minus_di_from_below", True))
            if plus_di.iloc[-1] < minus_di.iloc[-1] and plus_di.iloc[-2] >= minus_di.iloc[-2]:
                results.append((stock, "plus_di_crossing_minus_di_from_above", True))
            if plus_di.iloc[-1] > 25 and plus_di.iloc[-2] <= 25:
                results.append((stock, "plus_di_crossing_25_from_below", True))
            if minus_di.iloc[-1] > 25 and minus_di.iloc[-2] <= 25:
                results.append((stock, "minus_di_crossing_25_from_below", True))

        except Exception as e:
            print(f"Error calculating ADX scans for {stock}: {e}")

    def calculate_atr_scans(self, stock, data, results):
        """Calculate Average True Range (ATR) scans."""
        if len(data) < 7:  # Ensure enough data for ATR calculation
            return

        try:
            # Calculate True Range (TR)
            high_low = data['High'] - data['Low']
            high_close = abs(data['High'] - data['Close'].shift())
            low_close = abs(data['Low'] - data['Close'].shift())
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

            # Calculate ATR for 3, 5, and 7 days
            atr_3 = true_range.rolling(window=3).mean()
            atr_5 = true_range.rolling(window=5).mean()
            atr_7 = true_range.rolling(window=7).mean()

            # Add ATR-based scans
            if len(atr_3) > 1 and atr_3.iloc[-1] > atr_3.iloc[-2]:
                results.append((stock, "atr_3_days_increasing", True))
            if len(atr_3) > 1 and atr_3.iloc[-1] < atr_3.iloc[-2]:
                results.append((stock, "atr_3_days_decreasing", True))

            if len(atr_5) > 1 and atr_5.iloc[-1] > atr_5.iloc[-2]:
                results.append((stock, "atr_5_days_increasing", True))
            if len(atr_5) > 1 and atr_5.iloc[-1] < atr_5.iloc[-2]:
                results.append((stock, "atr_5_days_decreasing", True))

            if len(atr_7) > 1 and atr_7.iloc[-1] > atr_7.iloc[-2]:
                results.append((stock, "atr_7_days_increasing", True))
            if len(atr_7) > 1 and atr_7.iloc[-1] < atr_7.iloc[-2]:
                results.append((stock, "atr_7_days_decreasing", True))

        except Exception as e:
            print(f"Error calculating ATR scans for {stock}: {e}")

    def calculate_bollinger_band_scans(self, stock, data, results):
        """Calculate Bollinger Band scans."""
        if len(data) < 20:  # Ensure enough data for Bollinger Band calculation
            return

        try:
            # Calculate Bollinger Bands
            sma = data['Close'].rolling(window=20).mean()
            std_dev = data['Close'].rolling(window=20).std()
            upper_band = sma + (2 * std_dev)
            lower_band = sma - (2 * std_dev)
            band_width = upper_band - lower_band

            # Add Bollinger Band-based scans
            if data['Close'].iloc[-1] > upper_band.iloc[-1]:
                results.append((stock, "close_above_upper_bollinger_band", True))
            if data['Close'].iloc[-1] < lower_band.iloc[-1]:
                results.append((stock, "close_below_lower_bollinger_band", True))
            if len(data) > 1 and data['Close'].iloc[-2] <= upper_band.iloc[-2] and data['Close'].iloc[-1] > upper_band.iloc[-1]:
                results.append((stock, "close_crossing_upper_bollinger_band_from_below", True))
            if len(data) > 1 and data['Close'].iloc[-2] >= lower_band.iloc[-2] and data['Close'].iloc[-1] < lower_band.iloc[-1]:
                results.append((stock, "close_crossing_lower_bollinger_band_from_above", True))
            if len(data) > 1 and data['Close'].iloc[-2] >= upper_band.iloc[-2] and data['Close'].iloc[-1] < upper_band.iloc[-1]:
                results.append((stock, "close_crossing_upper_bollinger_band_from_above", True))
            if len(data) > 1 and data['Close'].iloc[-2] <= lower_band.iloc[-2] and data['Close'].iloc[-1] > lower_band.iloc[-1]:
                results.append((stock, "close_crossing_lower_bollinger_band_from_below", True))
            if len(band_width) > 1 and band_width.iloc[-1] < band_width.iloc[-2]:
                results.append((stock, "bollinger_band_narrowing", True))
            if len(band_width) > 1 and band_width.iloc[-1] > band_width.iloc[-2]:
                results.append((stock, "bollinger_band_widening", True))

        except Exception as e:
            print(f"Error calculating Bollinger Band scans for {stock}: {e}")

    def calculate_stochastic_scans(self, stock, data, results):
        """Calculate Stochastic Oscillator-based scans."""
        if len(data) < 14:  # Ensure enough data for Stochastic calculation
            return

        try:
            # Calculate %K and %D
            highest_high = data['High'].rolling(window=14).max()
            lowest_low = data['Low'].rolling(window=14).min()
            percent_k = 100 * (data['Close'] - lowest_low) / (highest_high - lowest_low)
            percent_d = percent_k.rolling(window=3).mean()

            # Add Stochastic-based scans
            if percent_k.iloc[-1] > 80:
                results.append((stock, "stochastic_in_overbought_zone", True))
            if percent_k.iloc[-1] < 20:
                results.append((stock, "stochastic_in_oversold_zone", True))
            if len(percent_k) > 1 and percent_k.iloc[-2] <= 80 < percent_k.iloc[-1]:
                results.append((stock, "stochastic_entering_overbought_zone", True))
            if len(percent_k) > 1 and percent_k.iloc[-2] >= 20 > percent_k.iloc[-1]:
                results.append((stock, "stochastic_entering_oversold_zone", True))
            if len(percent_k) > 1 and percent_k.iloc[-1] > percent_k.iloc[-2]:
                results.append((stock, "stochastic_trending_up", True))
            if len(percent_k) > 1 and percent_k.iloc[-1] < percent_k.iloc[-2]:
                results.append((stock, "stochastic_trending_down", True))
            if len(percent_k) > 1 and percent_k.iloc[-2] < percent_d.iloc[-2] and percent_k.iloc[-1] > percent_d.iloc[-1]:
                results.append((stock, "stochastic_k_crossing_d_from_below", True))
            if len(percent_k) > 1 and percent_k.iloc[-2] > percent_d.iloc[-2] and percent_k.iloc[-1] < percent_d.iloc[-1]:
                results.append((stock, "stochastic_k_crossing_d_from_above", True))

        except Exception as e:
            print(f"Error calculating Stochastic scans for {stock}: {e}")

    def calculate_parabolic_sar_scans(self, stock, data, results):
        """Calculate Parabolic SAR-based scans."""
        if len(data) < 2:  # Ensure enough data for Parabolic SAR calculation
            return

        try:
            # Calculate Parabolic SAR using TA-Lib or a custom implementation
            import talib
            psar = talib.SAR(data['High'], data['Low'], acceleration=0.02, maximum=0.2)

            # Add Parabolic SAR-based scans
            if data['Close'].iloc[-1] > psar.iloc[-1]:
                results.append((stock, "psar_bullish_reversal", True))
            if data['Close'].iloc[-1] < psar.iloc[-1]:
                results.append((stock, "psar_bearish_reversal", True))

        except Exception as e:
            print(f"Error calculating Parabolic SAR scans for {stock}: {e}")

    def calculate_weekly_parabolic_sar_scans(self, stock, weekly_data, results):
        """Calculate weekly Parabolic SAR-based scans."""
        if len(weekly_data) < 2:  # Ensure enough data for Parabolic SAR calculation
            return

        try:
            # Calculate Parabolic SAR using TA-Lib or a custom implementation
            import talib
            psar = talib.SAR(weekly_data['High'], weekly_data['Low'], acceleration=0.02, maximum=0.2)

            # Add weekly Parabolic SAR-based scans
            if weekly_data['Close'].iloc[-1] > psar.iloc[-1]:
                results.append((stock, "psar_weekly_bullish_reversal", True))
            if weekly_data['Close'].iloc[-1] < psar.iloc[-1]:
                results.append((stock, "psar_weekly_bearish_reversal", True))

        except Exception as e:
            print(f"Error calculating weekly Parabolic SAR scans for {stock}: {e}")

    def calculate_1_day_behaviors(self, stock, data):
        """Calculate 1-day behavior scans."""
        results = []
        if len(data) < 1:  # Ensure at least 1 day of data
            return results

        try:
            # Check if the stock opened at its high
            if data['Open'].iloc[-1] == data['High'].iloc[-1]:
                results.append((stock, "behavior_1_day_opening_high", True))

            # Check if the stock opened at its low
            if data['Open'].iloc[-1] == data['Low'].iloc[-1]:
                results.append((stock, "behavior_1_day_opening_low", True))

        except Exception as e:
            print(f"Error calculating 1-day behaviors for {stock}: {e}")

        return results

    def calculate_2_day_behaviors(self, stock, data):
        """Calculate 2-day behavior scans."""
        results = []
        if len(data) < 2:  # Ensure at least 2 days of data
            return results

        try:
            # Check for higher highs over the last 2 days
            if data['High'].iloc[-1] > data['High'].iloc[-2]:
                results.append((stock, "behavior_2_day_higher_highs", True))

            # Check for lower lows over the last 2 days
            if data['Low'].iloc[-1] < data['Low'].iloc[-2]:
                results.append((stock, "behavior_2_day_lower_lows", True))

        except Exception as e:
            print(f"Error calculating 2-day behaviors for {stock}: {e}")

        return results

    def calculate_3_day_behaviors(self, stock, data):
        """Calculate 3-day behavior scans."""
        results = []
        if len(data) < 3:  # Ensure at least 3 days of data
            return results

        try:
            # Check for higher highs over the last 3 days
            if data['High'].iloc[-1] > data['High'].iloc[-2] > data['High'].iloc[-3]:
                results.append((stock, "behavior_3_day_higher_highs", True))

            # Check for lower lows over the last 3 days
            if data['Low'].iloc[-1] < data['Low'].iloc[-2] < data['Low'].iloc[-3]:
                results.append((stock, "behavior_3_day_lower_lows", True))

        except Exception as e:
            print(f"Error calculating 3-day behaviors for {stock}: {e}")

        return results


    def calculate_ema_scans(self, stock, data, results):
        """Calculate EMA-based scans."""
        # Calculate daily EMAs
        ema5 = data['Close'].ewm(span=5, adjust=False).mean()
        ema20 = data['Close'].ewm(span=20, adjust=False).mean()
        ema50 = data['Close'].ewm(span=50, adjust=False).mean()
        ema100 = data['Close'].ewm(span=100, adjust=False).mean()
        ema200 = data['Close'].ewm(span=200, adjust=False).mean()
    
        # Check for close price near EMAs (within 0.5%)
        close = data['Close'].iloc[-1]
    
        # Price near EMAs
        if abs(close - ema20.iloc[-1]) / ema20.iloc[-1] < 0.005:
            results.append((stock, "close_near_ema_20", True))
    
        if abs(close - ema50.iloc[-1]) / ema50.iloc[-1] < 0.005:
            results.append((stock, "close_near_ema_50", True))
    
        if abs(close - ema100.iloc[-1]) / ema100.iloc[-1] < 0.005:
            results.append((stock, "close_near_ema_100", True))
    
        if abs(close - ema200.iloc[-1]) / ema200.iloc[-1] < 0.005:
            results.append((stock, "close_near_ema_200", True))
    
        # Price crossing EMAs
        if len(data) > 2:
            # Crossing 20 EMA
            if data['Close'].iloc[-2] < ema20.iloc[-2] and data['Close'].iloc[-1] > ema20.iloc[-1]:
                results.append((stock, "close_crossing_ema_20_above", True))
            if data['Close'].iloc[-2] > ema20.iloc[-2] and data['Close'].iloc[-1] < ema20.iloc[-1]:
                results.append((stock, "close_crossing_ema_20_below", True))
    
            # Crossing 50 EMA
            if data['Close'].iloc[-2] < ema50.iloc[-2] and data['Close'].iloc[-1] > ema50.iloc[-1]:
                results.append((stock, "close_crossing_ema_50_above", True))
            if data['Close'].iloc[-2] > ema50.iloc[-2] and data['Close'].iloc[-1] < ema50.iloc[-1]:
                results.append((stock, "close_crossing_ema_50_below", True))
    
            # Crossing 100 EMA
            if data['Close'].iloc[-2] < ema100.iloc[-2] and data['Close'].iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "close_crossing_ema_100_above", True))
            if data['Close'].iloc[-2] > ema100.iloc[-2] and data['Close'].iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "close_crossing_ema_100_below", True))
    
            # Crossing 200 EMA
            if data['Close'].iloc[-2] < ema200.iloc[-2] and data['Close'].iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "close_crossing_ema_200_above", True))
            if data['Close'].iloc[-2] > ema200.iloc[-2] and data['Close'].iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "close_crossing_ema_200_below", True))
    
        # EMA crossovers
        if len(data) > 2:
            # 5 EMA crossing 20 EMA
            if ema5.iloc[-2] < ema20.iloc[-2] and ema5.iloc[-1] > ema20.iloc[-1]:
                results.append((stock, "ema_5_crossing_ema_20_above", True))
            if ema5.iloc[-2] > ema20.iloc[-2] and ema5.iloc[-1] < ema20.iloc[-1]:
                results.append((stock, "ema_5_crossing_ema_20_below", True))
    
            # 20 EMA crossing 50 EMA
            if ema20.iloc[-2] < ema50.iloc[-2] and ema20.iloc[-1] > ema50.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_50_above", True))
            if ema20.iloc[-2] > ema50.iloc[-2] and ema20.iloc[-1] < ema50.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_50_below", True))
    
            # 20 EMA crossing 100 EMA
            if ema20.iloc[-2] < ema100.iloc[-2] and ema20.iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_100_above", True))
            if ema20.iloc[-2] > ema100.iloc[-2] and ema20.iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_100_below", True))
    
            # 20 EMA crossing 200 EMA
            if ema20.iloc[-2] < ema200.iloc[-2] and ema20.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_200_above", True))
            if ema20.iloc[-2] > ema200.iloc[-2] and ema20.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_20_crossing_ema_200_below", True))
    
            # 50 EMA crossing 100 EMA
            if ema50.iloc[-2] < ema100.iloc[-2] and ema50.iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "ema_50_crossing_ema_100_above", True))
            if ema50.iloc[-2] > ema100.iloc[-2] and ema50.iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "ema_50_crossing_ema_100_below", True))
    
            # 50 EMA crossing 200 EMA
            if ema50.iloc[-2] < ema200.iloc[-2] and ema50.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_50_crossing_ema_200_above", True))
            if ema50.iloc[-2] > ema200.iloc[-2] and ema50.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_50_crossing_ema_200_below", True))
    
            # 100 EMA crossing 200 EMA
            if ema100.iloc[-2] < ema200.iloc[-2] and ema100.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_100_crossing_ema_200_above", True))
            if ema100.iloc[-2] > ema200.iloc[-2] and ema100.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_100_crossing_ema_200_below", True))


    def calculate_weekly_ema_scans(self, stock, weekly_data, results):
        """Calculate weekly EMA-based scans."""
        if len(weekly_data) < 200:
            return
    
        # Calculate weekly EMAs
        ema5 = weekly_data['Close'].ewm(span=5, adjust=False).mean()
        ema20 = weekly_data['Close'].ewm(span=20, adjust=False).mean()
        ema50 = weekly_data['Close'].ewm(span=50, adjust=False).mean()
        ema100 = weekly_data['Close'].ewm(span=100, adjust=False).mean()
        ema200 = weekly_data['Close'].ewm(span=200, adjust=False).mean()
    
        # Check for close price near weekly EMAs
        close = weekly_data['Close'].iloc[-1]
    
        if abs(close - ema20.iloc[-1]) / ema20.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_ema_20", True))
    
        if abs(close - ema50.iloc[-1]) / ema50.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_ema_50", True))
    
        if abs(close - ema100.iloc[-1]) / ema100.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_ema_100", True))
    
        if abs(close - ema200.iloc[-1]) / ema200.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_ema_200", True))
    
        # Price crossing weekly EMAs
        if len(weekly_data) > 2:
            # Crossing 20 EMA
            if weekly_data['Close'].iloc[-2] < ema20.iloc[-2] and weekly_data['Close'].iloc[-1] > ema20.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_20_above", True))
            if weekly_data['Close'].iloc[-2] > ema20.iloc[-2] and weekly_data['Close'].iloc[-1] < ema20.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_20_below", True))
    
            # Crossing 50 EMA
            if weekly_data['Close'].iloc[-2] < ema50.iloc[-2] and weekly_data['Close'].iloc[-1] > ema50.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_50_above", True))
            if weekly_data['Close'].iloc[-2] > ema50.iloc[-2] and weekly_data['Close'].iloc[-1] < ema50.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_50_below", True))
    
            # Crossing 100 EMA
            if weekly_data['Close'].iloc[-2] < ema100.iloc[-2] and weekly_data['Close'].iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_100_above", True))
            if weekly_data['Close'].iloc[-2] > ema100.iloc[-2] and weekly_data['Close'].iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_100_below", True))
    
            # Crossing 200 EMA
            if weekly_data['Close'].iloc[-2] < ema200.iloc[-2] and weekly_data['Close'].iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_200_above", True))
            if weekly_data['Close'].iloc[-2] > ema200.iloc[-2] and weekly_data['Close'].iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "close_crossing_weekly_ema_200_below", True))
    
        # Weekly EMA crossovers
        if len(weekly_data) > 2:
            # 5 EMA crossing 20 EMA
            if ema5.iloc[-2] < ema20.iloc[-2] and ema5.iloc[-1] > ema20.iloc[-1]:
                results.append((stock, "ema_5_crossing_weekly_ema_20_above", True))
            if ema5.iloc[-2] > ema20.iloc[-2] and ema5.iloc[-1] < ema20.iloc[-1]:
                results.append((stock, "ema_5_crossing_weekly_ema_20_below", True))
    
            # 20 EMA crossing 50 EMA
            if ema20.iloc[-2] < ema50.iloc[-2] and ema20.iloc[-1] > ema50.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_50_above", True))
            if ema20.iloc[-2] > ema50.iloc[-2] and ema20.iloc[-1] < ema50.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_50_below", True))
    
            # 20 EMA crossing 100 EMA
            if ema20.iloc[-2] < ema100.iloc[-2] and ema20.iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_100_above", True))
            if ema20.iloc[-2] > ema100.iloc[-2] and ema20.iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_100_below", True))
    
            # 20 EMA crossing 200 EMA
            if ema20.iloc[-2] < ema200.iloc[-2] and ema20.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_200_above", True))
            if ema20.iloc[-2] > ema200.iloc[-2] and ema20.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_20_crossing_weekly_ema_200_below", True))
    
            # 50 EMA crossing 100 EMA
            if ema50.iloc[-2] < ema100.iloc[-2] and ema50.iloc[-1] > ema100.iloc[-1]:
                results.append((stock, "ema_50_crossing_weekly_ema_100_above", True))
            if ema50.iloc[-2] > ema100.iloc[-2] and ema50.iloc[-1] < ema100.iloc[-1]:
                results.append((stock, "ema_50_crossing_weekly_ema_100_below", True))
    
            # 50 EMA crossing 200 EMA
            if ema50.iloc[-2] < ema200.iloc[-2] and ema50.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_50_crossing_weekly_ema_200_above", True))
            if ema50.iloc[-2] > ema200.iloc[-2] and ema50.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_50_crossing_weekly_ema_200_below", True))
    
            # 100 EMA crossing 200 EMA
            if ema100.iloc[-2] < ema200.iloc[-2] and ema100.iloc[-1] > ema200.iloc[-1]:
                results.append((stock, "ema_100_crossing_weekly_ema_200_above", True))
            if ema100.iloc[-2] > ema200.iloc[-2] and ema100.iloc[-1] < ema200.iloc[-1]:
                results.append((stock, "ema_100_crossing_weekly_ema_200_below", True))
        
    def calculate_sma_scans(self, stock, data, results):
        """Calculate SMA-based scans."""
        # Calculate daily SMAs
        sma5 = data['Close'].rolling(window=5).mean()
        sma20 = data['Close'].rolling(window=20).mean()
        sma50 = data['Close'].rolling(window=50).mean()
        sma100 = data['Close'].rolling(window=100).mean()
        sma200 = data['Close'].rolling(window=200).mean()
    
        # Check for close price near SMAs
        close = data['Close'].iloc[-1]
    
        if len(data) >= 20 and abs(close - sma20.iloc[-1]) / sma20.iloc[-1] < 0.005:
            results.append((stock, "close_near_sma_20", True))
    
        if len(data) >= 50 and abs(close - sma50.iloc[-1]) / sma50.iloc[-1] < 0.005:
            results.append((stock, "close_near_sma_50", True))
    
        if len(data) >= 100 and abs(close - sma100.iloc[-1]) / sma100.iloc[-1] < 0.005:
            results.append((stock, "close_near_sma_100", True))
    
        if len(data) >= 200 and abs(close - sma200.iloc[-1]) / sma200.iloc[-1] < 0.005:
            results.append((stock, "close_near_sma_200", True))
    
        # Price crossing SMAs
        if len(data) > 21:  # Need at least two data points after SMA calculation
            # Crossing 20 SMA
            if data['Close'].iloc[-2] < sma20.iloc[-2] and data['Close'].iloc[-1] > sma20.iloc[-1]:
                results.append((stock, "close_crossing_sma_20_above", True))
            if data['Close'].iloc[-2] > sma20.iloc[-2] and data['Close'].iloc[-1] < sma20.iloc[-1]:
                results.append((stock, "close_crossing_sma_20_below", True))
    
        if len(data) > 51:
            # Crossing 50 SMA
            if data['Close'].iloc[-2] < sma50.iloc[-2] and data['Close'].iloc[-1] > sma50.iloc[-1]:
                results.append((stock, "close_crossing_sma_50_above", True))
            if data['Close'].iloc[-2] > sma50.iloc[-2] and data['Close'].iloc[-1] < sma50.iloc[-1]:
                results.append((stock, "close_crossing_sma_50_below", True))
    
        if len(data) > 101:
            # Crossing 100 SMA
            if data['Close'].iloc[-2] < sma100.iloc[-2] and data['Close'].iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "close_crossing_sma_100_above", True))
            if data['Close'].iloc[-2] > sma100.iloc[-2] and data['Close'].iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "close_crossing_sma_100_below", True))
    
        if len(data) > 201:
            # Crossing 200 SMA
            if data['Close'].iloc[-2] < sma200.iloc[-2] and data['Close'].iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "close_crossing_sma_200_above", True))
            if data['Close'].iloc[-2] > sma200.iloc[-2] and data['Close'].iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "close_crossing_sma_200_below", True))
    
        # SMA crossovers
        if len(data) > 21:  # Need at least SMA5 and SMA20
            # 5 SMA crossing 20 SMA
            if sma5.iloc[-2] < sma20.iloc[-2] and sma5.iloc[-1] > sma20.iloc[-1]:
                results.append((stock, "sma_5_crossing_sma_20_above", True))
            if sma5.iloc[-2] > sma20.iloc[-2] and sma5.iloc[-1] < sma20.iloc[-1]:
                results.append((stock, "sma_5_crossing_sma_20_below", True))
    
        if len(data) > 51:  # Need SMA20 and SMA50
            # 20 SMA crossing 50 SMA
            if sma20.iloc[-2] < sma50.iloc[-2] and sma20.iloc[-1] > sma50.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_50_above", True))
            if sma20.iloc[-2] > sma50.iloc[-2] and sma20.iloc[-1] < sma50.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_50_below", True))
    
        if len(data) > 101:  # Need SMA20 and SMA100
            # 20 SMA crossing 100 SMA
            if sma20.iloc[-2] < sma100.iloc[-2] and sma20.iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_100_above", True))
            if sma20.iloc[-2] > sma100.iloc[-2] and sma20.iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_100_below", True))
    
        if len(data) > 201:  # Need SMA20 and SMA200
            # 20 SMA crossing 200 SMA
            if sma20.iloc[-2] < sma200.iloc[-2] and sma20.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_200_above", True))
            if sma20.iloc[-2] > sma200.iloc[-2] and sma20.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_20_crossing_sma_200_below", True))
    
        if len(data) > 101:  # Need SMA50 and SMA100
            # 50 SMA crossing 100 SMA
            if sma50.iloc[-2] < sma100.iloc[-2] and sma50.iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "sma_50_crossing_sma_100_above", True))
            if sma50.iloc[-2] > sma100.iloc[-2] and sma50.iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "sma_50_crossing_sma_100_below", True))
    
        if len(data) > 201:  # Need SMA50 and SMA200
            # 50 SMA crossing 200 SMA
            if sma50.iloc[-2] < sma200.iloc[-2] and sma50.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_50_crossing_sma_200_above", True))
            if sma50.iloc[-2] > sma200.iloc[-2] and sma50.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_50_crossing_sma_200_below", True))
    
        if len(data) > 201:  # Need SMA100 and SMA200
            # 100 SMA crossing 200 SMA
            if sma100.iloc[-2] < sma200.iloc[-2] and sma100.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_100_crossing_sma_200_above", True))
            if sma100.iloc[-2] > sma200.iloc[-2] and sma100.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_100_crossing_sma_200_below", True))
    
    def calculate_weekly_sma_scans(self, stock, weekly_data, results):
        """Calculate weekly SMA-based scans."""
        if len(weekly_data) < 200:
            return
    
        # Calculate weekly SMAs
        sma5 = weekly_data['Close'].rolling(window=5).mean()
        sma20 = weekly_data['Close'].rolling(window=20).mean()
        sma50 = weekly_data['Close'].rolling(window=50).mean()
        sma100 = weekly_data['Close'].rolling(window=100).mean()
        sma200 = weekly_data['Close'].rolling(window=200).mean()
    
        # Check for close price near weekly SMAs
        close = weekly_data['Close'].iloc[-1]
    
        if len(weekly_data) >= 20 and abs(close - sma20.iloc[-1]) / sma20.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_sma_20", True))
    
        if len(weekly_data) >= 50 and abs(close - sma50.iloc[-1]) / sma50.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_sma_50", True))
    
        if len(weekly_data) >= 100 and abs(close - sma100.iloc[-1]) / sma100.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_sma_100", True))
    
        if len(weekly_data) >= 200 and abs(close - sma200.iloc[-1]) / sma200.iloc[-1] < 0.005:
            results.append((stock, "close_near_weekly_sma_200", True))
    
        # Price crossing weekly SMAs
        if len(weekly_data) > 21:  # Need enough data for SMA + comparison
            # Crossing 20 SMA
            if weekly_data['Close'].iloc[-2] < sma20.iloc[-2] and weekly_data['Close'].iloc[-1] > sma20.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_20_above", True))
            if weekly_data['Close'].iloc[-2] > sma20.iloc[-2] and weekly_data['Close'].iloc[-1] < sma20.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_20_below", True))
    
        if len(weekly_data) > 51:
            # Crossing 50 SMA
            if weekly_data['Close'].iloc[-2] < sma50.iloc[-2] and weekly_data['Close'].iloc[-1] > sma50.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_50_above", True))
            if weekly_data['Close'].iloc[-2] > sma50.iloc[-2] and weekly_data['Close'].iloc[-1] < sma50.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_50_below", True))
    
        if len(weekly_data) > 101:
            # Crossing 100 SMA
            if weekly_data['Close'].iloc[-2] < sma100.iloc[-2] and weekly_data['Close'].iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_100_above", True))
            if weekly_data['Close'].iloc[-2] > sma100.iloc[-2] and weekly_data['Close'].iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_100_below", True))
    
        if len(weekly_data) > 201:
            # Crossing 200 SMA
            if weekly_data['Close'].iloc[-2] < sma200.iloc[-2] and weekly_data['Close'].iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_200_above", True))
            if weekly_data['Close'].iloc[-2] > sma200.iloc[-2] and weekly_data['Close'].iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "close_crossing_weekly_sma_200_below", True))
    
        # Weekly SMA crossovers
        if len(weekly_data) > 21:  # Need at least SMA5 and SMA20
            # 5 SMA crossing 20 SMA
            if sma5.iloc[-2] < sma20.iloc[-2] and sma5.iloc[-1] > sma20.iloc[-1]:
                results.append((stock, "sma_5_crossing_weekly_sma_20_above", True))
            if sma5.iloc[-2] > sma20.iloc[-2] and sma5.iloc[-1] < sma20.iloc[-1]:
                results.append((stock, "sma_5_crossing_weekly_sma_20_below", True))
    
        if len(weekly_data) > 51:  # Need SMA20 and SMA50
            # 20 SMA crossing 50 SMA
            if sma20.iloc[-2] < sma50.iloc[-2] and sma20.iloc[-1] > sma50.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_50_above", True))
            if sma20.iloc[-2] > sma50.iloc[-2] and sma20.iloc[-1] < sma50.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_50_below", True))
    
        if len(weekly_data) > 101:  # Need SMA20 and SMA100
            # 20 SMA crossing 100 SMA
            if sma20.iloc[-2] < sma100.iloc[-2] and sma20.iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_100_above", True))
            if sma20.iloc[-2] > sma100.iloc[-2] and sma20.iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_100_below", True))
    
        if len(weekly_data) > 201:  # Need SMA20 and SMA200
            # 20 SMA crossing 200 SMA
            if sma20.iloc[-2] < sma200.iloc[-2] and sma20.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_200_above", True))
            if sma20.iloc[-2] > sma200.iloc[-2] and sma20.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_20_crossing_weekly_sma_200_below", True))
    
        if len(weekly_data) > 101:  # Need SMA50 and SMA100
            # 50 SMA crossing 100 SMA
            if sma50.iloc[-2] < sma100.iloc[-2] and sma50.iloc[-1] > sma100.iloc[-1]:
                results.append((stock, "sma_50_crossing_weekly_sma_100_above", True))
            if sma50.iloc[-2] > sma100.iloc[-2] and sma50.iloc[-1] < sma100.iloc[-1]:
                results.append((stock, "sma_50_crossing_weekly_sma_100_below", True))
    
        if len(weekly_data) > 201:  # Need SMA50 and SMA200
            # 50 SMA crossing 200 SMA
            if sma50.iloc[-2] < sma200.iloc[-2] and sma50.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_50_crossing_weekly_sma_200_above", True))
            if sma50.iloc[-2] > sma200.iloc[-2] and sma50.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_50_crossing_weekly_sma_200_below", True))
    
        if len(weekly_data) > 201:  # Need SMA100 and SMA200
            # 100 SMA crossing 200 SMA
            if sma100.iloc[-2] < sma200.iloc[-2] and sma100.iloc[-1] > sma200.iloc[-1]:
                results.append((stock, "sma_100_crossing_weekly_sma_200_above", True))
            if sma100.iloc[-2] > sma200.iloc[-2] and sma100.iloc[-1] < sma200.iloc[-1]:
                results.append((stock, "sma_100_crossing_weekly_sma_200_below", True))
                
        
    def calculate_absolute_returns(self, stock, data):
        """Calculate absolute returns for various timeframes."""
        results = []
        if data.empty or "Close" not in data:
            return results

        try:
            # Define timeframes for absolute returns
            timeframes = {
                "1_month": 21,  # Approx. 21 trading days in a month
                "3_months": 63,  # Approx. 63 trading days in 3 months
                "6_months": 126,  # Approx. 126 trading days in 6 months
                "1_year": 252  # Approx. 252 trading days in a year
            }

            # Calculate absolute returns for each timeframe
            for name, days in timeframes.items():
                if len(data) >= days:
                    past_close = data["Close"].iloc[-days]
                    current_close = data["Close"].iloc[-1]
                    absolute_return = ((current_close - past_close) / past_close) * 100

                    # Add result if the absolute return is positive
                    if absolute_return > 0:
                        results.append((stock, f"absolute_return_{name}", True))

        except Exception as e:
            print(f"Error calculating absolute returns for {stock}: {e}")

        return results

    def is_white_marubozu(self, candle):
        """Check if the candle is a White Marubozu."""
        body = candle["Close"] - candle["Open"]
        upper_shadow = candle["High"] - candle["Close"]
        lower_shadow = candle["Open"] - candle["Low"]
        return body > 0 and upper_shadow == 0 and lower_shadow == 0

    def is_black_marubozu(self, candle):
        """Check if the candle is a Black Marubozu."""
        body = candle["Open"] - candle["Close"]
        upper_shadow = candle["High"] - candle["Open"]
        lower_shadow = candle["Close"] - candle["Low"]
        return body > 0 and upper_shadow == 0 and lower_shadow == 0

    def is_hammer(self, candle):
        """Check if the candle is a Hammer."""
        body = abs(candle["Close"] - candle["Open"])
        lower_shadow = candle["Open"] - candle["Low"] if candle["Close"] > candle["Open"] else candle["Close"] - candle["Low"]
        upper_shadow = candle["High"] - candle["Close"] if candle["Close"] > candle["Open"] else candle["High"] - candle["Open"]
        return body > 0 and lower_shadow > 2 * body and upper_shadow < body

    def is_shooting_star(self, candle):
        """Check if the candle is a Shooting Star."""
        body = abs(candle["Close"] - candle["Open"])
        upper_shadow = candle["High"] - max(candle["Close"], candle["Open"])
        lower_shadow = min(candle["Close"], candle["Open"]) - candle["Low"]
        return (
                upper_shadow > 2 * body and
                lower_shadow < 0.1 * (candle["High"] - candle["Low"]) and
                body > 0
        )

    def is_bullish_engulfing(self, candles):
        """Check if the pattern is a Bullish Engulfing."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
            prev_candle["Close"] < prev_candle["Open"]
            and current_candle["Close"] > current_candle["Open"]
            and current_candle["Open"] < prev_candle["Close"]
            and current_candle["Close"] > prev_candle["Open"]
        )

    def is_bearish_engulfing(self, candles):
        """Check if the pattern is a Bearish Engulfing."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
                prev_candle["Close"] > prev_candle["Open"] and
                current_candle["Close"] < current_candle["Open"] and
                current_candle["Open"] > prev_candle["Close"] and
                current_candle["Close"] < prev_candle["Open"]
        )

    def is_morning_star(self, candles):
        """Check if the pattern is a Morning Star."""
        if len(candles) < 3:
            return False
        first, second, third = candles.iloc[-3], candles.iloc[-2], candles.iloc[-1]
        return (
            first["Close"] < first["Open"] and
            second["Close"] < second["Open"] and
            third["Close"] > third["Open"] and
            third["Close"] > (first["Open"] + first["Close"]) / 2
        )

    def is_evening_star(self, candles):
        """Check if the pattern is an Evening Star."""
        if len(candles) < 3:
            return False
        first, second, third = candles.iloc[-3], candles.iloc[-2], candles.iloc[-1]
        return (
            first["Close"] > first["Open"] and
            abs(second["Close"] - second["Open"]) < 0.3 * (first["Close"] - first["Open"]) and
            third["Close"] < third["Open"] and
            third["Close"] < (first["Open"] + first["Close"]) / 2
        )

    def is_dragonfly_doji(self, candle):
        """Check if the candle is a Dragonfly Doji."""
        body = abs(candle["Close"] - candle["Open"])
        lower_shadow = candle["Close"] - candle["Low"]
        upper_shadow = candle["High"] - candle["Close"]
        return (
            body < 0.1 * (candle["High"] - candle["Low"]) and  # Small body
            lower_shadow > 2 * body and  # Long lower shadow
            upper_shadow < 0.1 * (candle["High"] - candle["Low"])  # Minimal upper shadow
        )

    def is_inverted_hammer(self, candle):
        """Check if the candle is an Inverted Hammer."""
        body = abs(candle["Close"] - candle["Open"])
        upper_shadow = candle["High"] - max(candle["Close"], candle["Open"])
        lower_shadow = min(candle["Close"], candle["Open"]) - candle["Low"]
        return (
            body > 0 and  # Non-zero body
            upper_shadow > 2 * body and  # Long upper shadow
            lower_shadow < body  # Minimal lower shadow
        )

    def is_piercing_line(self, candles):
        """Check if the pattern is a Piercing Line."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
            prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
            current_candle["Close"] > current_candle["Open"] and  # Current candle is bullish
            current_candle["Open"] < prev_candle["Close"] and  # Current opens below previous close
            current_candle["Close"] > (prev_candle["Open"] + prev_candle["Close"]) / 2  # Closes above midpoint
        )

    def is_bullish_harami(self, candles):
        """Check if the pattern is a Bullish Harami."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
            prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
            current_candle["Close"] > current_candle["Open"] and  # Current candle is bullish
            current_candle["Open"] > prev_candle["Close"] and  # Current opens above previous close
            current_candle["Close"] < prev_candle["Open"]  # Current closes below previous open
        )

    def is_bullish_harami_cross(self, candles):
        """Check if the pattern is a Bullish Harami Cross."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
            prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
            abs(current_candle["Close"] - current_candle["Open"]) < 0.1 * (current_candle["High"] - current_candle["Low"]) and  # Current is a doji
            current_candle["Open"] > prev_candle["Close"] and  # Current opens above previous close
            current_candle["Close"] < prev_candle["Open"]  # Current closes below previous open
        )

    def is_three_white_soldiers(self, candles):
        """Check if the pattern is Three White Soldiers."""
        if len(candles) < 3:
            return False
        # Use explicit indexing to avoid unpacking issues
        first = candles.iloc[-3]
        second = candles.iloc[-2]
        third = candles.iloc[-1]
        return (
            first["Close"] > first["Open"] and
            second["Close"] > second["Open"] and
            third["Close"] > third["Open"] and
            second["Open"] > first["Close"] and
            third["Open"] > second["Close"]
        )

    def is_downside_tasuki_gap(self, candles):
        """Check if the pattern is a Downside Tasuki Gap."""
        if len(candles) < 3:
            return False
        first, second, third = candles.iloc[-3], candles.iloc[-2], candles.iloc[-1]
        return (
            first["Close"] < first["Open"] and  # First candle is bearish
            second["Close"] < second["Open"] and  # Second candle is bearish
            second["High"] < first["Low"] and  # Gap between first and second
            third["Close"] > third["Open"] and  # Third candle is bullish
            third["Close"] > second["Close"] and  # Third closes above second close
            third["Open"] < second["Close"]  # Third opens below second close
        )

    def is_falling_three_methods(self, candles):
        """Check if the pattern is Falling Three Methods."""
        if len(candles) < 5:
            return False
        # Use explicit indexing to avoid unpacking issues
        first = candles.iloc[-5]
        second = candles.iloc[-4]
        third = candles.iloc[-3]
        fourth = candles.iloc[-2]
        fifth = candles.iloc[-1]
        return (
            first["Close"] < first["Open"] and
            second["Close"] > second["Open"] and
            third["Close"] > third["Open"] and
            fourth["Close"] > fourth["Open"] and
            fifth["Close"] < fifth["Open"] and
            second["Close"] < first["Open"] and
            fifth["Close"] < first["Close"]
        )

    def is_in_neck(self, candles):
        """Check if the pattern is In Neck."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
            prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
            current_candle["Close"] > current_candle["Open"] and  # Current candle is bullish
            current_candle["Close"] == prev_candle["Low"]  # Current closes at previous low
        )

    def is_on_neck(self, candles):
        """Check if the pattern is On Neck."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
                prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
                current_candle["Close"] > current_candle["Open"] and  # Current candle is bullish
                prev_candle["Low"] < current_candle["Close"] < prev_candle["Open"]  # Current closes below previous open
        )

    def is_thrusting(self, candles):
        """Check if the pattern is Thrusting."""
        if len(candles) < 2:
            return False
        prev_candle, current_candle = candles.iloc[-2], candles.iloc[-1]
        return (
                prev_candle["Close"] < prev_candle["Open"] and  # Previous candle is bearish
                current_candle["Open"] < current_candle["Close"] < prev_candle["Open"] and  # Current closes below previous open
                current_candle["Close"] > (prev_candle["Open"] + prev_candle["Close"]) / 2  # Closes above midpoint
        )

    def check_candlestick_hanging_man(self, data):
        """
        Check for Hanging Man pattern.
        
        A hanging man has:
        1. Small body (open/close are close together)
        2. Long lower shadow (at least 2x the body)
        3. Little or no upper shadow
        4. Occurs in an uptrend
        """
        if len(data) < 5:
            return False
    
        # Get the last candle
        df = data.tail(5)
        last_candle = df.iloc[-1]
    
        # Check for uptrend (closing prices higher than 3 bars ago)
        uptrend = last_candle['Close'] > df.iloc[-4]['Close']
    
        body_size = abs(last_candle['Open'] - last_candle['Close'])
    
        # Calculate shadows
        high = last_candle['High']
        low = last_candle['Low']
        close = last_candle['Close']
        open_price = last_candle['Open']
    
        upper_shadow = high - max(open_price, close)
        lower_shadow = min(open_price, close) - low
    
        # Criteria for hanging man
        small_body = body_size < (high - low) * 0.3
        long_lower_shadow = lower_shadow > body_size * 2
        small_upper_shadow = upper_shadow < body_size * 0.1
    
        return uptrend and small_body and long_lower_shadow and small_upper_shadow
    
    def check_candlestick_dark_cloud_cover(self, data):
        """
        Check for Dark Cloud Cover pattern.
        
        1. First candle: strong bullish (white/green)
        2. Second candle: opens above first candle's high, closes below midpoint of first candle
        """
        if len(data) < 2:
            return False
    
        prev_candle = data.iloc[-2]
        curr_candle = data.iloc[-1]
    
        # Check if previous candle is bullish
        prev_bullish = prev_candle['Close'] > prev_candle['Open']
    
        # Check if previous candle has significant body
        prev_body_size = abs(prev_candle['Close'] - prev_candle['Open'])
        significant_prev_body = prev_body_size > (prev_candle['High'] - prev_candle['Low']) * 0.5
    
        # Check if current candle opens above previous high
        gap_up = curr_candle['Open'] > prev_candle['High']
    
        # Check if current candle is bearish
        curr_bearish = curr_candle['Close'] < curr_candle['Open']
    
        # Check if current candle closes below midpoint of previous candle's body
        prev_midpoint = (prev_candle['Open'] + prev_candle['Close']) / 2
        closes_below_midpoint = curr_candle['Close'] < prev_midpoint
    
        return prev_bullish and significant_prev_body and gap_up and curr_bearish and closes_below_midpoint
    
    def check_candlestick_bearish_harami(self, data):
        """
        Check for Bearish Harami pattern.
        
        1. First candle: large bullish (white/green)
        2. Second candle: bearish (black/red) with body completely inside first candle's body
        """
        if len(data) < 2:
            return False
    
        prev_candle = data.iloc[-2]
        curr_candle = data.iloc[-1]
    
        # Check if previous candle is bullish with significant body
        prev_bullish = prev_candle['Close'] > prev_candle['Open']
        prev_body_size = abs(prev_candle['Close'] - prev_candle['Open'])
        significant_prev_body = prev_body_size > (prev_candle['High'] - prev_candle['Low']) * 0.5
    
        # Check if current candle is bearish
        curr_bearish = curr_candle['Close'] < curr_candle['Open']
    
        # Check if current candle's body is inside previous candle's body
        body_inside = (curr_candle['Open'] < prev_candle['Close'] and
            curr_candle['Open'] > prev_candle['Open'] and
            curr_candle['Close'] > prev_candle['Open'] and
            curr_candle['Close'] < prev_candle['Close'])
    
        return prev_bullish and significant_prev_body and curr_bearish and body_inside
    
    def check_candlestick_bearish_harami_cross(self, data):
        """
        Check for Bearish Harami Cross pattern (Bearish Harami with a doji).
        
        1. First candle: large bullish (white/green)
        2. Second candle: doji completely inside first candle's body
        """
        if len(data) < 2:
            return False
    
        prev_candle = data.iloc[-2]
        curr_candle = data.iloc[-1]
    
        # Check if previous candle is bullish with significant body
        prev_bullish = prev_candle['Close'] > prev_candle['Open']
        prev_body_size = abs(prev_candle['Close'] - prev_candle['Open'])
        significant_prev_body = prev_body_size > (prev_candle['High'] - prev_candle['Low']) * 0.5
    
        # Check if current candle is a doji (open and close are very close)
        curr_body_size = abs(curr_candle['Close'] - curr_candle['Open'])
        curr_range = curr_candle['High'] - curr_candle['Low']
        is_doji = curr_body_size <= curr_range * 0.1
    
        # Check if doji is inside previous candle's body
        inside_prev_body = (curr_candle['High'] < prev_candle['Close'] and
                            curr_candle['Low'] > prev_candle['Open'])
    
        return prev_bullish and significant_prev_body and is_doji and inside_prev_body
    
    def check_candlestick_three_black_crows(self, data):
        """
        Check for Three Black Crows pattern.
        
        1. Three consecutive bearish candles
        2. Each opens within the previous candle's body
        3. Each closes lower than the previous close
        4. Each has little or no lower shadow
        """
        if len(data) < 3:
            return False
    
        # Get last three candles
        candles = data.iloc[-3:].reset_index(drop=True)
    
        # Check if all three candles are bearish
        all_bearish = all(candles.iloc[i]['Close'] < candles.iloc[i]['Open'] for i in range(3))
    
        # Check if each opens within previous body and closes lower
        opens_within_closes_lower = True
        for i in range(1, 3):
            # Opens within previous body
            opens_within = (candles.iloc[i]['Open'] < candles.iloc[i-1]['Open'] and
                            candles.iloc[i]['Open'] > candles.iloc[i-1]['Close'])
    
            # Closes lower than previous close
            closes_lower = candles.iloc[i]['Close'] < candles.iloc[i-1]['Close']
    
            if not (opens_within and closes_lower):
                opens_within_closes_lower = False
                break
    
        # Check for little lower shadows
        little_lower_shadows = True
        for i in range(3):
            body_size = abs(candles.iloc[i]['Open'] - candles.iloc[i]['Close'])
            lower_shadow = min(candles.iloc[i]['Open'], candles.iloc[i]['Close']) - candles.iloc[i]['Low']
    
            if lower_shadow > body_size * 0.3:
                little_lower_shadows = False
                break
    
        return all_bearish and opens_within_closes_lower and little_lower_shadows
    
    def check_candlestick_doji(self, data):
        """
        Check for Doji pattern.
        
        A doji has open and close prices that are virtually equal, with upper and lower shadows.
        """
        if len(data) < 1:
            return False
    
        last_candle = data.iloc[-1]
    
        body_size = abs(last_candle['Open'] - last_candle['Close'])
        candle_range = last_candle['High'] - last_candle['Low']
    
        # To qualify as a doji, body should be very small compared to the range
        # Typically body is less than 5% of the total range
        if candle_range == 0:  # Avoid division by zero
            return False
    
        return body_size <= (candle_range * 0.05)
    
    def check_candlestick_near_doji(self, data):
        """
        Check for Near Doji pattern.
        
        Similar to doji but slightly less strict (10% threshold instead of 5%)
        """
        if len(data) < 1:
            return False
    
        last_candle = data.iloc[-1]
    
        body_size = abs(last_candle['Open'] - last_candle['Close'])
        candle_range = last_candle['High'] - last_candle['Low']
    
        # To qualify as a near doji, body should be small compared to the range
        # Using a 10% threshold for near doji
        if candle_range == 0:  # Avoid division by zero
            return False
    
        return body_size <= (candle_range * 0.1)

    def calculate_candlestick_scans(self, stock, data):
        """Calculate candlestick pattern scans."""
        results = []
        if len(data) < 2:  # Ensure at least 2 days of data for patterns like engulfing
            return results

        try:
            # Check for White Marubozu
            if self.is_white_marubozu(data.iloc[-1]):
                results.append((stock, "candlestick_white_marubozu", True))

            # Check for Black Marubozu
            if self.is_black_marubozu(data.iloc[-1]):
                results.append((stock, "candlestick_black_marubozu", True))

            # Check for Hammer
            if self.is_hammer(data.iloc[-1]):
                results.append((stock, "candlestick_hammer", True))

            # Check for Shooting Star
            if self.is_shooting_star(data.iloc[-1]):
                results.append((stock, "candlestick_shooting_star", True))

            # Check for Bullish Engulfing
            if self.is_bullish_engulfing(data):
                results.append((stock, "candlestick_bullish_engulfing", True))

            # Check for Bearish Engulfing
            if self.is_bearish_engulfing(data):
                results.append((stock, "candlestick_bearish_engulfing", True))

            # Check for Morning Star
            if self.is_morning_star(data):
                results.append((stock, "candlestick_morning_star", True))

            # Check for Evening Star
            if self.is_evening_star(data):
                results.append((stock, "candlestick_evening_star", True))

            # Check for Dragonfly Doji
            if self.is_dragonfly_doji(data.iloc[-1]):
                results.append((stock, "candlestick_dragonfly_doji", True))

            # Check for Inverted Hammer
            if self.is_inverted_hammer(data.iloc[-1]):
                results.append((stock, "candlestick_inverted_hammer", True))

            # Check for Piercing Line
            if self.is_piercing_line(data):
                results.append((stock, "candlestick_piercing_line", True))

            # Check for Bullish Harami
            if self.is_bullish_harami(data):
                results.append((stock, "candlestick_bullish_harami", True))

            # Check for Bullish Harami Cross
            if self.is_bullish_harami_cross(data):
                results.append((stock, "candlestick_bullish_harami_cross", True))

            # Check for Three White Soldiers
            if self.is_three_white_soldiers(data):
                results.append((stock, "candlestick_three_white_soldiers", True))

            # Check for Downside Tasuki Gap
            if self.is_downside_tasuki_gap(data):
                results.append((stock, "candlestick_downside_tasuki_gap", True))

            # Check for Falling Three Methods
            if self.is_falling_three_methods(data):
                results.append((stock, "candlestick_falling_three_methods", True))

            # Check for In Neck
            if self.is_in_neck(data):
                results.append((stock, "candlestick_in_neck", True))

            # Check for On Neck
            if self.is_on_neck(data):
                results.append((stock, "candlestick_on_neck", True))

            # Check for Thrusting
            if self.is_thrusting(data):
                results.append((stock, "candlestick_thrusting", True))

            if self.check_candlestick_hanging_man(data):
                results.append((stock, "candlestick_hanging_man", True))

            if self.check_candlestick_dark_cloud_cover(data):
                results.append((stock, "candlestick_dark_cloud_cover", True))

            if self.check_candlestick_bearish_harami(data):
                results.append((stock, "candlestick_bearish_harami", True))

            if self.check_candlestick_doji(data):
                results.append((stock, "candlestick_doji", True))

            if self.check_candlestick_near_doji(data):
                results.append((stock, "candlestick_near_doji", True))

            if self.check_candlestick_three_black_crows(data):
                results.append((stock, "candlestick_three_black_crows", True))
                
            if self.check_candlestick_bearish_harami_cross(data):
                results.append((stock, "candlestick_bearish_harami_cross", True))


        except Exception as e:
            print(f"Error calculating candlestick scans for {stock}: {e}")

        return results

    def calculate_momentum_scans(self, stock, data):
        """Calculate momentum score scans for various timeframes."""
        results = []
        if data.empty or "Close" not in data:
            return results

        try:
            # Define timeframes for momentum scores
            timeframes = {
                "1m": 21,  # Approx. 21 trading days in a month
                "3m": 63,  # Approx. 63 trading days in 3 months
                "6m": 126  # Approx. 126 trading days in 6 months
            }

            for name, days in timeframes.items():
                if len(data) >= days:
                    # Calculate momentum score as the percentage change over the timeframe
                    past_close = data["Close"].iloc[-days]
                    current_close = data["Close"].iloc[-1]
                    momentum_score = ((current_close - past_close) / past_close) * 100

                    # Add results based on momentum score thresholds
                    if momentum_score > 0:
                        results.append((stock, f"momentum_score_{name}_bullish_zone", True))
                    elif momentum_score < 0:
                        results.append((stock, f"momentum_score_{name}_bearish_zone", True))

                    # Check for entering bullish or bearish zones
                    if len(data) > days + 1:
                        previous_close = data["Close"].iloc[-(days + 1)]
                        previous_momentum_score = ((current_close - previous_close) / previous_close) * 100
                        if previous_momentum_score <= 0 < momentum_score:
                            results.append((stock, f"momentum_score_{name}_entering_bullish_zone", True))
                        elif previous_momentum_score >= 0 > momentum_score:
                            results.append((stock, f"momentum_score_{name}_entering_bearish_zone", True))

                    # Check for increasing or decreasing momentum
                    if len(data) > days + 1:
                        if momentum_score > previous_momentum_score:
                            results.append((stock, f"momentum_score_{name}_increasing", True))
                        elif momentum_score < previous_momentum_score:
                            results.append((stock, f"momentum_score_{name}_decreasing", True))

        except Exception as e:
            print(f"Error calculating momentum scans for {stock}: {e}")

        return results


    def save_scanner_results(self, results):
        """Save scanner results to the database with individual boolean columns."""
        if not results:
            return

        # Prepare data for insertion
        today = datetime.now(pytz.timezone("Asia/Kolkata")).date()
        data = {}

        # Initialize data dictionary for each stock
        for stock, scanner_name, result in results:
            if stock not in data:
                data[stock] = {
                    "stock_name": stock,
                    "scan_date": today,
                    # Initialize all columns to False
                    "previous_day_breakout": False,
                    "previous_day_breakdown": False,
                    "one_week_breakout": False,
                    "one_week_breakdown": False,
                    "one_month_breakout": False,
                    "one_month_breakdown": False,
                    "fifty_two_week_breakout": False,
                    "fifty_two_week_breakdown": False,
                    "all_time_breakout": False,
                    "all_time_breakdown": False,
                    "srs_ars_above_zero": False,
                    "srs_ars_below_zero": False,
                    "srs_crossing_above_zero": False,
                    "srs_crossing_below_zero": False,
                    "ars_crossing_above_zero": False,
                    "ars_crossing_below_zero": False,
                    "both_above_zero_and_ars_rising": False,
                    "both_above_zero_and_srs_rising": False,
                    "candlestick_white_marubozu": False,
                    "candlestick_black_marubozu": False,
                    "candlestick_hammer": False,
                    "candlestick_shooting_star": False,
                    "candlestick_bullish_engulfing": False,
                    "candlestick_bearish_engulfing": False,
                    "candlestick_morning_star": False,
                    "candlestick_evening_star": False,
                    "candlestick_dragonfly_doji": False,
                    "candlestick_inverted_hammer": False,
                    "candlestick_piercing_line": False,
                    "candlestick_bullish_harami": False,
                    "candlestick_bullish_harami_cross": False,
                    "candlestick_three_white_soldiers": False,
                    "candlestick_downside_tasuki_gap": False,
                    "candlestick_falling_three_methods": False,
                    "candlestick_in_neck": False,
                    "candlestick_on_neck": False,
                    "candlestick_thrusting": False,
                    "candlestick_hanging_man": False,
                    "candlestick_dark_cloud_cover": False,
                    "candlestick_bearish_harami": False,
                    "candlestick_bearish_harami_cross": False,
                    "candlestick_three_black_crows": False,
                    "candlestick_doji": False,
                    "candlestick_near_doji": False,
                    "absolute_return_1_month": False,
                    "absolute_return_3_months": False,
                    "absolute_return_6_months": False,
                    "absolute_return_1_year": False,
                    "behavior_1_day_opening_high": False,
                    "behavior_1_day_opening_low": False,
                    "behavior_2_day_higher_highs": False,
                    "behavior_2_day_lower_lows": False,
                    "behavior_3_day_higher_highs": False,
                    "behavior_3_day_lower_lows": False,
                    # EMA fields
                    "close_near_sma_20": False,
                    "close_crossing_sma_20_above": False,
                    "close_crossing_sma_20_below": False,
                    "close_near_sma_50": False,
                    "close_crossing_sma_50_above": False,
                    "close_crossing_sma_50_below": False,
                    "close_near_sma_100": False,
                    "close_crossing_sma_100_above": False,
                    "close_crossing_sma_100_below": False,
                    "close_near_sma_200": False,
                    "close_crossing_sma_200_above": False,
                    "close_crossing_sma_200_below": False,

                    "sma_5_crossing_sma_20_above": False,
                    "sma_5_crossing_sma_20_below": False,
                    "sma_20_crossing_sma_50_above": False,
                    "sma_20_crossing_sma_50_below": False,
                    "sma_20_crossing_sma_100_above": False,
                    "sma_20_crossing_sma_100_below": False,
                    "sma_20_crossing_sma_200_above": False,
                    "sma_20_crossing_sma_200_below": False,
                    "sma_50_crossing_sma_100_above": False,
                    "sma_50_crossing_sma_100_below": False,
                    "sma_50_crossing_sma_200_above": False,
                    "sma_50_crossing_sma_200_below": False,
                    "sma_100_crossing_sma_200_above": False,
                    "sma_100_crossing_sma_200_below": False,

                    "close_near_weekly_sma_20": False,
                    "close_crossing_weekly_sma_20_above": False,
                    "close_crossing_weekly_sma_20_below": False,
                    "close_near_weekly_sma_50": False,
                    "close_crossing_weekly_sma_50_above": False,
                    "close_crossing_weekly_sma_50_below": False,
                    "close_near_weekly_sma_100": False,
                    "close_crossing_weekly_sma_100_above": False,
                    "close_crossing_weekly_sma_100_below": False,
                    "close_near_weekly_sma_200": False,
                    "close_crossing_weekly_sma_200_above": False,
                    "close_crossing_weekly_sma_200_below": False,

                    "sma_5_crossing_weekly_sma_20_above": False,
                    "sma_5_crossing_weekly_sma_20_below": False,
                    "sma_20_crossing_weekly_sma_50_above": False,
                    "sma_20_crossing_weekly_sma_50_below": False,
                    "sma_20_crossing_weekly_sma_100_above": False,
                    "sma_20_crossing_weekly_sma_100_below": False,
                    "sma_20_crossing_weekly_sma_200_above": False,
                    "sma_20_crossing_weekly_sma_200_below": False,
                    "sma_50_crossing_weekly_sma_100_above": False,
                    "sma_50_crossing_weekly_sma_100_below": False,
                    "sma_50_crossing_weekly_sma_200_above": False,
                    "sma_50_crossing_weekly_sma_200_below": False,
                    "sma_100_crossing_weekly_sma_200_above": False,
                    "sma_100_crossing_weekly_sma_200_below": False,

                    "close_near_ema_20": False,
                    "close_crossing_ema_20_above": False,
                    "close_crossing_ema_20_below": False,
                    "close_near_ema_50": False,
                    "close_crossing_ema_50_above": False,
                    "close_crossing_ema_50_below": False,
                    "close_near_ema_100": False,
                    "close_crossing_ema_100_above": False,
                    "close_crossing_ema_100_below": False,
                    "close_near_ema_200": False,
                    "close_crossing_ema_200_above": False,
                    "close_crossing_ema_200_below": False,

                    "ema_5_crossing_ema_20_above": False,
                    "ema_5_crossing_ema_20_below": False,
                    "ema_20_crossing_ema_50_above": False,
                    "ema_20_crossing_ema_50_below": False,
                    "ema_20_crossing_ema_100_above": False,
                    "ema_20_crossing_ema_100_below": False,
                    "ema_20_crossing_ema_200_above": False,
                    "ema_20_crossing_ema_200_below": False,
                    "ema_50_crossing_ema_100_above": False,
                    "ema_50_crossing_ema_100_below": False,
                    "ema_50_crossing_ema_200_above": False,
                    "ema_50_crossing_ema_200_below": False,
                    "ema_100_crossing_ema_200_above": False,
                    "ema_100_crossing_ema_200_below": False,

                    "close_near_weekly_ema_20": False,
                    "close_crossing_weekly_ema_20_above": False,
                    "close_crossing_weekly_ema_20_below": False,
                    "close_near_weekly_ema_50": False,
                    "close_crossing_weekly_ema_50_above": False,
                    "close_crossing_weekly_ema_50_below": False,
                    "close_near_weekly_ema_100": False,
                    "close_crossing_weekly_ema_100_above": False,
                    "close_crossing_weekly_ema_100_below": False,
                    "close_near_weekly_ema_200": False,
                    "close_crossing_weekly_ema_200_above": False,
                    "close_crossing_weekly_ema_200_below": False,

                    "ema_5_crossing_weekly_ema_20_above": False,
                    "ema_5_crossing_weekly_ema_20_below": False,
                    "ema_20_crossing_weekly_ema_50_above": False,
                    "ema_20_crossing_weekly_ema_50_below": False,
                    "ema_20_crossing_weekly_ema_100_above": False,
                    "ema_20_crossing_weekly_ema_100_below": False,
                    "ema_20_crossing_weekly_ema_200_above": False,
                    "ema_20_crossing_weekly_ema_200_below": False,
                    "ema_50_crossing_weekly_ema_100_above": False,
                    "ema_50_crossing_weekly_ema_100_below": False,
                    "ema_50_crossing_weekly_ema_200_above": False,
                    "ema_50_crossing_weekly_ema_200_below": False,
                    "ema_100_crossing_weekly_ema_200_above": False,
                    "ema_100_crossing_weekly_ema_200_below": False,

                    "momentum_score_1m_increasing": False,
                    "momentum_score_1m_decreasing": False,
                    "momentum_score_1m_bullish_zone": False,
                    "momentum_score_1m_bearish_zone": False,
                    "momentum_score_3m_increasing": False,
                    "momentum_score_3m_decreasing": False,
                    "momentum_score_3m_bullish_zone": False,
                    "momentum_score_3m_bearish_zone": False,
                    "momentum_score_6m_increasing": False,
                    "momentum_score_6m_decreasing": False,
                    "momentum_score_6m_bullish_zone": False,
                    "momentum_score_6m_bearish_zone": False,

                    "cci_bullish": False,
                    "cci_trending_up": False,
                    "cci_crossed_above_100": False,
                    "cci_bearish": False,
                    "cci_trending_down": False,
                    "cci_crossed_below_100": False,
                    "cci_overbought_zone": False,
                    "cci_oversold_zone": False,

                    "rsi_overbought_zone": False,
                    "rsi_oversold_zone": False,
                    "rsi_crossed_above_70": False,
                    "rsi_crossed_below_30": False,
                    "rsi_bullish": False,
                    "rsi_bearish": False,
                    "rsi_trending_up": False,
                    "rsi_trending_down": False,
                    "rsi_weekly_overbought_zone": False,
                    "rsi_weekly_oversold_zone": False,
                    "rsi_weekly_crossed_above_70": False,
                    "rsi_weekly_crossed_below_30": False,
                    "rsi_weekly_bullish": False,
                    "rsi_weekly_bearish": False,
                    "rsi_weekly_trending_up": False,
                    "rsi_weekly_trending_down": False,

                    "mfi_bullish": False,
                    "mfi_trending_up": False,
                    "mfi_crossed_80_from_below": False,
                    "mfi_bearish": False,
                    "mfi_trending_down": False,
                    "mfi_crossed_20_from_above": False,
                    "mfi_above_80": False,
                    "mfi_below_20": False,
                    "williams_r_bullish": False,
                    "williams_r_trending_up": False,
                    "williams_r_crossed_20_from_below": False,
                    "williams_r_bearish": False,
                    "williams_r_trending_down": False,
                    "williams_r_crossed_80_from_above": False,
                    "williams_r_above_20": False,
                    "williams_r_below_80": False,

                    "roc_trending_up": False,
                    "roc_trending_down": False,
                    "roc_increasing": False,
                    "roc_decreasing": False,

                    "macd_crossing_signal_line_above": False,
                    "macd_crossing_signal_line_below": False,
                    "macd_above_zero": False,
                    "macd_below_zero": False,
                    "macd_crossed_signal_line_from_below": False,
                    "macd_crossed_signal_line_from_above": False,

                    "adx_crossing_25_from_below": False,
                    "adx_crossing_25_from_above": False,
                    "adx_crossing_40_from_below": False,
                    "adx_crossing_40_from_above": False,
                    "adx_crossing_10_from_below": False,
                    "adx_crossing_10_from_above": False,
                    "plus_di_crossing_minus_di_from_below": False,
                    "plus_di_crossing_minus_di_from_above": False,
                    "plus_di_crossing_25_from_below": False,
                    "minus_di_crossing_25_from_below": False,

                    "atr_3_days_increasing": False,
                    "atr_3_days_decreasing": False,
                    "atr_5_days_increasing": False,
                    "atr_5_days_decreasing": False,
                    "atr_7_days_increasing": False,
                    "atr_7_days_decreasing": False,

                    "close_above_upper_bollinger_band": False,
                    "close_below_lower_bollinger_band": False,
                    "close_crossing_upper_bollinger_band_from_below": False,
                    "close_crossing_lower_bollinger_band_from_above": False,
                    "close_crossing_upper_bollinger_band_from_above": False,
                    "close_crossing_lower_bollinger_band_from_below": False,
                    "bollinger_band_narrowing": False,
                    "bollinger_band_widening": False,

                    "stochastic_in_overbought_zone": False,
                    "stochastic_in_oversold_zone": False,
                    "stochastic_entering_overbought_zone": False,
                    "stochastic_entering_oversold_zone": False,
                    "stochastic_trending_up": False,
                    "stochastic_trending_down": False,
                    "stochastic_k_crossing_d_from_below": False,
                    "stochastic_k_crossing_d_from_above": False,
                    "psar_bullish_reversal": False,
                    "psar_bearish_reversal": False,
                    "psar_weekly_bullish_reversal": False,
                    "psar_weekly_bearish_reversal": False,

                    "nr4": False,
                    "nr7": False,
                    "nr4_breakout": False,
                    "nr4_breakdown": False,
                    "nr7_breakout": False,
                    "nr7_breakdown": False,
                    "supertrend_signal_changed_to_buy": False,
                    "supertrend_signal_changed_to_sell": False,
                    "price_nearing_supertrend_support": False,
                    "price_nearing_supertrend_resistance": False,

                    "weekly_supertrend_signal_changed_to_buy": False,
                    "weekly_supertrend_signal_changed_to_sell": False,
                    "price_nearing_weekly_supertrend_support": False,
                    "price_nearing_weekly_supertrend_resistance": False,

                    # Add new futures OI fields
                    "futures_high_oi_increase": False,
                    "futures_high_oi_decrease": False,

                    "call_options_high_oi_increase": False,
                    "call_options_high_oi_decrease": False,
                    "put_options_high_oi_increase": False,
                    "put_options_high_oi_decrease": False,

                    # Add to the data[stock] dictionary initialization in save_scanner_results
                    "pattern_double_top": False,
                    "pattern_double_bottom": False,

                    "pattern_head_and_shoulders": False,
                    "pattern_inverse_head_and_shoulders": False,

                    "pattern_rising_wedge": False,
                    "pattern_falling_wedge": False,
                    "pattern_triangle_bullish": False,
                    "pattern_triangle_bearish": False,
                    "pattern_pennant_bullish": False,
                    "pattern_pennant_bearish": False,
                    "pattern_rectangle_bullish": False,

                    "pattern_rectangle_bearish": False,
                    "pattern_flag_bullish": False,
                    "pattern_flag_bearish": False,
                    "pattern_channel_rising": False,

                    "pattern_channel_falling": False,
                    "pattern_channel_horizontal": False,
                    "pattern_triple_top": False,
                    "pattern_triple_bottom": False,

                    "pattern_cup_and_handle": False,
                    "pattern_rounding_bottom": False,
                    "pattern_diamond_bullish": False,
                    "pattern_diamond_bearish": False
                }

            # Update the specific column for the scanner_name
            if scanner_name in data[stock]:
                data[stock][scanner_name] = result

        # Convert data to a list of tuples for batch insertion
        rows = []
        for stock_data in data.values():
            rows.append(tuple(stock_data.values()))

        # Delete existing results for today
        with self.database_service._get_cursor() as cur:
            cur.execute("DELETE FROM scanner_results WHERE scan_date = %s", (today,))

            # Insert new results
            sql = """
                INSERT INTO scanner_results (
                    stock_name, scan_date,
                    previous_day_breakout, previous_day_breakdown,
                    one_week_breakout, one_week_breakdown,
                    one_month_breakout, one_month_breakdown,
                    fifty_two_week_breakout, fifty_two_week_breakdown,
                    all_time_breakout, all_time_breakdown,
                    srs_ars_above_zero, srs_ars_below_zero,
                    srs_crossing_above_zero, srs_crossing_below_zero,
                    ars_crossing_above_zero, ars_crossing_below_zero,
                    both_above_zero_and_ars_rising, both_above_zero_and_srs_rising,
                    candlestick_white_marubozu, candlestick_black_marubozu,
                    candlestick_hammer, candlestick_shooting_star,
                    candlestick_bullish_engulfing, candlestick_bearish_engulfing,
                    candlestick_morning_star, candlestick_evening_star,
                    candlestick_dragonfly_doji, candlestick_inverted_hammer,
                    candlestick_piercing_line, candlestick_bullish_harami,
                    candlestick_bullish_harami_cross, candlestick_three_white_soldiers,
                    candlestick_downside_tasuki_gap, candlestick_falling_three_methods,
                    candlestick_in_neck, candlestick_on_neck,
                    candlestick_thrusting, candlestick_hanging_man,
                    candlestick_dark_cloud_cover, candlestick_bearish_harami,
                    candlestick_bearish_harami_cross, candlestick_three_black_crows,
                    candlestick_doji, candlestick_near_doji,
                    absolute_return_1_month, absolute_return_3_months,
                    absolute_return_6_months, absolute_return_1_year,
                    behavior_1_day_opening_high, behavior_1_day_opening_low,
                    behavior_2_day_higher_highs, behavior_2_day_lower_lows,
                    behavior_3_day_higher_highs, behavior_3_day_lower_lows,
                    
                    close_near_sma_20,
                    close_crossing_sma_20_above,
                    close_crossing_sma_20_below,
                    close_near_sma_50,
                    close_crossing_sma_50_above,
                    close_crossing_sma_50_below,
                    close_near_sma_100,
                    close_crossing_sma_100_above,
                    close_crossing_sma_100_below,
                    close_near_sma_200,
                    close_crossing_sma_200_above,
                    close_crossing_sma_200_below,
                    
                    sma_5_crossing_sma_20_above,
                    sma_5_crossing_sma_20_below,
                    sma_20_crossing_sma_50_above,
                    sma_20_crossing_sma_50_below,
                    sma_20_crossing_sma_100_above,
                    sma_20_crossing_sma_100_below,
                    sma_20_crossing_sma_200_above,
                    sma_20_crossing_sma_200_below,
                    sma_50_crossing_sma_100_above,
                    sma_50_crossing_sma_100_below,
                    sma_50_crossing_sma_200_above,
                    sma_50_crossing_sma_200_below,
                    sma_100_crossing_sma_200_above,
                    sma_100_crossing_sma_200_below,
                    
                    close_near_weekly_sma_20,
                    close_crossing_weekly_sma_20_above,
                    close_crossing_weekly_sma_20_below,
                    close_near_weekly_sma_50,
                    close_crossing_weekly_sma_50_above,
                    close_crossing_weekly_sma_50_below,
                    close_near_weekly_sma_100,
                    close_crossing_weekly_sma_100_above,
                    close_crossing_weekly_sma_100_below,
                    close_near_weekly_sma_200,
                    close_crossing_weekly_sma_200_above,
                    close_crossing_weekly_sma_200_below,
                    
                    sma_5_crossing_weekly_sma_20_above,
                    sma_5_crossing_weekly_sma_20_below,
                    sma_20_crossing_weekly_sma_50_above,
                    sma_20_crossing_weekly_sma_50_below,
                    sma_20_crossing_weekly_sma_100_above,
                    sma_20_crossing_weekly_sma_100_below,
                    sma_20_crossing_weekly_sma_200_above,
                    sma_20_crossing_weekly_sma_200_below,
                    sma_50_crossing_weekly_sma_100_above,
                    sma_50_crossing_weekly_sma_100_below,
                    sma_50_crossing_weekly_sma_200_above,
                    sma_50_crossing_weekly_sma_200_below,
                    sma_100_crossing_weekly_sma_200_above,
                    sma_100_crossing_weekly_sma_200_below,
                    
                    close_near_ema_20,
                    close_crossing_ema_20_above,
                    close_crossing_ema_20_below,
                    close_near_ema_50,
                    close_crossing_ema_50_above,
                    close_crossing_ema_50_below,
                    close_near_ema_100,
                    close_crossing_ema_100_above,
                    close_crossing_ema_100_below,
                    close_near_ema_200,
                    close_crossing_ema_200_above,
                    close_crossing_ema_200_below,
                    
                    ema_5_crossing_ema_20_above,
                    ema_5_crossing_ema_20_below,
                    ema_20_crossing_ema_50_above,
                    ema_20_crossing_ema_50_below,
                    ema_20_crossing_ema_100_above,
                    ema_20_crossing_ema_100_below,
                    ema_20_crossing_ema_200_above,
                    ema_20_crossing_ema_200_below,
                    ema_50_crossing_ema_100_above,
                    ema_50_crossing_ema_100_below,
                    ema_50_crossing_ema_200_above,
                    ema_50_crossing_ema_200_below,
                    ema_100_crossing_ema_200_above,
                    ema_100_crossing_ema_200_below,
                    
                    close_near_weekly_ema_20,
                    close_crossing_weekly_ema_20_above,
                    close_crossing_weekly_ema_20_below,
                    close_near_weekly_ema_50,
                    close_crossing_weekly_ema_50_above,
                    close_crossing_weekly_ema_50_below,
                    close_near_weekly_ema_100,
                    close_crossing_weekly_ema_100_above,
                    close_crossing_weekly_ema_100_below,
                    close_near_weekly_ema_200,
                    close_crossing_weekly_ema_200_above,
                    close_crossing_weekly_ema_200_below,
                    
                    ema_5_crossing_weekly_ema_20_above,
                    ema_5_crossing_weekly_ema_20_below,
                    ema_20_crossing_weekly_ema_50_above,
                    ema_20_crossing_weekly_ema_50_below,
                    ema_20_crossing_weekly_ema_100_above,
                    ema_20_crossing_weekly_ema_100_below,
                    ema_20_crossing_weekly_ema_200_above,
                    ema_20_crossing_weekly_ema_200_below,
                    ema_50_crossing_weekly_ema_100_above,
                    ema_50_crossing_weekly_ema_100_below,
                    ema_50_crossing_weekly_ema_200_above,
                    ema_50_crossing_weekly_ema_200_below,
                    ema_100_crossing_weekly_ema_200_above,
                    ema_100_crossing_weekly_ema_200_below,
                     
                    momentum_score_1m_increasing, momentum_score_1m_decreasing, 
                    momentum_score_1m_bullish_zone, momentum_score_1m_bearish_zone, 
                    momentum_score_3m_increasing, momentum_score_3m_decreasing, 
                    momentum_score_3m_bullish_zone, momentum_score_3m_bearish_zone, 
                    momentum_score_6m_increasing, momentum_score_6m_decreasing, 
                    momentum_score_6m_bullish_zone, momentum_score_6m_bearish_zone,
                    
                    cci_bullish, cci_trending_up, cci_crossed_above_100, cci_bearish,
                    cci_trending_down, cci_crossed_below_100, cci_overbought_zone, cci_oversold_zone,
                    
                    rsi_overbought_zone,
                    rsi_oversold_zone,
                    rsi_crossed_above_70,
                    rsi_crossed_below_30,
                    rsi_bullish,
                    rsi_bearish,
                    rsi_trending_up,
                    rsi_trending_down,
                    rsi_weekly_overbought_zone,
                    rsi_weekly_oversold_zone,
                    rsi_weekly_crossed_above_70,
                    rsi_weekly_crossed_below_30,
                    rsi_weekly_bullish,
                    rsi_weekly_bearish,
                    rsi_weekly_trending_up,
                    rsi_weekly_trending_down,
                    
                    mfi_bullish,
                    mfi_trending_up,
                    mfi_crossed_80_from_below,
                    mfi_bearish,
                    mfi_trending_down,
                    mfi_crossed_20_from_above,
                    mfi_above_80,
                    mfi_below_20,
                    williams_r_bullish,
                    williams_r_trending_up,
                    williams_r_crossed_20_from_below,
                    williams_r_bearish,
                    williams_r_trending_down,
                    williams_r_crossed_80_from_above,
                    williams_r_above_20,
                    williams_r_below_80,
                    
                    roc_trending_up,
                    roc_trending_down,
                    roc_increasing,
                    roc_decreasing,
                    
                    macd_crossing_signal_line_above,
                    macd_crossing_signal_line_below,
                    macd_above_zero,
                    macd_below_zero,
                    macd_crossed_signal_line_from_below,
                    macd_crossed_signal_line_from_above,
                    
                    adx_crossing_25_from_below,
                    adx_crossing_25_from_above,
                    adx_crossing_40_from_below,
                    adx_crossing_40_from_above,
                    adx_crossing_10_from_below,
                    adx_crossing_10_from_above,
                    plus_di_crossing_minus_di_from_below,
                    plus_di_crossing_minus_di_from_above,
                    plus_di_crossing_25_from_below,
                    minus_di_crossing_25_from_below,
                    
                    atr_3_days_increasing,
                    atr_3_days_decreasing,
                    atr_5_days_increasing,
                    atr_5_days_decreasing,
                    atr_7_days_increasing,
                    atr_7_days_decreasing,
                    
                    close_above_upper_bollinger_band,
                    close_below_lower_bollinger_band,
                    close_crossing_upper_bollinger_band_from_below,
                    close_crossing_lower_bollinger_band_from_above,
                    close_crossing_upper_bollinger_band_from_above,
                    close_crossing_lower_bollinger_band_from_below,
                    bollinger_band_narrowing,
                    bollinger_band_widening,
                    
                    stochastic_in_overbought_zone,
                    stochastic_in_oversold_zone,
                    stochastic_entering_overbought_zone,
                    stochastic_entering_oversold_zone,
                    stochastic_trending_up,
                    stochastic_trending_down,
                    stochastic_k_crossing_d_from_below,
                    stochastic_k_crossing_d_from_above,
                    psar_bullish_reversal,
                    psar_bearish_reversal,
                    psar_weekly_bullish_reversal,
                    psar_weekly_bearish_reversal,

                    nr4,
                    nr7,
                    nr4_breakout,
                    nr4_breakdown,
                    nr7_breakout,
                    nr7_breakdown,
                    supertrend_signal_changed_to_buy,
                    supertrend_signal_changed_to_sell,
                    price_nearing_supertrend_support,
                    price_nearing_supertrend_resistance,
                    
                    weekly_supertrend_signal_changed_to_buy,
                    weekly_supertrend_signal_changed_to_sell,
                    price_nearing_weekly_supertrend_support,
                    price_nearing_weekly_supertrend_resistance,
                    
                    futures_high_oi_increase,
                    futures_high_oi_decrease,
                    
                    call_options_high_oi_increase,
                    call_options_high_oi_decrease,
                    put_options_high_oi_increase,
                    put_options_high_oi_decrease,
                    pattern_double_top,
                    pattern_double_bottom,
                    pattern_head_and_shoulders,
                    pattern_inverse_head_and_shoulders,
                    
                    pattern_rising_wedge,
                    pattern_falling_wedge,
                    pattern_triangle_bullish,
                    pattern_triangle_bearish,
                    pattern_pennant_bullish,
                    pattern_pennant_bearish,
                    pattern_rectangle_bullish,
                    pattern_rectangle_bearish,
                    pattern_flag_bullish,
                    pattern_flag_bearish,
                    pattern_channel_rising,
                    pattern_channel_falling,
                    pattern_channel_horizontal,
                    pattern_triple_top,
                    pattern_triple_bottom,
                    
                    pattern_cup_and_handle,
                    pattern_rounding_bottom, 
                    pattern_diamond_bullish, 
                    pattern_diamond_bearish
                    

   
                ) VALUES ({})
            """.format(', '.join(['%s'] * len(rows[0])))

            execute_batch(cur, sql, rows)

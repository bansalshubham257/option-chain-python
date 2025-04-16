import json
import logging
import sys
import os

# Add correct imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.database import DatabaseService
from services.option_chain import OptionChainService

class ScannerService:
    def __init__(self, database_service, option_chain_service):
        self.database_service = database_service
        self.option_chain_service = option_chain_service

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


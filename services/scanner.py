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
                            symbol,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            pivot,
                            r1, r2, r3,
                            s1, s2, s3,
                            vwap,
                            macd_line,
                            macd_signal,
                            macd_histogram,
                            adx,
                            adx_di_positive,
                            adx_di_negative,
                            upper_bollinger,
                            lower_bollinger,
                            parabolic_sar,
                            rsi,
                            stoch_rsi,
                            ichimoku_base,
                            ichimoku_conversion,
                            ichimoku_span_a,
                            ichimoku_span_b,
                            ichimoku_cloud_top,
                            ichimoku_cloud_bottom,
                            price_change,
                            percent_change,
                            ha_open, ha_close, ha_high, ha_low,
                            supertrend,
                            acc_dist,
                            cci,
                            cmf,
                            mfi,
                            on_balance_volume,
                            williams_r,
                            bollinger_b,
                            intraday_intensity,
                            force_index,
                            stc,
                            sma10, sma20, sma50, sma100, sma200,
                            ema10, ema50, ema100, ema200,
                            wma10, wma50, wma100, wma200,
                            tma10, tma50, tma100, tma200,
                            rma10, rma50, rma100, rma200,
                            tema10, tema50, tema100, tema200,
                            hma10, hma50, hma100, hma200,
                            vwma10, vwma50, vwma100, vwma200,
                            std10, std50, std100, std200,
                            ATR, TRIX, ROC, Keltner_Middle, Keltner_Upper, Keltner_Lower,
                            Donchian_High, Donchian_Low, Chaikin_Oscillator,
                            timestamp, true_range, aroon_up, aroon_down, aroon_osc,
                            buyer_initiated_trades, buyer_initiated_quantity, buyer_avg_quantity,
                            seller_initiated_trades, seller_initiated_quantity, seller_avg_quantity,
                            buyer_seller_ratio, buyer_seller_quantity_ratio, buyer_vwap, seller_vwap,
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
                    d{main_idx}.percent_change as pChange
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
                            'pChange': percent_change
                        })
                    except Exception as e:
                        print(f"Error processing result for {row[0]}: {str(e)}")

                return results

        except Exception as e:
            print(f"SQL scanner error: {str(e)}")
            return []


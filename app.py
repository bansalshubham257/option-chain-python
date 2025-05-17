import asyncio
import concurrent
import logging
from functools import lru_cache

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import pytz
from datetime import datetime
import threading
import time
from services.option_chain import OptionChainService
from services.market_data import MarketDataService
from services.stock_analysis import StockAnalysisService
from services.database import DatabaseService
from services.scanner import ScannerService
from upstox_feed import UpstoxFeedWorker
from config import Config
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

from curl_cffi import requests
import yfinance as yf


session = requests.Session(impersonate="chrome110")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": [
    "https://swingtradingwithme.blogspot.com",
    "https://aitradinglab.blogspot.com",
    "https://www.aitradinglab.in",
    "https://bansalshubham257.github.io",
    "http://localhost:63342"
]}})

# Initialize services
database_service = DatabaseService()
option_chain_service = OptionChainService()
market_data_service = MarketDataService(
    database_service=database_service,
    option_chain_service=option_chain_service
)
stock_analysis_service = StockAnalysisService()
scanner_service = ScannerService(database_service, option_chain_service)


# API Routes
@app.route("/stocks", methods=["GET"])
def get_all_stocks():
    return jsonify(stock_analysis_service.fetch_all_nse_stocks())

@app.route("/fno-stocks", methods=["GET"])
def get_all_Fno_stocks():
    return jsonify(option_chain_service.get_fno_stocks_with_symbols())

@app.route("/analyze", methods=["GET"])
def analyze():
    # Get the raw query string and extract the symbol parameter
    query_string = request.environ.get('QUERY_STRING', '')
    
    # Handle the case where symbol contains ampersand
    if query_string.startswith('symbol='):
        symbol = query_string[7:]  # Remove 'symbol=' prefix to get the full symbol
    else:
        # Fallback to the regular method (for other parameters or if format changes)
        symbol = request.args.get("symbol")
    
    if not symbol:
        return jsonify({"error": "Stock symbol is required"}), 400

    return jsonify(stock_analysis_service.analyze_stock(symbol))

@app.route('/get-orders', methods=['GET'])
def get_orders():
    if not market_data_service.is_market_open():
        return jsonify({'status': 'Market is closed'})
    return jsonify(database_service.get_options_orders())

@app.route('/get-futures-orders', methods=['GET'])
def get_futures_orders():
    if not market_data_service.is_market_open():
        return jsonify({'status': 'Market is closed'})
    return jsonify(database_service.get_futures_orders())

@app.route('/get_fno_stocks', methods=['GET'])
def get_upstox_fno_stocks():
    return jsonify(option_chain_service.get_fno_stocks())

@app.route('/get_fno_data', methods=['GET'])
def get_fno_data():
    stock = request.args.get('stock')
    expiry = request.args.get('expiry')
    strike = request.args.get('strike')
    option_type = request.args.get('option_type')
    return jsonify(database_service.get_fno_data(stock, expiry, strike, option_type))

@app.route('/get_fno_data_bulk', methods=['GET'])
def get_fno_data_bulk():
    """Fetch multiple strike prices and option types in a single request"""
    stock = request.args.get('stock')
    expiry = request.args.get('expiry')
    strikes = request.args.get('strikes')
    option_types = request.args.get('option_types')
    
    if not all([stock, expiry, strikes, option_types]):
        return jsonify({"error": "Missing required parameters", "data": []})
    
    try:
        strikes_list = strikes.split(',')
        option_types_list = option_types.split(',')
        
        with database_service._get_cursor() as cur:
            query = """
                SELECT display_time, oi, volume, price, strike_price, option_type
                FROM oi_volume_history
                WHERE symbol = %s 
                  AND expiry_date = %s
                  AND strike_price IN ({})
                  AND option_type IN ({})
                ORDER BY display_time
            """.format(','.join(['%s'] * len(strikes_list)), 
                       ','.join(['%s'] * len(option_types_list)))
            
            params = [stock, expiry] + strikes_list + option_types_list
            cur.execute(query, params)
            
            results = cur.fetchall()
            data = [{
                'time': r[0], 
                'oi': float(r[1]) if r[1] else 0,
                'volume': float(r[2]) if r[2] else 0, 
                'price': float(r[3]) if r[3] else 0,
                'strike': str(r[4]), 
                'optionType': r[5]
            } for r in results]
            
            return jsonify({"status": "success", "data": data})
    
    except Exception as e:
        print(f"Error in bulk fetch: {str(e)}")
        return jsonify({"status": "error", "message": str(e), "data": []})


@app.route('/get_option_data', methods=['GET'])
def get_option_data():
    stock = request.args.get('stock')
    if not stock:
        return jsonify({"error": "Stock symbol is required"}), 400
    return jsonify(database_service.get_option_data(stock))

@app.route('/api/fii-dii', methods=['GET'])
def get_fii_dii_data():
    year_month = request.args.get('year_month')
    request_type = request.args.get('request_type')
    return jsonify(market_data_service.get_fii_dii_data(year_month, request_type))

@app.route("/fetch_stocks", methods=["GET"])
def get_nse_bse_stocks():
    return jsonify(option_chain_service.fetch_stocks())

@app.route("/fetch_price", methods=["GET"])
def get_price():
    instrument_key = request.args.get("instrument_key")
    return jsonify(option_chain_service.fetch_price(instrument_key))

@app.route("/fetch_bulk_prices", methods=["GET"])
def get_bulk_prices():
    instrument_keys = request.args.get("instrument_keys", "").split(',')
    return jsonify(option_chain_service.fetch_bulk_prices(instrument_keys))

@app.route('/api/global-market-data', methods=['GET'])
def get_global_market_data():
    return jsonify(market_data_service.get_global_market_data())

@app.route('/api/market-data', methods=['GET'])
def get_market_data():
    return jsonify(market_data_service.get_indian_market_data())

@app.route('/api/ipos')
def get_ipos():
    return jsonify(market_data_service.get_ipos())

@app.route("/api/latest-price", methods=["GET"])
def get_latest_price():
    """Get the latest price for a stock symbol"""
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "Stock symbol is required"}), 400

    # Ensure symbol has .NS suffix for consistency
    if not symbol.endswith('.NS'):
        symbol = f"{symbol}.NS"

    try:
        with database_service._get_cursor() as cur:
            cur.execute("""
                SELECT close, price_change, percent_change, timestamp, last_updated
                FROM stock_data_cache
                WHERE symbol = %s AND interval = '1d'
            """, (symbol,))

            result = cur.fetchone()
            if not result:
                return jsonify({"error": "Stock data not found"}), 404

            return jsonify({
                "symbol": symbol,
                "price": float(result[0]) if result[0] is not None else None,
                "price_change": float(result[1]) if result[1] is not None else None,
                "percent_change": float(result[2]) if result[2] is not None else None,
                "timestamp": result[3].isoformat() if hasattr(result[3], 'isoformat') else str(result[3]),
                "last_updated": result[4].isoformat() if hasattr(result[4], 'isoformat') else str(result[4])
            })

    except Exception as e:
        logging.error(f"Error fetching latest price for {symbol}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/latest-option-price', methods=['GET'])
def get_latest_option_price():
    """Get the latest price for a specific option"""
    symbol = request.args.get("symbol")
    expiry = request.args.get("expiry")
    strike = request.args.get("strike")
    option_type = request.args.get("optionType")

    if not all([symbol, expiry, strike, option_type]):
        return jsonify({"error": "All parameters (symbol, expiry, strike, optionType) are required"}), 400

    try:
        with database_service._get_cursor() as cur:
            cur.execute("""
                SELECT price, display_time, pct_change
                FROM oi_volume_history
                WHERE symbol = %s 
                  AND expiry_date = %s 
                  AND strike_price = %s 
                  AND option_type = %s
                ORDER BY display_time DESC
                LIMIT 1
            """, (symbol, expiry, strike, option_type))

            result = cur.fetchone()
            if not result:
                return jsonify({"error": "Option data not found"}), 404

            return jsonify({
                "price": float(result[0]) if result[0] else 0,
                "timestamp": result[1].isoformat() if hasattr(result[1], 'isoformat') else str(result[1]),
                "pct_change": float(result[2]) if result[2] else 0
            })

    except Exception as e:
        logging.error(f"Error fetching latest option price: {str(e)}")
        return jsonify({"error": str(e)}), 500

def run_script():
    """Background worker for market hours processing"""
    option_chain_service.run_market_processing()

def run_market_data_worker():
    """Background worker for market data updates"""
    while True:
        try:
            market_data_service.update_all_market_data()
            time.sleep(300)  # 5 minutes between updates
        except Exception as e:
            print(f"Error in market data worker: {e}")
            time.sleep(60)


@app.route('/api/fno-analytics', methods=['GET'])
def get_fno_analytics():
    try:
        analytics_type = request.args.get('type')  # 'buildup' or 'oi_analytics'
        category = request.args.get('category')    # e.g. 'futures_long', 'oi_gainer'
        limit = int(request.args.get('limit', 20))

        # Build query based on parameters
        query = """
            SELECT symbol, analytics_type, category, strike, option_type,
                   price_change, oi_change, volume_change, absolute_oi, timestamp
            FROM fno_analytics
            WHERE created_at >= NOW() - INTERVAL '1 day'
        """
        params = []

        if analytics_type:
            query += " AND analytics_type = %s"
            params.append(analytics_type)

        if category:
            query += " AND category = %s"
            params.append(category)

        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)

        with database_service._get_cursor() as cur:
            cur.execute(query, params)
            results = []
            for row in cur.fetchall():
                results.append({
                    'symbol': row[0],
                    'analytics_type': row[1],
                    'category': row[2],
                    'strike': float(row[3]) if row[3] is not None else None,
                    'option_type': row[4],
                    'price_change': float(row[5]) if row[5] is not None else None,
                    'oi_change': float(row[6]) if row[6] is not None else None,
                    'volume_change': float(row[7]) if row[7] is not None else None,
                    'absolute_oi': int(row[8]) if row[8] is not None else None,
                    'timestamp': row[9].isoformat() if hasattr(row[9], 'isoformat') else row[9]
                })

            return jsonify({
                "status": "success",
                "data": results,
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
            })

    except Exception as e:
        logging.error(f"Error fetching analytics: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/oi-extremes', methods=['GET'])
def get_oi_extremes():
    try:
        limit = int(request.args.get('limit', 10))
        with database_service._get_cursor() as cur:
            # Get OI gainers separately
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change,
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'oi_analytics'
                AND oi_change > 0
                ORDER BY oi_change DESC
                LIMIT %s
            """, (limit,))

            oi_gainers = []
            for row in cur.fetchall():
                oi_gainers.append({
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]) if row[5] is not None else 0,
                    'timestamp': row[6]
                })

            # Get OI losers separately
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change,
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'oi_analytics'
                AND oi_change < 0
                ORDER BY oi_change ASC
                LIMIT %s
            """, (limit,))

            oi_losers = []
            for row in cur.fetchall():
                oi_losers.append({
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]) if row[5] is not None else 0,
                    'timestamp': row[6]
                })

            # Get volume gainers separately
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change,
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'buildup'
                AND volume_change > 0
                ORDER BY volume_change DESC
                LIMIT %s
            """, (limit,))

            volume_gainers = []
            for row in cur.fetchall():
                volume_gainers.append({
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]),
                    'timestamp': row[6]
                })

            # Get volume losers separately
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change,
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'buildup'
                AND volume_change < 0
                ORDER BY volume_change ASC
                LIMIT %s
            """, (limit,))

            volume_losers = []
            for row in cur.fetchall():
                volume_losers.append({
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]),
                    'timestamp': row[6]
                })

            return jsonify({
                "status": "success",
                "data": {
                    "oi_gainers": oi_gainers,
                    "oi_losers": oi_losers,
                    "volume_gainers": volume_gainers,
                    "volume_losers": volume_losers
                },
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
            })

    except Exception as e:
        logging.error(f"Error fetching OI extremes: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/price-patterns', methods=['GET'])
def get_price_patterns():
    try:
        with database_service._get_cursor() as cur:
            sql = """
                SELECT sr.stock_name,
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_double_top THEN 'Double Top' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_double_bottom THEN 'Double Bottom' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_head_and_shoulders THEN 'Head and Shoulders' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_inverse_head_and_shoulders THEN 'Inverse Head and Shoulders' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_rising_wedge THEN 'Rising Wedge' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_falling_wedge THEN 'Falling Wedge' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_triangle_bullish THEN 'Bullish Triangle' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_triangle_bearish THEN 'Bearish Triangle' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_pennant_bullish THEN 'Bullish Pennant' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_pennant_bearish THEN 'Bearish Pennant' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_rectangle_bullish THEN 'Bullish Rectangle' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_rectangle_bearish THEN 'Bearish Rectangle' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_flag_bullish THEN 'Bullish Flag' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_flag_bearish THEN 'Bearish Flag' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_channel_rising THEN 'Rising Channel' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_channel_falling THEN 'Falling Channel' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_channel_horizontal THEN 'Horizontal Channel' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_triple_top THEN 'Triple Top' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_triple_bottom THEN 'Triple Bottom' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_cup_and_handle THEN 'Cup and Handle' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_rounding_bottom THEN 'Rounding Bottom' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_diamond_bullish THEN 'Bullish Diamond' END) ||
                    ARRAY_AGG(DISTINCT CASE WHEN pattern_diamond_bearish THEN 'Bearish Diamond' END) as patterns,
                    sr.scan_date
                FROM scanner_results sr 
                WHERE sr.scan_date = CURRENT_DATE
                    AND (
                        pattern_double_top OR pattern_double_bottom OR pattern_head_and_shoulders OR
                        pattern_inverse_head_and_shoulders OR pattern_rising_wedge OR pattern_falling_wedge OR
                        pattern_triangle_bullish OR pattern_triangle_bearish OR pattern_pennant_bullish OR
                        pattern_pennant_bearish OR pattern_rectangle_bullish OR pattern_rectangle_bearish OR
                        pattern_flag_bullish OR pattern_flag_bearish OR pattern_channel_rising OR
                        pattern_channel_falling OR pattern_channel_horizontal OR pattern_triple_top OR
                        pattern_triple_bottom OR pattern_cup_and_handle OR pattern_rounding_bottom OR
                        pattern_diamond_bullish OR pattern_diamond_bearish
                    )
                GROUP BY sr.stock_name, sr.scan_date
                ORDER BY sr.stock_name;
            """
            cur.execute(sql)
            results = cur.fetchall()

            patterns_data = []
            for row in results:
                stock_name = row[0]
                # Filter out None values and flatten the array
                patterns = [p for p in row[1] if p is not None]
                scan_date = row[2].strftime('%Y-%m-%d')

                patterns_data.append({
                    'symbol': stock_name,
                    'patterns': patterns,
                    'scan_date': scan_date
                })

            return jsonify({
                'status': 'success',
                'data': patterns_data
            })

    except Exception as e:
        print(f"Error fetching price patterns: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


def run_option_chain_worker():
    """Background worker for option chain processing"""
    option_chain_service.run_market_processing()

def run_instrument_keys_worker():
    """Background worker to periodically fetch and store instrument keys in the database."""
    print(f"Starting instrument keys worker at {datetime.now()}")
    
    last_update_date = None
    
    while True:
        try:
            current_date = datetime.now().date()
            
            # Update instrument keys once per day or on first run
            if last_update_date is None or last_update_date != current_date:
                print(f"Fetching instrument keys at {datetime.now()}")
                
                # Fetch instrument data from Upstox with strike filtering
                instrument_dict = option_chain_service.fetch_instrument_keys()
                
                if instrument_dict:
                    print(f"Successfully fetched {len(instrument_dict)} filtered instruments")
                    
                    # Save to database
                    batch_size = 500
                    filtered_instruments = []
                    
                    # Extract strike prices for better filtering
                    for key, details in instrument_dict.items():
                        # Add instrument details including strike price from trading symbol
                        instrument_data = {
                            'instrument_key': key,
                            'tradingsymbol': details.get('trading_symbol'),
                            'name': details.get('name'),
                            'instrument_type': details.get('instrument_type'),
                            'exchange': details.get('exchange'),
                            'expiry': details.get('expiry'),
                            'lot_size': details.get('lot_size', 0)
                        }
                        
                        # Parse the trading symbol to get strike price
                        if 'trading_symbol' in details:
                            parsed = option_chain_service._parse_option_symbol(details['trading_symbol'])
                            if parsed:
                                instrument_data['strike_price'] = parsed.get('strike_price')
                            else:
                                # For non-options like futures
                                instrument_data['strike_price'] = None
                        
                        filtered_instruments.append(instrument_data)
                    
                    # Save to database in batches
                    total_instruments = len(filtered_instruments)
                    print(f"Preparing to save {total_instruments} instruments to database")
                    
                    for i in range(0, total_instruments, batch_size):
                        batch = filtered_instruments[i:i+batch_size]
                        database_service.save_instrument_keys(batch)
                        print(f"Saved batch {i//batch_size + 1}/{(total_instruments-1)//batch_size + 1} ({len(batch)} instruments)")
                    
                    last_update_date = current_date
                    print(f"Successfully updated instrument keys at {datetime.now()}")
                else:
                    print("No instrument data available")
            
            # Sleep for 6 hours before next check
            run_prev_close_worker()
            sleep_time = 6 * 3600
            print(f"Instrument keys worker sleeping for {sleep_time//3600} hours")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"Error in instrument keys worker: {e}")
            import traceback
            traceback.print_exc()
            # Shorter sleep on error
            time.sleep(1800)  # 30 minutes

def run_prev_close_worker():
    """Background worker to fetch previous close prices for all instrument keys and update the database."""
    print(f"Starting prev close worker at {datetime.now()}")
    while True:
        try:
            # Fetch all instrument keys
            instrument_keys = database_service.get_all_instrument_keys()
            if not instrument_keys:
                print("No instrument keys found.")
                time.sleep(3600)  # Sleep for 1 hour if no keys are found
                continue

            # Fetch previous close prices
            prev_close_data = option_chain_service.fetch_prev_close_prices(instrument_keys)

            # Update the database with the fetched data
            if prev_close_data:
                database_service.update_instrument_keys_with_prev_close(prev_close_data)
                print(f"Updated previous close prices for {len(prev_close_data)} instruments.")
            else:
                print("No previous close data fetched.")

            # Sleep for 6 hours before the next run
            time.sleep(6 * 3600)
        except Exception as e:
            print(f"Error in prev close worker: {e}")
            time.sleep(1800)  # Retry after 30 minutes on error

# Helper function to extract strike price from trading symbol
def get_strike_price(trading_symbol):
    """Extract strike price from trading symbol if it's an option"""
    if not trading_symbol:
        return None
    
    try:
        # Try to match patterns like "RELIANCE24APR4000CE" or "NIFTY24APR24000CE"
        import re
        match = re.match(r"[A-Z]+\d{2}[A-Z]{3}(\d+)(?:CE|PE)$", trading_symbol)
        if match:
            return float(match.group(1))
        return None
    except Exception:
        return None

@app.route("/52-week-extremes", methods=["GET"])
def get_52_week_extremes():
    limit = int(request.args.get("limit", 50))
    threshold = float(request.args.get("threshold", 5.0))  # in percentage

    # Get data from database
    data = stock_analysis_service.get_52_week_extremes(threshold=threshold/100)
    if not data:
        return jsonify({"error": "No 52-week data available", "timestamp": datetime.now().isoformat()})

    # Filter by threshold if provided
    if threshold:
        filtered_data = [
            item for item in data
            if (item['status'] == 'near_high' and item['pct_from_high'] <= threshold) or
               (item['status'] == 'near_low' and item['pct_from_low'] <= threshold)
        ]
    else:
        filtered_data = data

    return jsonify({
        "data": filtered_data,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/api/scanner/save', methods=['POST'])
def save_scanner():
    try:
        data = request.json
        if not data or 'name' not in data or 'conditions' not in data:
            return jsonify({"error": "Name and conditions are required"}), 400

        result = scanner_service.save_scanner(
            data['name'],
            data['conditions'],
            data.get('stock_type', 'fno'),
            data.get('logic', 'AND')  # Pass the logic parameter
        )
        return jsonify(result)

    except Exception as e:
        logging.error(f"Error saving scanner: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scanner/load', methods=['GET'])
def load_scanners():
    try:
        result = scanner_service.load_scanners()
        return jsonify(result)
    except Exception as e:
        logging.error(f"Error loading scanners: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scanner/delete/<int:scanner_id>', methods=['DELETE'])
def delete_scanner(scanner_id):
    try:
        result = scanner_service.delete_scanner(scanner_id)
        return jsonify(result)
    except Exception as e:
        logging.error(f"Error deleting scanner: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/scanner/run', methods=['POST'])
def run_scanner():
    try:
        data = request.json
        if not data or 'conditions' not in data or 'stock_type' not in data:
            return jsonify({"error": "Conditions and stock type are required"}), 400

        # Default interval is used when an indicator doesn't specify one
        default_interval = data.get('interval', '1d')

        # Execute scanner query with SQL approach
        results = scanner_service.query_stocks_with_sql(
            data['conditions'],
            data['stock_type'],
            default_interval,
            data.get('logic', 'AND')
        )

        return jsonify({
            "status": "success",
            "data": results,
            "count": len(results)
        })

    except Exception as e:
        logging.error(f"Error running scanner: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Scanner routes

@app.route('/api/most-gainer-strikes', methods=['GET'])
def get_most_gainer_strikes():
    limit = int(request.args.get('limit', 10))
    offset = int(request.args.get('offset', 0))
    data = database_service.get_top_strikes(metric="pct_change", limit=limit, offset=offset)
    return jsonify(data)

@app.route('/api/most-active-strikes', methods=['GET'])
def get_most_active_strikes():
    limit = int(request.args.get('limit', 10))
    offset = int(request.args.get('offset', 0))
    data = database_service.get_top_strikes(metric="volume", limit=limit, offset=offset)
    return jsonify(data)

def run_stock_data_updater():
    """Optimized stock data updater that only processes 1d interval"""
    last_cache_clear_date = None
    
    # Only process 1d interval
    interval = '1d'
    
    # Configuration for batch processing
    stocks_per_worker = 20
    max_workers = 20  # Increase workers since we're only handling one interval

    # Define the main indices with their Yahoo Finance symbols
    main_indices = [
        "^NSEI",      # NIFTY 50
        "^NSEBANK",   # NIFTY BANK
        "NIFTY_FIN_SERVICE.NS",    # FINNIFTY
        "^NSEMDCP50",   # MIDCPNIFTY
        "^BSESN",     # SENSEX
        "BSE-BANK.BO"    # BANKEX
    ]
    
    # Define mapping from Yahoo symbols to our internal names for consistency
    index_mapping = {
        "^NSEI": "NIFTY.NS",
        "^NSEBANK": "BANKNIFTY.NS",
        "NIFTY_FIN_SERVICE.NS": "FINNIFTY.NS",
        "^NSEMDCP50": "MIDCPNIFTY.NS",
        "^BSESN": "SENSEX.NS",
        "BSE-BANK.BO": "BANKEX.NS"
    }

    while True:
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.now(ist)
            current_date = now.date()

            # Clear cache on new day
            if last_cache_clear_date != current_date:
                database_service.pivot_calculation_cache.clear()
                last_cache_clear_date = current_date
                print(f"{now}: New day - cleared pivot calculation cache")

            # Check market hours
            if now.weekday() in Config.TRADING_DAYS and Config.MARKET_OPEN <= now.time() <= Config.MARKET_CLOSE:
                print(f"{now}: Running stock data update for 1d interval only...")
                
                # Get FNO stocks
                fno_stocks = option_chain_service.get_fno_stocks_with_symbols()
                
                # First update the main indices as they are more important
                print(f"{now}: Updating main indices data...")
                start_time = time.time()
                
                # Create a Tickers object for all indices together
                indices_tickers = yf.Tickers(" ".join(main_indices), session=session)
                
                for yahoo_symbol, local_name in index_mapping.items():
                    try:
                        # Get data from the batch request
                        data = indices_tickers.tickers[yahoo_symbol].history(period='1y', interval='1d')

                        if not data.empty:
                            # For indices, use simplified method that only stores price data without calculating indicators
                            success = database_service.update_index_data(local_name, interval, data)
                            if success:
                                print(f"✅ Updated {local_name} index data successfully")
                            else:
                                print(f"❌ Failed to update {local_name} index data")
                    except Exception as e:
                        print(f"Error updating {local_name} ({yahoo_symbol}) index: {e}")
                
                indices_time = time.time() - start_time
                print(f"✅ Indices update completed in {indices_time:.2f}s")
                
                # Then update the regular FNO stocks
                print(f"{now}: Updating FNO stocks data...")
                start_time = time.time()
                success_count = update_stocks_for_interval(fno_stocks, interval, stocks_per_worker, max_workers)
                total_time = time.time() - start_time
                
                print(f"✅ FNO stocks completed: {success_count} stocks in {total_time:.2f}s")
                print(f"{now}: 1d interval stock data update completed")
                
                # Wait longer between updates since we're only processing 1d data
                time.sleep(30)  # 30 minutes between updates
                market_data_service.update_all_market_data()
                time.sleep(30)  # 30 minutes between updates
            else:
                # Sleep when market is closed
                time.sleep(1800)  # 30 minutes

        except Exception as e:
            print(f"Error in stock data updater: {e}")
            time.sleep(300)

def update_stocks_for_interval(stocks, interval, stocks_per_worker, max_workers):
    """Process all stocks for a specific interval with parallel workers"""
    from concurrent.futures import ThreadPoolExecutor
    import time

    start_time = time.time()
    success_count = 0

    # Filter out any empty symbols first
    valid_stocks = [stock for stock in stocks if stock and stock.strip()]
    
    if not valid_stocks:
        print(f"No valid stocks to process for {interval} interval")
        return 0, 0

    # Divide stocks into chunks for parallel processing
    stock_chunks = [valid_stocks[i:i+stocks_per_worker] for i in range(0, len(valid_stocks), stocks_per_worker)]

    # Create a thread pool to process chunks in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        chunk_futures = []

        # Submit each chunk for processing
        for chunk_idx, chunk in enumerate(stock_chunks):
            chunk_futures.append(
                executor.submit(
                    process_stock_chunk,
                    chunk,
                    interval,
                    chunk_idx,
                    len(stock_chunks)
                )
            )

        # Process results as they complete
        for future in concurrent.futures.as_completed(chunk_futures):
            try:
                chunk_success_count = future.result()
                success_count += chunk_success_count
            except Exception as e:
                print(f"Error in chunk processing: {e}")

    total_time = time.time() - start_time
    return success_count, total_time

def process_stock_chunk(stock_chunk, interval, chunk_idx, total_chunks):
    """Process a chunk of stocks for the given interval"""
    success_count = 0

    # Get period settings for this interval
    period_map = {
        '1d': ('1y', '1d'),  # Use 1 year of data for daily to properly calculate 52-week high/low
        '1h': ('7d', '1h'),
        '15m': ('5d', '15m'),
        '5m': ('5d', '5m')
    }
    period, interval_str = period_map.get(interval, ('1mo', interval))
    
    # Filter out any empty symbols that might have slipped through
    valid_stocks = [stock for stock in stock_chunk if stock and stock.strip()]
    
    if not valid_stocks:
        print(f"No valid stocks in chunk {chunk_idx+1}/{total_chunks} for {interval} interval")
        return 0
        
    # Optimize by creating a single YFinance session
    yf_session = yf.Tickers(" ".join(valid_stocks), session=session)

    # Process each stock in the chunk
    for stock in valid_stocks:
        try:
            # Get data using the shared session
            data = yf_session.tickers[stock].history(period=period, interval=interval_str)
  
            # Fetch company info data
            info_data = yf_session.tickers[stock].info

            # The 52-week high/low data will be calculated inside the update_stock_data method
            # Only for daily interval data (to avoid redundant calculations)
            
            if not data.empty:
                # Process the data
                success = database_service.update_stock_data(stock, interval, data, info_data)
                if success:
                    success_count += 1
            else:
                print(f"No data for {stock} {interval}")

        except Exception as e:
            print(f"Error updating {stock} {interval}: {e}")

    print(f"Completed chunk {chunk_idx+1}/{total_chunks} for {interval}: {success_count}/{len(valid_stocks)} successful")
    return success_count

def update_stock_batch(stocks, interval):
    """Update a batch of stocks using the bulk Tickers API"""
    try:
        # Get period settings
        period_map = {
            '1d': ('1y', '1d'),
            '1h': ('7d', '1h'),
            '15m': ('5d', '15m'),
            '5m': ('5d', '5m')
        }
        period, interval_str = period_map.get(interval, ('1mo', interval))

        # Create a single Tickers object for all stocks in batch
        tickers = yf.Tickers(" ".join(stocks), session=session)

        success_count = 0
        for stock in stocks:
            try:
                # Get data from the batch request
                data = tickers.tickers[stock].history(period=period, interval=interval_str)

                if not data.empty:
                    success = database_service.update_stock_data(stock, interval, data)
                    if success:
                        success_count += 1
            except Exception as e:
                print(f"Error in batch update for {stock}: {e}")

        return success_count
    except Exception as e:
        print(f"Batch update failed: {e}")
        return 0

@app.route('/api/market-breadth', methods=['GET'])
def get_market_breadth():
    """API endpoint to fetch market breadth data with technical indicators"""
    try:
        with database_service._get_cursor() as cur:
            # Fetch stock data with technical indicators
            cur.execute("""
                SELECT 
                    symbol, interval, close, price_change, percent_change, volume,
                    pivot, sma20, sma50, sma100, sma200, ema50, ema100, vwap,
                    rsi, macd_line, macd_signal,
                    pe_ratio, market_cap, beta, dividend_yield, price_to_book,
                    industry, sector
                FROM stock_data_cache
                ORDER BY market_cap DESC NULLS LAST
            """)

            # Convert DB rows to list of dictionaries with proper type conversion
            results = []
            for row in cur.fetchall():
                # Create base data dictionary with None for NaN/NULL values
                data_dict = {
                    'symbol': row[0],
                    'interval': row[1],
                    'close': float(row[2]) if row[2] is not None else None,
                    'price_change': float(row[3]) if row[3] is not None else 0.0,
                    'percent_change': float(row[4]) if row[4] is not None else 0.0,
                    'volume': float(row[5]) if row[5] is not None else None,
                    'pivot': float(row[6]) if row[6] is not None else None,
                    'sma20': float(row[7]) if row[7] is not None else None,
                    'sma50': float(row[8]) if row[8] is not None else None,
                    'sma100': float(row[9]) if row[9] is not None else None,
                    'sma200': float(row[10]) if row[10] is not None else None,
                    'ema50': float(row[11]) if row[11] is not None else None,
                    'ema100': float(row[12]) if row[12] is not None else None,
                    'vwap': float(row[13]) if row[13] is not None else None,
                    'rsi': float(row[14]) if row[14] is not None else None,
                    'macd_line': float(row[15]) if row[15] is not None else None,
                    'macd_signal': float(row[16]) if row[16] is not None else None,
                    'pe_ratio': float(row[17]) if row[17] is not None else None,
                    'market_cap': float(row[18]) if row[18] is not None else None,
                    'beta': float(row[19]) if row[19] is not None else None,
                    'dividend_yield': float(row[20]) if row[20] is not None else None,
                    'price_to_book': float(row[21]) if row[21] is not None else None,
                    'industry': row[22],
                    'sector': row[23],
                    'isFnO': row[0].replace('.NS', '') in option_chain_service.fno_stocks
                }

                # Explicitly check for NaN values and replace with None
                import math
                for key, value in list(data_dict.items()):
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        data_dict[key] = None

                results.append(data_dict)

            return jsonify({
                "status": "success",
                "stocks": results,
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
            })

    except Exception as e:
        logging.error(f"Error fetching market breadth data: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scanner-results', methods=['GET'])
def get_scanner_results():
    try:
        stock_name = request.args.get('stock_name')
        scan_date = request.args.get('scan_date')

        # Fetch results from the database
        results = scanner_service.get_scanner_results(stock_name, scan_date)
        return jsonify(results)
    except Exception as e:
        logging.error(f"Error fetching scanner results: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/financial-results', methods=['GET'])
def get_financial_results():
    """API endpoint to fetch upcoming and declared financial results."""
    try:
        # Fetch all upcoming and declared results from the database
        upcoming_results = database_service.get_upcoming_financial_results()
        declared_results = database_service.get_declared_financial_results()
        # Replace NaN values with None in the results
        import math
        def sanitize_data(data):
            for item in data:
                for key, value in item.items():
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        item[key] = None
            return data
        sanitized_upcoming = sanitize_data(upcoming_results)
        sanitized_declared = sanitize_data(declared_results)
        # Ensure no filtering is applied here; return all results
        return jsonify({
            "status": "success",
            "upcoming": sanitized_upcoming,
            "declared": sanitized_declared
        })
    except Exception as e:
        logging.error(f"Error fetching financial results: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
def run_financials_worker():
    """Background worker to fetch and store quarterly financial data with optimized performance."""
    while True:
        try:
            print("Starting financial data collection worker")
            start_time = time.time()
            
            # Execute the optimized batch collection process
            stats = stock_analysis_service.fetch_and_store_financials()
            
            elapsed_time = time.time() - start_time
            print(f"Financial data collection completed in {elapsed_time/60:.1f} minutes")
            print(f"Successfully processed {stats['success_count']} stocks with financial data")
            print(f"Processed {stats['skipped_count']} stocks with only earnings dates")
            print(f"Failed to process {stats['failed_count']} stocks")
            
            # Calculate next run time - wait longer if we had good coverage
            coverage_ratio = (stats['success_count'] + stats['skipped_count']) / stats['total_stocks']
            if coverage_ratio > 0.8:
                # If we got good coverage, wait longer
                wait_time = 4 * 3600  # 4 hours
            else:
                # If coverage was poor, try again sooner
                wait_time = 2 * 3600  # 2 hours
            scanner_service.run_hourly_scanner()
            print(f"Sleeping for {wait_time/3600:.1f} hours before next run")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Error in financials worker: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1800)  # Retry after 30 minutes on error

@app.route('/api/stock-data', methods=['GET'])
def get_stock_data():
    """API endpoint to fetch stock data for heatmap visualization"""
    try:
        with database_service._get_cursor() as cur:
            # Fetch the latest stock data with interval '1d' (daily)
            cur.execute("""
                SELECT 
                    symbol, interval, close, price_change, percent_change, volume,
                    pe_ratio, market_cap, beta, dividend_yield, price_to_book,
                    industry, sector
                FROM stock_data_cache
                WHERE interval = '1d'
                ORDER BY market_cap DESC NULLS LAST
            """)

            # Convert DB rows to list of dictionaries with proper type conversion
            results = []
            for row in cur.fetchall():
                # Determine if stock is an F&O stock
                is_fno = False
                symbol_without_ns = row[0].replace('.NS', '')
                if symbol_without_ns in option_chain_service.fno_stocks:
                    is_fno = True

                # Add to results, converting decimal values to floats
                results.append({
                    'symbol': row[0],
                    'interval': row[1],
                    'close': float(row[2]) if row[2] is not None else None,
                    'price_change': float(row[3]) if row[3] is not None else 0.0,
                    'percent_change': float(row[4]) if row[4] is not None else 0.0,
                    'volume': float(row[5]) if row[5] is not None else None,
                    'pe_ratio': float(row[6]) if row[6] is not None else None,
                    'market_cap': float(row[7]) if row[7] is not None else None,
                    'beta': float(row[8]) if row[8] is not None else None,
                    'dividend_yield': float(row[9]) if row[9] is not None else None,
                    'price_to_book': float(row[10]) if row[10] is not None else None,
                    'industry': row[11],
                    'sector': row[12],
                    'isFnO': is_fno
                })

            return jsonify(results)
    except Exception as e:
        logging.error(f"Error fetching stock data: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/option-metadata', methods=['GET'])
def get_option_metadata():
    """Get all option metadata (symbols, expiries, strikes) in a single API call"""
    try:
        with database_service._get_cursor() as cur:
            # First get all unique symbols
            cur.execute("""
                SELECT DISTINCT symbol 
                FROM oi_volume_history 
                ORDER BY symbol
            """)
            symbols = [row[0] for row in cur.fetchall()]
            
            # Then get all symbol-expiry combinations
            cur.execute("""
                SELECT DISTINCT symbol, expiry_date 
                FROM oi_volume_history 
                ORDER BY symbol, expiry_date
            """)
            expiry_map = {}
            for row in cur.fetchall():
                symbol = row[0]
                expiry_date = row[1].strftime('%Y-%m-%d')
                
                if symbol not in expiry_map:
                    expiry_map[symbol] = []
                
                expiry_map[symbol].append(expiry_date)
            
            # Finally get all symbol-expiry-strike combinations
            cur.execute("""
                SELECT DISTINCT symbol, expiry_date, strike_price
                FROM oi_volume_history 
                ORDER BY symbol, expiry_date, strike_price
            """)
            strike_map = {}
            for row in cur.fetchall():
                symbol = row[0]
                expiry_date = row[1].strftime('%Y-%m-%d')
                strike = float(row[2])
                
                key = f"{symbol}|{expiry_date}"
                if key not in strike_map:
                    strike_map[key] = []
                
                strike_map[key].append(strike)
            
            return jsonify({
                "status": "success",
                "data": {
                    "symbols": symbols,
                    "expiry_map": expiry_map,
                    "strike_map": strike_map
                }
            })
            
    except Exception as e:
        logging.error(f"Error fetching option metadata: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/option-price-prediction', methods=['POST'])
def predict_option_price():
    """Calculate predicted option price based on changes to option parameters"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Extract required parameters
        base_price = float(data.get('base_price', 0))
        delta = float(data.get('delta', 0))
        gamma = float(data.get('gamma', 0))
        theta = float(data.get('theta', 0))
        vega = float(data.get('vega', 0))
        current_iv = float(data.get('current_iv', 0))

        # Get parameter changes from request - convert to daily values for intraday
        underlying_change = float(data.get('underlying_change', 0)) / 100  # Convert % to decimal
        iv_change = float(data.get('iv_change', 0)) / 100  # Convert % to decimal
        days_to_expiry = float(data.get('days_to_expiry', 1)) / 365  # Convert days to year fraction

        # Calculate price impact of each factor
        delta_effect = delta * underlying_change
        gamma_effect = 0.5 * gamma * (underlying_change ** 2)
        theta_effect = theta * days_to_expiry  # Already in year fraction
        vega_effect = vega * iv_change

        # Calculate predicted price
        predicted_price = base_price + delta_effect + gamma_effect + theta_effect + vega_effect

        # Ensure price doesn't go negative
        predicted_price = max(0, predicted_price)

        return jsonify({
            "status": "success",
            "predicted_price": predicted_price,
            "price_impact": {
                "delta_effect": delta_effect,
                "gamma_effect": gamma_effect,
                "theta_effect": theta_effect,
                "vega_effect": vega_effect
            }
        })
    except Exception as e:
        logging.error(f"Error calculating option price prediction: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/option-analysis', methods=['GET'])
def get_option_analysis():
    """Get detailed option analysis data for a specific option"""
    symbol = request.args.get('symbol')
    expiry = request.args.get('expiry')
    strike = request.args.get('strike')
    option_type = request.args.get('optionType')

    if not all([symbol, expiry, strike, option_type]):
        return jsonify({"error": "All parameters (symbol, expiry, strike, option_type) are required"}), 400

    try:
        with database_service._get_cursor() as cur:
            cur.execute("""
                SELECT 
                    display_time, oi, volume, price, pct_change,
                    vega, theta, gamma, delta, iv, pop
                FROM oi_volume_history
                WHERE symbol = %s 
                  AND expiry_date = %s 
                  AND strike_price = %s 
                  AND option_type = %s
                ORDER BY display_time
            """, (symbol, expiry, strike, option_type))

            results = cur.fetchall()
            data = [{
                'time': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                'oi': int(row[1]) if row[1] else 0,
                'volume': int(row[2]) if row[2] else 0,
                'price': float(row[3]) if row[3] else 0,
                'pct_change': float(row[4]) if row[4] else 0,
                'vega': float(row[5]) if row[5] else 0,
                'theta': float(row[6]) if row[6] else 0,
                'gamma': float(row[7]) if row[7] else 0,
                'delta': float(row[8]) if row[8] else 0,
                'iv': float(row[9]) if row[9] else 0,
                'pop': float(row[10]) if row[10] else 0
            } for row in results]

            return jsonify({
                "status": "success",
                "data": data,
                "symbol": symbol,
                "expiry": expiry,
                "strike": strike,
                "option_type": option_type
            })

    except Exception as e:
        logging.error(f"Error fetching option analysis: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def run_scanner_worker():
    """Background worker to run the scanner every hour."""
    while True:
        try:
            scanner_service.run_hourly_scanner()
            time.sleep(5400)  # Run every hour
        except Exception as e:
            print(f"Error in scanner worker: {e}")
            time.sleep(300)  # Retry after 5 minutes on error

def run_db_clearing_worker():
    """Background worker that runs during the configured window to clear old database entries."""
    last_clear_date = None
    ist = pytz.timezone('Asia/Kolkata')

    while True:
        try:
            now = datetime.now(ist)
            current_time = now.time()
            current_date = now.date()

            # Check if current time is within the DB clearing window and we haven't cleared today
            if (now.weekday() in Config.TRADING_DAYS and
                Config.DB_CLEARING_START <= current_time <= Config.DB_CLEARING_END and
                last_clear_date != current_date):

                print(f"{now}: Running database clearing operations...")
                database_service.clear_old_data()
                last_clear_date = current_date
                run_instrument_keys_worker()
                print(f"{now}: Database cleared successfully")

                # Sleep until next day after successful clearing
                sleep_seconds = (
                    (24 - now.hour - 1) * 3600 +
                    (60 - now.minute - 1) * 60 +
                    (60 - now.second)
                )
                time.sleep(sleep_seconds)
            else:
                # Outside clearing window, check every minute
                time.sleep(300)
        except Exception as e:
            print(f"Error in DB clearing worker: {e}")
            time.sleep(300)  # Sleep for 5 minutes on error before retrying


def run_background_workers():
    """Run all background workers in separate threads"""

    option_chain_thread = threading.Thread(target=run_option_chain_worker, daemon=True)
    oi_buildup_thread = threading.Thread(target=option_chain_service.run_analytics_worker, daemon=True)
    stock_data_thread = threading.Thread(target=run_stock_data_updater, daemon=True)
    scanner_thread = threading.Thread(target=run_scanner_worker, daemon=True)
    financials_thread = threading.Thread(target=run_financials_worker, daemon=True)
    db_clearing_thread = threading.Thread(target=run_db_clearing_worker, daemon=True)
    # Add new instrument keys worker
    instrument_keys_thread = threading.Thread(target=run_instrument_keys_worker, daemon=True)
    prev_close_thread = threading.Thread(target=run_prev_close_worker, daemon=True)

    upstox_feed_worker = UpstoxFeedWorker(database_service)
    upstox_feed_thread = threading.Thread(
        target=lambda: asyncio.run(upstox_feed_worker.run_feed()),
        daemon=True
    )

    #upstox_feed_thread.start()

    option_chain_thread.start()
    oi_buildup_thread.start()
    stock_data_thread.start()
    #financials_thread.start()
    #db_clearing_thread.start()
    #instrument_keys_thread.start()
    #prev_close_thread.start()
    print("Background workers started successfully")

    # Keep main thread alive
    while True:
        time.sleep(3600)

@app.route('/api/instrument-keys', methods=['GET'])
def get_instrument_keys():
    """API endpoint to fetch instrument keys"""
    try:
        symbol = request.args.get('symbol')
        instrument_type = request.args.get('type')
        limit = int(request.args.get('limit', 100))
        
        result = database_service.get_instrument_keys(symbol, instrument_type)
        
        # Apply limit after fetching
        if len(result) > limit:
            result = result[:limit]
        
        return jsonify({
            "status": "success",
            "count": len(result),
            "data": result
        })
    except Exception as e:
        logging.error(f"Error fetching instrument keys: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    if os.getenv('BACKGROUND_WORKER', 'false').lower() == 'true':
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        current_time = now.time()
        is_weekday = now.weekday() in Config.TRADING_DAYS
        
        if is_weekday and (Config.MARKET_OPEN <= current_time <= Config.MARKET_CLOSE):
            print("Market is open, starting background workers...")
            run_background_workers()
        elif is_weekday and (Config.POST_MARKET_START <= current_time <= Config.POST_MARKET_END):
            # Run financial data worker only during the post-market window
            print(f"Post-market window ({Config.POST_MARKET_START.strftime('%H:%M')}-{Config.POST_MARKET_END.strftime('%H:%M')}): Running financial data worker only...")
            run_financials_worker()
        else:
            if not is_weekday:
                print("Market closed (weekend)")
            else:
                print(f"Market closed (current time: {current_time})")

            # Sleep until next market open
            sleep_seconds = market_data_service.get_seconds_until_next_open()
            print(f"Sleeping for {sleep_seconds//3600}h {(sleep_seconds%3600)//60}m until next market open")
            time.sleep(sleep_seconds)
    else:
        print("Starting web service ONLY")
        port = int(os.environ.get("PORT", 10000))
        db = DatabaseService()
        if db.test_connection():
            print("✅ Database connection successful")

            # Test basic query
            with db._get_cursor() as cur:
                cur.execute("SELECT current_database()")
                print(f"Connected to database: {cur.fetchone()[0]}")
        else:
            print("❌ Database connection failed")
        app.run(host="0.0.0.0", port=port, debug=True)





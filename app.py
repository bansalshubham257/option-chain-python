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
    "https://bansalshubham257.github.io"
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
                fno_stocks = option_chain_service.get_fno_stocks_with_symbols()

                # Process 1d interval
                start_time = time.time()
                success_count = update_stocks_for_interval(fno_stocks, interval, stocks_per_worker, max_workers)
                total_time = time.time() - start_time
                
                print(f"✅ {interval} completed: {success_count} stocks in {total_time:.2f}s")
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
                time.sleep(60)
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

    option_chain_thread.start()
    oi_buildup_thread.start()
    stock_data_thread.start()
    #financials_thread.start()
    db_clearing_thread.start()
    print("Background workers started successfully")

    # Keep main thread alive
    while True:
        time.sleep(3600)

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
        app.run(host="0.0.0.0", port=port)


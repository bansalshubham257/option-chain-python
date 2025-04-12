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
from config import Config


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


# API Routes
@app.route("/stocks", methods=["GET"])
def get_all_stocks():
    return jsonify(stock_analysis_service.fetch_all_nse_stocks())

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

from flask_caching import Cache

cache = Cache(config={'CACHE_TYPE': 'simple'})
cache.init_app(app)

@app.route("/fetch_stocks", methods=["GET"])
@cache.cached(timeout=3600)  # Cache for 1 hour
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
            # Get OI extremes
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change, 
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'oi_analytics'
                ORDER BY 
                    CASE WHEN category = 'oi_gainer' THEN 0 ELSE 1 END,
                    ABS(oi_change) DESC
                LIMIT %s
            """, (limit * 2,))

            oi_results = {
                'oi_gainers': [],
                'oi_losers': []
            }

            for row in cur.fetchall():
                item = {
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]) if row[5] is not None else 0,
                    'timestamp': row[6]
                }
                if item['oi_change'] >= 0:
                    oi_results['oi_gainers'].append(item)
                else:
                    oi_results['oi_losers'].append(item)

            # Get volume extremes - NEW QUERY
            cur.execute("""
                SELECT symbol, strike, option_type as type,
                       absolute_oi, oi_change, volume_change, 
                       timestamp
                FROM fno_analytics
                WHERE analytics_type = 'buildup'
                AND volume_change IS NOT NULL
                ORDER BY ABS(volume_change) DESC
                LIMIT %s
            """, (limit * 2,))

            volume_data = []
            for row in cur.fetchall():
                item = {
                    'symbol': row[0],
                    'strike': float(row[1]) if row[1] else 0,
                    'type': row[2],
                    'oi': int(row[3]),
                    'oi_change': float(row[4]),
                    'volume_change': float(row[5]),
                    'timestamp': row[6]
                }
                volume_data.append(item)

            # Split volume data into gainers/losers
            volume_gainers = []
            volume_losers = []
            
            for item in volume_data:
                if item['volume_change'] >= 0:
                    volume_gainers.append(item)
                else:
                    volume_losers.append(item)
            
            # Sort gainers descending, losers ascending
            volume_gainers.sort(key=lambda x: -x['volume_change'])
            volume_losers.sort(key=lambda x: x['volume_change'])
            
            return jsonify({
                "status": "success",
                "data": {
                    "oi_gainers": oi_results['oi_gainers'],
                    "oi_losers": oi_results['oi_losers'],
                    "volume_gainers": volume_gainers[:limit],
                    "volume_losers": volume_losers[:limit]
                },
                "timestamp": datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
            })

    except Exception as e:
        logging.error(f"Error fetching OI extremes: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
def run_option_chain_worker():
    """Background worker for option chain processing"""
    option_chain_service.run_market_processing()

def run_background_workers():
    """Run all background workers in separate threads"""
    # Use threading for parallel execution
    market_data_thread = threading.Thread(target=run_market_data_worker, daemon=True)
    option_chain_thread = threading.Thread(target=run_option_chain_worker, daemon=True)
    oi_buildup_thread = threading.Thread(target=option_chain_service.run_analytics_worker, daemon=True)

    market_data_thread.start()
    option_chain_thread.start()
    oi_buildup_thread.start()
    print("Background workers started successfully")

    # Keep main thread alive
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    if os.getenv('BACKGROUND_WORKER', 'false').lower() == 'true':
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        current_time = now.time()
        is_weekday = now.weekday() < 5  #
        if is_weekday and (Config.MARKET_OPEN <= current_time <= Config.MARKET_CLOSE):
            print("Market is open, starting background workers...")
            run_background_workers()
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

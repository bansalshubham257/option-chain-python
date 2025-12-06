"""
Unified Whale Orders API Server
Combines raw data endpoints and aggregated dashboard analytics
Deploy independently on Railway - no dependencies on main app.py

Usage:
    python unified_api.py

Environment Variables:
    DATABASE_URL: PostgreSQL connection string (required)
    PORT: Server port (default: 5001)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import psycopg2
from contextlib import contextmanager
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway')

@contextmanager
def get_db_connection():
    """Get a database connection with automatic cleanup"""
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"❌ Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# RAW DATA ENDPOINTS (from whale_api.py)
# ============================================================================

@app.route('/successes', methods=['GET'])
def get_whale_successes():
    """
    Get successful whale orders from database

    Query Parameters:
        status: 'all', 'open', or 'done' (default: 'all')
        limit: number of records to return (default: 1000)
        offset: pagination offset (default: 0)

    Returns:
        JSON array of whale success records with PnL details
    """
    try:
        status = request.args.get('status', 'all')
        limit = int(request.args.get('limit', 1000))
        offset = int(request.args.get('offset', 0))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if status != 'all':
                    cur.execute("""
                        SELECT id, timestamp, instrument_key, symbol, strike, option_type, side, qty, price, ltp,
                               vol_diff, oi_diff, moneyness, pcr, oi_same, oi_opposite,
                               stock_current_price, stock_pct_change_today, live_ltp, live_pnl, live_pnl_pct,
                               last_live_update, status, progress_label, progress_history, max_return_high, 
                               max_return_low, stock_pct_at_exit
                        FROM whale_successes
                        WHERE status = %s
                        ORDER BY timestamp DESC
                        LIMIT %s OFFSET %s
                    """, (status, limit, offset))
                else:
                    cur.execute("""
                        SELECT id, timestamp, instrument_key, symbol, strike, option_type, side, qty, price, ltp,
                               vol_diff, oi_diff, moneyness, pcr, oi_same, oi_opposite,
                               stock_current_price, stock_pct_change_today, live_ltp, live_pnl, live_pnl_pct,
                               last_live_update, status, progress_label, progress_history, max_return_high, 
                               max_return_low, stock_pct_at_exit
                        FROM whale_successes
                        ORDER BY timestamp DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))

                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].isoformat() if row[1] else None,
                        'instrument_key': row[2],
                        'symbol': row[3],
                        'strike': float(row[4]) if row[4] else None,
                        'option_type': row[5],
                        'side': row[6],
                        'qty': row[7],
                        'price': float(row[8]),
                        'ltp': float(row[9]) if row[9] else None,
                        'vol_diff': row[10],
                        'oi_diff': row[11],
                        'moneyness': row[12],
                        'pcr': float(row[13]) if row[13] else None,
                        'oi_same': row[14],
                        'oi_opposite': row[15],
                        'stock_current_price': float(row[16]) if row[16] else None,
                        'stock_pct_change_today': float(row[17]) if row[17] else None,
                        'live_ltp': float(row[18]) if row[18] else None,
                        'live_pnl': float(row[19]) if row[19] else None,
                        'live_pnl_pct': float(row[20]) if row[20] else None,
                        'last_live_update': row[21].isoformat() if row[21] else None,
                        'status': row[22],
                        'progress_label': row[23],
                        'progress_history': row[24],
                        'max_return_high': float(row[25]) if row[25] else None,
                        'max_return_low': float(row[26]) if row[26] else None,
                        'stock_pct_at_exit': float(row[27]) if row[27] else None,
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching whale successes: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/entries', methods=['GET'])
def get_whale_entries():
    """
    Get current whale entries (live positions) from database

    Query Parameters:
        limit: number of records to return (default: 1000)
        offset: pagination offset (default: 0)

    Returns:
        JSON array of whale entry records (live orders)
    """
    try:
        limit = int(request.args.get('limit', 1000))
        offset = int(request.args.get('offset', 0))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, timestamp, instrument_key, symbol, option_type, side, qty, price, ltp,
                           moneyness, pcr, oi_same, oi_opposite, stock_current_price, stock_pct_change_today
                    FROM whale_entries
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))

                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].isoformat() if row[1] else None,
                        'instrument_key': row[2],
                        'symbol': row[3],
                        'option_type': row[4],
                        'side': row[5],
                        'qty': row[6],
                        'price': float(row[7]),
                        'ltp': float(row[8]) if row[8] else None,
                        'moneyness': row[9],
                        'pcr': float(row[10]) if row[10] else None,
                        'oi_same': row[11],
                        'oi_opposite': row[12],
                        'stock_current_price': float(row[13]) if row[13] else None,
                        'stock_pct_change_today': float(row[14]) if row[14] else None,
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching whale entries: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_whale_stats():
    """
    Get summary statistics for whale orders

    Returns:
        JSON object with:
        - total_successes: Total number of successful trades
        - open_positions: Number of open positions
        - done_positions: Number of closed/done positions
        - live_entries: Number of live whale entries
        - avg_pnl_pct: Average PnL percentage for closed positions
        - win_rate_pct: Win rate percentage of closed positions
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Total successes
                cur.execute("SELECT COUNT(*) FROM whale_successes")
                total_successes = cur.fetchone()[0]

                # Open positions
                cur.execute("SELECT COUNT(*) FROM whale_successes WHERE status = 'open'")
                open_positions = cur.fetchone()[0]

                # Closed positions
                cur.execute("SELECT COUNT(*) FROM whale_successes WHERE status = 'done'")
                done_positions = cur.fetchone()[0]

                # Live entries
                cur.execute("SELECT COUNT(*) FROM whale_entries")
                live_entries = cur.fetchone()[0]

                # Average PnL for closed positions
                cur.execute("""
                    SELECT AVG(live_pnl_pct) FROM whale_successes 
                    WHERE status = 'done' AND live_pnl_pct IS NOT NULL
                """)
                avg_pnl = cur.fetchone()[0]

                # Win rate
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes 
                    WHERE status = 'done' AND live_pnl_pct > 0
                """)
                winning_positions = cur.fetchone()[0]

                win_rate = (winning_positions / done_positions * 100) if done_positions > 0 else 0

        return jsonify({
            'status': 'success',
            'data': {
                'total_successes': total_successes,
                'open_positions': open_positions,
                'done_positions': done_positions,
                'live_entries': live_entries,
                'avg_pnl_pct': round(avg_pnl, 2) if avg_pnl else None,
                'win_rate_pct': round(win_rate, 2)
            }
        })
    except Exception as e:
        logging.error(f"Error fetching whale stats: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ============================================================================
# DASHBOARD ANALYTICS ENDPOINTS (from dashboard_api.py)
# ============================================================================

@app.route('/overview', methods=['GET'])
def get_dashboard_overview():
    """
    Get comprehensive dashboard overview with all key metrics

    Returns:
        JSON object with dashboard statistics and charts data
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Total stats
                cur.execute("SELECT COUNT(*) FROM whale_successes")
                total_successes = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_successes WHERE status = 'open'")
                open_positions = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_successes WHERE status = 'done'")
                done_positions = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_entries")
                live_entries = cur.fetchone()[0]

                # PnL statistics
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(live_pnl), 0) as total_pnl,
                        COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct,
                        COALESCE(MAX(live_pnl_pct), 0) as max_pnl_pct,
                        COALESCE(MIN(live_pnl_pct), 0) as min_pnl_pct
                    FROM whale_successes 
                    WHERE live_pnl IS NOT NULL
                """)
                pnl_row = cur.fetchone()
                total_pnl = float(pnl_row[0]) if pnl_row[0] else 0
                avg_pnl_pct = float(pnl_row[1]) if pnl_row[1] else 0
                max_pnl_pct = float(pnl_row[2]) if pnl_row[2] else 0
                min_pnl_pct = float(pnl_row[3]) if pnl_row[3] else 0

                # Win rate
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes 
                    WHERE status = 'done' AND live_pnl_pct > 0
                """)
                winning_positions = cur.fetchone()[0]
                win_rate = (winning_positions / done_positions * 100) if done_positions > 0 else 0

                # Today's trades
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes
                    WHERE DATE(timestamp) = CURRENT_DATE
                """)
                today_trades = cur.fetchone()[0]

                # Average fill time (in minutes)
                cur.execute("""
                    SELECT AVG(EXTRACT(EPOCH FROM (timestamp - created_at))/60)
                    FROM whale_successes
                    WHERE timestamp IS NOT NULL AND created_at IS NOT NULL
                """)
                avg_fill_time = cur.fetchone()[0]

                # Side distribution (BUY vs SELL)
                cur.execute("""
                    SELECT side, COUNT(*) as count FROM whale_successes
                    GROUP BY side
                """)
                side_dist = {}
                for row in cur.fetchall():
                    side_dist[row[0]] = row[1]

                # Moneyness distribution
                cur.execute("""
                    SELECT moneyness, COUNT(*) as count FROM whale_successes
                    WHERE moneyness IS NOT NULL
                    GROUP BY moneyness
                    ORDER BY count DESC
                """)
                moneyness_dist = {}
                for row in cur.fetchall():
                    moneyness_dist[row[0]] = row[1]

        return jsonify({
            'status': 'success',
            'data': {
                'summary': {
                    'total_successes': total_successes,
                    'open_positions': open_positions,
                    'done_positions': done_positions,
                    'live_entries': live_entries,
                    'today_trades': today_trades,
                },
                'performance': {
                    'total_pnl': round(total_pnl, 2),
                    'avg_pnl_pct': round(avg_pnl_pct, 2),
                    'max_pnl_pct': round(max_pnl_pct, 2),
                    'min_pnl_pct': round(min_pnl_pct, 2),
                    'win_rate_pct': round(win_rate, 2),
                    'avg_fill_time_minutes': round(avg_fill_time, 2) if avg_fill_time else None,
                },
                'distribution': {
                    'by_side': side_dist,
                    'by_moneyness': moneyness_dist,
                }
            }
        })
    except Exception as e:
        logging.error(f"Error fetching dashboard overview: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/success-table', methods=['GET'])
def get_success_table():
    """
    Get whale successes data formatted for dashboard table

    Query Parameters:
        status: 'all', 'open', 'done' (default: 'all')
        limit: records per page (default: 50)
        offset: pagination offset (default: 0)
        sort_by: 'timestamp', 'pnl', 'side', 'symbol' (default: 'timestamp')
        sort_order: 'asc', 'desc' (default: 'desc')

    Returns:
        JSON array with table data
    """
    try:
        status = request.args.get('status', 'all')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        sort_by = request.args.get('sort_by', 'timestamp')
        sort_order = request.args.get('sort_order', 'desc').upper()

        # Validate sort parameters
        valid_sorts = ['timestamp', 'live_pnl_pct', 'side', 'symbol']
        if sort_by not in valid_sorts:
            sort_by = 'timestamp'
        if sort_order not in ['ASC', 'DESC']:
            sort_order = 'DESC'

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                where_clause = "" if status == 'all' else f"WHERE status = '{status}'"
                order_clause = f"ORDER BY {sort_by} {sort_order}"

                query = f"""
                    SELECT 
                        id, timestamp, instrument_key, symbol, strike, option_type, 
                        side, qty, price, ltp, live_pnl, live_pnl_pct, 
                        status, progress_label, moneyness
                    FROM whale_successes
                    {where_clause}
                    {order_clause}
                    LIMIT %s OFFSET %s
                """

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

                # Get total count
                count_query = f"SELECT COUNT(*) FROM whale_successes {where_clause}"
                cur.execute(count_query)
                total_count = cur.fetchone()[0]

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None,
                        'instrument_key': row[2],
                        'symbol': row[3],
                        'strike': float(row[4]) if row[4] else None,
                        'option_type': row[5],
                        'side': row[6],
                        'qty': row[7],
                        'entry_price': float(row[8]),
                        'ltp': float(row[9]) if row[9] else None,
                        'pnl': float(row[10]) if row[10] else None,
                        'pnl_pct': float(row[11]) if row[11] else None,
                        'status': row[12],
                        'progress': row[13],
                        'moneyness': row[14],
                    })

        return jsonify({
            'status': 'success',
            'total': total_count,
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching success table: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/entries-table', methods=['GET'])
def get_entries_table():
    """
    Get whale entries data formatted for dashboard table

    Query Parameters:
        limit: records per page (default: 50)
        offset: pagination offset (default: 0)

    Returns:
        JSON array with live entries data
    """
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        id, timestamp, instrument_key, symbol, option_type, 
                        side, qty, price, ltp, moneyness, pcr
                    FROM whale_entries
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s
                """

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

                # Get total count
                cur.execute("SELECT COUNT(*) FROM whale_entries")
                total_count = cur.fetchone()[0]

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None,
                        'instrument_key': row[2],
                        'symbol': row[3],
                        'option_type': row[4],
                        'side': row[5],
                        'qty': row[6],
                        'price': float(row[7]),
                        'ltp': float(row[8]) if row[8] else None,
                        'moneyness': row[9],
                        'pcr': float(row[10]) if row[10] else None,
                    })

        return jsonify({
            'status': 'success',
            'total': total_count,
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching entries table: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/pnl-timeline', methods=['GET'])
def get_pnl_timeline():
    """
    Get PnL data over time for charts

    Query Parameters:
        days: number of days to look back (default: 7)
        interval: 'hourly', 'daily' (default: 'daily')

    Returns:
        JSON array with timeline data
    """
    try:
        days = int(request.args.get('days', 7))
        interval = request.args.get('interval', 'daily')

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if interval == 'hourly':
                    query = """
                        SELECT 
                            DATE_TRUNC('hour', timestamp) as time_bucket,
                            COUNT(*) as trade_count,
                            COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct,
                            COALESCE(SUM(live_pnl), 0) as total_pnl,
                            COALESCE(MAX(live_pnl_pct), 0) as max_pnl_pct
                        FROM whale_successes
                        WHERE timestamp >= NOW() - INTERVAL '%s days'
                        GROUP BY DATE_TRUNC('hour', timestamp)
                        ORDER BY time_bucket DESC
                    """
                else:  # daily
                    query = """
                        SELECT 
                            DATE(timestamp) as time_bucket,
                            COUNT(*) as trade_count,
                            COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct,
                            COALESCE(SUM(live_pnl), 0) as total_pnl,
                            COALESCE(MAX(live_pnl_pct), 0) as max_pnl_pct
                        FROM whale_successes
                        WHERE timestamp >= NOW() - INTERVAL '%s days'
                        GROUP BY DATE(timestamp)
                        ORDER BY time_bucket DESC
                    """

                cur.execute(query, (days,))
                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'time': row[0].isoformat() if row[0] else None,
                        'trade_count': row[1],
                        'avg_pnl_pct': round(float(row[2]), 2),
                        'total_pnl': round(float(row[3]), 2),
                        'max_pnl_pct': round(float(row[4]), 2),
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching PnL timeline: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/symbol-performance', methods=['GET'])
def get_symbol_performance():
    """
    Get performance metrics per symbol

    Returns:
        JSON array with per-symbol statistics
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        symbol,
                        COUNT(*) as trade_count,
                        SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sell_count,
                        COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct,
                        COALESCE(MAX(live_pnl_pct), 0) as max_pnl_pct,
                        COALESCE(MIN(live_pnl_pct), 0) as min_pnl_pct,
                        COALESCE(SUM(live_pnl), 0) as total_pnl,
                        SUM(CASE WHEN live_pnl_pct > 0 THEN 1 ELSE 0 END)::float / 
                            NULLIF(COUNT(*), 0) * 100 as win_rate_pct
                    FROM whale_successes
                    WHERE symbol IS NOT NULL
                    GROUP BY symbol
                    ORDER BY trade_count DESC
                """

                cur.execute(query)
                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'symbol': row[0],
                        'trade_count': row[1],
                        'buy_count': row[2],
                        'sell_count': row[3],
                        'avg_pnl_pct': round(float(row[4]), 2),
                        'max_pnl_pct': round(float(row[5]), 2),
                        'min_pnl_pct': round(float(row[6]), 2),
                        'total_pnl': round(float(row[7]), 2),
                        'win_rate_pct': round(float(row[8]), 2) if row[8] else 0,
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching symbol performance: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/progress-distribution', methods=['GET'])
def get_progress_distribution():
    """
    Get distribution of trades by progress level

    Returns:
        JSON with progress distribution for open positions
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        progress_label,
                        COUNT(*) as count,
                        COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct
                    FROM whale_successes
                    WHERE status = 'open' AND progress_label IS NOT NULL
                    GROUP BY progress_label
                    ORDER BY 
                        CASE progress_label
                            WHEN '-75%' THEN 1
                            WHEN '-50%' THEN 2
                            WHEN '-25%' THEN 3
                            WHEN 'running' THEN 4
                            WHEN '+25%' THEN 5
                            WHEN '+50%' THEN 6
                            WHEN '+75%' THEN 7
                            WHEN 'done' THEN 8
                            ELSE 9
                        END
                """

                cur.execute(query)
                rows = cur.fetchall()

                data = {}
                for row in rows:
                    data[row[0]] = {
                        'count': row[1],
                        'avg_pnl_pct': round(float(row[2]), 2),
                    }

        return jsonify({
            'status': 'success',
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching progress distribution: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/top-performers', methods=['GET'])
def get_top_performers():
    """
    Get top performing trades

    Query Parameters:
        limit: number of top trades to return (default: 10)

    Returns:
        JSON array with top trades by PnL
    """
    try:
        limit = int(request.args.get('limit', 10))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        id, timestamp, symbol, strike, option_type, side, qty, 
                        price, ltp, live_pnl, live_pnl_pct, progress_label
                    FROM whale_successes
                    WHERE live_pnl_pct IS NOT NULL
                    ORDER BY live_pnl_pct DESC
                    LIMIT %s
                """

                cur.execute(query, (limit,))
                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None,
                        'symbol': row[2],
                        'strike': float(row[3]) if row[3] else None,
                        'option_type': row[4],
                        'side': row[5],
                        'qty': row[6],
                        'entry_price': float(row[7]),
                        'ltp': float(row[8]) if row[8] else None,
                        'pnl': float(row[9]) if row[9] else None,
                        'pnl_pct': float(row[10]) if row[10] else None,
                        'progress': row[11],
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching top performers: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/bottom-performers', methods=['GET'])
def get_bottom_performers():
    """
    Get worst performing trades (highest losses)

    Query Parameters:
        limit: number of bottom trades to return (default: 10)

    Returns:
        JSON array with bottom trades by PnL
    """
    try:
        limit = int(request.args.get('limit', 10))

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        id, timestamp, symbol, strike, option_type, side, qty, 
                        price, ltp, live_pnl, live_pnl_pct, progress_label
                    FROM whale_successes
                    WHERE live_pnl_pct IS NOT NULL
                    ORDER BY live_pnl_pct ASC
                    LIMIT %s
                """

                cur.execute(query, (limit,))
                rows = cur.fetchall()

                data = []
                for row in rows:
                    data.append({
                        'id': row[0],
                        'timestamp': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None,
                        'symbol': row[2],
                        'strike': float(row[3]) if row[3] else None,
                        'option_type': row[4],
                        'side': row[5],
                        'qty': row[6],
                        'entry_price': float(row[7]),
                        'ltp': float(row[8]) if row[8] else None,
                        'pnl': float(row[9]) if row[9] else None,
                        'pnl_pct': float(row[10]) if row[10] else None,
                        'progress': row[11],
                    })

        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data
        })
    except Exception as e:
        logging.error(f"Error fetching bottom performers: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.route('/', methods=['GET'])
def root():
    """API information and endpoint listing"""
    return jsonify({
        'name': 'Unified Whale Orders API',
        'version': '2.0.0',
        'status': 'running',
        'description': 'Combined API for raw whale orders data and advanced dashboard analytics',
        'endpoints': {
            'raw_data': {
                'successes': 'GET /successes - Get successful whale orders',
                'entries': 'GET /entries - Get current whale entries (live positions)',
                'stats': 'GET /stats - Get summary statistics',
            },
            'dashboard_analytics': {
                'overview': 'GET /overview - Comprehensive dashboard overview',
                'success_table': 'GET /success-table - Formatted success data for tables',
                'entries_table': 'GET /entries-table - Formatted entries data for tables',
                'pnl_timeline': 'GET /pnl-timeline - PnL data over time for charts',
                'symbol_performance': 'GET /symbol-performance - Per-symbol statistics',
                'progress_distribution': 'GET /progress-distribution - Progress level distribution',
                'top_performers': 'GET /top-performers - Top performing trades',
                'bottom_performers': 'GET /bottom-performers - Worst performing trades',
            },
            'utility': {
                'health': 'GET /health - Health check',
                'root': 'GET / - This endpoint',
            }
        }
    })


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    debug = os.getenv('FLASK_ENV', 'production') == 'development'

    logger.info(f"🚀 Unified Whale Orders API Server Starting...")
    logger.info(f"📊 Port: {port}")
    logger.info(f"🗄️ Database: {DATABASE_URL[:50]}...")
    logger.info(f"🔗 Base URL: http://localhost:{port}")
    logger.info(f"📝 Features:")
    logger.info(f"   - Raw data endpoints: /successes, /entries, /stats")
    logger.info(f"   - Dashboard analytics: /overview, /success-table, /pnl-timeline, /symbol-performance, /progress-distribution, /top-performers, /bottom-performers")

    # Only run development server if not using Gunicorn
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=False)

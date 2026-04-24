"""
Unified Whale Orders API Server
Combines raw data endpoints and aggregated dashboard analytics
Deploy independently on Railway - no dependencies on main app.py

Usage:
    python unified_api.py

Environment Variables:
    DATABASE_URL: PostgreSQL connection string (required)
    PORT: Server port (default: 5002)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import psycopg2
from contextlib import contextmanager
import os
from datetime import datetime
import requests
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway')

# AngelOne API Configuration
ANGELONE_API_KEY     = os.getenv('ANGELONE_API_KEY',   '')
ANGELONE_SECRET_KEY  = os.getenv('ANGELONE_SECRET_KEY',   '')
ANGELONE_CLIENT_ID   = os.getenv('ANGELONE_CLIENT_ID',   '')
ANGELONE_PASSWORD    = os.getenv('ANGELONE_PASSWORD',    '')
ANGELONE_TOTP_SECRET = os.getenv('ANGELONE_TOTP_SECRET', '')
BASE_URL_ANGEL       = "https://apiconnect.angelbroking.com"
MAX_TOKENS_PER_CALL  = 50
_angel_token         = None   # cached JWT token

def get_angel_headers(with_auth=False):
    """Build AngelOne request headers."""
    h = {
        'Content-Type':     'application/json',
        'Accept':           'application/json',
        'X-UserType':       'USER',
        'X-SourceID':       'WEB',
        'X-ClientLocalIP':  '127.0.0.1',
        'X-ClientPublicIP': '127.0.0.1',
        'X-MACAddress':     '00:00:00:00:00:00',
        'X-PrivateKey':     ANGELONE_API_KEY,
    }
    if with_auth and _angel_token:
        h['Authorization'] = f'Bearer {_angel_token}'
    return h


def get_access_token():
    """Login to AngelOne and return JWT token (cached in module-level _angel_token)."""
    global _angel_token
    if _angel_token:
        return _angel_token
    client_id   = ANGELONE_CLIENT_ID
    password    = ANGELONE_PASSWORD
    totp_secret = ANGELONE_TOTP_SECRET
    if not client_id or not password:
        logging.error("Set ANGELONE_CLIENT_ID and ANGELONE_PASSWORD env vars")
        return None
    totp_code = ""
    if totp_secret:
        try:
            import pyotp
            totp_code = pyotp.TOTP(totp_secret).now()
        except Exception as e:
            logging.warning(f"TOTP generation failed: {e}")
    try:
        resp = requests.post(
            f"{BASE_URL_ANGEL}/rest/auth/angelbroking/user/v1/loginByPassword",
            headers=get_angel_headers(),
            json={"clientcode": client_id, "password": password, "totp": totp_code},
            timeout=10
        )
        data = resp.json()
        if data.get('status'):
            _angel_token = data['data']['jwtToken']
            logging.info(f"AngelOne login OK. Token: {_angel_token[:30]}...")
            return _angel_token
        else:
            logging.error(f"AngelOne login failed: {data.get('message')}")
            return None
    except Exception as e:
        logging.error(f"AngelOne login error: {e}")
        return None

def chunked(lst, size):
    """Split list into chunks of given size"""
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def fetch_live_ltp_from_angelone(instrument_tokens):
    """
    Fetch live LTP from AngelOne API for given instrument tokens.

    Accepts:
      - AngelOne style keys: 'NFO:12345'  (exchange:token)
      - Bare numeric tokens: '12345'      (assumes NFO)
      - Legacy Upstox style: 'NSE_FO|12345' (mapped to NFO:12345)

    Returns:
        dict: {original_key: ltp_value, 'EXCHANGE:token': ltp_value, ...}
    """
    if not instrument_tokens:
        return {}

    token = get_access_token()
    if not token:
        logging.error("AngelOne access token not available")
        return {}

    import re
    exchange_map = {}   # exchange -> [token_str, ...]
    key_lookup   = {}   # 'EXCHANGE:token' -> original key

    upstox_to_angel = {
        'NSE_FO': 'NFO', 'BSE_FO': 'BFO', 'MCX_FO': 'MCX',
        'NSE_EQ': 'NSE', 'BSE_EQ': 'BSE'
    }

    for key in instrument_tokens:
        key = str(key).strip()
        if ':' in key:
            exch, tok = key.split(':', 1)
            exchange_map.setdefault(exch, []).append(tok)
            key_lookup[f"{exch}:{tok}"] = key
        elif re.match(r'^\d+$', key):
            exchange_map.setdefault('NFO', []).append(key)
            key_lookup[f"NFO:{key}"] = key
        elif '|' in key:
            exch, tok = key.split('|', 1)
            exch = upstox_to_angel.get(exch, exch)
            exchange_map.setdefault(exch, []).append(tok)
            key_lookup[f"{exch}:{tok}"] = key

    ltp_map = {}
    for exch, tokens in exchange_map.items():
        for chunk in chunked(tokens, MAX_TOKENS_PER_CALL):
            try:
                resp = requests.post(
                    f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
                    headers=get_angel_headers(with_auth=True),
                    json={"mode": "LTP", "exchangeTokens": {exch: chunk}},
                    timeout=10
                )
                data = resp.json()
                if data.get('status'):
                    for item in data.get('data', {}).get('fetched', []):
                        tok     = str(item.get('symbolToken', ''))
                        ltp     = float(item.get('ltp', 0) or 0)
                        api_key = f"{exch}:{tok}"
                        orig    = key_lookup.get(api_key, api_key)
                        ltp_map[orig]    = ltp
                        ltp_map[api_key] = ltp
                else:
                    logging.warning(f"AngelOne LTP error ({exch}): {data.get('message', '')}")
                time.sleep(0.05)
            except requests.Timeout:
                logging.warning(f"Timeout fetching LTP for chunk ({exch})")
            except Exception as e:
                logging.error(f"Error fetching LTP ({exch}): {e}")

    logging.info(f"Fetched live LTP for {len(ltp_map)} instruments from AngelOne")
    return ltp_map


# Keep old name as alias for backward compat
fetch_live_ltp_from_upstox = fetch_live_ltp_from_angelone

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
def get_whale_successes_2():
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
                        FROM whale_successes_2
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
                        FROM whale_successes_2
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
def get_whale_entries_2():
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
                    FROM whale_entries_2
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
                cur.execute("SELECT COUNT(*) FROM whale_successes_2")
                total_successes = cur.fetchone()[0]

                # Open positions
                cur.execute("SELECT COUNT(*) FROM whale_successes_2 WHERE status = 'open'")
                open_positions = cur.fetchone()[0]

                # Closed positions
                cur.execute("SELECT COUNT(*) FROM whale_successes_2 WHERE status = 'done'")
                done_positions = cur.fetchone()[0]

                # Live entries
                cur.execute("SELECT COUNT(*) FROM whale_entries_2")
                live_entries = cur.fetchone()[0]

                # Average PnL for closed positions
                cur.execute("""
                    SELECT AVG(live_pnl_pct) FROM whale_successes_2 
                    WHERE status = 'done' AND live_pnl_pct IS NOT NULL
                """)
                avg_pnl = cur.fetchone()[0]

                # Win rate
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes_2 
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
                cur.execute("SELECT COUNT(*) FROM whale_successes_2")
                total_successes = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_successes_2 WHERE status = 'open'")
                open_positions = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_successes_2 WHERE status = 'done'")
                done_positions = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM whale_entries_2")
                live_entries = cur.fetchone()[0]

                # PnL statistics
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(live_pnl), 0) as total_pnl,
                        COALESCE(AVG(live_pnl_pct), 0) as avg_pnl_pct,
                        COALESCE(MAX(live_pnl_pct), 0) as max_pnl_pct,
                        COALESCE(MIN(live_pnl_pct), 0) as min_pnl_pct
                    FROM whale_successes_2 
                    WHERE live_pnl IS NOT NULL
                """)
                pnl_row = cur.fetchone()
                total_pnl = float(pnl_row[0]) if pnl_row[0] else 0
                avg_pnl_pct = float(pnl_row[1]) if pnl_row[1] else 0
                max_pnl_pct = float(pnl_row[2]) if pnl_row[2] else 0
                min_pnl_pct = float(pnl_row[3]) if pnl_row[3] else 0

                # Win rate
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes_2 
                    WHERE status = 'done' AND live_pnl_pct > 0
                """)
                winning_positions = cur.fetchone()[0]
                win_rate = (winning_positions / done_positions * 100) if done_positions > 0 else 0

                # Today's trades
                cur.execute("""
                    SELECT COUNT(*) FROM whale_successes_2
                    WHERE DATE(timestamp) = CURRENT_DATE
                """)
                today_trades = cur.fetchone()[0]

                # Average fill time (in minutes)
                cur.execute("""
                    SELECT AVG(EXTRACT(EPOCH FROM (timestamp - created_at))/60)
                    FROM whale_successes_2
                    WHERE timestamp IS NOT NULL AND created_at IS NOT NULL
                """)
                avg_fill_time = cur.fetchone()[0]

                # Side distribution (BUY vs SELL)
                cur.execute("""
                    SELECT side, COUNT(*) as count FROM whale_successes_2
                    GROUP BY side
                """)
                side_dist = {}
                for row in cur.fetchall():
                    side_dist[row[0]] = row[1]

                # Moneyness distribution
                cur.execute("""
                    SELECT moneyness, COUNT(*) as count FROM whale_successes_2
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
                    FROM whale_successes_2
                    {where_clause}
                    {order_clause}
                    LIMIT %s OFFSET %s
                """

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

                # Get total count
                count_query = f"SELECT COUNT(*) FROM whale_successes_2 {where_clause}"
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
                    FROM whale_entries_2
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s
                """

                cur.execute(query, (limit, offset))
                rows = cur.fetchall()

                # Get total count
                cur.execute("SELECT COUNT(*) FROM whale_entries_2")
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
                        FROM whale_successes_2
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
                        FROM whale_successes_2
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
                    FROM whale_successes_2
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
                    FROM whale_successes_2
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
                    FROM whale_successes_2
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
                    FROM whale_successes_2
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
# LIVE LTP UPDATE ENDPOINTS
# ============================================================================

@app.route('/update-live-ltp', methods=['GET'])
def update_live_ltp():
    """
    Fetch live LTP from AngelOne API for all whale_successes_2 records and return updated data.
    
    This endpoint:
    1. Fetches all instrument tokens from whale_successes_2 table
    2. Calls Upstox API to get live LTP for those instruments
    3. Calculates live PnL based on latest LTP
    4. Returns dashboard data with live LTP and PnL
    
    Query Parameters:
        status: 'all', 'open', or 'done' (default: 'all')
        limit: number of records to return (default: 1000)
    
    Returns:
        JSON with live LTP data and calculated PnL for dashboard display
    """
    try:
        status = request.args.get('status', 'all')
        limit = int(request.args.get('limit', 1000))
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Fetch all records with their instrument tokens and entry details
                if status != 'all':
                    cur.execute("""
                        SELECT id, instrument_token, symbol, option_type, side, qty, price, status, 
                               progress_label, max_return_high, max_return_low
                        FROM whale_successes_2
                        WHERE status = %s AND instrument_token IS NOT NULL
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (status, limit))
                else:
                    cur.execute("""
                        SELECT id, instrument_token, symbol, option_type, side, qty, price, status, 
                               progress_label, max_return_high, max_return_low
                        FROM whale_successes_2
                        WHERE instrument_token IS NOT NULL
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (limit,))
                
                rows = cur.fetchall()
        
        if not rows:
            return jsonify({
                'status': 'success',
                'count': 0,
                'data': [],
                'message': 'No records with instrument tokens found'
            })
        
        # Extract instrument tokens and fetch live LTP
        instrument_tokens = [str(row[1]) for row in rows if row[1]]
        logging.info(f"📊 Fetching live LTP for {len(instrument_tokens)} instruments")
        
        ltp_map = fetch_live_ltp_from_angelone(instrument_tokens)
        
        if not ltp_map:
            logging.warning("⚠️ No LTP data received from AngelOne API")
            return jsonify({
                'status': 'error',
                'message': 'Failed to fetch LTP from AngelOne API',
                'count': 0,
                'data': []
            }), 500
        
        # Build response with live LTP and calculated PnL
        data = []
        updated_count = 0
        
        for row in rows:
            record_id, instrument_token, symbol, option_type, side, qty, entry_price, rec_status, progress_label, max_high, max_low = row
            
            # Get live LTP
            live_ltp = ltp_map.get(instrument_token)
            
            if live_ltp is None:
                logging.debug(f"⚠️ No LTP found for token: {instrument_token}")
                continue
            
            if entry_price is None or entry_price == 0:
                logging.debug(f"⚠️ Invalid entry price for {instrument_token}")
                continue
            
            try:
                entry_price = float(entry_price)
                qty = float(qty) if qty else 1
                live_ltp = float(live_ltp)
            except (ValueError, TypeError):
                continue
            
            # Calculate live PnL
            pnl_per_unit = live_ltp - entry_price
            pnl_value = pnl_per_unit * qty
            pnl_pct = (pnl_per_unit / entry_price) * 100 if entry_price != 0 else 0
            
            # Determine progress label based on live PnL
            if pnl_pct >= 94:
                live_progress = "done"
            elif pnl_pct >= 75:
                live_progress = "+75%"
            elif pnl_pct >= 50:
                live_progress = "+50%"
            elif pnl_pct >= 25:
                live_progress = "+25%"
            elif pnl_pct >= -25:
                live_progress = "running"
            elif pnl_pct >= -50:
                live_progress = "-25%"
            elif pnl_pct >= -75:
                live_progress = "-50%"
            else:
                live_progress = "-75%"
            
            data.append({
                'id': record_id,
                'instrument_token': instrument_token,
                'symbol': symbol,
                'option_type': option_type,
                'side': side,
                'qty': qty,
                'entry_price': round(float(entry_price), 2),
                'live_ltp': round(live_ltp, 2),
                'live_pnl': round(pnl_value, 2),
                'live_pnl_pct': round(pnl_pct, 2),
                'status': rec_status,
                'progress_label': progress_label,
                'live_progress': live_progress,
                'max_return_high': float(max_high) if max_high else None,
                'max_return_low': float(max_low) if max_low else None,
                'timestamp': datetime.now().isoformat()
            })
            updated_count += 1
        
        logging.info(f"✅ Updated live LTP for {updated_count} positions")
        
        return jsonify({
            'status': 'success',
            'count': len(data),
            'updated_count': updated_count,
            'message': f'Live LTP fetched for {updated_count} instruments',
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        logging.error(f"Error updating live LTP: {str(e)}")
        import traceback
        traceback.print_exc()
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
        'name': 'Unified Whale Orders API v2 (AngelOne)',
        'version': '2.1.0',
        'status': 'running',
        'description': 'Combined API v2 — AngelOne edition (tables: whale_entries_2 / whale_successes_2)',
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
            'live_data': {
                'update_live_ltp': 'GET /update-live-ltp - Fetch live LTP from AngelOne and update dashboard with latest prices & PnL',
            },
            'utility': {
                'health': 'GET /health - Health check',
                'root': 'GET / - This endpoint',
            }
        }
    })


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5002))
    debug = os.getenv('FLASK_ENV', 'production') == 'development'

    logger.info(f"🚀 Unified Whale Orders API v2 Server Starting (AngelOne)...")
    logger.info(f"📊 Port: {port}")
    logger.info(f"🗄️ Database: {DATABASE_URL[:50]}...")
    logger.info(f"🔗 Base URL: http://localhost:{port}")
    logger.info(f"📝 Features:")
    logger.info(f"   - Raw data endpoints: /successes, /entries, /stats")
    logger.info(f"   - Dashboard analytics: /overview, /success-table, /pnl-timeline, /symbol-performance, /progress-distribution, /top-performers, /bottom-performers")
    logger.info(f"   - Live data: /update-live-ltp (fetch live LTP from AngelOne API)")

    # Only run development server if not using Gunicorn
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=False)

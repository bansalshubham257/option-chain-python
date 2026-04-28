import os
import time
import re
import pandas as pd
import requests
import yfinance as yf
import psycopg2
import pyotp
from contextlib import contextmanager
from datetime import datetime, timedelta
import pytz

from services.database import DatabaseService

# ==== ANGELONE CONFIG ====
ANGELONE_API_KEY     = os.getenv('ANGELONE_API_KEY',   '')
ANGELONE_SECRET_KEY  = os.getenv('ANGELONE_SECRET_KEY',   '')
ANGELONE_CLIENT_ID   = os.getenv('ANGELONE_CLIENT_ID',   '')
ANGELONE_PASSWORD    = os.getenv('ANGELONE_PASSWORD',    '')
ANGELONE_TOTP_SECRET = os.getenv('ANGELONE_TOTP_SECRET', '')

BASE_URL_ANGEL  = "https://apiconnect.angelbroking.com"
INSTRUMENT_URL  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

MAX_TOKENS_PER_CALL = 50

# Database
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway'
)

# Global AngelOne JWT token
ACCESS_TOKEN = None

db = DatabaseService()


# ===================== ANGELONE AUTH =====================
def get_angel_headers(with_auth=False):
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
    if with_auth and ACCESS_TOKEN:
        h['Authorization'] = f'Bearer {ACCESS_TOKEN}'
    return h


def login_angelone():
    """Login to AngelOne SmartAPI and refresh global ACCESS_TOKEN."""
    global ACCESS_TOKEN
    client_id   = ANGELONE_CLIENT_ID
    password    = ANGELONE_PASSWORD
    totp_secret = ANGELONE_TOTP_SECRET

    if not client_id or not password:
        print("Set env vars: ANGELONE_CLIENT_ID, ANGELONE_PASSWORD (optionally ANGELONE_TOTP_SECRET)")
        return None

    totp_code = ""
    if totp_secret:
        try:
            totp_code = pyotp.TOTP(totp_secret).now()
        except Exception as e:
            print(f"TOTP generation failed: {e}")

    try:
        resp = requests.post(
            f"{BASE_URL_ANGEL}/rest/auth/angelbroking/user/v1/loginByPassword",
            headers=get_angel_headers(),
            json={"clientcode": client_id, "password": password, "totp": totp_code},
            timeout=10
        )
        data = resp.json()
        if data.get('status'):
            ACCESS_TOKEN = data['data']['jwtToken']
            print(f"AngelOne login successful. Token: {ACCESS_TOKEN[:30]}...")
            return ACCESS_TOKEN
        else:
            print(f"AngelOne login failed: {data.get('message', 'Unknown')}")
            return None
    except Exception as e:
        print(f"AngelOne login error: {e}")
        return None


# ================= DATABASE HELPERS =================
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def create_whale_tables():
    """Create whale_entries_2, whale_successes_2, and symbol_instrument_mapping tables if they don't exist."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS whale_entries_2 (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    instrument_key VARCHAR(100) NOT NULL,
                    instrument_token VARCHAR(100),
                    symbol VARCHAR(50) NOT NULL,
                    option_type CHAR(2),
                    side VARCHAR(10) NOT NULL,
                    qty INTEGER NOT NULL,
                    price DECIMAL(10, 4) NOT NULL,
                    ltp DECIMAL(10, 4),
                    moneyness VARCHAR(20),
                    pcr DECIMAL(10, 4),
                    oi_same INTEGER,
                    oi_opposite INTEGER,
                    stock_current_price DECIMAL(10, 4),
                    stock_pct_change_today DECIMAL(10, 4),
                    unique_key VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_unique_key ON whale_entries_2(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_timestamp ON whale_entries_2(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_symbol ON whale_entries_2(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_instrument_token ON whale_entries_2(instrument_token);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS whale_successes_2 (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    instrument_key VARCHAR(100) NOT NULL,
                    instrument_token VARCHAR(100),
                    symbol VARCHAR(50) NOT NULL,
                    strike DECIMAL(10, 2),
                    option_type CHAR(2),
                    side VARCHAR(10) NOT NULL,
                    qty INTEGER NOT NULL,
                    price DECIMAL(10, 4) NOT NULL,
                    ltp DECIMAL(10, 4),
                    vol_diff INTEGER,
                    oi_diff INTEGER,
                    moneyness VARCHAR(20),
                    pcr DECIMAL(10, 4),
                    oi_same INTEGER,
                    oi_opposite INTEGER,
                    stock_current_price DECIMAL(10, 4),
                    stock_pct_change_today DECIMAL(10, 4),
                    live_ltp DECIMAL(10, 4),
                    live_pnl DECIMAL(12, 4),
                    live_pnl_pct DECIMAL(10, 4),
                    last_live_update TIMESTAMPTZ,
                    status VARCHAR(20) DEFAULT 'open',
                    progress_label VARCHAR(20),
                    progress_history TEXT,
                    max_return_high DECIMAL(10, 4),
                    max_return_low DECIMAL(10, 4),
                    stock_pct_at_exit DECIMAL(10, 4),
                    unique_key VARCHAR(100) UNIQUE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_unique_key ON whale_successes_2(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_timestamp ON whale_successes_2(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_symbol ON whale_successes_2(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_status ON whale_successes_2(status);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_instrument_token ON whale_successes_2(instrument_token);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS symbol_instrument_mapping (
                    id BIGSERIAL PRIMARY KEY,
                    instrument_key VARCHAR(100) NOT NULL UNIQUE,
                    trading_symbol VARCHAR(100) NOT NULL,
                    symbol VARCHAR(50),
                    exchange VARCHAR(20),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_symbol_mapping_instrument_key ON symbol_instrument_mapping(instrument_key);
                CREATE INDEX IF NOT EXISTS idx_symbol_mapping_trading_symbol ON symbol_instrument_mapping(trading_symbol);
            """)
    print("All tables created/verified")


# ===================== HELPERS =====================
def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def extract_underlying_from_symbol(symbol):
    """
    Extract underlying from instrument_key or trading_symbol.
    Handles AngelOne keys like 'NFO:12345' (uses ScripMaster name lookup),
    and legacy 'NSE_FO:VEDL25DEC520CE' style keys.
    """
    if not symbol:
        return None
    try:
        # AngelOne token key: 'NFO:12345' — underlying not parseable from key alone
        # Return the token as-is for cache key purposes
        if re.match(r'^[A-Z]+:\d+$', str(symbol)):
            return symbol  # Use full key as cache key

        # Legacy NSE_FO:SYMBOL style
        if ':' in symbol:
            symbol = symbol.split(':')[1]

        if symbol.endswith('CE') or symbol.endswith('PE'):
            symbol = symbol[:-2]

        match = re.match(r'^([A-Z&]+)', symbol)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


# ===================== ANGELONE MARKET DATA =====================
def get_daily_pct_change_for_underlying(instrument_key):
    """
    Get today's % price change for the underlying of an instrument.
    For NIFTY/SENSEX uses AngelOne LTP API.
    For stocks uses yfinance.
    instrument_key: 'NFO:12345' or 'NSE_FO:SYMBOL' style
    Returns: (current_price, pct_change)
    """
    underlying = extract_underlying_from_symbol(instrument_key)
    if not underlying:
        return None, None

    # For AngelOne token keys, we don't know the underlying from the key alone.
    # Return None and let the caller handle via symbol column.
    if re.match(r'^[A-Z]+:\d+$', str(instrument_key)):
        return None, None

    if underlying == "NIFTY":
        try:
            resp = requests.post(
                f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
                headers=get_angel_headers(with_auth=True),
                json={"mode": "FULL", "exchangeTokens": {"NSE": ["26000"]}},
                timeout=10
            )
            data = resp.json()
            if data.get('status'):
                items = data.get('data', {}).get('fetched', [])
                if items:
                    item    = items[0]
                    current = float(item.get('ltp', 0) or 0)
                    open_p  = float(item.get('open', 0) or 0)
                    if current > 0 and open_p > 0:
                        return current, round((current - open_p) / open_p * 100, 2)
        except Exception:
            pass
        return None, None

    elif underlying == "SENSEX":
        try:
            resp = requests.post(
                f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
                headers=get_angel_headers(with_auth=True),
                json={"mode": "FULL", "exchangeTokens": {"BSE": ["1"]}},
                timeout=10
            )
            data = resp.json()
            if data.get('status'):
                items = data.get('data', {}).get('fetched', [])
                if items:
                    item    = items[0]
                    current = float(item.get('ltp', 0) or 0)
                    open_p  = float(item.get('open', 0) or 0)
                    if current > 0 and open_p > 0:
                        return current, round((current - open_p) / open_p * 100, 2)
        except Exception:
            pass
        return None, None

    else:
        try:
            hist = yf.Ticker(underlying + ".NS").history(period="1d", interval="1m")
            if hist is not None and not hist.empty:
                current_price = float(hist["Close"].iloc[-1])
                open_price    = float(hist["Open"].iloc[0])
                if open_price > 0:
                    return current_price, round((current_price - open_price) / open_price * 100, 2)
        except Exception:
            pass
        return None, None


def fetch_ltp_for_tokens(instrument_keys):
    """
    Fetch live LTP from AngelOne for a list of instrument keys.

    Accepts both:
      - AngelOne style: 'NFO:12345'  (exchange:token)
      - Numeric token only: '12345'  (assumes NFO exchange)

    Returns: dict { 'NFO:12345': ltp_float, ... }
    """
    if not instrument_keys:
        return {}

    # Group by exchange
    exchange_map = {}   # exchange -> [token, ...]
    key_lookup   = {}   # 'EXCHANGE:token' -> original key (for result mapping)

    for key in instrument_keys:
        key = str(key).strip()
        if ':' in key:
            exch, token = key.split(':', 1)
            exchange_map.setdefault(exch, []).append(token)
            key_lookup[f"{exch}:{token}"] = key
        elif re.match(r'^\d+$', key):
            # Bare numeric token — assume NFO
            exchange_map.setdefault('NFO', []).append(key)
            key_lookup[f"NFO:{key}"] = key
        else:
            # Legacy NSE_FO|token style
            if '|' in key:
                exch, token = key.split('|', 1)
                # Map Upstox exchange names -> AngelOne exchange names
                exch_map = {'NSE_FO': 'NFO', 'BSE_FO': 'BFO', 'MCX_FO': 'MCX',
                            'NSE_EQ': 'NSE', 'BSE_EQ': 'BSE'}
                exch = exch_map.get(exch, exch)
                exchange_map.setdefault(exch, []).append(token)
                key_lookup[f"{exch}:{token}"] = key

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
                        token    = str(item.get('symbolToken', ''))
                        ltp      = float(item.get('ltp', 0) or 0)
                        api_key  = f"{exch}:{token}"
                        orig_key = key_lookup.get(api_key, api_key)
                        ltp_map[orig_key] = ltp
                        ltp_map[api_key]  = ltp   # also store normalised key
                else:
                    print(f"AngelOne LTP error ({exch}): {data.get('message', '')}")
            except Exception as e:
                print(f"Error fetching LTP chunk ({exch}): {e}")

    print(f"Fetched LTP for {len(ltp_map)} entries")
    return ltp_map


# ==== MAIN UPDATE LOGIC ====
def update_success_ltp_once():
    """Update live LTP and PnL for all open positions in whale_successes_2."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, instrument_key, instrument_token, price, qty,
                           progress_label, max_return_high, max_return_low,
                           status, progress_history
                    FROM whale_successes_2
                    WHERE status != 'done' AND instrument_token IS NOT NULL
                    ORDER BY timestamp DESC
                """)
                rows = cur.fetchall()

        if not rows:
            print("No open positions found")
            return

        # Build list of instrument keys to fetch LTP for.
        # instrument_key ('NFO:12345') is preferred; fall back to instrument_token.
        keys_to_fetch = []
        for row in rows:
            inst_key   = str(row[1]).strip() if row[1] else None
            inst_token = str(row[2]).strip() if row[2] else None
            key = inst_key or inst_token
            if key:
                keys_to_fetch.append(key)

        keys_to_fetch = list(set(keys_to_fetch))
        print(f"Found {len(rows)} open positions, fetching LTP for {len(keys_to_fetch)} unique keys")
        print(f"   Sample keys: {keys_to_fetch[:3]}")

        ltp_map = fetch_ltp_for_tokens(keys_to_fetch)

        if not ltp_map:
            print("No LTP data received from AngelOne API")
            return

        now = time.strftime("%Y-%m-%d %H:%M:%S")
        stock_pct_cache   = {}
        MILESTONE_THRESHOLDS = [25, 50, 75, 90]

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                updates_count = 0
                updates_batch = []

                for row in rows:
                    (record_id, inst_key, inst_token, entry_price, qty,
                     current_progress, max_high, max_low, current_status, progress_history) = row

                    # Try instrument_key first, then instrument_token
                    ltp = ltp_map.get(str(inst_key or '').strip())
                    if ltp is None:
                        ltp = ltp_map.get(str(inst_token or '').strip())

                    if ltp is None:
                        print(f"  No LTP found for key: {inst_key} / token: {inst_token}")
                        continue

                    if entry_price is None or entry_price == 0:
                        continue

                    try:
                        entry_price = float(entry_price)
                        qty         = float(qty) if qty else 1
                        ltp         = float(ltp)
                    except (ValueError, TypeError):
                        continue

                    pnl_per_unit = ltp - entry_price
                    pnl_value    = pnl_per_unit * qty
                    pnl_pct      = (pnl_per_unit / entry_price) * 100 if entry_price != 0 else 0

                    # Progress label
                    if pnl_pct >= 94:
                        new_progress = "done"
                    elif pnl_pct >= 75:
                        new_progress = "+75%"
                    elif pnl_pct >= 50:
                        new_progress = "+50%"
                    elif pnl_pct >= 25:
                        new_progress = "+25%"
                    elif pnl_pct >= -25:
                        new_progress = "running"
                    elif pnl_pct >= -50:
                        new_progress = "-25%"
                    elif pnl_pct >= -75:
                        new_progress = "-50%"
                    else:
                        new_progress = "-75%"

                    progress_label = new_progress
                    new_status     = "done" if pnl_pct >= 94 else (current_status or "open")

                    updated_progress_history = progress_history or ""
                    if progress_label != current_progress:
                        entry_str = f"{now}:{progress_label}"
                        updated_progress_history = (
                            f"{updated_progress_history}|{entry_str}"
                            if updated_progress_history else entry_str
                        )

                    if max_high is None or pnl_pct > max_high:
                        max_high = pnl_pct
                    if max_low is None or pnl_pct < max_low:
                        max_low = pnl_pct

                    # Get stock % change (cached per underlying)
                    cache_key = str(inst_key or inst_token or '')
                    if cache_key not in stock_pct_cache:
                        _, stock_pct = get_daily_pct_change_for_underlying(cache_key)
                        stock_pct_cache[cache_key] = stock_pct
                    else:
                        stock_pct = stock_pct_cache[cache_key]

                    should_store_ltp = pnl_pct >= 90 or any(pnl_pct >= t for t in MILESTONE_THRESHOLDS)

                    updates_batch.append((
                        progress_label,
                        round(max_high, 2) if max_high is not None else None,
                        round(max_low,  2) if max_low  is not None else None,
                        new_status,
                        stock_pct,
                        round(ltp,       2) if should_store_ltp else None,
                        round(pnl_value, 2) if should_store_ltp else None,
                        round(pnl_pct,   2) if should_store_ltp else None,
                        updated_progress_history,
                        record_id
                    ))
                    updates_count += 1

                if updates_batch:
                    print(f"Executing batch update for {len(updates_batch)} records...")

                    cur.execute("""
                        CREATE TEMP TABLE batch_updates (
                            progress_label VARCHAR(20),
                            max_return_high DECIMAL(10, 4),
                            max_return_low DECIMAL(10, 4),
                            status VARCHAR(20),
                            stock_pct_change_today DECIMAL(10, 4),
                            live_ltp DECIMAL(10, 4),
                            live_pnl DECIMAL(12, 4),
                            live_pnl_pct DECIMAL(10, 4),
                            progress_history TEXT,
                            id BIGINT
                        )
                    """)

                    cur.executemany(
                        "INSERT INTO batch_updates VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        updates_batch
                    )

                    cur.execute("""
                        UPDATE whale_successes_2 ws
                        SET
                            last_live_update = NOW(),
                            progress_label = bu.progress_label,
                            max_return_high = bu.max_return_high,
                            max_return_low = bu.max_return_low,
                            status = bu.status,
                            stock_pct_change_today = bu.stock_pct_change_today,
                            live_ltp = COALESCE(bu.live_ltp, ws.live_ltp),
                            live_pnl = COALESCE(bu.live_pnl, ws.live_pnl),
                            live_pnl_pct = COALESCE(bu.live_pnl_pct, ws.live_pnl_pct),
                            progress_history = COALESCE(NULLIF(bu.progress_history, ''), ws.progress_history)
                        FROM batch_updates bu
                        WHERE ws.id = bu.id
                    """)

                    print("Batch update executed successfully")

        print(f"Updated LTP & PnL for {updates_count} positions at {now}")

    except Exception as e:
        print(f"Error updating successes: {e}")
        import traceback
        traceback.print_exc()


# ===================== MARKET HOURS =====================
def is_market_open():
    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=14, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=31, second=0, microsecond=0)
    return market_open <= now <= market_close


def wait_until_market_open():
    IST = pytz.timezone('Asia/Kolkata')
    while not is_market_open():
        now = datetime.now(IST)
        if now.hour >= 15 and now.minute >= 30:
            next_open = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
            wait_s    = (next_open - now).total_seconds()
            print(f"Market closed. Next open: {next_open.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"Waiting {int(wait_s/3600)}h {int((wait_s%3600)/60)}m...")
        else:
            market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
            wait_s = (market_open_time - now).total_seconds()
            print(f"Waiting for market open at {market_open_time.strftime('%H:%M:%S %Z')}")
            print(f"Time remaining: {int(wait_s/60)}m...")
        time.sleep(60)


def refresh_daily_token():
    """Re-login to AngelOne at 6:00 AM IST daily."""
    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)
    if now.hour == 6 and now.minute == 0:
        print(f"\nDaily token refresh at {now.strftime('%H:%M:%S %Z')}")
        if login_angelone():
            print("AngelOne token refreshed.")
            time.sleep(65)
        else:
            print("Token refresh failed.")


# ===================== MAIN =====================
def main():
    print("Success LTP Updater started (AngelOne / Database mode)...")
    print("Broker  : AngelOne SmartAPI")
    print("Tables  : whale_successes_2 / whale_entries_2")
    print("Hours   : Mon-Fri  09:15 - 15:30 IST\n")

    create_whale_tables()

    print("Logging into AngelOne...")
    if not login_angelone():
        print("AngelOne login failed. Set ANGELONE_CLIENT_ID and ANGELONE_PASSWORD env vars. Exiting.")
        return

    while True:
        try:
            if not is_market_open():
                IST = pytz.timezone('Asia/Kolkata')
                print(f"Market is closed ({datetime.now(IST).strftime('%H:%M %A')})")
                wait_until_market_open()
                print("\nMarket reopening - refreshing AngelOne token...")
                login_angelone()

            refresh_daily_token()
            update_success_ltp_once()

        except Exception as e:
            print(f"Error in update loop: {e}")

        time.sleep(10)


if __name__ == "__main__":
    main()


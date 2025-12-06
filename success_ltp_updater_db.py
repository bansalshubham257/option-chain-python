import os
import time
import io
import gzip
import re
import pandas as pd
import requests
import yfinance as yf
import psycopg2
from contextlib import contextmanager
from datetime import datetime, timedelta
import pytz
import os

from services.database import DatabaseService

# ==== CONFIG ====
db = DatabaseService()
ACCESS_TOKEN = db.get_access_token(account_id=5)

BASE_URL_V3 = "https://api.upstox.com/v3"

SUCCESS_FILE = "successful_whale_orders.csv"
MAPPING_FILE = "symbol_instrument_map.csv"

MAX_KEYS_PER_CALL = 450

# Database connection parameters
DB_CONN_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'swingtrade_db'),
    'user': os.getenv('DB_USER', 'swingtrade_db_user'),
    'password': os.getenv('DB_PASSWORD', 'ZlewRq8aZKimqMwrP2LdRTuFsvhi9qDw'),
    'host': os.getenv('DB_HOST', 'dpg-cvh8gfpu0jms73bj6gm0-a.oregon-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432')
}
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway')

# Upstox instruments CSVs
URL_NSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
URL_BSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.csv.gz"
URL_MCX_FO = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"

# ================= DATABASE HELPERS =================

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
        print(f"❌ Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_whale_tables():
    """Create whale_entries and whale_successes tables if they don't exist"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Create whale_entries table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS whale_entries (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    instrument_key VARCHAR(100) NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_whale_entries_unique_key ON whale_entries(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_timestamp ON whale_entries(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_symbol ON whale_entries(symbol);
            """)

            # Create whale_successes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS whale_successes (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    instrument_key VARCHAR(100) NOT NULL,
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
                CREATE INDEX IF NOT EXISTS idx_whale_successes_unique_key ON whale_successes(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_timestamp ON whale_successes(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_symbol ON whale_successes(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_status ON whale_successes(status);
            """)
    print("✅ Whale tables created/verified")




def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]


def extract_underlying_from_symbol(symbol):
    """
    Extract underlying stock/index symbol from trading symbol.
    E.g., 'NSE_FO:VEDL25DEC520CE' -> 'VEDL'
    """
    if not symbol:
        return None
    try:
        # Remove exchange prefix if present
        if ':' in symbol:
            symbol = symbol.split(':')[1]

        # Remove date and option type (last 2-4 chars like 'CE', 'PE')
        if symbol.endswith('CE') or symbol.endswith('PE'):
            symbol = symbol[:-2]

        # Remove strike price (last 1-5 digits)
        match = re.match(r'^([A-Z&]+)', symbol)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def get_daily_pct_change_for_underlying(symbol):
    """
    Get today's % price change for a stock underlying.

    Returns: (current_price, pct_change)
      - current_price: today's last traded price
      - pct_change: (current - open) / open * 100
      - If no data, returns (None, None)
    """
    underlying = extract_underlying_from_symbol(symbol)
    if not underlying:
        return None, None

    # For indices, fetch live via Upstox
    if underlying == "NIFTY":
        try:
            ticker_key = "NSE_INDEX|Nifty 50"
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
            url = f"{BASE_URL_V3}/market-quote/quotes"
            params = {'instrument_key': ticker_key}
            resp = requests.get(url, headers=headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == 'success':
                    quote = data['data'].get(ticker_key, {})
                    current = quote.get('last_price')
                    ohlc = quote.get('ohlc', {})
                    open_price = ohlc.get('open')
                    if current and open_price and open_price > 0:
                        pct_change = ((float(current) - float(open_price)) / float(open_price)) * 100
                        return float(current), round(pct_change, 2)
        except Exception:
            pass
        return None, None

    elif underlying == "SENSEX":
        try:
            ticker_key = "BSE_INDEX|SENSEX"
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
            url = f"{BASE_URL_V3}/market-quote/quotes"
            params = {'instrument_key': ticker_key}
            resp = requests.get(url, headers=headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == 'success':
                    quote = data['data'].get(ticker_key, {})
                    current = quote.get('last_price')
                    ohlc = quote.get('ohlc', {})
                    open_price = ohlc.get('open')
                    if current and open_price and open_price > 0:
                        pct_change = ((float(current) - float(open_price)) / float(open_price)) * 100
                        return float(current), round(pct_change, 2)
        except Exception:
            pass
        return None, None

    else:
        # For stocks, use yfinance
        try:
            ticker = underlying + ".NS"
            yf_ticker = yf.Ticker(ticker)
            # Get today's data only
            hist = yf_ticker.history(period="1d", interval="1m")
            if hist is not None and not hist.empty:
                current_price = float(hist["Close"].iloc[-1])
                open_price = float(hist["Open"].iloc[0])
                if open_price > 0:
                    pct_change = ((current_price - open_price) / open_price) * 100
                    return current_price, round(pct_change, 2)
        except Exception:
            pass
        return None, None


# ==== INSTRUMENTS & MAPPING ====

def download_instruments(url, exchange_filter):
    """
    Download instruments CSV for a given exchange (e.g. 'NSE_FO', 'BSE_FO', 'MCX_FO')
    and return as pandas DataFrame.
    """
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        with gzip.open(io.BytesIO(resp.content), 'rt') as f:
            df = pd.read_csv(f)
        if 'exchange' in df.columns:
            df = df[df['exchange'] == exchange_filter]
        return df
    except Exception as e:
        print(f"⚠️ Error downloading instruments for {exchange_filter}: {e}")
        return pd.DataFrame()


def load_symbol_mapping():
    """
    Load symbol -> instrument_key mapping from CSV if present.
    Returns dict: { 'NSE_FO:VEDL25DEC520CE': 'NSE_FO|12345', ... }
    """
    mapping = {}
    if not os.path.exists(MAPPING_FILE):
        return mapping

    try:
        df_map = pd.read_csv(MAPPING_FILE)
        for _, row in df_map.iterrows():
            sym = str(row.get("symbol", "")).strip()
            ik = str(row.get("instrument_key", "")).strip()
            if sym and ik:
                mapping[sym] = ik
    except Exception as e:
        print(f"⚠️ Could not load mapping file: {e}")

    return mapping


def build_symbol_instrument_mapping(symbols):
    """
    Ensure we have instrument_key mapping for all given trading symbols.

    symbols: list like ['NSE_FO:VEDL25DEC520CE', 'NSE_FO:SBIN25DEC980CE', ...]
    Writes / appends to symbol_instrument_map.csv:

        symbol, instrument_key
        NSE_FO:VEDL25DEC520CE, NSE_FO|139528
    """
    if not symbols:
        return

    symbols = sorted(set(str(s).strip() for s in symbols if s))

    # Load existing mapping (so we only add missing ones)
    existing = load_symbol_mapping()
    missing = [s for s in symbols if s not in existing]

    if not missing:
        return

    # Download instruments once per exchange
    df_nse = download_instruments(URL_NSE_FO, "NSE_FO")
    df_bse = download_instruments(URL_BSE_FO, "BSE_FO")
    df_mcx = download_instruments(URL_MCX_FO, "MCX_FO")

    # If Upstox changes column names, we try both possibilities
    def find_row_for_symbol(full_symbol):
        """
        full_symbol: 'NSE_FO:VEDL25DEC520CE'
        We split to 'NSE_FO' and 'VEDL25DEC520CE' and match on tradingsymbol/symbol.
        """
        if ":" not in full_symbol:
            return None

        exch_prefix, tsymbol = full_symbol.split(":", 1)
        tsymbol = tsymbol.strip()

        if exch_prefix == "NSE_FO":
            df = df_nse
        elif exch_prefix == "BSE_FO":
            df = df_bse
        elif exch_prefix == "MCX_FO":
            df = df_mcx
        else:
            return None

        if df.empty:
            return None

        if "tradingsymbol" in df.columns:
            sub = df[df["tradingsymbol"] == tsymbol]
        elif "symbol" in df.columns:
            sub = df[df["symbol"] == tsymbol]
        else:
            return None

        if sub.empty:
            return None
        return sub.iloc[0]

    new_rows = []

    for sym in missing:
        row = find_row_for_symbol(sym)
        if row is None:
            continue
        inst_key = str(row.get("instrument_key", "")).strip()
        if not inst_key:
            continue
        existing[sym] = inst_key
        new_rows.append({"symbol": sym, "instrument_key": inst_key})

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        # Append or create mapping file
        if os.path.exists(MAPPING_FILE):
            df_new.to_csv(MAPPING_FILE, mode="a", header=False, index=False)
        else:
            df_new.to_csv(MAPPING_FILE, index=False)

        print(f"✅ Added {len(new_rows)} new symbol->instrument_key mappings.")
    else:
        print("ℹ️ No new mappings added.")


# ==== LTP FETCH USING v3 & instrument_key ====

def fetch_ltp_for_instrument_keys(instrument_keys):
    """
    instrument_keys: list like ['NSE_FO|139528', 'NSE_FO|98765', ...]
    Uses Upstox v3 /market-quote/ltp with instrument_key param.
    Returns: dict { 'NSE_FO|139528': 24.35, ... }
    """
    if not instrument_keys:
        return {}

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    }
    url = f"{BASE_URL_V3}/market-quote/ltp"
    ltp_map = {}

    for chunk in chunked(instrument_keys, MAX_KEYS_PER_CALL):
        try:
            params = {"instrument_key": ",".join(chunk)}
            resp = requests.get(url, headers=headers, params=params)
            try:
                data = resp.json()
            except Exception:
                print("⚠️ Failed to decode LTP JSON:", resp.text)
                continue

            print("Fetching LTP for chunk, API status:", data.get("status"))

            if resp.status_code != 200 or data.get("status") != "success":
                print("  ↳ Error body:", data)
                continue

            for key, details in data["data"].items():
                try:
                    ltp_map[key] = float(details.get("last_price", 0) or 0)
                except Exception:
                    continue
        except Exception as e:
            print(f"⚠️ Error in LTP fetch chunk: {e}")
    #print("LTP fetch complete, total keys fetched:", ltp_map)
    return ltp_map


# ==== MAIN UPDATE LOGIC ====

def update_success_ltp_once():
    """Update live LTP and PnL for all open positions in database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get all open positions
                cur.execute("""
                    SELECT id, instrument_key, price, qty, progress_label, max_return_high, max_return_low, status
                    FROM whale_successes
                    WHERE status != 'done'
                    ORDER BY timestamp DESC
                """)
                rows = cur.fetchall()

        if not rows:
            print("⚠️ No open positions found")
            return

        # Collect all instrument keys for batch LTP fetch
        instrument_keys = [str(row[1]).split(':')[1] if ':' in str(row[1]) else str(row[1]) for row in rows]

        # Fetch LTPs using Upstox API (if possible, otherwise we update with available data)
        ltp_map = fetch_ltp_batch(instrument_keys)

        now = time.strftime("%Y-%m-%d %H:%M:%S")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    record_id, instrument_key, entry_price, qty, current_progress, max_high, max_low, current_status = row

                    sym_key = str(instrument_key).split(':')[1] if ':' in str(instrument_key) else str(instrument_key)
                    ltp = ltp_map.get(sym_key)

                    if ltp is None or entry_price is None or entry_price == 0:
                        continue

                    try:
                        entry_price = float(entry_price)
                        qty = float(qty) if qty else 1
                        ltp = float(ltp)
                    except (ValueError, TypeError):
                        continue

                    # Calculate PnL
                    pnl_per_unit = ltp - entry_price
                    pnl_value = pnl_per_unit * qty
                    pnl_pct = (pnl_per_unit / entry_price) * 100 if entry_price != 0 else 0

                    # Determine progress label
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

                    # Progress order tracking
                    progress_order = ["-75%", "-50%", "-25%", "running", "+25%", "+50%", "+75%", "done"]

                    try:
                        current_idx = progress_order.index(str(current_progress).strip() or "running")
                    except ValueError:
                        current_idx = 3

                    try:
                        new_idx = progress_order.index(new_progress)
                    except ValueError:
                        new_idx = 3

                    # Allow update if moving forward
                    if str(current_progress).strip() == "running" or new_idx >= current_idx:
                        progress_label = new_progress
                    else:
                        progress_label = current_progress or "running"

                    # Track progress history
                    if progress_label:
                        new_status = "done" if pnl_pct >= 94 else (current_status or "open")

                        # Update max high and low
                        if max_high is None or pnl_pct > max_high:
                            max_high = pnl_pct
                        if max_low is None or pnl_pct < max_low:
                            max_low = pnl_pct

                        # Get current stock % if needed
                        _, stock_pct_change = get_daily_pct_change_for_underlying(instrument_key)

                        cur.execute("""
                            UPDATE whale_successes
                            SET
                                live_ltp = %s,
                                live_pnl = %s,
                                live_pnl_pct = %s,
                                last_live_update = NOW(),
                                progress_label = %s,
                                max_return_high = %s,
                                max_return_low = %s,
                                status = %s,
                                stock_pct_change_today = %s
                            WHERE id = %s
                        """, (
                            round(ltp, 2), round(pnl_value, 2), round(pnl_pct, 2),
                            progress_label, round(max_high, 2), round(max_low, 2),
                            new_status, stock_pct_change, record_id
                        ))

        print(f"✅ Updated LTP & PnL for {len(rows)} positions at {now}")

    except Exception as e:
        print(f"⚠️ Error updating successes: {e}")

def fetch_ltp_batch(instrument_keys):
    """Fetch LTP for batch of instruments"""
    ltp_map = {}
    if not instrument_keys:
        return ltp_map

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    }
    url = f"{BASE_URL_V3}/market-quote/ltp"

    # Build full instrument keys with NSE_FO prefix if needed
    full_keys = []
    for key in instrument_keys:
        if '|' not in str(key) and ':' not in str(key):
            full_keys.append(f"NSE_FO|{key}")
        else:
            full_keys.append(str(key))

    for chunk in chunked(full_keys, MAX_KEYS_PER_CALL):
        try:
            params = {"instrument_key": ",".join(chunk)}
            resp = requests.get(url, headers=headers, params=params, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    for key, details in data.get("data", {}).items():
                        try:
                            ltp = float(details.get("last_price", 0) or 0)
                            ltp_map[key] = ltp
                        except (ValueError, TypeError):
                            continue
        except Exception as e:
            print(f"⚠️ Error fetching LTP chunk: {e}")

    return ltp_map


def is_market_open():
    """Check if market is open (Mon-Fri, 9:15 AM - 3:30 PM IST)"""
    import pytz
    from datetime import datetime

    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)

    # Check if weekday (Monday=0, Sunday=6)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False

    # Check if time is between 9:15 AM and 3:30 PM
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    return market_open <= now <= market_close


def wait_until_market_open():
    """Wait until market opens, checking every 60 seconds"""
    import pytz
    from datetime import datetime, timedelta

    IST = pytz.timezone('Asia/Kolkata')

    while not is_market_open():
        now = datetime.now(IST)

        # If after market close (3:30 PM), wait until next day 9:15 AM
        if now.hour >= 15 and now.minute >= 30:
            next_open = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
            wait_seconds = (next_open - now).total_seconds()
            print(f"⏸️  Market closed. Next open: {next_open.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"   Waiting {int(wait_seconds / 3600)}h {int((wait_seconds % 3600) / 60)}m...")
        else:
            # Before market open
            market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
            wait_seconds = (market_open_time - now).total_seconds()
            print(f"⏸️  Waiting for market to open at {market_open_time.strftime('%H:%M:%S %Z')}")
            print(f"   Time remaining: {int(wait_seconds / 60)}m...")

        time.sleep(60)  # Check every minute


def refresh_daily_token():
    """Refresh token if it's 6:00 AM IST (before market opens at 9:15 AM)"""
    import pytz
    from datetime import datetime

    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)

    # If between 6:00 AM and 6:01 AM, refresh token
    if now.hour == 6 and now.minute == 0:
        print(f"\n🔄 Daily token refresh at {now.strftime('%H:%M:%S %Z')}")
        try:
            global ACCESS_TOKEN
            ACCESS_TOKEN = db.get_access_token(account_id=5)
            print(f"✅ Token refreshed: {ACCESS_TOKEN[:30]}...")
            time.sleep(65)  # Wait 65 seconds to avoid re-refreshing
        except Exception as e:
            print(f"❌ Token refresh failed: {e}")


def main():
    print("🟢 Success LTP updater started (Database mode)...")
    print("📊 Timezone: Asia/Kolkata (IST)")
    print("📅 Operating hours: Monday-Friday, 9:15 AM - 3:30 PM")
    print("🔄 Token refresh: Daily at 6:00 AM\n")

    # Create tables if they don't exist
    create_whale_tables()

    # Refresh token before first run
    print("🔄 Fetching fresh token...")
    try:
        global ACCESS_TOKEN
        ACCESS_TOKEN = db.get_access_token(account_id=5)
        print(f"✅ Token obtained: {ACCESS_TOKEN[:30]}...\n")
    except Exception as e:
        print(f"❌ Failed to get token: {e}\n")

    while True:
        try:
            # Check if market is open
            if not is_market_open():
                print(f"⏸️  Market is closed ({datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M %A')})")
                wait_until_market_open()

                # Market just opened - fetch fresh token from DB (it was updated while closed)
                print(f"\n✨ Market reopening - fetching fresh token from database...")
                try:
                    global ACCESS_TOKEN
                    ACCESS_TOKEN = db.get_access_token(account_id=5)
                    print(f"✅ Fresh token obtained: {ACCESS_TOKEN[:30]}...\n")
                except Exception as e:
                    print(f"❌ Failed to get fresh token: {e}\n")

                # Refresh token at 6 AM daily if needed
                refresh_daily_token()

            # Refresh token at 6 AM daily if needed
            refresh_daily_token()

            # Update LTP
            update_success_ltp_once()

        except Exception as e:
            print(f"⚠️ Error in update loop: {e}")

        time.sleep(10)  # update every 10 seconds


if __name__ == "__main__":
    main()

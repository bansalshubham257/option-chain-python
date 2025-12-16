import re
import sys
import time
import requests
import pandas as pd
import io
import gzip
import os
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import psycopg2
from contextlib import contextmanager

from services.database import DatabaseService

# ================= USER CONFIGURATION =================
# 1. ENTER YOUR ACCESS TOKEN (Only 1 needed)
#ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2NEFLNjciLCJqdGkiOiI2OTJiY2Y2NGJhYzQ4MDMwYmFiODM0OTUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2NDQ3ODgyMCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzY0NTQwMDAwfQ.ycEnEcYokcR5U04gEWgwK9nBDqywgSjsLMlfUv5vvlM"
db = DatabaseService()
ACCESS_TOKEN = db.get_access_token(account_id=6)

# 2. ENTER EXPIRY DATES (Format: YYYY-MM-DD)
NIFTY_EXPIRY      = "2029-12-02"   # NIFTY options expiry
SENSEX_EXPIRY     = "2029-12-04"   # SENSEX options expiry (BSE_FO)
STOCK_FNO_EXPIRY  = "2025-12-30"   # NSE stock options expiry (OPTSTK)
CRUDE_EXPIRY      = "2029-12-16"   # MCX Crudeoil expiry
NG_EXPIRY         = "2029-12-23"   # MCX Natural Gas expiry

# MINIMUM NOTIONAL VALUE FOR "BIG ORDER"
MIN_ORDER_VALUE = 950000  # ₹9L

# API CONFIG
BASE_URL_V2 = "https://api.upstox.com/v2"
URL_NSE_FO  = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
URL_BSE_FO  = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.csv.gz"
URL_MCX_FO  = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"

MAX_KEYS_PER_CALL = 450
DEFAULT_LOT_SIZE  = 50  # fallback if CSV lot_size missing

# ================= GLOBAL STATE =================

underlying_pcr   = {}  # underlying symbol -> latest PCR (PE_OI / CE_OI)
history          = {}  # per-instrument_key snapshot (vol/oi + last whale)
instrument_meta  = {}  # instrument_key -> metadata
underlying_spots = {}  # underlying symbol (SBICARD, ICICIPRU, etc.) -> CMP
index_spots      = {}  # NIFTY / SENSEX spot

success_keys      = set()  # to avoid duplicate success records
entry_keys        = set()  # to avoid duplicate entry rows
success_positions = {}     # in-memory map of open positions (optional)

# Database connection parameters
DB_CONN_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'railway'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'LZjgyzthYpacmWhOSAnDMnMWxkntEEqe'),
    'host': os.getenv('DB_HOST', 'switchback.proxy.rlwy.net'),
    'port': os.getenv('DB_PORT', '22297')
}
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway')

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
                CREATE INDEX IF NOT EXISTS idx_whale_entries_unique_key ON whale_entries(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_timestamp ON whale_entries(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_symbol ON whale_entries(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_instrument_token ON whale_entries(instrument_token);
            """)

            # Create whale_successes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS whale_successes (
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
                CREATE INDEX IF NOT EXISTS idx_whale_successes_unique_key ON whale_successes(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_timestamp ON whale_successes(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_symbol ON whale_successes(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_status ON whale_successes(status);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_instrument_token ON whale_successes(instrument_token);
            """)
    print("✅ Whale tables created/verified")



# ================= GENERIC HELPERS =================

def chunked(lst, size):
    """Yield successive chunks of size 'size' from list."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def download_instruments(url, exchange_filter):
    """Download and return instrument CSV for a given exchange."""
    print(f"\n⏳ Downloading {exchange_filter} instruments...")
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        with gzip.open(io.BytesIO(resp.content), 'rt') as f:
            df = pd.read_csv(f)

        if 'exchange' in df.columns:
            df = df[df['exchange'] == exchange_filter]

        return df
    except Exception as e:
        print(f"❌ Error downloading instruments for {exchange_filter}: {e}")
        return pd.DataFrame()


def load_existing_successes():
    """
    Load existing success records from whale_successes table.
    Fills success_keys to avoid duplicates.
    """
    global success_keys
    success_keys.clear()

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unique_key FROM whale_successes")
                rows = cur.fetchall()
                for row in rows:
                    success_keys.add(row[0])

        print(f"📄 Loaded {len(success_keys)} existing success records from database")
    except Exception as e:
        print(f"⚠️ Could not load success records: {e}")


def load_existing_entries():
    """
    Load existing entry records from whale_entries table.
    Fills entry_keys to avoid duplicates.
    """
    global entry_keys
    entry_keys.clear()

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unique_key FROM whale_entries")
                rows = cur.fetchall()
                for row in rows:
                    entry_keys.add(row[0])

        print(f"📄 Loaded {len(entry_keys)} existing entry records from database")
    except Exception as e:
        print(f"⚠️ Could not load entry records: {e}")



def record_success(symbol, side, qty, price, vol_diff, oi_diff, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token=None):
    """
    Store a successful whale fill into database.
    Uniqueness key: underlying + strike + option_type + price.

    Args:
        symbol: instrument_key in format 'NSE_FO:VEDL25DEC520CE'
        instrument_token: actual Upstox instrument token like 'NSE_FO|67718' (optional)
    """
    global success_keys

    parsed      = parse_underlying_from_symbol(symbol)
    underlying  = parsed["underlying"]
    strike      = parsed["strike"]
    option_type = parsed["option_type"]

    if underlying is None or strike is None or option_type is None or price is None:
        return

    # 🔑 single canonical unique key (matches loader)
    unique_key = f"{underlying}|{strike}|{option_type}|{price}"
    if unique_key in success_keys:
        return
    success_keys.add(unique_key)

    ts = datetime.now()

    # Get daily % change of underlying stock/index
    underlying_price, stock_pct_change = get_daily_pct_change_for_underlying(symbol)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO whale_successes (
                        timestamp, instrument_key, instrument_token, symbol, strike, option_type, side, qty, price, ltp,
                        vol_diff, oi_diff, moneyness, pcr, oi_same, oi_opposite,
                        stock_current_price, stock_pct_change_today, unique_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                """, (
                    ts, symbol, instrument_token, underlying, strike, option_type, str(side).upper(), qty, float(price), float(ltp),
                    vol_diff, oi_diff, moneyness, pcr, oi_same, oi_opposite,
                    underlying_price, stock_pct_change, unique_key
                ))
    except Exception as e:
        print(f"⚠️ Could not write success record: {e}")


def record_entry(symbol, side, qty, price, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token=None):
    """
    Store whale ENTRY into database, deduped by underlying+strike+option_type+price.

    Args:
        symbol: instrument_key in format 'NSE_FO:VEDL25DEC520CE'
        instrument_token: actual Upstox instrument token like 'NSE_FO|67718' (optional)
    """
    global entry_keys

    parsed      = parse_underlying_from_symbol(symbol)
    underlying  = parsed["underlying"]
    strike      = parsed["strike"]
    option_type = parsed["option_type"]

    if underlying is None or strike is None or option_type is None or price is None:
        return

    # 🔑 same unique key rule as success
    unique_key = f"{underlying}|{strike}|{option_type}|{price}"
    if unique_key in entry_keys:
        return
    entry_keys.add(unique_key)

    ts = datetime.now()

    # Get daily % change of underlying stock/index
    underlying_price, stock_pct_change = get_daily_pct_change_for_underlying(symbol)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO whale_entries (
                        timestamp, instrument_key, instrument_token, symbol, option_type, side, qty, price, ltp,
                        moneyness, pcr, oi_same, oi_opposite, stock_current_price, stock_pct_change_today, unique_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    ts, symbol, instrument_token, underlying, option_type, str(side).upper(), qty, float(price), float(ltp),
                    moneyness, pcr, oi_same, oi_opposite, underlying_price, stock_pct_change, unique_key
                ))
    except Exception as e:
        print(f"⚠️ Could not write entry record: {e}")



def get_spot_indices():
    """Fetch NIFTY and SENSEX spot once, store in index_spots."""
    global index_spots
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    request_keys = "NSE_INDEX|Nifty 50,BSE_INDEX|SENSEX"
    url = f"{BASE_URL_V2}/market-quote/ltp"
    params = {'instrument_key': request_keys}
    try:
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()

        def find_price(data_dict, partial_key):
            for k, v in data_dict.items():
                if partial_key in k:
                    return v['last_price']
            return None

        nifty_ltp  = find_price(data['data'], 'Nifty 50') or 24000
        sensex_ltp = find_price(data['data'], 'SENSEX') or 80000

        index_spots['NIFTY']  = float(nifty_ltp)
        index_spots['SENSEX'] = float(sensex_ltp)
        print(f"📊 Index spots → NIFTY: {nifty_ltp}, SENSEX: {sensex_ltp}")
    except Exception as e:
        print(f"⚠️ Could not fetch index spots: {e}")
        index_spots['NIFTY']  = 24000.0
        index_spots['SENSEX'] = 80000.0


def fetch_ltp_for_keys(instrument_keys):
    """Call /ltp for a list of instrument keys (<= MAX_KEYS_PER_CALL each chunk)."""
    print("Fetching LTP for underlying stocks via Upstox..." , instrument_keys)
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    url = f"{BASE_URL_V2}/market-quote/ltp"
    all_data = {}
    for chunk in chunked(instrument_keys, MAX_KEYS_PER_CALL):
        try:
            params = {'instrument_key': ",".join(chunk)}
            resp = requests.get(url, headers=headers, params=params)
            data = resp.json()
            if data.get('status') == 'success':
                all_data.update(data['data'])
        except Exception as e:
            print(f"⚠️ Error fetching LTP for chunk: {e}")
            continue
    return all_data


def build_underlying_spots(stock_symbols):
    """
    For all F&O stock underlyings, fetch NSE_EQ CMP and store in underlying_spots.
    stock_symbols: iterable of symbols like SBICARD, ICICIPRU, SHREECEM, etc.
    """
    print("Stock symbols for underlying spots:", stock_symbols)
    global underlying_spots
    stock_symbols = sorted({str(n).upper() for n in stock_symbols if isinstance(n, str)})
    if not stock_symbols:
        return

    print(f"\n⏳ Fetching underlying NSE_EQ CMP for {len(stock_symbols)} F&O stocks (Upstox)...")

    eq_keys = [f"NSE_EQ|{name}" for name in stock_symbols]
    ltp_data = fetch_ltp_for_keys(eq_keys)

    count = 0
    for key, val in ltp_data.items():
        try:
            name = key.split("|", 1)[1].upper()
            underlying_spots[name] = float(val['last_price'])
            count += 1
        except Exception:
            continue

    print(f"✅ Loaded CMP for {count} F&O underlyings via NSE_EQ.")


def get_lot_size_for_symbol(symbol_key):
    """Return lot size from instrument_meta, fallback to DEFAULT_LOT_SIZE."""
    meta = instrument_meta.get(symbol_key, {})
    lot = meta.get('lot_size')
    try:
        if lot is not None and not pd.isna(lot) and float(lot) > 0:
            return float(lot)
    except Exception:
        pass
    return DEFAULT_LOT_SIZE


def get_whale_order(depth_list, lot_size):
    """
    Pick the 'whale' from order depth if:
      - total notional value (qty * price) >= MIN_ORDER_VALUE
      - average lots per order >= 1.5
      - price >= 10
    Only uses top 2 levels for speed.
    """
    if not depth_list:
        return None

    top_levels = depth_list[:2]
    biggest = max(top_levels, key=lambda x: x.get('quantity', 0))

    qty    = biggest.get('quantity', 0)
    orders = biggest.get('orders', 0)
    price  = biggest.get('price', 0)

    if orders == 0 or price == 0 or price < 10:
        return None

    total_order_value = qty * price
    total_lots = qty / float(lot_size) if lot_size else 0.0
    avg_qty  = qty / orders
    avg_lots = avg_qty / float(lot_size) if lot_size else 0.0

    if total_order_value >= MIN_ORDER_VALUE and avg_lots >= 1.5 and total_lots > 50:
        avg_qty  = qty / orders
        avg_lots = avg_qty / lot_size
        if avg_lots >= 1.5:
            return (qty, price, orders, avg_lots)

    return None

def extract_underlying_from_key(symbol_key: str) -> str:
    """
    Extract the underlying from an Upstox FO instrument key.

    Examples:
      'NSE_FO:BOSCHLTD25DEC37500PE' -> 'BOSCHLTD'
      'NSE_FO:SBIN25DEC620CE'       -> 'SBIN'
      'NSE_FO|BOSCHLTD25DEC37500PE' -> 'BOSCHLTD'
    """
    if not symbol_key:
        return ""

    # Strip prefix like 'NSE_FO:' or 'NSE_FO|'
    if ":" in symbol_key:
        raw = symbol_key.split(":", 1)[1]
    elif "|" in symbol_key:
        raw = symbol_key.split("|", 1)[1]
    else:
        raw = symbol_key

    raw = raw.upper()

    # Prefer strict format: UNDERLYING + DDMMM + STRIKE + (CE|PE|FUT)
    m = re.match(r"^([A-Z0-9]+?)(?=\d{2}[A-Z]{3}\d+(CE|PE|FUT)$)", raw)
    if m:
        return m.group(1)

    # Fallback: take characters until first digit
    i = 0
    while i < len(raw) and not raw[i].isdigit():
        i += 1
    if i == 0:
        return raw
    return raw[:i]


def get_daily_pct_change_for_underlying(symbol):
    """
    Get today's % price change for a stock underlying.

    Returns: (current_price, pct_change)
      - current_price: today's last traded price
      - pct_change: (current - open) / open * 100
      - If no data, returns (None, None)
    """
    underlying = extract_underlying_from_key(symbol)
    if not underlying:
        return None, None

    # For indices, fetch live via Upstox
    if underlying == "NIFTY":
        try:
            ticker_key = "NSE_INDEX|Nifty 50"
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
            url = f"{BASE_URL_V2}/market-quote/quotes"
            params = {'instrument_key': ticker_key}
            resp = requests.get(url, headers=headers, params=params)
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
            url = f"{BASE_URL_V2}/market-quote/quotes"
            params = {'instrument_key': ticker_key}
            resp = requests.get(url, headers=headers, params=params)
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


def get_underlying_cmp(symbol, meta=None, data=None):
    """
    Given an instrument key like 'NSE_FO:BOSCHLTD25DEC37500PE',
    return CMP of the underlying using yfinance:

      - Parse underlying: 'BOSCHLTD'
      - Build ticker: 'BOSCHLTD.NS'
      - Fetch LTP via yfinance and cache in underlying_spots
      - For indices (NIFTY / SENSEX) still use index_spots
      - Fallback to data['underlying_price'] / ['underlying_value'] if needed
    """
    global underlying_spots

    # 1) Parse underlying symbol from key
    underlying = extract_underlying_from_key(symbol)
    if not underlying:
        return None

    # 2) Handle indices via index_spots as before
    if underlying == "NIFTY":
        price = index_spots.get("NIFTY")
    elif underlying == "SENSEX":
        price = index_spots.get("SENSEX")
    else:
        # 3) Try cached CMP from previous yfinance calls
        price = underlying_spots.get(underlying)

        # 4) If not cached, fetch from yfinance once
        if price is None:
            ticker = underlying + ".NS"   # e.g. BOSCHLTD.NS
            try:
                yf_ticker = yf.Ticker(ticker)
                hist = yf_ticker.history(period="1d", interval="1m")
                if hist is not None and not hist.empty:
                    price = float(hist["Close"].iloc[-1])
                    underlying_spots[underlying] = price
            except Exception:
                price = None

    # 5) Fallback to live underlying_price / underlying_value from quote, if any
    if (price is None or price <= 0) and data is not None:
        live = data.get("underlying_price") or data.get("underlying_value")
        try:
            if live is not None:
                price = float(live)
        except Exception:
            pass

    # 6) Final validation
    if price is None:
        return None
    try:
        price = float(price)
        return price if price > 0 else None
    except Exception:
        return None


def classify_strike(symbol_key, data):
    """
    Classify as ATM / ITM / OTM / DEEP_ITM / DEEP_OTM / UNKNOWN.
    Uses:
      - instrument_meta
      - parsed meta (underlying, strike, option_type)
      - CMP from index_spots / underlying_spots / data['underlying_price']
    """
    meta = instrument_meta.get(symbol_key, {})
    parsed = parse_underlying_from_symbol(symbol_key)

    option_type = (parsed["option_type"] or str(meta.get("option_type", ""))).upper()
    strike      = parsed["strike"] if parsed["strike"] is not None else meta.get("strike")

    if option_type not in ("CE", "PE"):
        return "UNKNOWN"
    if strike is None:
        return "UNKNOWN"

    strike = float(strike)
    underlying_price = get_underlying_cmp(symbol_key, meta, data)
    if not underlying_price or underlying_price <= 0:
        return "UNKNOWN"

    underlying_price = float(underlying_price)
    diff     = strike - underlying_price
    rel_diff = abs(diff) / underlying_price

    # ±1% → ATM
    if rel_diff <= 0.01:
        return "ATM"

    # >3% → DEEP
    if option_type == "CE":
        if diff < 0:
            return "ITM" if rel_diff <= 0.03 else "DEEP_ITM"
        else:
            return "OTM" if rel_diff <= 0.03 else "DEEP_OTM"
    else:  # PE
        if diff > 0:
            return "ITM" if rel_diff <= 0.03 else "DEEP_ITM"
        else:
            return "OTM" if rel_diff <= 0.03 else "DEEP_OTM"


# ================= INSTRUMENT BUILDING =================

def build_all_instruments():
    """
    Build full list of instrument_keys for:
      - NIFTY options (NSE_FO)
      - SENSEX options (BSE_FO)
      - NSE stock options (OPTSTK)
      - MCX CRUDEOIL & NATURALGAS
    Fills instrument_meta and returns list of all keys (before filtering).
    """
    global instrument_meta

    all_keys = []
    instrument_meta.clear()

    # ----- NSE_FO -----
    df_nse_fo = download_instruments(URL_NSE_FO, "NSE_FO")
    if df_nse_fo.empty:
        print("🔴 No NSE_FO instruments loaded.")
    else:
        # NIFTY options
        df_nifty = df_nse_fo[df_nse_fo['name'].str.contains("Nifty", case=False, na=False)]
        df_nifty = df_nifty[
            (df_nifty['expiry'] == NIFTY_EXPIRY) &
            (df_nifty['option_type'].isin(['CE', 'PE']))
            ]

        for _, row in df_nifty.iterrows():
            key = row['instrument_key']
            all_keys.append(key)
            instrument_meta[key] = {
                "name": row.get('name'),
                "symbol": str(row.get('symbol') or "NIFTY").upper(),
                "strike": row.get('strike'),
                "expiry": row.get('expiry'),
                "instrument_type": row.get('instrument_type'),
                "option_type": row.get('option_type'),
                "lot_size": row.get('lot_size', DEFAULT_LOT_SIZE),
            }

        print(f"✅ Loaded {len(df_nifty)} NIFTY option contracts.")

        # Stock options: OPTSTK
        df_fno_stk = df_nse_fo[df_nse_fo['instrument_type'].isin(['OPTSTK'])]
        df_fno_stk = df_fno_stk[df_fno_stk['expiry'] == STOCK_FNO_EXPIRY]

        for _, row in df_fno_stk.iterrows():
            key = row['instrument_key']
            all_keys.append(key)
            instrument_meta[key] = {
                "name": row.get('name'),
                "symbol": str(row.get('symbol') or "").upper(),  # e.g. SBICARD, ICICIPRU
                "strike": row.get('strike'),
                "expiry": row.get('expiry'),
                "instrument_type": row.get('instrument_type'),
                "option_type": row.get('option_type'),
                "lot_size": row.get('lot_size', DEFAULT_LOT_SIZE),
            }

        print(f"✅ Loaded {len(df_fno_stk)} stock option contracts (OPTSTK).")

    # ----- BSE_FO (SENSEX) -----
    df_bse_fo = download_instruments(URL_BSE_FO, "BSE_FO")
    if df_bse_fo.empty:
        print("🔴 No BSE_FO instruments loaded.")
    else:
        df_sensex = df_bse_fo[df_bse_fo['name'].str.contains("SENSEX", case=False, na=False)]
        df_sensex = df_sensex[
            (df_sensex['expiry'] == SENSEX_EXPIRY) &
            (df_sensex['option_type'].isin(['CE', 'PE']))
            ]

        for _, row in df_sensex.iterrows():
            key = row['instrument_key']
            all_keys.append(key)
            instrument_meta[key] = {
                "name": row.get('name'),
                "symbol": "SENSEX",
                "strike": row.get('strike'),
                "expiry": row.get('expiry'),
                "instrument_type": row.get('instrument_type'),
                "option_type": row.get('option_type'),
                "lot_size": row.get('lot_size', DEFAULT_LOT_SIZE),
            }

        print(f"✅ Loaded {len(df_sensex)} SENSEX option contracts.")

    # ----- MCX_FO (CRUDEOIL & NATURALGAS) -----
    df_mcx_fo = download_instruments(URL_MCX_FO, "MCX_FO")
    if df_mcx_fo.empty:
        print("🔴 No MCX_FO instruments loaded.")
    else:
        df_crude = df_mcx_fo[df_mcx_fo['name'].str.contains("CRUDEOIL", case=False, na=False)]
        df_crude = df_crude[df_crude['expiry'] == CRUDE_EXPIRY]

        for _, row in df_crude.iterrows():
            key = row['instrument_key']
            all_keys.append(key)
            instrument_meta[key] = {
                "name": row.get('name'),
                "symbol": "CRUDEOIL",
                "strike": row.get('strike'),
                "expiry": row.get('expiry'),
                "instrument_type": row.get('instrument_type'),
                "option_type": row.get('option_type'),
                "lot_size": row.get('lot_size', DEFAULT_LOT_SIZE),
            }

        print(f"✅ Loaded {len(df_crude)} CRUDEOIL MCX contracts.")

        df_ng = df_mcx_fo[df_mcx_fo['name'].str.contains("NATURALGAS", case=False, na=False)]
        df_ng = df_ng[df_ng['expiry'] == NG_EXPIRY]

        for _, row in df_ng.iterrows():
            key = row['instrument_key']
            all_keys.append(key)
            instrument_meta[key] = {
                "name": row.get('name'),
                "symbol": "NATURALGAS",
                "strike": row.get('strike'),
                "expiry": row.get('expiry'),
                "instrument_type": row.get('instrument_type'),
                "option_type": row.get('option_type'),
                "lot_size": row.get('lot_size', DEFAULT_LOT_SIZE),
            }

        print(f"✅ Loaded {len(df_ng)} NATURALGAS MCX contracts.")

    print(f"\n📦 Total instruments before filtering: {len(all_keys)}")

    # Build list of stock underlyings (for NSE_EQ CMP via Upstox)
    stock_symbols = [
        str(meta.get('symbol')).upper()
        for _, meta in instrument_meta.items()
        if meta.get('instrument_type') == 'OPTSTK' and meta.get('symbol')
    ]
    build_underlying_spots(stock_symbols)

    return all_keys


def parse_underlying_from_symbol(symbol_key):
    """
    Use instrument_meta to extract:
      - underlying symbol (SBICARD, ICICIPRU, NIFTY, SENSEX, etc.)
      - expiry
      - strike
      - option_type (CE/PE)
    If meta missing, fallback to basic parsing.
    """
    meta = instrument_meta.get(symbol_key)
    if meta:
        underlying  = str(meta.get("symbol") or meta.get("name") or "").upper() or None
        expiry      = meta.get("expiry")
        strike      = meta.get("strike")
        option_type = (str(meta.get("option_type") or "").upper() or None)
        return {
            "underlying": underlying,
            "expiry": expiry,
            "strike": float(strike) if strike is not None and not pd.isna(strike) else None,
            "option_type": option_type,
        }

    # Fallback: if we ever pass symbols like "NSE_FO:BLUESTARCO25DEC1760CE"
    parts = symbol_key.split(":", 1)
    raw = parts[1] if len(parts) > 1 else parts[0]

    m = re.match(r"^([A-Z0-9]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$", raw)
    if m:
        return {
            "underlying": m[1].upper(),
            "expiry": m[2].upper(),
            "strike": float(m[3]),
            "option_type": m[4].upper()
        }

    return {
        "underlying": raw.upper(),
        "expiry": None,
        "strike": None,
        "option_type": None
    }


def limit_option_strikes(all_keys):
    """
    For each (underlying, option_type), keep around CMP:
        - Sort strikes for that underlying+option_type
        - CMP from index_spots / underlying_spots
        - ATM index = argmin_i |strike[i] - CMP|
        - Keep strikes in [ATM-3, ATM+3]
    → max 7 strikes per option type.
    Futures (if any) are kept as-is.
    """
    selected_keys = set()
    groups = {}  # (underlying, option_type) -> list of (key, strike)

    for key in all_keys:
        meta        = instrument_meta.get(key, {})
        inst_type   = str(meta.get('instrument_type', '')).upper()
        option_type = str(meta.get('option_type', '')).upper()
        strike      = meta.get('strike')

        # Futures (if you later add FUTSTK): keep as-is
        if "FUT" in inst_type and not option_type:
            selected_keys.add(key)
            continue

        if option_type not in ["CE", "PE"] or strike is None or pd.isna(strike):
            # non-standard → keep
            selected_keys.add(key)
            continue

        underlying = str(meta.get('symbol') or meta.get('name') or "").upper()
        grp_key = (underlying, option_type)
        groups.setdefault(grp_key, []).append((key, float(strike)))

    for (underlying, opt_type), items in groups.items():
        if underlying == "NIFTY":
            cmp_price = index_spots.get("NIFTY")
        elif underlying == "SENSEX":
            cmp_price = index_spots.get("SENSEX")
        else:
            cmp_price = underlying_spots.get(underlying)

        strikes_sorted = sorted(items, key=lambda ks: ks[1])
        strikes_only   = [s for _, s in strikes_sorted]

        if not strikes_only:
            continue

        if not cmp_price or cmp_price <= 0:
            atm_idx = len(strikes_only) // 2
        else:
            atm_idx = min(
                range(len(strikes_only)),
                key=lambda i: abs(strikes_only[i] - cmp_price)
            )

        start_idx = max(0, atm_idx - 3)
        end_idx   = min(len(strikes_only) - 1, atm_idx + 3)

        allowed_strikes = set(strikes_only[start_idx:end_idx + 1])
        for key, strike in strikes_sorted:
            if strike in allowed_strikes:
                selected_keys.add(key)

    print(f"🎯 Total instruments after strict ATM±3-strike limiting: {len(selected_keys)}")
    return [k for k in all_keys if k in selected_keys]

def get_oi_pair_for_symbol(symbol_key, batch_data):
    """
    Return (oi_same, oi_opposite) for the given symbol_key
    using the current quotes batch (batch_data = data['data']).

    - oi_same     = OI of this option
    - oi_opposite = OI of opposite option type (CE <-> PE) for same strike
                    0 if not found in this batch.
    """
    oi_same = 0
    oi_opposite = 0

    if not batch_data:
        return oi_same, oi_opposite

    # OI of this leg (same symbol)
    if symbol_key in batch_data:
        oi_same = batch_data[symbol_key].get("oi", 0) or 0

    # Try simple CE <-> PE flip on the raw key
    opposite_key = None
    if symbol_key.endswith("CE"):
        opposite_key = symbol_key[:-2] + "PE"
    elif symbol_key.endswith("PE"):
        opposite_key = symbol_key[:-2] + "CE"

    # If simple flip failed, fall back to parsing + search
    if opposite_key and opposite_key in batch_data:
        oi_opposite = batch_data[opposite_key].get("oi", 0) or 0
        return oi_same, oi_opposite

    # ---- Fallback: parse & search if naming is weird ----
    parsed     = parse_underlying_from_symbol(symbol_key)
    underlying = parsed["underlying"]
    strike     = parsed["strike"]
    opt_type   = parsed["option_type"]

    if not underlying or strike is None or opt_type not in ("CE", "PE"):
        return oi_same, oi_opposite

    opposite_opt = "PE" if opt_type == "CE" else "CE"

    for k, details in batch_data.items():
        pk = parse_underlying_from_symbol(k)
        if (
                pk["underlying"] == underlying and
                pk["strike"] == strike and
                pk["option_type"] == opposite_opt
        ):
            oi_opposite = details.get("oi", 0) or 0
            break

    return oi_same, oi_opposite

# ================= CORE ANALYSIS =================

def analyze_instrument(symbol_key, data, batch_data):
    global history

    lot_size = get_lot_size_for_symbol(symbol_key)

    parsed = parse_underlying_from_symbol(symbol_key)
    underlying = parsed["underlying"]
    pcr        = underlying_pcr.get(underlying)

    # NEW: get OI for this leg and opposite leg
    oi_same, oi_opposite = get_oi_pair_for_symbol(symbol_key, batch_data)

    # Extract instrument_token from stream data (e.g., "NSE_FO|67718")
    instrument_token = data.get('instrument_token')
    depth = data.get('depth', {})
    bids  = depth.get('buy', [])
    asks  = depth.get('sell', [])

    curr_vol        = data.get('volume', 0)
    curr_oi         = data.get('oi', 0)
    curr_total_buy  = data.get('total_buy_quantity', 0)
    curr_total_sell = data.get('total_sell_quantity', 0)
    ltp             = data.get('last_price', 0)

    curr_bid = get_whale_order(bids, lot_size)
    curr_ask = get_whale_order(asks, lot_size)

    prev_bid = None
    prev_ask = None

    if symbol_key in history:
        prev        = history[symbol_key]
        vol_diff    = curr_vol - prev['vol']
        oi_diff     = curr_oi  - prev['oi']
        prev_bid    = prev['bid']
        prev_ask    = prev['ask']

        # BUYER FILLED
        if prev_bid and not curr_bid:
            prev_qty, prev_price, _, _ = prev_bid

            if vol_diff < (prev_qty * 0.2):
                print(f"\n⚠️  FAKE BUYER REMOVED: {symbol_key}")
                print(f"   ❌ Support at {prev_price} was FAKE.")
            else:
                print(f"\n✅ BIG PLAYER BOUGHT SUCCESSFULLY: {symbol_key} @ {prev_price}")
                moneyness = classify_strike(symbol_key, data)
                record_success(symbol_key, "BUY", prev_qty, prev_price, vol_diff, oi_diff, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token)

        # SELLER FILLED
        if prev_ask and not curr_ask:
            prev_qty, prev_price, _, _ = prev_ask

            if vol_diff < (prev_qty * 0.2):
                print(f"\n⚠️  FAKE SELLER REMOVED: {symbol_key}")
                print(f"   ❌ Resistance at {prev_price} was FAKE.")
            else:
                print(f"\n🔴 BIG PLAYER SOLD SUCCESSFULLY: {symbol_key} @ {prev_price}")
                moneyness = classify_strike(symbol_key, data)
                record_success(symbol_key, "SELL", prev_qty, prev_price, vol_diff, oi_diff, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token)

    # LIVE STATUS (ENTRY)
    if curr_bid and curr_bid != prev_bid:
        qty, price, _, _ = curr_bid
        moneyness = classify_strike(symbol_key, data)
        print(f"\n🚀 BIG BUYER SITTING: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | {moneyness}")
        record_entry(symbol_key, "BUY", qty, price, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token)

    if curr_ask and curr_ask != prev_ask:
        qty, price, _, _ = curr_ask
        moneyness = classify_strike(symbol_key, data)
        print(f"\n🔻 BIG SELLER SITTING: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | {moneyness}")
        record_entry(symbol_key, "SELL", qty, price, ltp, moneyness, pcr, oi_same, oi_opposite, instrument_token)

    history[symbol_key] = {
        'vol': curr_vol,
        'oi': curr_oi,
        'total_buy': curr_total_buy,
        'total_sell': curr_total_sell,
        'bid': curr_bid,
        'ask': curr_ask
    }


# ================= MAIN LOOP =================

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
    global ACCESS_TOKEN
    import pytz
    from datetime import datetime

    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)

    # If between 6:00 AM and 6:01 AM, refresh token
    if now.hour == 6 and now.minute == 0:
        print(f"\n🔄 Daily token refresh at {now.strftime('%H:%M:%S %Z')}")
        try:
            ACCESS_TOKEN = db.get_access_token(account_id=6)
            print(f"✅ Token refreshed: {ACCESS_TOKEN[:30]}...")
            time.sleep(65)  # Wait 65 seconds to avoid re-refreshing
        except Exception as e:
            print(f"❌ Token refresh failed: {e}")


def main():
    print("\n🟢 SYSTEM STARTING (FULL TRADING ASSISTANT)...")
    print("📊 Timezone: Asia/Kolkata (IST)")
    print("📅 Operating hours: Monday-Friday, 9:15 AM - 3:30 PM")
    print("🔄 Token refresh: Daily at 6:00 AM\n")

    # Create tables if they don't exist
    create_whale_tables()

    load_existing_successes()
    load_existing_entries()
    get_spot_indices()     # NIFTY & SENSEX via Upstox

    # Fetch all instruments once
    all_keys = build_all_instruments()

    # Strict filter: only 7 strikes per underlying+option_type
    all_keys = limit_option_strikes(all_keys)

    if not all_keys:
        print("🔴 No instruments to scan after filtering. Exiting.")
        return

    key_chunks = list(chunked(all_keys, MAX_KEYS_PER_CALL))
    print(f"📚 Total chunks: {len(key_chunks)} (max {MAX_KEYS_PER_CALL} instruments per API call)")

    # Refresh token before first run
    print("🔄 Fetching fresh token...")
    try:
        global ACCESS_TOKEN
        ACCESS_TOKEN = db.get_access_token(account_id=6)
        print(f"✅ Token obtained: {ACCESS_TOKEN[:30]}...\n")
    except Exception as e:
        print(f"❌ Failed to get token: {e}\n")

    spinner = ['|', '/', '-', '\\']
    idx = 0

    print("🔎 SCANNING FOR TRADES...\n")

    try:
        while True:
            # Check if market is open
            if not is_market_open():
                import pytz
                from datetime import datetime
                IST = pytz.timezone('Asia/Kolkata')
                print(f"⏸️  Market is closed ({datetime.now(IST).strftime('%H:%M %A')})")
                wait_until_market_open()

                # Market just opened - fetch fresh token from DB (it was updated while closed)
                print(f"\n✨ Market reopening - fetching fresh token from database...")
                try:
                    ACCESS_TOKEN = db.get_access_token(account_id=6)
                    print(f"✅ Fresh token obtained: {ACCESS_TOKEN[:30]}...\n")
                except Exception as e:
                    print(f"❌ Failed to get fresh token: {e}\n")

                # Refresh token at 6 AM daily if needed
                refresh_daily_token()

            # Refresh token at 6 AM daily if needed
            refresh_daily_token()

            # Update headers with latest token (in case it was refreshed)
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
            url = f"{BASE_URL_V2}/market-quote/quotes"

            # ...existing code...
            for chunk in key_chunks:
                sys.stdout.write(f"\r{spinner[idx]} Scanning {len(chunk)} symbols in this batch... ")
                sys.stdout.flush()
                idx = (idx + 1) % 4

                try:
                    payload = {'instrument_key': ",".join(chunk)}
                    resp = requests.get(url, headers=headers, params=payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get('status') == 'success':
                            chunk_data = data['data']

                            # 1) Build CE/PE OI sums per underlying for this batch
                            ce_pe_agg = {}  # underlying -> {"CE": total_oi, "PE": total_oi}
                            for sym, details in chunk_data.items():
                                parsed     = parse_underlying_from_symbol(sym)
                                underlying = parsed["underlying"]
                                opt_type   = parsed["option_type"]

                                if opt_type not in ("CE", "PE"):
                                    continue

                                oi = details.get("oi", 0) or 0
                                d = ce_pe_agg.setdefault(underlying, {"CE": 0.0, "PE": 0.0})
                                d[opt_type] += float(oi)

                            # 2) Compute PCR for each underlying in this batch
                            for underlying, vals in ce_pe_agg.items():
                                ce_oi = vals["CE"]
                                pe_oi = vals["PE"]
                                if ce_oi > 0:
                                    underlying_pcr[underlying] = pe_oi / ce_oi
                                else:
                                    underlying_pcr[underlying] = None

                            # 3) Analyze each instrument in this batch
                            for sym, details in chunk_data.items():
                                analyze_instrument(sym, details, chunk_data)

                except Exception as e:
                    print(f"\n⚠️ Error in batch: {e}")
                    time.sleep(1)

                time.sleep(0.5)  # delay between chunks

            time.sleep(1.5)      # delay after full cycle

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")


if __name__ == "__main__":
    main()

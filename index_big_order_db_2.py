"""
index_big_order_db_2.py  –  AngelOne Edition
Tables : whale_entries_2 / whale_successes_2
Version: 2
"""

import re
import sys
import time
import requests
import pandas as pd
import os
import pyotp
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import psycopg2
from contextlib import contextmanager

from services.database import DatabaseService

# ===================== USER CONFIGURATION =====================
# AngelOne credentials
ANGELONE_API_KEY     = "WQl601Ee"
ANGELONE_SECRET_KEY  = "cf422f7a-7eb8-46b3-8558-a4cf31139021"
ANGELONE_CLIENT_ID   = os.getenv('ANGELONE_CLIENT_ID',   'S58264557')   # e.g. "A123456"
ANGELONE_PASSWORD    = os.getenv('ANGELONE_PASSWORD',    '1282')   # login PIN / password
ANGELONE_TOTP_SECRET = os.getenv('ANGELONE_TOTP_SECRET', 'W6G2U6GQW4QFC7KXSPUCR5E5GA')   # base32 TOTP secret (optional)

# Expiry dates in AngelOne format  DDMMMYYYY  (uppercase)
NIFTY_EXPIRY      = os.getenv('NIFTY_EXPIRY_2',      "28APR2026")
SENSEX_EXPIRY     = os.getenv('SENSEX_EXPIRY_2',     "30APR2026")
STOCK_FNO_EXPIRY  = os.getenv('STOCK_FNO_EXPIRY_2',  "30APR2029")
CRUDE_EXPIRY      = os.getenv('CRUDE_EXPIRY_2',      "16DEC2029")
NG_EXPIRY         = os.getenv('NG_EXPIRY_2',         "23DEC2029")

MIN_ORDER_VALUE   = 950000   # Rs 9.5 L
INCLUDE_FUTURES   = False

# AngelOne API base
BASE_URL_ANGEL  = "https://apiconnect.angelbroking.com"
INSTRUMENT_URL  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

MAX_TOKENS_PER_CALL = 50    # AngelOne FULL mode: max 50 tokens per exchange per call
DEFAULT_LOT_SIZE    = 50

# Detection tuning
MIN_WHALE_PERSISTENCE_S  = 3
MIN_FILL_VOL_RATIO       = 0.3
FUT_MIN_TOTAL_LOTS       = 1035
MIN_OI_FOR_ENTRY         = 500
MIN_OI_DELTA_FOR_SUCCESS = 300

# ===================== GLOBAL STATE =====================
ACCESS_TOKEN      = None   # AngelOne JWT token

underlying_pcr    = {}     # underlying -> PCR
history           = {}     # per-key snapshot
instrument_meta   = {}     # "NFO:12345" -> metadata dict
underlying_spots  = {}     # underlying symbol -> CMP
index_spots       = {}     # NIFTY / SENSEX spot

success_keys      = set()
entry_keys        = set()
success_positions = {}

db = DatabaseService()
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:LZjgyzthYpacmWhOSAnDMnMWxkntEEqe@switchback.proxy.rlwy.net:22297/railway'
)

# ===================== DATABASE HELPERS =====================
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
    """Create whale_entries_2 and whale_successes_2 tables if they don't exist."""
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
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_unique_key        ON whale_entries_2(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_timestamp         ON whale_entries_2(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_symbol            ON whale_entries_2(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_entries_2_instrument_token  ON whale_entries_2(instrument_token);
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
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_unique_key        ON whale_successes_2(unique_key);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_timestamp         ON whale_successes_2(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_symbol            ON whale_successes_2(symbol);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_status            ON whale_successes_2(status);
                CREATE INDEX IF NOT EXISTS idx_whale_successes_2_instrument_token  ON whale_successes_2(instrument_token);
            """)
    print("Whale tables (v2) created/verified")


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
    """Login to AngelOne SmartAPI and store JWT token globally."""
    global ACCESS_TOKEN
    client_id   = ANGELONE_CLIENT_ID
    password    = ANGELONE_PASSWORD
    totp_secret = ANGELONE_TOTP_SECRET

    if not client_id or not password:
        print("Set env vars: ANGELONE_CLIENT_ID, ANGELONE_PASSWORD (and optionally ANGELONE_TOTP_SECRET)")
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
            print(f"AngelOne login failed: {data.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"AngelOne login error: {e}")
        return None


# ===================== DB RECORD HELPERS =====================
def load_existing_successes():
    global success_keys
    success_keys.clear()
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unique_key FROM whale_successes_2")
                for row in cur.fetchall():
                    success_keys.add(row[0])
        print(f"Loaded {len(success_keys)} existing success records")
    except Exception as e:
        print(f"Could not load success records: {e}")


def load_existing_entries():
    global entry_keys
    entry_keys.clear()
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unique_key FROM whale_entries_2")
                for row in cur.fetchall():
                    entry_keys.add(row[0])
        print(f"Loaded {len(entry_keys)} existing entry records")
    except Exception as e:
        print(f"Could not load entry records: {e}")


def record_success(symbol, side, qty, price, vol_diff, oi_diff, ltp,
                   moneyness, pcr, oi_same, oi_opposite, instrument_token=None):
    global success_keys
    parsed            = parse_underlying_from_symbol(symbol)
    underlying        = parsed["underlying"]
    strike            = parsed["strike"]
    option_type       = parsed["option_type"]
    meta              = instrument_meta.get(symbol, {})
    is_fut            = is_future_symbol(symbol)
    expiry            = meta.get('expiry')
    option_type_value = "FU" if is_fut else option_type
    strike_value      = None if is_fut else strike

    if is_fut:
        if underlying is None or price is None:
            return
        unique_key = f"{underlying}|FUT|{expiry or 'NA'}|{price}"
    else:
        if underlying is None or strike is None or option_type_value is None or price is None:
            return
        unique_key = f"{underlying}|{strike}|{option_type_value}|{price}"

    if unique_key in success_keys:
        return
    success_keys.add(unique_key)

    ts = datetime.now()
    underlying_price, stock_pct_change = get_daily_pct_change_for_underlying(symbol)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO whale_successes_2 (
                        timestamp, instrument_key, instrument_token, symbol, strike, option_type,
                        side, qty, price, ltp, vol_diff, oi_diff, moneyness, pcr,
                        oi_same, oi_opposite, stock_current_price, stock_pct_change_today, unique_key
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    ts, symbol, instrument_token, underlying, strike_value, option_type_value,
                    str(side).upper(), qty, float(price), float(ltp),
                    vol_diff, oi_diff, moneyness, pcr,
                    oi_same, oi_opposite, underlying_price, stock_pct_change, unique_key
                ))
    except Exception as e:
        print(f"Could not write success record: {e}")


def record_entry(symbol, side, qty, price, ltp,
                 moneyness, pcr, oi_same, oi_opposite, instrument_token=None):
    global entry_keys
    parsed            = parse_underlying_from_symbol(symbol)
    underlying        = parsed["underlying"]
    strike            = parsed["strike"]
    option_type       = parsed["option_type"]
    meta              = instrument_meta.get(symbol, {})
    is_fut            = is_future_symbol(symbol)
    expiry            = meta.get('expiry')
    option_type_value = "FU" if is_fut else option_type

    if is_fut:
        if underlying is None or price is None:
            return
        unique_key = f"{underlying}|FUT|{expiry or 'NA'}|{round(float(price), 2)}"
    else:
        if underlying is None or strike is None or option_type_value is None or price is None:
            return
        unique_key = f"{underlying}|{strike}|{option_type_value}|{round(float(price), 2)}"

    if unique_key in entry_keys:
        return
    entry_keys.add(unique_key)

    ts = datetime.now()
    underlying_price, stock_pct_change = get_daily_pct_change_for_underlying(symbol)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO whale_entries_2 (
                        timestamp, instrument_key, instrument_token, symbol, option_type,
                        side, qty, price, ltp, moneyness, pcr, oi_same, oi_opposite,
                        stock_current_price, stock_pct_change_today, unique_key
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    ts, symbol, instrument_token, underlying, option_type_value,
                    str(side).upper(), qty, float(price), float(ltp),
                    moneyness, pcr, oi_same, oi_opposite,
                    underlying_price, stock_pct_change, unique_key
                ))
    except Exception as e:
        print(f"Could not write entry record: {e}")


# ===================== ANGELONE MARKET DATA =====================
def get_spot_indices():
    """Fetch NIFTY (token 26000/NSE) and SENSEX (token 1/BSE) spot via AngelOne."""
    global index_spots
    try:
        resp = requests.post(
            f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
            headers=get_angel_headers(with_auth=True),
            json={"mode": "LTP", "exchangeTokens": {"NSE": ["26000"], "BSE": ["1"]}},
            timeout=10
        )
        data = resp.json()
        if data.get('status'):
            for item in data.get('data', {}).get('fetched', []):
                token = str(item.get('symbolToken', ''))
                ltp   = float(item.get('ltp', 0) or 0)
                if token == "26000":
                    index_spots['NIFTY']  = ltp
                elif token == "1":
                    index_spots['SENSEX'] = ltp
        print(f"Index spots -> NIFTY: {index_spots.get('NIFTY')}, SENSEX: {index_spots.get('SENSEX')}")
    except Exception as e:
        print(f"Could not fetch index spots: {e}")
    index_spots.setdefault('NIFTY',  24000.0)
    index_spots.setdefault('SENSEX', 80000.0)


def fetch_ltp_for_eq_tokens(nse_tokens: list) -> dict:
    """Fetch NSE EQ LTP for a list of AngelOne tokens -> { token: ltp }."""
    results = {}
    if not nse_tokens:
        return results
    for i in range(0, len(nse_tokens), MAX_TOKENS_PER_CALL):
        chunk = nse_tokens[i:i + MAX_TOKENS_PER_CALL]
        try:
            resp = requests.post(
                f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
                headers=get_angel_headers(with_auth=True),
                json={"mode": "LTP", "exchangeTokens": {"NSE": chunk}},
                timeout=10
            )
            data = resp.json()
            if data.get('status'):
                for item in data.get('data', {}).get('fetched', []):
                    tok = str(item.get('symbolToken', ''))
                    results[tok] = float(item.get('ltp', 0) or 0)
        except Exception as e:
            print(f"Error fetching EQ LTP: {e}")
    return results


def build_underlying_spots(stock_symbols):
    """Fetch CMP for all F&O stock underlyings via AngelOne NSE EQ tokens."""
    global underlying_spots
    eq_token_map = getattr(build_underlying_spots, '_eq_token_map', {})
    if not eq_token_map:
        print("No EQ token map available — underlying spots will use yfinance fallback.")
        return
    tokens = list(eq_token_map.keys())
    print(f"Fetching underlying NSE EQ CMP for {len(tokens)} stocks (AngelOne)...")
    ltp_results = fetch_ltp_for_eq_tokens(tokens)
    count = 0
    for token, ltp in ltp_results.items():
        name = eq_token_map.get(token)
        if name and ltp > 0:
            underlying_spots[name] = ltp
            count += 1
    print(f"Loaded CMP for {count} F&O underlyings via AngelOne.")


# ===================== INSTRUMENT BUILDING =====================
def _normalize_strike(strike_val: float) -> float:
    """AngelOne may store strikes *100 for some instruments."""
    if strike_val and strike_val > 1_000_000:
        return strike_val / 100.0
    return strike_val


def _extract_option_type(trading_symbol: str) -> str:
    ts = trading_symbol.strip().upper()
    if ts.endswith('CE'):
        return 'CE'
    if ts.endswith('PE'):
        return 'PE'
    return ''


def download_angelone_instruments() -> pd.DataFrame:
    print("Downloading AngelOne ScripMaster...")
    try:
        resp = requests.get(INSTRUMENT_URL, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        df.columns = [c.lower().strip() for c in df.columns]
        print(f"Loaded {len(df)} instruments from AngelOne ScripMaster.")
        return df
    except Exception as e:
        print(f"Error downloading AngelOne ScripMaster: {e}")
        return pd.DataFrame()


def build_all_instruments():
    global instrument_meta
    instrument_meta.clear()
    all_keys = []

    df_all = download_angelone_instruments()
    if df_all.empty:
        print("No instruments loaded.")
        return []

    # Normalise
    if 'expiry' in df_all.columns:
        df_all['expiry'] = df_all['expiry'].astype(str).str.strip().str.upper()
    if 'strike' in df_all.columns:
        df_all['strike'] = pd.to_numeric(df_all['strike'], errors='coerce')
    if 'instrumenttype' not in df_all.columns:
        df_all['instrumenttype'] = ''
    if 'exch_seg' not in df_all.columns:
        df_all['exch_seg'] = ''

    eq_token_map = {}   # token -> underlying_name for NSE EQ spot lookup

    # ---------- NFO (NSE F&O) ----------
    df_nfo = df_all[df_all['exch_seg'] == 'NFO'].copy()

    # NIFTY options
    df_nifty = df_nfo[
        (df_nfo['name'].str.upper() == 'NIFTY') &
        (df_nfo['expiry'] == NIFTY_EXPIRY) &
        (df_nfo['instrumenttype'].str.upper() == 'OPTIDX')
    ]
    for _, row in df_nifty.iterrows():
        token = str(row['token'])
        key   = f"NFO:{token}"
        all_keys.append(key)
        instrument_meta[key] = {
            "name": "NIFTY", "symbol": "NIFTY",
            "token": token, "exchange": "NFO",
            "trading_symbol": str(row.get('symbol', '')),
            "strike": _normalize_strike(float(row.get('strike', 0) or 0)),
            "expiry": str(row.get('expiry', '')),
            "instrument_type": "OPTIDX",
            "option_type": _extract_option_type(str(row.get('symbol', ''))),
            "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
        }
    print(f"Loaded {len(df_nifty)} NIFTY option contracts.")

    # Stock options (OPTSTK)
    df_stk = df_nfo[
        (df_nfo['instrumenttype'].str.upper() == 'OPTSTK') &
        (df_nfo['expiry'] == STOCK_FNO_EXPIRY)
    ]
    for _, row in df_stk.iterrows():
        token      = str(row['token'])
        key        = f"NFO:{token}"
        underlying = str(row.get('name', '')).upper()
        all_keys.append(key)
        instrument_meta[key] = {
            "name": underlying, "symbol": underlying,
            "token": token, "exchange": "NFO",
            "trading_symbol": str(row.get('symbol', '')),
            "strike": _normalize_strike(float(row.get('strike', 0) or 0)),
            "expiry": str(row.get('expiry', '')),
            "instrument_type": "OPTSTK",
            "option_type": _extract_option_type(str(row.get('symbol', ''))),
            "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
        }
    print(f"Loaded {len(df_stk)} stock option contracts (OPTSTK).")

    # Futures (nearest expiry per symbol)
    if INCLUDE_FUTURES:
        df_fut = df_nfo[df_nfo['instrumenttype'].str.upper().isin(['FUTIDX', 'FUTSTK'])].copy()
        if not df_fut.empty:
            try:
                df_fut['expiry_dt'] = pd.to_datetime(df_fut['expiry'], format='%d%b%Y', errors='coerce').dt.date
                today   = datetime.now().date()
                df_fut  = df_fut[df_fut['expiry_dt'] >= today]
                df_fut['min_expiry'] = df_fut.groupby('name')['expiry_dt'].transform('min')
                df_fut  = df_fut[df_fut['expiry_dt'] == df_fut['min_expiry']]
                for _, row in df_fut.iterrows():
                    token      = str(row['token'])
                    key        = f"NFO:{token}"
                    underlying = str(row.get('name', '')).upper()
                    all_keys.append(key)
                    instrument_meta[key] = {
                        "name": underlying, "symbol": underlying,
                        "token": token, "exchange": "NFO",
                        "trading_symbol": str(row.get('symbol', '')),
                        "strike": None, "expiry": str(row.get('expiry', '')),
                        "instrument_type": str(row.get('instrumenttype', '')).upper(),
                        "option_type": None,
                        "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
                    }
                print(f"Loaded {len(df_fut)} near-month futures (NFO).")
            except Exception as e:
                print(f"Futures loading error: {e}")
    else:
        print("Skipping futures (INCLUDE_FUTURES = False).")

    # ---------- BFO (BSE F&O) — SENSEX ----------
    df_bfo = df_all[df_all['exch_seg'] == 'BFO'].copy()
    if not df_bfo.empty:
        df_sensex = df_bfo[
            (df_bfo['name'].str.upper().str.contains('SENSEX', na=False)) &
            (df_bfo['expiry'] == SENSEX_EXPIRY) &
            (df_bfo['instrumenttype'].str.upper() == 'OPTIDX')
        ]
        for _, row in df_sensex.iterrows():
            token = str(row['token'])
            key   = f"BFO:{token}"
            all_keys.append(key)
            instrument_meta[key] = {
                "name": "SENSEX", "symbol": "SENSEX",
                "token": token, "exchange": "BFO",
                "trading_symbol": str(row.get('symbol', '')),
                "strike": _normalize_strike(float(row.get('strike', 0) or 0)),
                "expiry": str(row.get('expiry', '')),
                "instrument_type": "OPTIDX",
                "option_type": _extract_option_type(str(row.get('symbol', ''))),
                "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
            }
        print(f"Loaded {len(df_sensex)} SENSEX option contracts.")

    # ---------- MCX ----------
    df_mcx = df_all[df_all['exch_seg'] == 'MCX'].copy()
    if not df_mcx.empty:
        df_crude = df_mcx[
            (df_mcx['name'].str.upper().str.contains('CRUDEOIL', na=False)) &
            (df_mcx['expiry'] == CRUDE_EXPIRY)
        ]
        for _, row in df_crude.iterrows():
            token = str(row['token'])
            key   = f"MCX:{token}"
            all_keys.append(key)
            instrument_meta[key] = {
                "name": "CRUDEOIL", "symbol": "CRUDEOIL",
                "token": token, "exchange": "MCX",
                "trading_symbol": str(row.get('symbol', '')),
                "strike": _normalize_strike(float(row.get('strike', 0) or 0)),
                "expiry": str(row.get('expiry', '')),
                "instrument_type": str(row.get('instrumenttype', '')).upper(),
                "option_type": _extract_option_type(str(row.get('symbol', ''))),
                "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
            }
        print(f"Loaded {len(df_crude)} CRUDEOIL MCX contracts.")

        df_ng = df_mcx[
            (df_mcx['name'].str.upper().str.contains('NATURALGAS', na=False)) &
            (df_mcx['expiry'] == NG_EXPIRY)
        ]
        for _, row in df_ng.iterrows():
            token = str(row['token'])
            key   = f"MCX:{token}"
            all_keys.append(key)
            instrument_meta[key] = {
                "name": "NATURALGAS", "symbol": "NATURALGAS",
                "token": token, "exchange": "MCX",
                "trading_symbol": str(row.get('symbol', '')),
                "strike": _normalize_strike(float(row.get('strike', 0) or 0)),
                "expiry": str(row.get('expiry', '')),
                "instrument_type": str(row.get('instrumenttype', '')).upper(),
                "option_type": _extract_option_type(str(row.get('symbol', ''))),
                "lot_size": int(float(row.get('lotsize', DEFAULT_LOT_SIZE) or DEFAULT_LOT_SIZE)),
            }
        print(f"Loaded {len(df_ng)} NATURALGAS MCX contracts.")

    print(f"\nTotal instruments before filtering: {len(all_keys)}")

    # Build EQ token map for underlying spot prices
    df_eq = df_all[(df_all['exch_seg'] == 'NSE') & (df_all['instrumenttype'].str.upper() == 'EQ')]
    if not df_eq.empty:
        stock_underlyings = {
            str(m.get('symbol')).upper()
            for m in instrument_meta.values()
            if m.get('instrument_type') == 'OPTSTK' and m.get('symbol')
        }
        for _, row in df_eq.iterrows():
            name = str(row.get('name', '')).upper()
            if name in stock_underlyings:
                eq_token_map[str(row['token'])] = name

    build_underlying_spots._eq_token_map = eq_token_map
    build_underlying_spots(set(
        m.get('symbol') for m in instrument_meta.values()
        if m.get('instrument_type') == 'OPTSTK'
    ))

    return all_keys


# ===================== PARSE / EXTRACT HELPERS =====================
def parse_underlying_from_symbol(symbol_key):
    meta = instrument_meta.get(symbol_key)
    if meta:
        strike_raw = meta.get("strike")
        return {
            "underlying":  str(meta.get("symbol") or meta.get("name") or "").upper() or None,
            "expiry":      meta.get("expiry"),
            "strike":      float(strike_raw) if strike_raw is not None and not pd.isna(strike_raw) else None,
            "option_type": (str(meta.get("option_type") or "").upper() or None),
        }
    return {"underlying": symbol_key.upper(), "expiry": None, "strike": None, "option_type": None}


def extract_underlying_from_key(symbol_key: str) -> str:
    meta = instrument_meta.get(symbol_key)
    if meta:
        return str(meta.get("symbol") or meta.get("name") or "").upper()
    return symbol_key.upper()


def is_future_symbol(symbol_key: str) -> bool:
    meta  = instrument_meta.get(symbol_key, {})
    itype = str(meta.get('instrument_type', '')).upper()
    return 'FUT' in itype


def get_lot_size_for_symbol(symbol_key):
    meta = instrument_meta.get(symbol_key, {})
    lot  = meta.get('lot_size')
    try:
        if lot is not None and float(lot) > 0:
            return float(lot)
    except Exception:
        pass
    return DEFAULT_LOT_SIZE


# ===================== UNDERLYING SPOT / PCT CHANGE =====================
def get_daily_pct_change_for_underlying(symbol):
    underlying = extract_underlying_from_key(symbol)
    if not underlying:
        return None, None

    if underlying in ("NIFTY", "SENSEX"):
        token    = "26000" if underlying == "NIFTY" else "1"
        exchange = "NSE"   if underlying == "NIFTY" else "BSE"
        try:
            resp = requests.post(
                f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
                headers=get_angel_headers(with_auth=True),
                json={"mode": "FULL", "exchangeTokens": {exchange: [token]}},
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

    # Stocks -> yfinance
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


def get_underlying_cmp(symbol, meta=None, data=None):
    global underlying_spots
    underlying = extract_underlying_from_key(symbol)
    if not underlying:
        return None

    if underlying == "NIFTY":
        price = index_spots.get("NIFTY")
    elif underlying == "SENSEX":
        price = index_spots.get("SENSEX")
    else:
        price = underlying_spots.get(underlying)
        if price is None:
            try:
                hist = yf.Ticker(underlying + ".NS").history(period="1d", interval="1m")
                if hist is not None and not hist.empty:
                    price = float(hist["Close"].iloc[-1])
                    underlying_spots[underlying] = price
            except Exception:
                price = None

    if (price is None or price <= 0) and data is not None:
        live = data.get("underlying_price") or data.get("underlying_value")
        try:
            if live is not None:
                price = float(live)
        except Exception:
            pass

    if price is None:
        return None
    try:
        price = float(price)
        return price if price > 0 else None
    except Exception:
        return None


# ===================== STRIKE CLASSIFICATION =====================
def classify_strike(symbol_key, data):
    meta        = instrument_meta.get(symbol_key, {})
    parsed      = parse_underlying_from_symbol(symbol_key)
    option_type = (parsed["option_type"] or str(meta.get("option_type", ""))).upper()
    strike      = parsed["strike"] if parsed["strike"] is not None else meta.get("strike")

    if option_type not in ("CE", "PE"):
        return "UNKNOWN"
    if strike is None:
        return "UNKNOWN"

    strike           = float(strike)
    underlying_price = get_underlying_cmp(symbol_key, meta, data)
    if not underlying_price or underlying_price <= 0:
        return "UNKNOWN"

    diff     = strike - underlying_price
    rel_diff = abs(diff) / underlying_price

    if rel_diff <= 0.01:
        return "ATM"
    if option_type == "CE":
        if diff < 0:
            return "ITM" if rel_diff <= 0.03 else "DEEP_ITM"
        else:
            return "OTM" if rel_diff <= 0.03 else "DEEP_OTM"
    else:
        if diff > 0:
            return "ITM" if rel_diff <= 0.03 else "DEEP_ITM"
        else:
            return "OTM" if rel_diff <= 0.03 else "DEEP_OTM"


# ===================== WHALE DETECTION =====================
def get_whale_order(depth_list, lot_size, symbol_key=None):
    if not depth_list:
        return None
    top_levels = depth_list[:2]
    biggest    = max(top_levels, key=lambda x: x.get('quantity', 0))

    qty    = biggest.get('quantity', 0)
    orders = biggest.get('orders', 0)
    price  = biggest.get('price', 0)

    if orders == 0 or price == 0 or price < 10:
        return None

    total_order_value = qty * price
    total_lots        = qty / float(lot_size) if lot_size else 0.0
    avg_qty           = qty / orders
    avg_lots          = avg_qty / float(lot_size) if lot_size else 0.0

    if symbol_key and is_future_symbol(symbol_key):
        if total_lots <= FUT_MIN_TOTAL_LOTS:
            return None
        return (qty, price, orders, avg_lots)
    else:
        if total_lots <= 50:
            return None

    if total_order_value >= MIN_ORDER_VALUE and avg_lots >= 1.5:
        avg_qty  = qty / orders
        avg_lots = avg_qty / lot_size
        if avg_lots >= 1.5:
            return (qty, price, orders, avg_lots)
    return None


# ===================== OI PAIR =====================
def get_oi_pair_for_symbol(symbol_key, batch_data):
    oi_same     = 0
    oi_opposite = 0
    if not batch_data:
        return oi_same, oi_opposite

    if symbol_key in batch_data:
        oi_same = batch_data[symbol_key].get("oi", 0) or 0

    parsed     = parse_underlying_from_symbol(symbol_key)
    underlying = parsed["underlying"]
    strike     = parsed["strike"]
    opt_type   = parsed["option_type"]

    if not underlying or strike is None or opt_type not in ("CE", "PE"):
        return oi_same, oi_opposite

    opposite_opt = "PE" if opt_type == "CE" else "CE"
    for k, details in batch_data.items():
        pk = parse_underlying_from_symbol(k)
        if (pk["underlying"] == underlying and
                pk["strike"] == strike and
                pk["option_type"] == opposite_opt):
            oi_opposite = details.get("oi", 0) or 0
            break

    return oi_same, oi_opposite


# ===================== UTILITY =====================
def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def limit_option_strikes(all_keys):
    selected_keys = set()
    groups        = {}

    for key in all_keys:
        meta        = instrument_meta.get(key, {})
        inst_type   = str(meta.get('instrument_type', '')).upper()
        option_type = str(meta.get('option_type', '')).upper()
        strike      = meta.get('strike')

        if "FUT" in inst_type and not option_type:
            selected_keys.add(key)
            continue
        if option_type not in ["CE", "PE"] or strike is None or pd.isna(strike):
            selected_keys.add(key)
            continue

        underlying = str(meta.get('symbol') or meta.get('name') or "").upper()
        groups.setdefault((underlying, option_type), []).append((key, float(strike)))

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
            atm_idx = min(range(len(strikes_only)),
                          key=lambda i: abs(strikes_only[i] - cmp_price))

        atm_start = max(0, atm_idx - 7)
        atm_end   = min(len(strikes_only) - 1, atm_idx + 7)
        allowed   = set(range(atm_start, atm_end + 1))

        if opt_type == "CE":
            if atm_idx > 0:                            allowed.add(atm_idx - 1)
            if atm_idx + 7 < len(strikes_only):        allowed.add(atm_idx + 7)
        else:
            if atm_idx < len(strikes_only) - 1:        allowed.add(atm_idx + 1)
            if atm_idx - 7 >= 0:                       allowed.add(atm_idx - 7)

        allowed_strikes = {strikes_only[i] for i in allowed}
        for key, strike in strikes_sorted:
            if strike in allowed_strikes:
                selected_keys.add(key)

    print(f"Total instruments after ATM filtering: {len(selected_keys)}")
    return [k for k in all_keys if k in selected_keys]


# ===================== ANGELONE QUOTE FETCH =====================
def fetch_angel_quotes(keys_by_exchange: dict) -> dict:
    """
    Fetch FULL quotes from AngelOne.
    keys_by_exchange: { "NFO": ["tok1","tok2"], "BFO": ["tok3"], ... }
    Returns: { "NFO:tok1": normalised_data_dict, ... }
    AngelOne FULL response fields mapped:
      ltp            -> last_price
      tradeVolume    -> volume
      opnInterest    -> oi
      totBuyQuan     -> total_buy_quantity
      totSellQuan    -> total_sell_quantity
      depth.buy/sell -> depth.buy/sell  (same structure: price, quantity, orders)
    """
    result = {}
    if not keys_by_exchange:
        return result
    try:
        resp = requests.post(
            f"{BASE_URL_ANGEL}/rest/secure/angelbroking/market/v1/quote/",
            headers=get_angel_headers(with_auth=True),
            json={"mode": "FULL", "exchangeTokens": keys_by_exchange},
            timeout=15
        )
        data = resp.json()
        if data.get('status'):
            for item in data.get('data', {}).get('fetched', []):
                exchange = str(item.get('exchange', ''))
                token    = str(item.get('symbolToken', ''))
                key      = f"{exchange}:{token}"
                result[key] = {
                    'last_price':          float(item.get('ltp', 0) or 0),
                    'volume':              int(item.get('tradeVolume', 0) or 0),
                    'oi':                  int(item.get('opnInterest', 0) or 0),
                    'total_buy_quantity':  int(item.get('totBuyQuan', 0) or 0),
                    'total_sell_quantity': int(item.get('totSellQuan', 0) or 0),
                    'instrument_token':    token,
                    'open':                float(item.get('open', 0) or 0),
                    'high':                float(item.get('high', 0) or 0),
                    'low':                 float(item.get('low', 0) or 0),
                    'close':               float(item.get('close', 0) or 0),
                    'depth': {
                        'buy':  item.get('depth', {}).get('buy', []),
                        'sell': item.get('depth', {}).get('sell', []),
                    },
                }
        else:
            print(f"AngelOne quote API error: {data.get('message', '')}")
    except Exception as e:
        print(f"AngelOne fetch error: {e}")
    return result


# ===================== CORE ANALYSIS =====================
_depth_debug_printed = False   # print depth sample once at startup to verify AngelOne field names

def analyze_instrument(symbol_key, data, batch_data):
    global history, _depth_debug_printed

    lot_size   = get_lot_size_for_symbol(symbol_key)
    parsed     = parse_underlying_from_symbol(symbol_key)
    underlying = parsed["underlying"]
    pcr        = underlying_pcr.get(underlying)
    is_fut     = is_future_symbol(symbol_key)

    oi_same, oi_opposite = get_oi_pair_for_symbol(symbol_key, batch_data)

    instrument_token = data.get('instrument_token')
    depth            = data.get('depth', {})
    bids             = depth.get('buy', [])
    asks             = depth.get('sell', [])

    # --- DEBUG: print a sample depth record once to verify AngelOne field names ---
    if not _depth_debug_printed and (bids or asks):
        sample = bids[0] if bids else asks[0]
        print(f"\n[DEBUG] AngelOne depth sample fields: {list(sample.keys())} | values: {sample}")
        _depth_debug_printed = True

    curr_vol        = data.get('volume', 0) or 0
    curr_oi         = data.get('oi', 0) or 0
    curr_total_buy  = data.get('total_buy_quantity', 0) or 0
    curr_total_sell = data.get('total_sell_quantity', 0) or 0
    ltp             = data.get('last_price', 0) or 0

    curr_bid = get_whale_order(bids, lot_size, symbol_key)
    curr_ask = get_whale_order(asks, lot_size, symbol_key)

    now_ts        = time.time()
    prev_bid      = prev_ask = None
    prev_seen_bid = prev_seen_ask = None
    vol_diff      = oi_diff = 0

    if symbol_key in history:
        prev          = history[symbol_key]
        vol_diff      = curr_vol - prev['vol']
        oi_diff       = curr_oi  - prev['oi']
        prev_bid      = prev['bid']
        prev_ask      = prev['ask']
        prev_seen_bid = prev.get('seen_bid_ts')
        prev_seen_ask = prev.get('seen_ask_ts')

        # Guard: if cumulative volume never moved, vol_diff is meaningless — skip fill detection
        vol_is_stale = (curr_vol == 0 or curr_vol == prev['vol'])

        # BUYER FILLED
        if prev_bid and not curr_bid:
            prev_qty, prev_price, _, _ = prev_bid
            persisted = prev_seen_bid and (now_ts - prev_seen_bid) >= MIN_WHALE_PERSISTENCE_S
            if not persisted:
                print(f"\n⚠️  BUYER LEFT QUICKLY (likely spoof): {symbol_key} @ {prev_price}")
            elif vol_is_stale:
                print(f"\n⚠️  BUYER REMOVED (vol data stale — cannot confirm fill): {symbol_key} @ {prev_price}")
            elif vol_diff < (prev_qty * MIN_FILL_VOL_RATIO):
                print(f"\n⚠️  FAKE BUYER REMOVED: {symbol_key}")
                print(f"   ❌ Support at {prev_price} was FAKE.")
            else:
                print(f"\n✅ BIG PLAYER BOUGHT SUCCESSFULLY: {symbol_key} @ {prev_price}")
                if not is_fut and MIN_OI_DELTA_FOR_SUCCESS > 0 and abs(oi_diff) < MIN_OI_DELTA_FOR_SUCCESS:
                    print(f"   ⏭️ Skipping success — OI change {oi_diff} below {MIN_OI_DELTA_FOR_SUCCESS}")
                else:
                    moneyness = "FUT" if is_fut else classify_strike(symbol_key, data)
                    record_success(symbol_key, "BUY", prev_qty, prev_price,
                                   vol_diff, oi_diff, ltp, moneyness, pcr,
                                   oi_same, oi_opposite, instrument_token)

        # SELLER FILLED
        if prev_ask and not curr_ask:
            prev_qty, prev_price, _, _ = prev_ask
            persisted = prev_seen_ask and (now_ts - prev_seen_ask) >= MIN_WHALE_PERSISTENCE_S
            if not persisted:
                print(f"\n⚠️  SELLER LEFT QUICKLY (likely spoof): {symbol_key} @ {prev_price}")
            elif vol_is_stale:
                print(f"\n⚠️  SELLER REMOVED (vol data stale — cannot confirm fill): {symbol_key} @ {prev_price}")
            elif vol_diff < (prev_qty * MIN_FILL_VOL_RATIO):
                print(f"\n⚠️  FAKE SELLER REMOVED: {symbol_key}")
                print(f"   ❌ Resistance at {prev_price} was FAKE.")
            else:
                print(f"\n🔴 BIG PLAYER SOLD SUCCESSFULLY: {symbol_key} @ {prev_price}")
                if not is_fut and MIN_OI_DELTA_FOR_SUCCESS > 0 and abs(oi_diff) < MIN_OI_DELTA_FOR_SUCCESS:
                    print(f"   ⏭️ Skipping success — OI change {oi_diff} below {MIN_OI_DELTA_FOR_SUCCESS}")
                else:
                    moneyness = "FUT" if is_fut else classify_strike(symbol_key, data)
                    record_success(symbol_key, "SELL", prev_qty, prev_price,
                                   vol_diff, oi_diff, ltp, moneyness, pcr,
                                   oi_same, oi_opposite, instrument_token)

    # LIVE ENTRY — BUY side
    # Only fire when the PRICE changes (not just orders/qty count), to avoid REST-poll noise
    prev_bid_price = prev_bid[1] if prev_bid else None
    curr_bid_price = curr_bid[1] if curr_bid else None
    if curr_bid and curr_bid_price != prev_bid_price:
        qty, price, _, avg_lots = curr_bid
        if not is_fut and MIN_OI_FOR_ENTRY > 0 and oi_same < MIN_OI_FOR_ENTRY:
            print(f"\n⏭️ Skipping entry (low OI {oi_same} < {MIN_OI_FOR_ENTRY}): {symbol_key}")
        else:
            moneyness = "FUT" if is_fut else classify_strike(symbol_key, data)
            print(f"\n🚀 BIG BUYER SITTING: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | moneyness: {moneyness}")
            print(f"\n Symbol: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | Moneyness: {moneyness} | PCR: {pcr} | OI (same): {oi_same} | OI (opp): {oi_opposite}")
            record_entry(symbol_key, "BUY", qty, price, ltp,
                         moneyness, pcr, oi_same, oi_opposite, instrument_token)

    # LIVE ENTRY — SELL side
    prev_ask_price = prev_ask[1] if prev_ask else None
    curr_ask_price = curr_ask[1] if curr_ask else None
    if curr_ask and curr_ask_price != prev_ask_price:
        qty, price, _, avg_lots = curr_ask
        if not is_fut and MIN_OI_FOR_ENTRY > 0 and oi_same < MIN_OI_FOR_ENTRY:
            print(f"\n⏭️ Skipping entry (low OI {oi_same} < {MIN_OI_FOR_ENTRY}): {symbol_key}")
        else:
            moneyness = "FUT" if is_fut else classify_strike(symbol_key, data)
            print(f"\n🔻 BIG SELLER SITTING: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | moneyness: {moneyness}")
            print(f"\n Symbol: {symbol_key} | Qty: {qty} | Price: {price} | LTP: {ltp} | Moneyness: {moneyness} | PCR: {pcr} | OI (same): {oi_same} | OI (opp): {oi_opposite}")
            record_entry(symbol_key, "SELL", qty, price, ltp,
                         moneyness, pcr, oi_same, oi_opposite, instrument_token)

    seen_bid_ts = None
    seen_ask_ts = None
    if curr_bid:
        # Keep the original seen timestamp if price is unchanged (whale still at same level)
        seen_bid_ts = prev_seen_bid if (curr_bid_price == prev_bid_price and prev_seen_bid) else now_ts
    if curr_ask:
        seen_ask_ts = prev_seen_ask if (curr_ask_price == prev_ask_price and prev_seen_ask) else now_ts

    history[symbol_key] = {
        'vol':         curr_vol,
        'oi':          curr_oi,
        'total_buy':   curr_total_buy,
        'total_sell':  curr_total_sell,
        'bid':         curr_bid,
        'ask':         curr_ask,
        'seen_bid_ts': seen_bid_ts,
        'seen_ask_ts': seen_ask_ts,
    }


# ===================== MARKET HOURS =====================
def is_market_open():
    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=8,  minute=14, second=0, microsecond=0)
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
    IST = pytz.timezone('Asia/Kolkata')
    now = datetime.now(IST)
    if now.hour == 6 and now.minute == 0:
        print(f"\nDaily token refresh at {now.strftime('%H:%M:%S %Z')}")
        if login_angelone():
            print("AngelOne token refreshed.")
            time.sleep(65)
        else:
            print("Token refresh failed.")


# ===================== CHUNK BUILDER =====================
def group_by_exchange(keys):
    groups = {}
    for k in keys:
        parts = k.split(":", 1)
        if len(parts) == 2:
            exch, token = parts
            groups.setdefault(exch, []).append(token)
    return groups


def build_exchange_chunks(all_keys):
    """Build list of {exchange:[tokens]} where each chunk <= MAX_TOKENS_PER_CALL total."""
    exchange_groups = group_by_exchange(all_keys)
    chunks  = []
    current = {}
    count   = 0
    for exch, tokens in exchange_groups.items():
        for tok in tokens:
            current.setdefault(exch, []).append(tok)
            count += 1
            if count >= MAX_TOKENS_PER_CALL:
                chunks.append(current)
                current = {}
                count   = 0
    if current:
        chunks.append(current)
    return chunks


# ===================== MAIN LOOP =====================
def main():
    print("\nSYSTEM STARTING — AngelOne Edition v2")
    print("Tables  : whale_entries_2 / whale_successes_2")
    print("Broker  : AngelOne SmartAPI")
    print("Hours   : Mon-Fri  09:15 - 15:30 IST\n")

    create_whale_tables()
    load_existing_successes()
    load_existing_entries()

    print("Logging into AngelOne...")
    if not login_angelone():
        print("AngelOne login failed. Set ANGELONE_CLIENT_ID and ANGELONE_PASSWORD env vars. Exiting.")
        return

    get_spot_indices()
    all_keys = build_all_instruments()
    all_keys = limit_option_strikes(all_keys)

    if not all_keys:
        print("No instruments to scan after filtering. Exiting.")
        return

    chunks = build_exchange_chunks(all_keys)
    print(f"Total filtered instruments : {len(all_keys)}")
    print(f"API call chunks            : {len(chunks)} (max {MAX_TOKENS_PER_CALL} tokens each)\n")

    spinner  = ['|', '/', '-', '\\']
    spin_idx = 0

    print("SCANNING FOR WHALE ORDERS...\n")

    try:
        while True:
            if not is_market_open():
                IST = pytz.timezone('Asia/Kolkata')
                print(f"Market is closed ({datetime.now(IST).strftime('%H:%M %A')})")
                wait_until_market_open()
                print("\nMarket reopening - refreshing AngelOne token & instruments...")
                login_angelone()
                all_keys = build_all_instruments()
                all_keys = limit_option_strikes(all_keys)
                chunks   = build_exchange_chunks(all_keys)

            refresh_daily_token()

            for chunk_idx, exch_chunk in enumerate(chunks):
                total_in_chunk = sum(len(v) for v in exch_chunk.values())
                sys.stdout.write(
                    f"\r{spinner[spin_idx]} Batch {chunk_idx+1}/{len(chunks)} "
                    f"({total_in_chunk} symbols)...   "
                )
                sys.stdout.flush()
                spin_idx = (spin_idx + 1) % 4

                try:
                    batch_raw = fetch_angel_quotes(exch_chunk)
                    if not batch_raw:
                        time.sleep(0.5)
                        continue

                    batch_data = dict(batch_raw)

                    # PCR computation
                    ce_pe_agg = {}
                    for sym, details in batch_data.items():
                        parsed     = parse_underlying_from_symbol(sym)
                        underlying = parsed["underlying"]
                        opt_type   = parsed["option_type"]
                        if opt_type not in ("CE", "PE"):
                            continue
                        oi = details.get("oi", 0) or 0
                        d  = ce_pe_agg.setdefault(underlying, {"CE": 0.0, "PE": 0.0})
                        d[opt_type] += float(oi)

                    for underlying, vals in ce_pe_agg.items():
                        ce_oi = vals["CE"]
                        pe_oi = vals["PE"]
                        underlying_pcr[underlying] = (pe_oi / ce_oi) if ce_oi > 0 else None

                    # Analyse each instrument
                    for sym_key, details in batch_data.items():
                        analyze_instrument(sym_key, details, batch_data)

                except Exception as e:
                    print(f"\nError in batch {chunk_idx+1}: {e}")
                    time.sleep(1)

                time.sleep(0.3)

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()


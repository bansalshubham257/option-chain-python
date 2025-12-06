import os
import time
import io
import gzip
import re
import pandas as pd
import requests
import yfinance as yf

from services.database import DatabaseService

# ==== CONFIG ====
db = DatabaseService()
ACCESS_TOKEN = db.get_access_token(account_id=5)

BASE_URL_V3 = "https://api.upstox.com/v3"

SUCCESS_FILE = "successful_whale_orders.csv"
MAPPING_FILE = "symbol_instrument_map.csv"

MAX_KEYS_PER_CALL = 450

# Upstox instruments CSVs
URL_NSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
URL_BSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.csv.gz"
URL_MCX_FO = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"


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


# ==== MAIN UPDATE LOGIC (NO LOCKS) ====

def update_success_csv_once():
    # 1) Read success file to get the list of symbols
    if not os.path.exists(SUCCESS_FILE):
        return

    df = pd.read_csv(SUCCESS_FILE)
    if df.empty or "instrument_key" not in df.columns:
        return

    # These are trading symbols, e.g. 'NSE_FO:VEDL25DEC520CE'
    symbols = (
        df["instrument_key"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if not symbols:
        print("⚠️ No symbols found in success file, skipping LTP fetch.")
        return

    # 2) Build mapping symbol -> instrument_key (if needed)
    build_symbol_instrument_mapping(symbols)
    symbol_map = load_symbol_mapping()

    # 3) Build list of actual instrument_keys we need LTP for
    instr_keys = sorted(set(
        symbol_map[s]
        for s in symbols
        if s in symbol_map
    ))
    if not instr_keys:
        print("⚠️ No instrument_keys resolved for symbols, skipping LTP fetch.")
        return

    # 4) Fetch LTP using v3 instrument_key
    ltp_map = fetch_ltp_for_instrument_keys(instr_keys)
    if not ltp_map:
        return

    # 5) Update CSV with live LTP & PnL (direct write, no locking)
    df = pd.read_csv(SUCCESS_FILE)
    if df.empty or "instrument_key" not in df.columns:
        return

    # Ensure columns exist
    if "actual_instrument_key" not in df.columns:
        df["actual_instrument_key"] = None
    if "live_ltp" not in df.columns:
        df["live_ltp"] = None
    if "live_pnl" not in df.columns:
        df["live_pnl"] = None
    if "live_pnl_pct" not in df.columns:
        df["live_pnl_pct"] = None
    if "last_live_update" not in df.columns:
        df["last_live_update"] = None
    if "status" not in df.columns:
        df["status"] = None
    if "progress_label" not in df.columns:
        df["progress_label"] = None
    if "max_return_high" not in df.columns:
        df["max_return_high"] = None
    if "max_return_low" not in df.columns:
        df["max_return_low"] = None
    if "stock_current_price" not in df.columns:
        df["stock_current_price"] = None
    if "stock_pct_change_today" not in df.columns:
        df["stock_pct_change_today"] = None
    if "stock_pct_at_exit" not in df.columns:
        df["stock_pct_at_exit"] = None
    if "progress_history" not in df.columns:
        df["progress_history"] = None

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    updated_rows = 0
    for idx, row in df.iterrows():
        sym = str(row.get("instrument_key", "")).strip()  # trading symbol

        if not sym:
            continue

        inst_key = symbol_map.get(sym)
        if not sym or sym not in ltp_map:
            continue

        ltp = ltp_map[sym]
        try:
            entry_price = float(row.get("price", 0) or 0)
            qty = float(row.get("qty", 0) or 0)
        except Exception:
            continue

        if entry_price <= 0:
            continue

        # Direct PnL calculation: (LTP - entry_price) / entry_price * 100
        pnl_per_unit = ltp - entry_price
        pnl_value = pnl_per_unit * qty
        pnl_pct = (pnl_per_unit / entry_price) * 100 if entry_price != 0 else 0

        df.at[idx, "actual_instrument_key"] = inst_key
        # overwrite original ltp and also fill live_* fields
        df.at[idx, "ltp"] = round(ltp, 2)
        df.at[idx, "live_ltp"] = round(ltp, 2)
        df.at[idx, "live_pnl"] = round(pnl_value, 2)
        df.at[idx, "live_pnl_pct"] = round(pnl_pct, 2)
        df.at[idx, "last_live_update"] = now_str

        # Calculate progress label based on pnl_pct
        # Progress levels: -75%, -50%, -25%, running, +25%, +50%, +75%, done (100%)
        # IMPORTANT: Only move in the direction of return, never backward
        # Once at +75%, stay at +75% even if price drops to +60%
        # But if at +50% and price moves to +75%, then update to +75%
        # Same logic applies for negative returns

        current_progress = str(row.get("progress_label", "running")).strip().lower()

        # Determine new progress level based on current return
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

        # Define progress order (from worst to best)
        progress_order = ["-75%", "-50%", "-25%", "running", "+25%", "+50%", "+75%", "done"]

        # Only update if new progress is moving forward (away from negative or towards positive)
        try:
            current_idx = progress_order.index(current_progress)
        except ValueError:
            current_idx = 3  # Default to "running"
        
        try:
            new_idx = progress_order.index(new_progress)
        except ValueError:
            new_idx = 3  # Default to "running"

        # Allow update if moving forward (higher index) or if starting from running
        if current_progress == "running" or new_idx >= current_idx:
            progress_label = new_progress
        else:
            # Maintain current progress if new progress is lower
            progress_label = current_progress

        df.at[idx, "progress_label"] = progress_label

        # Track progress history - record each change in order
        progress_history_str = str(row.get("progress_history", "")).strip()

        # Initialize with current progress label if empty
        if not progress_history_str or progress_history_str == "nan":
            progress_history_str = progress_label
        else:
            # Only append if progress_label is different from the last recorded one
            history_list = progress_history_str.split("|")
            if history_list[-1] != progress_label:
                progress_history_str = progress_history_str + "|" + progress_label

        df.at[idx, "progress_history"] = progress_history_str

        # Track maximum high and low returns touched
        try:
            max_high = float(row.get("max_return_high", 0) or 0)
            max_low = float(row.get("max_return_low", 0) or 0)
        except Exception:
            max_high = 0
            max_low = 0

        # Initialize max_high and max_low with current pnl_pct if they are 0
        # This ensures first update sets them to the current return
        if max_high == 0:
            max_high = pnl_pct
        if max_low == 0:
            max_low = entry_price

        # Update max_return_high if current pnl_pct is higher
        if pnl_pct > max_high:
            max_high = pnl_pct

        # Update max_return_low if current pnl_pct is lower (more negative)
        # Lower = more negative, so we check if pnl_pct < max_low
        if pnl_pct < max_low:
            max_low = pnl_pct

        df.at[idx, "max_return_high"] = round(max_high, 2)
        df.at[idx, "max_return_low"] = round(max_low, 2)

        # Mark status as 'done' if return reaches 100% or more
        # Once marked as 'done', preserve it - never change back to 'open'
        current_status = str(row.get("status", "")).strip().lower()
        if current_status != "done":
            if pnl_pct >= 94:
                df.at[idx, "status"] = "done"
                # Capture stock % change at exit (when status becomes done)
                _, stock_pct_at_exit = get_daily_pct_change_for_underlying(sym)
                if stock_pct_at_exit is not None:
                    df.at[idx, "stock_pct_at_exit"] = stock_pct_at_exit

        # Always update current stock price and % change
        _, current_stock_pct = get_daily_pct_change_for_underlying(sym)
        if current_stock_pct is not None:
            df.at[idx, "stock_pct_change_today"] = current_stock_pct

        updated_rows += 1

    # Direct write (no locks). If you want atomic-ish, you can still use tmp+replace.
    df.to_csv(SUCCESS_FILE, index=False)
    print(f"✅ Updated live LTP & PnL for {updated_rows} rows at {now_str}")


def main():
    print("🟢 Success LTP updater started. Watching successful_whale_orders.csv ...")
    while True:
        try:
            update_success_csv_once()
        except Exception as e:
            print(f"⚠️ Error in update loop: {e}")
        time.sleep(10)  # update every 10 seconds (tune as you like)


if __name__ == "__main__":
    main()

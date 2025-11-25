import sys
import time
import requests
import pandas as pd
import numpy as np
import io
import gzip
import concurrent.futures
from datetime import datetime

# ================= USER CONFIGURATION =================

# 1. ENTER YOUR 3 ACCESS TOKENS
TOKENS = [
    "YOUR_ACCESS_TOKEN_1", # Handles Batch 1
    "YOUR_ACCESS_TOKEN_2", # Handles Batch 2
    "YOUR_ACCESS_TOKEN_3"  # Handles Batch 3
]

# 2. EXPIRY DATES
NIFTY_EXPIRY  = "2025-11-25"
SENSEX_EXPIRY = "2025-11-27"
STOCK_EXPIRY  = "2025-11-25" # Expiry for Stocks

# 3. TOP 50 STOCKS LIST (You can edit this list)
TOP_STOCKS = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "LT", "SBIN", "AXISBANK",
    "KOTAKBANK", "ITC", "BHARTIARTL", "BAJFINANCE", "ASIANPAINT", "MARUTI", "TITAN",
    "ULTRACEMCO", "SUNPHARMA", "TATASTEEL", "NTPC", "M&M", "POWERGRID", "TATAMOTORS",
    "ADANIENT", "HCLTECH", "JSWSTEEL", "COALINDIA", "TATACHEM", "HINDALCO", "ONGC",
    "GRASIM", "CIPLA", "WIPRO", "BPCL", "SBILIFE", "DRREDDY", "BRITANNIA", "TECHM",
    "HDFCLIFE", "INDUSINDBK", "BAJAJFINSV", "ADANIPORTS", "DIVISLAB", "EICHERMOT",
    "APOLLOHOSP", "UPL", "TATACONSUM", "HEROMOTOCO", "HINDUNILVR", "VEDL", "DLF"
]

# WHALE SETTINGS
# For Indices (Fixed)
NIFTY_MIN_QTY = 100 * 75
SENSEX_MIN_QTY = 50 * 20

# For Stocks: Logic = Value based.
# A "Whale" in stocks is roughly ₹50 Lakhs+ value or > 50 Lots.
# We will use a generic multiplier: 50 * Lot Size of that stock.
STOCK_WHALE_MULTIPLIER = 50

# ================= API CONFIGURATION =================
BASE_URL_V2 = "https://api.upstox.com/v2"
BASE_URL_V3 = "https://api.upstox.com/v3"
URL_NSE_FO = "https://assets.upstox.com/feed/instruments/nse-fo.csv.gz"
URL_BSE_FO = "https://assets.upstox.com/feed/instruments/bse-fo.csv.gz"

# Global History
history = {}
stock_lot_sizes = {} # To store dynamic lot sizes

# ================= HELPER FUNCTIONS =================

def download_csv(url, name):
    print(f"⏳ Downloading {name} Master List...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            return pd.read_csv(f)
    except Exception as e:
        print(f"❌ Error downloading {name}: {e}")
        return pd.DataFrame()

def get_strikes_for_stock(df, symbol, expiry, num_itm=5, num_otm=5):
    """
    Finds the ATM strike based on the Future Price (FUTSTK) in the CSV,
    then selects -5 and +5 strikes.
    """
    # 1. Filter for this Stock and Expiry
    df_stock = df[
        (df['name'] == symbol) &
        (df['expiry'] == expiry)
        ]

    if df_stock.empty:
        return []

    # 2. Find Reference Price (Use Future Price if available, else median of strikes)
    # Using Futures is faster than API calls for 50 stocks
    futures = df_stock[df_stock['instrument_type'] == 'FUTSTK']

    reference_price = 0
    if not futures.empty:
        # Some CSVs have 'last_price', if not we estimate median strike
        if 'last_price' in futures.columns and futures.iloc[0]['last_price'] > 0:
            reference_price = futures.iloc[0]['last_price']

    # Fallback: Use median of all strikes
    unique_strikes = sorted(df_stock['strike'].unique())
    if reference_price == 0:
        reference_price = unique_strikes[len(unique_strikes)//2]

    # 3. Store Lot Size for this stock (Take first valid lot size)
    global stock_lot_sizes
    if symbol not in stock_lot_sizes:
        try:
            lot = df_stock.iloc[0]['lot_size']
            stock_lot_sizes[symbol] = int(lot)
        except:
            stock_lot_sizes[symbol] = 1 # Fallback

    # 4. Find ATM Index
    # Find strike closest to reference_price
    closest_strike = min(unique_strikes, key=lambda x: abs(x - reference_price))
    atm_idx = unique_strikes.index(closest_strike)

    # 5. Slice (ATM - 5 to ATM + 5)
    start_idx = max(0, atm_idx - num_itm)
    end_idx = min(len(unique_strikes), atm_idx + num_otm + 1) # +1 for ATM itself

    selected_strikes = unique_strikes[start_idx:end_idx]

    # 6. Get Keys for these strikes (CE & PE)
    mask = (
            (df_stock['strike'].isin(selected_strikes)) &
            (df_stock['instrument_type'].isin(['CE', 'PE']))
    )

    return df_stock[mask]['instrument_key'].tolist()

def generate_batches():
    print("\n⚙️ GENERATING WATCHLIST (INDICES + 50 STOCKS)...")

    # 1. Download Data
    df_nse = download_csv(URL_NSE_FO, "NSE_FO")
    df_bse = download_csv(URL_BSE_FO, "BSE_FO")

    all_keys = []

    # --- A. INDICES (Nifty/Sensex) ---
    # (Using simpler logic: Select range around hardcoded/approx level if API fails,
    # or we can just fetch all Index Options if user prefers, but stick to logic)
    # NOTE: Ideally fetch Spot from API, but for speed here we assume logic similar to stocks
    # Nifty
    nifty_keys = get_strikes_for_stock(df_nse, "Nifty", NIFTY_EXPIRY, 7, 7) # 15 strikes
    all_keys.extend(nifty_keys)
    print(f"   ✅ Added NIFTY: {len(nifty_keys)} Instruments")

    # Sensex
    sensex_keys = get_strikes_for_stock(df_bse, "SENSEX", SENSEX_EXPIRY, 7, 7)
    all_keys.extend(sensex_keys)
    print(f"   ✅ Added SENSEX: {len(sensex_keys)} Instruments")

    # --- B. TOP 50 STOCKS ---
    stock_count = 0
    for stock in TOP_STOCKS:
        # 5 ITM, 1 ATM, 5 OTM = 11 Strikes * 2 (CE/PE) = 22 Instruments
        keys = get_strikes_for_stock(df_nse, stock, STOCK_EXPIRY, 5, 5)
        if keys:
            all_keys.extend(keys)
            stock_count += 1

    print(f"   ✅ Added {stock_count}/{len(TOP_STOCKS)} Stocks (Total Instruments: {len(all_keys)})")

    # --- C. SPLIT INTO 3 BATCHES ---
    # Max 400 per batch
    total = len(all_keys)
    batch_size = int(np.ceil(total / 3))

    b1 = all_keys[:batch_size]
    b2 = all_keys[batch_size:batch_size*2]
    b3 = all_keys[batch_size*2:]

    print(f"\n🔹 Batch 1: {len(b1)} keys -> Token 1")
    print(f"🔹 Batch 2: {len(b2)} keys -> Token 2")
    print(f"🔹 Batch 3: {len(b3)} keys -> Token 3")

    return [",".join(b1), ",".join(b2), ",".join(b3)]

# ================= ANALYSIS LOGIC =================

def get_whale_order(depth_list, min_qty_threshold, lot_size):
    if not depth_list: return None
    sorted_orders = sorted(depth_list, key=lambda x: x['quantity'], reverse=True)
    biggest = sorted_orders[0]

    qty = biggest.get('quantity', 0)
    orders = biggest.get('orders', 0)
    price = biggest.get('price', 0)

    if orders == 0: return None

    if qty >= min_qty_threshold:
        avg_qty = qty / orders
        avg_lots = avg_qty / lot_size
        if avg_lots >= 1.5: # Concentration Filter
            return (qty, price, orders, avg_lots)
    return None

def analyze_data(data_dict):
    global history

    for symbol, data in data_dict.items():
        # Determine Settings
        threshold = NIFTY_MIN_QTY
        lot_size = 75 # Default

        # Identify Instrument Type
        if "Nifty" in symbol or "NIFTY" in symbol:
            threshold = NIFTY_MIN_QTY
            lot_size = 75
        elif "SENSEX" in symbol or "BSX" in symbol:
            threshold = SENSEX_MIN_QTY
            lot_size = 20
        else:
            # It is a STOCK
            # Extract name from symbol (e.g., NSE_FO:RELIANCE25NOV...)
            # Simplified: check which stock name is in symbol
            for s_name in TOP_STOCKS:
                if s_name in symbol:
                    lot_size = stock_lot_sizes.get(s_name, 1)
                    threshold = STOCK_WHALE_MULTIPLIER * lot_size
                    break

        # Extract Data
        depth = data.get('depth', {})
        bids = depth.get('buy', [])
        asks = depth.get('sell', [])
        curr_vol = data.get('volume', 0)
        curr_oi = data.get('oi', 0)

        curr_bid = get_whale_order(bids, threshold, lot_size)
        curr_ask = get_whale_order(asks, threshold, lot_size)

        prev_bid = None
        prev_ask = None

        if symbol in history:
            prev = history[symbol]
            vol_diff = curr_vol - prev['vol']
            oi_diff = curr_oi - prev['oi']
            prev_bid = prev['bid']
            prev_ask = prev['ask']

            # BUYER ANALYSIS
            if prev_bid and not curr_bid:
                qty, price, _, _ = prev_bid
                if vol_diff < (qty * 0.2):
                    print(f"\n⚠️ FAKE BUYER REMOVED: {symbol} (Price: {price})")
                else:
                    print(f"\n✅ REAL BUYING IN {symbol} @ {price}")
                    if oi_diff > 0: print("   🔥 FRESH LONG BUILDUP (Bullish)")
                    elif oi_diff < 0: print("   ⚡ SHORT COVERING (Bounce)")
                    print(f"   👉 ACTION: Buy CALL on Retest.")

            # SELLER ANALYSIS
            if prev_ask and not curr_ask:
                qty, price, _, _ = prev_ask
                if vol_diff < (qty * 0.2):
                    print(f"\n⚠️ FAKE SELLER REMOVED: {symbol} (Price: {price})")
                else:
                    print(f"\n🔴 REAL SELLING IN {symbol} @ {price}")
                    if oi_diff > 0: print("   🔻 FRESH SHORT BUILDUP (Bearish)")
                    elif oi_diff < 0: print("   ⚠️ LONG UNWINDING (Drop)")
                    print(f"   👉 ACTION: Buy PUT on Retest.")

        # STORE HISTORY
        history[symbol] = {
            'vol': curr_vol,
            'oi': curr_oi,
            'bid': curr_bid,
            'ask': curr_ask
        }

# ================= THREADED REQUESTER =================

def fetch_batch(token, keys):
    """Fetches data for a single batch using a specific token."""
    if not keys: return {}

    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    url = f"{BASE_URL_V2}/market-quote/quotes"
    params = {'instrument_key': keys}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5)
        if resp.status_code == 200:
            d = resp.json()
            if d['status'] == 'success':
                return d['data']
    except:
        pass
    return {}

# ================= MAIN LOOP =================

def main():
    print("\n🟢 ENTERPRISE WHALE TRACKER (3 TOKENS | 50 STOCKS)...")

    # 1. Generate Batches
    batches = generate_batches()
    if not any(batches):
        print("🔴 Failed to generate batches.")
        return

    print("\n🔎 SCANNING 3 BATCHES IN PARALLEL...\n")

    spinner = ['|', '/', '-', '\\']
    idx = 0

    while True:
        try:
            t_start = time.time()
            sys.stdout.write(f"\r{spinner[idx]} Scanning Markets... ")
            sys.stdout.flush()
            idx = (idx + 1) % 4

            # 2. Parallel Execution using ThreadPool
            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # Map tokens to batches
                futures = {
                    executor.submit(fetch_batch, TOKENS[0], batches[0]): "Batch 1",
                    executor.submit(fetch_batch, TOKENS[1], batches[1]): "Batch 2",
                    executor.submit(fetch_batch, TOKENS[2], batches[2]): "Batch 3"
                }

                for future in concurrent.futures.as_completed(futures):
                    batch_data = future.result()
                    if batch_data:
                        results.update(batch_data)

            # 3. Analyze Combined Data
            analyze_data(results)

            # Sleep to match rate limits (approx 1.5s loop time)
            elapsed = time.time() - t_start
            if elapsed < 1.5:
                time.sleep(1.5 - elapsed)

        except KeyboardInterrupt:
            print("\n🔴 Stopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
import sys
import time
import requests
import pandas as pd
import numpy as np
import io
import gzip
import re
from datetime import datetime

# ================= USER CONFIGURATION =================
# 1. ENTER YOUR ACCESS TOKEN (Only 1 needed)
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyWEJSUFMiLCJqdGkiOiI2OTIzZDcwNjU0ZDU3NTI1YTFiNGZjZmUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2Mzk1NjQ4NiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzY0MDIxNjAwfQ.hva1xKOQw9o-kZ_hyBNY8rcbR4jLIyeKTSgj3HxaWA0"

# 2. ENTER EXPIRY DATES (Format: YYYY-MM-DD)
# Based on trading symbols like "SENSEX 92300 CE 27 NOV 25"
# This extracts dates like "27 NOV 25" and converts to "2025-11-27"
NIFTY_EXPIRY  = "2025-11-25"    # 25 NOV 25 (Weekly/Monthly expiry)
SENSEX_EXPIRY = "2025-11-27"    # 27 NOV 25 (Monthly expiry)

# LOT SIZES (Nov 2025 Standard)
NIFTY_LOT_SIZE = 75
SENSEX_LOT_SIZE = 20

# THRESHOLD: Total Quantity must be at least 100 Lots
NIFTY_MIN_QTY = 100 * NIFTY_LOT_SIZE
SENSEX_MIN_QTY = 100 * SENSEX_LOT_SIZE

# ================= API CONFIGURATION =================
BASE_URL_V2 = "https://api.upstox.com/v2"
BASE_URL_V3 = "https://api.upstox.com/v3"
URL_NSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
URL_BSE_FO = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.csv.gz"
MIN_ORDER_VALUE = 200000
# GLOBAL MEMORY (To track previous orders)
# HISTORY STORAGE
# HISTORY
history = {}

# ================= HELPER FUNCTIONS =================

def download_instruments(url, exchange_filter, name_filter, expiry_date):
    print(f"\n⏳ Downloading {exchange_filter} data...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            df = pd.read_csv(f)

        if 'exchange' in df.columns:
            df = df[df['exchange'] == exchange_filter]

        df = df[df['name'].str.contains(name_filter, case=False, na=False)]
        df = df[df['expiry'] == expiry_date]

        if 'option_type' in df.columns:
            df = df[df['option_type'].isin(['CE', 'PE'])]

        return df
    except Exception as e:
        print(f"❌ Error processing CSV: {e}")
        return pd.DataFrame()

def get_spot_prices():
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    request_keys = "NSE_INDEX|Nifty 50,BSE_INDEX|SENSEX"
    url = f"{BASE_URL_V2}/market-quote/ltp"
    params = {'instrument_key': request_keys}

    try:
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()

        def find_price(data_dict, partial_key):
            for key, val in data_dict.items():
                if partial_key in key:
                    return val['last_price']
            return None

        nifty_ltp = find_price(data['data'], 'Nifty 50')
        sensex_ltp = find_price(data['data'], 'SENSEX')

        if not nifty_ltp: nifty_ltp = 24000
        if not sensex_ltp: sensex_ltp = 80000

        return nifty_ltp, sensex_ltp
    except Exception:
        return 24000, 80000

def get_watchlist():
    nifty_ltp, sensex_ltp = get_spot_prices()
    watchlist_keys = []

    nifty_atm = round(nifty_ltp / 50) * 50
    sensex_atm = round(sensex_ltp / 100) * 100

    nifty_strikes = [nifty_atm + (i * 50) for i in range(-7, 8)]
    sensex_strikes = [sensex_atm + (i * 100) for i in range(-7, 8)]

    df_nifty = download_instruments(URL_NSE_FO, 'NSE_FO', 'Nifty', NIFTY_EXPIRY)
    if not df_nifty.empty:
        matched = df_nifty[df_nifty['strike'].isin(nifty_strikes)]
        watchlist_keys.extend(matched['instrument_key'].tolist())
        print(f"✅ Loaded {len(matched)} NIFTY Contracts")

    df_sensex = download_instruments(URL_BSE_FO, 'BSE_FO', 'SENSEX', SENSEX_EXPIRY)
    if not df_sensex.empty:
        matched = df_sensex[df_sensex['strike'].isin(sensex_strikes)]
        watchlist_keys.extend(matched['instrument_key'].tolist())
        print(f"✅ Loaded {len(matched)} SENSEX Contracts")

    return ",".join(watchlist_keys)

# ================= WHALE LOGIC =================

def get_whale_order(depth_list, min_qty_threshold, lot_size):
    if not depth_list: return None

    sorted_orders = sorted(depth_list, key=lambda x: x['quantity'], reverse=True)
    biggest = sorted_orders[0]

    qty = biggest.get('quantity', 0)
    orders = biggest.get('orders', 0)
    price = biggest.get('price', 0)

    if orders == 0 or price == 0: return None

    total_order_value = qty * price # e.g., 7500 qty * 100 rs = 7,50,000

    if qty >= min_qty_threshold and total_order_value >= MIN_ORDER_VALUE:
        avg_qty = qty / orders
        avg_lots = avg_qty / lot_size
        if avg_lots >= 1.5:
            return (qty, price, orders, avg_lots)
    return None

def analyze_instrument(symbol, data):
    global history

    threshold = NIFTY_MIN_QTY
    lot_size = NIFTY_LOT_SIZE
    if "BSE" in symbol or "SENSEX" in str(symbol):
        threshold = SENSEX_MIN_QTY
        lot_size = SENSEX_LOT_SIZE

    depth = data.get('depth', {})
    bids = depth.get('buy', [])
    asks = depth.get('sell', [])

    curr_vol = data.get('volume', 0)
    curr_oi  = data.get('oi', 0)
    curr_total_buy = data.get('total_buy_quantity', 0)
    curr_total_sell = data.get('total_sell_quantity', 0)
    ltp = data.get('last_price', 0)

    curr_bid = get_whale_order(bids, threshold, lot_size)
    curr_ask = get_whale_order(asks, threshold, lot_size)

    prev_bid = None
    prev_ask = None

    if symbol in history:
        prev = history[symbol]
        vol_diff = curr_vol - prev['vol']
        oi_diff  = curr_oi - prev['oi']

        prev_bid = prev['bid']
        prev_ask = prev['ask']

        # ---------------- BUYER FILLED (SUPPORT) ----------------
        if prev_bid and not curr_bid:
            prev_qty, prev_price, _, _ = prev_bid

            # SPOOFING (Fake)
            if vol_diff < (prev_qty * 0.2):
                print(f"\n⚠️  FAKE BUYER REMOVED: {symbol}")
                print(f"   ❌ Logic: Whale removed orders. Support at {prev_price} is FAKE.")
                print(f"   👉 ACTION (CE): Do NOT Buy. If holding, Exit.")
                print(f"   👉 ACTION (PE): Good time to Buy PUTs as support is broken.")

            # REAL FILL
            else:
                print(f"\n✅ BIG PLAYER BOUGHT SUCCESSFULLY: {symbol} @ {prev_price}")

                if oi_diff > 0:
                    print(f"   🔥 FRESH BUYING (Bullish). Strong Support established.")
                    print(f"   👉 ACTION (CE): BUY CALL now or on dip to {prev_price}.")
                    print(f"   👉 ACTION (PE): EXIT all PUT positions immediately.")

                elif oi_diff < 0:
                    print(f"   ⚡ SHORT COVERING (Bounce). Sellers exiting.")
                    print(f"   👉 ACTION (CE): Quick Scalp Buy only.")
                    print(f"   👉 ACTION (PE): Tighten Stop Loss. Market might bounce up temporarily.")

                else:
                    print(f"   ⚠️ OI FLAT: Volatility expected.")
                    print(f"   👉 ACTION: Wait for next candle.")

        # ---------------- SELLER FILLED (RESISTANCE) ----------------
        if prev_ask and not curr_ask:
            prev_qty, prev_price, _, _ = prev_ask

            # SPOOFING (Fake)
            if vol_diff < (prev_qty * 0.2):
                print(f"\n⚠️  FAKE SELLER REMOVED: {symbol}")
                print(f"   ❌ Logic: Resistance at {prev_price} was fake.")
                print(f"   👉 ACTION (CE): Good time to Buy CALLs (Breakout likely).")
                print(f"   👉 ACTION (PE): Do NOT Buy Puts. Exit if holding.")

            # REAL FILL
            else:
                print(f"\n🔴 BIG PLAYER SOLD SUCCESSFULLY: {symbol} @ {prev_price}")

                if oi_diff > 0:
                    print(f"   🔻 FRESH SELLING (Bearish). Strong Resistance.")
                    print(f"   👉 ACTION (CE): EXIT all CALL positions immediately.")
                    print(f"   👉 ACTION (PE): BUY PUT now or on retest of {prev_price}.")

                elif oi_diff < 0:
                    print(f"   ⚠️ LONG UNWINDING (Bearish). Buyers are giving up.")
                    print(f"   👉 ACTION (CE): EXIT Calls immediately. Do not average.")
                    print(f"   👉 ACTION (PE): Safe to BUY PUTs. Market is weak.")

    # --- LIVE STATUS ---
    if curr_bid and curr_bid != prev_bid:
        qty, price, _, _ = curr_bid
        print(f"\n🚀 BIG BUYER SITTING: {symbol} | Qty: {qty} | Price: {price} | LTP: {ltp}")

    if curr_ask and curr_ask != prev_ask:
        qty, price, _, _ = curr_ask
        print(f"\n🔻 BIG SELLER SITTING: {symbol} | Qty: {qty} | Price: {price} | LTP: {ltp}")

    # Update History
    history[symbol] = {
        'vol': curr_vol,
        'oi': curr_oi,
        'total_buy': curr_total_buy,
        'total_sell': curr_total_sell,
        'bid': curr_bid,
        'ask': curr_ask
    }

# ================= MAIN LOOP =================

def main():
    print("\n🟢 SYSTEM STARTING (FULL TRADING ASSISTANT)...")
    keys = get_watchlist()

    if not keys:
        print("🔴 Watchlist Empty.")
        return

    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
    url = f"{BASE_URL_V2}/market-quote/quotes"

    spinner = ['|', '/', '-', '\\']
    idx = 0

    print("\n🔎 SCANNING FOR TRADES...\n")

    while True:
        try:
            sys.stdout.write(f"\r{spinner[idx]} Scanning... ")
            sys.stdout.flush()
            idx = (idx + 1) % 4

            payload = {'instrument_key': keys}
            response = requests.get(url, headers=headers, params=payload)

            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'success':
                    for sym, details in data['data'].items():
                        analyze_instrument(sym, details)

            time.sleep(1.5)

        except Exception:
            time.sleep(5)
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()
import os
import json
import time
import requests
import pandas as pd
from config import ACCESS_TOKEN
from fetch_data import get_option_chain, get_fno_stocks, get_nearest_expiry, get_instrument_key
import time
import requests

LOT_THRESHOLD = 80  # Adjust threshold as needed

ALERT_FILE = "large_bid_alerts.json"

def get_lot_size(instrument_key):
    """Fetch lot size for a given instrument key."""
    url = "https://api.upstox.com/v2/option/contract"
    params = {"instrument_key": instrument_key}
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        if "data" in data and data["data"]:
            return data["data"][0].get("minimum_lot", None)
    except Exception as e:
        print(f"Error fetching lot size: {e}")

    return None

def load_alert_data():
    """Load existing bid alert data from file, handling empty or missing files."""
    if not os.path.exists(ALERT_FILE):
        return []  # If file doesn't exist, return empty list

    try:
        with open(ALERT_FILE, "r") as f:
            content = f.read().strip()  # Read and remove whitespace
            return json.loads(content) if content else []  # Handle empty file
    except json.JSONDecodeError:
        return []  # If file is corrupted or empty, return empty list

def save_alert_data(alerts):
    """Save detected large bid orders to a file."""
    with open(ALERT_FILE, "w") as f:
        json.dump(alerts, f, indent=4)

def filter_strikes(option_chain):
    """Select ATM, top 3 ITM, and top 3 OTM strikes."""
    strike_prices = sorted(set([opt["strike_price"] for opt in option_chain]))

    if not strike_prices:
        return []

    atm_strike = min(strike_prices, key=lambda x: abs(x - option_chain[0]["underlying_spot_price"]))
    atm_index = strike_prices.index(atm_strike)

    # Get ITM (lower strikes for calls, higher strikes for puts)
    itm_strikes = strike_prices[max(0, atm_index - 3):atm_index]

    # Get OTM (higher strikes for calls, lower strikes for puts)
    otm_strikes = strike_prices[atm_index + 1: atm_index + 4]

    return itm_strikes + [atm_strike] + otm_strikes  # Return sorted selection


def get_market_data(instrument_key, symbol):
    """Fetch market depth data (top 3 bid/ask) for a specific option instrument."""
    url = "https://api.upstox.com/v2/market-quote/quotes"
    params = {"instrument_key": instrument_key}
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        if "data" in data:
            # Find the correct key from response (matching the symbol)
            matched_key = next((key for key in data["data"] if key.startswith("NSE_FO:") and symbol in key), None)
            if matched_key:
                depth = data["data"][matched_key]["depth"]
                # Extract top 3 bid (buy side) & top 3 ask (sell side)
                top_3_bids = depth.get("buy", [])[:3]
                top_3_asks = depth.get("sell", [])[:3]

                return {
                    "bid_prices": [bid["price"] for bid in top_3_bids],
                    "bid_quantities": [bid["quantity"] for bid in top_3_bids],
                    "bid_orders": [bid["orders"] for bid in top_3_bids],  # Include bid order count
                    "ask_prices": [ask["price"] for ask in top_3_asks],
                    "ask_quantities": [ask["quantity"] for ask in top_3_asks],
                    "ask_orders": [ask["orders"] for ask in top_3_asks],  # Include ask order count
                }
            else:
                print(f"⚠️ No matching key found in response for {instrument_key} ({symbol})")

    except Exception as e:
        print(f"Error fetching market data for {instrument_key}: {e}")
    return {
        "bid_prices": [],
        "bid_quantities": [],
        "bid_orders": [],
        "ask_prices": [],
        "ask_quantities": [],
        "ask_orders": [],
    }  # Return empty in case of failure

EXCLUDED_STOCKS = ["MOTHERSON", "011NSETEST", "021NSETEST", "031NSETEST", "041NSETEST", "051NSETEST", "061NSETEST",
                   "071NSETEST", "081NSETEST", "091NSETEST", "101NSETEST", "111NSETEST", "121NSETEST", "131NSETEST"
    , "141NSETEST", "151NSETEST", "161NSETEST", "171NSETEST", "181NSETEST", "IDEA", "YESBANK" ]

def monitor_large_bid_orders():
    """Continuously check for large bid quantities in selected strikes."""
    fno_stocks = get_fno_stocks()  # Fetch F&O stocks
    alert_data = load_alert_data()

    while True:
        print("🔎 Checking for large bid orders...")

        for stock in fno_stocks:
            if stock in EXCLUDED_STOCKS:
                continue
            expiry_date = get_nearest_expiry(stock)  # Fetch nearest expiry
            instrument_key = get_instrument_key(stock)  # Fetch instrument key

            if not instrument_key:
                print(f"⚠️ No instrument key found for {stock}")
                continue

            lot_size = get_lot_size(instrument_key)  # Fetch lot size
            if not lot_size:
                print(f"⚠️ No lot size found for {stock}")
                continue

            option_chain = get_option_chain(instrument_key, expiry_date)  # Fetch option chain
            if not option_chain:
                print(f"⚠️ No option data for {stock}")
                continue

            selected_strikes = filter_strikes(option_chain)  # Select ITM, ATM, OTM

            for option in option_chain:
                if option["strike_price"] not in selected_strikes:
                    continue  # Skip if not in selected strikes

                strike = option["strike_price"]

                for option_type, key in [("CE", "call_options"), ("PE", "put_options")]:
                    option_instrument_key = option[key]["instrument_key"]  # Get instrument key

                    # Fetch market data for this option strike
                    market_data = get_market_data(option_instrument_key, stock)

                    if not market_data["bid_prices"]:
                        print(f"⚠️ No market data for {option_type} {strike} of {stock}")
                        continue

                    top_3_bids = sorted(zip(market_data["bid_prices"], market_data["bid_quantities"],market_data["bid_orders"]), reverse=True)[:3]
                    top_3_asks = sorted(zip(market_data["ask_prices"], market_data["ask_quantities"],market_data["ask_orders"]), reverse=True)[:3]

                    for (bid_price, bid_qty, bid_order), (ask_price, ask_qty, ask_order) in zip(top_3_bids, top_3_asks):
                        bid_lots = bid_qty / lot_size
                        ask_lots = ask_qty / lot_size

                        # Condition: Either bid or ask crosses the threshold, but not both
                        if (bid_lots >= LOT_THRESHOLD) ^ (ask_lots >= LOT_THRESHOLD):  # XOR condition
                            alert = {
                                "symbol": stock,
                                "expiry": expiry_date,
                                "strike_price": strike,
                                "option_type": option_type,
                                "bid_qty": bid_qty,
                                "bid_order" : bid_order,
                                "ask_qty": ask_qty,
                                "ask_order" : ask_order,
                                "lot_size": lot_size,
                                "cmp": bid_price,  # Corrected CMP (LTP)
                            }

                            print(f"🚨 Large bid/ask detected: {alert}")
                            alert_data.append(alert)

                            # Immediately update file after detecting an alert
                            save_alert_data(alert_data)


        print("⏳ Sleeping for 30 seconds...\n")
        time.sleep(30)  # Adjust polling time as needed

def monitor_large_bid_orders_one():
    """Continuously check for large bid or ask quantities in select strikes."""
    fno_stocks = get_fno_stocks()  # Fetch F&O stocks
    alert_data = load_alert_data()

    while True:
        print("🔎 Checking for large bid/ask orders...")

        for stock in fno_stocks:
            if stock in EXCLUDED_STOCKS:
                continue
            expiry_date = get_nearest_expiry(stock)  # Fetch nearest expiry
            instrument_key = get_instrument_key(stock)  # Fetch instrument key

            if not instrument_key:
                print(f"⚠️ No instrument key found for {stock}")
                continue

            lot_size = get_lot_size(instrument_key)  # Fetch lot size
            if not lot_size:
                print(f"⚠️ No lot size found for {stock}")
                continue

            option_chain = get_option_chain(instrument_key, expiry_date)  # Fetch option data
            if not option_chain:
                print(f"⚠️ No option data for {stock}")
                continue

            # Filter top 3 OTM, top 3 ITM, and ATM strike prices
            selected_strikes = filter_strikes(option_chain)

            for option in option_chain:
                if option["strike_price"] not in selected_strikes:
                    continue  # Skip if not in selected strikes

                strike = option["strike_price"]

                for option_type, key in [("CE", "call_options"), ("PE", "put_options")]:
                    data = option[key]["market_data"]
                    bid_qty = data.get("bid_qty", 0)  # Bid quantity
                    ask_qty = data.get("ask_qty", 0)  # Ask quantity
                    cmp = data.get("ltp", 0)  # Last traded price (CMP)

                    bid_lots = bid_qty / lot_size
                    ask_lots = ask_qty / lot_size

                    # Check alert condition
                    bid_exceeds = bid_lots >= LOT_THRESHOLD
                    ask_exceeds = ask_lots >= LOT_THRESHOLD

                    if bid_exceeds != ask_exceeds:  # Alert if only one side exceeds the threshold
                        alert = {
                            "symbol": stock,
                            "expiry": expiry_date,
                            "strike_price": strike,
                            "option_type": option_type,
                            "bid_qty": bid_qty,
                            "ask_qty": ask_qty,
                            "lot_size": lot_size,
                            "cmp": cmp
                        }

                        print(f"🚨 Large bid/ask detected: {alert}")

                        # Immediately update file

                        alert_data.append(alert)
                        save_alert_data(alert_data)

        print("⏳ Sleeping for 30 seconds...\n")
        time.sleep(30)  # Adjust polling time as needed

# Start monitoring
monitor_large_bid_orders_one()

# Start monitoring
#monitor_large_bid_orders()
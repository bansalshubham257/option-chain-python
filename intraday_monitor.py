import os
import json
import time
import datetime
from fetch_data import get_option_chain, get_fno_stocks
from fetch_data import get_nearest_expiry

DATA_FILE = "intraday_data.json"
SPIKE_THRESHOLDS = {"volume": 50, "oi": 30, "iv": 20}  # Percentage thresholds

# Load previous intraday data
def load_intraday_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {}

# Save updated intraday data
def save_intraday_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)

# Detect spikes
def detect_spikes(symbol, expiry_date, strike, option_type, new_data):
    intraday_data = load_intraday_data()
    key = f"{symbol}_{expiry_date}_{strike}_{option_type}"
    prev_data = intraday_data.get(key, {})

    spikes = {}
    for metric in ["volume", "oi", "iv"]:
        if metric in prev_data and metric in new_data:
            change = ((new_data[metric] - prev_data[metric]) / (prev_data[metric] + 1)) * 100
            if change > SPIKE_THRESHOLDS[metric]:
                spikes[f"{metric}_spike"] = True

    # Update the latest data
    intraday_data[key] = new_data
    save_intraday_data(intraday_data)

    return spikes

# Function to check if the script should run
def is_market_open():
    now = datetime.datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    # Run only Monday to Friday within market hours
    return now.weekday() < 5 and market_open <= now <= market_close

# Function to monitor all F&O stocks continuously
def monitor_all_fno_stocks():
    fno_stocks = get_fno_stocks()

    while True:
        if is_market_open():
            print("Market is open. Fetching data...")
            intraday_data = load_intraday_data()
            all_stock_alerts = {}

            for stock in fno_stocks:
                expiry_date = get_nearest_expiry(stock)
                current_data = get_option_chain(stock, expiry_date)

                if not current_data:
                    print(f"No data found for {stock}")
                    continue

                for option in current_data:
                    strike = option["strike_price"]

                    for option_type, key in [("CE", "call_options"), ("PE", "put_options")]:
                        data = option[key]["market_data"]
                        new_data = {
                            "volume": data.get("volume", 0),
                            "cmp": data.get("ltp", 0),
                            "oi": data.get("oi", 0),
                            "iv": data.get("iv", 0)
                        }

                        # Detect spikes
                        spikes = detect_spikes(stock, expiry_date, strike, option_type, new_data)
                        if spikes:
                            alert_msg = f"Spike in {stock} {strike} {option_type} - {spikes}"
                            all_stock_alerts.setdefault(stock, []).append(alert_msg)
                            print(alert_msg)

            print("Sleeping for 60 seconds...")
        else:
            print("Market is closed. Script sleeping...")

        time.sleep(60)  # Check every 60 seconds

# Start monitoring when Render runs this file
monitor_all_fno_stocks()

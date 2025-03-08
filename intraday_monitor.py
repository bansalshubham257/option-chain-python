import os
import json
import time
from fetch_data import get_option_chain, get_fno_stocks
from fetch_data import get_nearest_expiry

data_file = "intraday_data.json"

def load_previous_data():
    if os.path.exists(data_file):
        with open(data_file, "r") as file:
            return json.load(file)
    return {}

def save_data(data):
    with open(data_file, "w") as file:
        json.dump(data, file, indent=4)

def detect_spikes(previous_data, current_data):
    alerts = []
    for option in current_data:
        strike = option["strike"]
        call_oi = option["call_oi"]
        put_oi = option["put_oi"]
        call_vol = option["call_volume"]
        put_vol = option["put_volume"]
        
        prev_option = previous_data.get(str(strike), {})
        prev_call_oi = prev_option.get("call_oi", 0)
        prev_put_oi = prev_option.get("put_oi", 0)
        prev_call_vol = prev_option.get("call_volume", 0)
        prev_put_vol = prev_option.get("put_volume", 0)
        
        # Detect volume spike
        if call_vol - prev_call_vol > 5000:
            alerts.append(f"Call Volume Spike at {strike} - {call_vol}")
        if put_vol - prev_put_vol > 5000:
            alerts.append(f"Put Volume Spike at {strike} - {put_vol}")
        
        # Detect OI spike
        if call_oi - prev_call_oi > 10000:
            alerts.append(f"Call OI Spike at {strike} - {call_oi}")
        if put_oi - prev_put_oi > 10000:
            alerts.append(f"Put OI Spike at {strike} - {put_oi}")
        
        # Detect large trades (70+ lots in one go)
        if option["call_bid_qty"] >= 70 * 50:
            alerts.append(f"Large Call Trade at {strike}")
        if option["put_bid_qty"] >= 70 * 50:
            alerts.append(f"Large Put Trade at {strike}")
    
    return alerts

def monitor_all_fno_stocks():
    fno_stocks = get_fno_stocks()  # Get list of all F&O stocks
    previous_data = load_previous_data()

    while True:
        all_stock_alerts = {}
        for stock in fno_stocks:
            expiry_date = get_nearest_expiry(stock)  # Fetch nearest expiry date dynamically
            current_data = get_option_chain(stock, expiry_date)

            if not current_data:
                print(f"No data found for {stock}")
                continue

            # Detect spikes
            alerts = detect_spikes(previous_data.get(stock, {}), current_data)
            if alerts:
                all_stock_alerts[stock] = alerts
                print(f"ALERTS for {stock}:")
                for alert in alerts:
                    print(alert)

            # Store latest data
            previous_data[stock] = {str(opt["strike"]): opt for opt in current_data}

        # Save alerts and latest data
        save_data(previous_data)

        time.sleep(60)  # Run every 60 seconds

if __name__ == "__main__":
    monitor_all_fno_stocks()

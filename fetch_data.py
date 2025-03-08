import requests
import pandas as pd
from config import ACCESS_TOKEN, BASE_URL
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

def get_fno_stocks():
    """Fetch all F&O stocks dynamically."""
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        scripts = pd.read_json(url)

        # Filter only F&O stocks
        fno_stocks = scripts[scripts["segment"] == "NSE_FO"]["trading_symbol"].unique().tolist()

        return fno_stocks
    except Exception as e:
        print("Error fetching F&O stocks:", e)
        return []

def get_instrument_key(symbol):
    """Fetch instrument key for a given stock symbol."""
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        scripts = pd.read_json(url)
        instrument_key = scripts[scripts["trading_symbol"] == symbol]["instrument_key"].values
        return instrument_key[0] if len(instrument_key) > 0 else None
    except Exception as e:
        print(f"Error fetching instrument key: {e}")
        return None

def get_expiry_dates(instrument_key):
    """Fetch expiry dates for a given instrument key and sort them correctly by month."""
    url = f"{BASE_URL}/option/contract"
    params = {"instrument_key": instrument_key}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        expiry_dates = sorted(data["data"], key=lambda x: pd.to_datetime(x["expiry"])) if "data" in data and data["data"] else []
        return [item["expiry"] for item in expiry_dates]
    except Exception as e:
        print(f"Error fetching expiry dates: {e}")
        return []

def get_nearest_expiry(symbol):
    """Fetch the nearest expiry date for a stock."""
    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return None

    expiry_dates = get_expiry_dates(instrument_key)
    return expiry_dates[0] if expiry_dates else None

def get_option_chain(instrument_key, expiry_date):
    """Fetch option chain data."""
    url = f"{BASE_URL}/option/chain"
    params = {"instrument_key": instrument_key, "expiry_date": expiry_date}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        return data["data"] if "data" in data else []
    except Exception as e:
        print(f"Error fetching option chain: {e}")
        return []

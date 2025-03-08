import requests
import pandas as pd
from config import ACCESS_TOKEN, BASE_URL

headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

def get_instrument_key(symbol):
    """Fetch instrument key for a given stock symbol."""
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
        scripts = pd.read_json(url)
        instrument_key = scripts[scripts["trading_symbol"] == symbol]["instrument_key"].values
        return instrument_key[0] if len(instrument_key) > 0 else None
    except Exception as e:
        return None

def get_expiry_dates(instrument_key):
    """Fetch expiry dates for a given instrument key."""
    url = f"{BASE_URL}/option/contract"
    params = {"instrument_key": instrument_key}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        return [item["expiry"] for item in data["data"]] if "data" in data and data["data"] else []
    except Exception as e:
        return []

def get_option_chain(instrument_key, expiry_date):
    """Fetch option chain data."""
    url = f"{BASE_URL}/option/chain"
    params = {"instrument_key": instrument_key, "expiry_date": expiry_date}

    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        return data["data"] if "data" in data else []
    except Exception as e:
        return []

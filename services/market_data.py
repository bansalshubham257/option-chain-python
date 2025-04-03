import yfinance as yf
import requests
import pytz
from datetime import datetime
from config import Config

class MarketDataService:
    def __init__(self):
        self.INDIAN_INDICES = [
            {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
            {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
            {"name": "FinNifty", "symbol": "^NSEFIN", "color": "#2ca02c"},
            {"name": "Midcap Nifty", "symbol": "^NSEMDCP50", "color": "#d62728"},
            {"name": "Sensex", "symbol": "^BSESN", "color": "#9467bd"}
        ]

        self.NIFTY_50_STOCKS = [
            'RELIANCE.NS', 'HDFCBANK.NS', 'TCS.NS', 'BHARTIARTL.NS', 'ICICIBANK.NS',
            'SBIN.NS', 'INFY.NS', 'BAJFINANCE.NS', 'HINDUNILVR.NS', 'ITC.NS'
        ]
        self.market_open = Config.MARKET_OPEN
        self.market_close = Config.MARKET_CLOSE

    def is_market_open(self):
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)

        # Market is closed on weekends
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False

        # Check market hours from config
        current_time = now.time()
        return self.market_open <= current_time <= self.market_close

    def get_fii_dii_data(self, year_month, request_type):
        if not year_month:
            return {"error": "year_month parameter is required"}

        api_url = f"https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data?request_type={request_type}&year_month={year_month}"
        response = requests.get(api_url)
        return response.json() if response.status_code == 200 else {"error": "Failed to fetch data"}

    def get_global_market_data(self):
        try:
            return {
                'timestamp': datetime.now().isoformat(),
                'indices': self._get_global_indices(),
                'cryptocurrencies': self._get_top_crypto(),
                'commodities': self._get_commodities()
            }
        except Exception as e:
            return {'error': str(e)}

    def get_indian_market_data(self):
        ist = pytz.timezone('Asia/Kolkata')
        update_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

        try:
            indices = self._get_yfinance_indices()
            gainers, losers = self._get_yfinance_top_movers()
            return {
                "success": True,
                "indices": indices,
                "top_gainers": gainers,
                "top_losers": losers,
                "last_updated": update_time
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "last_updated": update_time
            }

    def get_ipos(self):
        response = requests.get('https://webapi.niftytrader.in/webapi/Other/ipo-company-list')
        return response.json().get('resultData', []) if response.status_code == 200 else {"error": "Failed to fetch IPOs"}

    # Helper methods
    def _get_global_indices(self):
        indices = {
            '^GSPC': 'S&P 500', '^DJI': 'Dow Jones', '^IXIC': 'NASDAQ',
            '^FTSE': 'FTSE 100', '^N225': 'Nikkei 225', '^HSI': 'Hang Seng'
        }

        results = []
        for symbol, name in indices.items():
            try:
                data = yf.Ticker(symbol).history(period='1d')
                if not data.empty:
                    last_close = data['Close'].iloc[-1]
                    prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                    change = last_close - prev_close
                    percent_change = (change / prev_close) * 100

                    results.append({
                        'symbol': symbol, 'name': name, 'price': round(last_close, 2),
                        'change': round(change, 2), 'percent_change': round(percent_change, 2)
                    })
            except Exception:
                continue

        return sorted(results, key=lambda x: abs(x['percent_change']), reverse=True)

    def _get_top_crypto(self, limit=5):
        cryptos = {
            'BTC-USD': 'Bitcoin', 'ETH-USD': 'Ethereum', 'BNB-USD': 'Binance Coin',
            'SOL-USD': 'Solana', 'XRP-USD': 'XRP'
        }

        results = []
        for symbol, name in list(cryptos.items())[:limit]:
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(period='1d')

                if not data.empty:
                    last_close = data['Close'].iloc[-1]
                    prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                    change = last_close - prev_close
                    percent_change = (change / prev_close) * 100

                    results.append({
                        'name': name, 'symbol': symbol.replace('-USD', ''),
                        'price': round(last_close, 2), 'percent_change_24h': round(percent_change, 2)
                    })
            except Exception:
                continue

        return results

    def _get_commodities(self):
        commodities = {
            'GC=F': 'Gold', 'SI=F': 'Silver', 'CL=F': 'Crude Oil',
            'NG=F': 'Natural Gas', 'HG=F': 'Copper'
        }

        results = []
        for symbol, name in commodities.items():
            try:
                data = yf.Ticker(symbol).history(period='1d')
                if not data.empty:
                    last_close = data['Close'].iloc[-1]
                    prev_close = data['Close'].iloc[-2] if len(data) > 1 else last_close
                    change = last_close - prev_close
                    percent_change = (change / prev_close) * 100

                    results.append({
                        'name': name, 'symbol': symbol,
                        'price': round(last_close, 2), 'change': round(change, 2),
                        'percent_change': round(percent_change, 2)
                    })
            except Exception:
                continue

        return results

    def _get_yfinance_indices(self):
        indices_data = []
        for index in self.INDIAN_INDICES:
            try:
                current_price = self._get_current_price(index["symbol"])
                prev_close = self._get_previous_close(index["symbol"])

                if current_price and prev_close:
                    change = round(current_price - prev_close, 2)
                    change_percent = round((change / prev_close) * 100, 2)

                    indices_data.append({
                        "name": index["name"], "current_price": current_price,
                        "change": change, "change_percent": change_percent,
                        "prev_close": prev_close, "color": index["color"],
                        "status_color": "#2ecc71" if change >= 0 else "#e74c3c"
                    })
            except Exception:
                continue

        return indices_data

    def _get_yfinance_top_movers(self):
        changes = []
        for symbol in self.NIFTY_50_STOCKS:
            try:
                prev_close = self._get_previous_close(symbol)
                current_price = self._get_current_price(symbol)

                if prev_close and current_price:
                    change = round(current_price - prev_close, 2)
                    pct = round((change/prev_close)*100, 2)
                    changes.append({
                        "symbol": symbol.replace(".NS", ""),
                        "lastPrice": current_price,
                        "change": change,
                        "pChange": pct
                    })
            except Exception:
                continue

        changes.sort(key=lambda x: x["pChange"], reverse=True)
        return changes[:5], changes[-5:][::-1]

    def _get_previous_close(self, symbol):
        try:
            hist = yf.Ticker(symbol).history(period="3d", interval="1d")
            return hist['Close'].iloc[-2] if len(hist) >= 2 else None
        except Exception:
            return None

    def _get_current_price(self, symbol):
        try:
            hist = yf.Ticker(symbol).history(period="1d", interval="1m")
            return hist['Close'].iloc[-1] if not hist.empty else None
        except Exception:
            return None

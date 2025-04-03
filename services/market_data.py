import time

import yfinance as yf
import requests
import pytz
from datetime import datetime
from config import Config
from services.option_chain import OptionChainService

class MarketDataService:
    def __init__(self, database_service=None, option_chain_service=None):
        self.option_chain_service = option_chain_service
        self.INDIAN_INDICES = [
            {"name": "Nifty 50", "symbol": "^NSEI", "color": "#1f77b4"},
            {"name": "Nifty Bank", "symbol": "^NSEBANK", "color": "#ff7f0e"},
            {"name": "FinNifty", "symbol": "NIFTY_FIN_SERVICE.NS", "color": "#2ca02c"},
            {"name": "Midcap Nifty", "symbol": "^NSEMDCP50", "color": "#d62728"},
            {"name": "Sensex", "symbol": "^BSESN", "color": "#9467bd"}
        ]

        self.NIFTY_50_STOCKS = [
            'RELIANCE.NS', 'HDFCBANK.NS', 'TCS.NS', 'BHARTIARTL.NS', 'ICICIBANK.NS',
            'SBIN.NS', 'INFY.NS', 'BAJFINANCE.NS', 'HINDUNILVR.NS', 'ITC.NS'
        ]
        self.market_open = Config.MARKET_OPEN
        self.market_close = Config.MARKET_CLOSE
        self.database = database_service
        self.update_interval = 300  # 5 minutes
        self.last_update = 0

    def is_market_open(self):
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)

        # Market is closed on weekends
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False

        # Check market hours from config
        current_time = now.time()
        return self.market_open <= current_time <= self.market_close

    def update_all_market_data(self):
        """Update all market data in one go"""
        current_time = time.time()
        if current_time - self.last_update < self.update_interval:
            return False

        try:
            # Indian market data
            indian_data = self._get_indian_market_data_direct()
            if self.database:
                self.database.save_market_data('indian_market', indian_data)

            # Global market data
            global_data = self._get_global_market_data_direct()
            if self.database:
                self.database.save_market_data('global_market', global_data)

            # FII/DII data (monthly, so we don't need to update frequently)
            if datetime.now().day == 1:  # Update on 1st of each month
                fii_dii = self._get_fii_dii_data_direct()
                if self.database:
                    self.database.save_market_data('fii_dii', fii_dii)

            # IPOs data
            ipos = self._get_ipos_direct()
            if self.database:
                self.database.save_market_data('ipos', ipos)

            self.last_update = current_time
            return True

        except Exception as e:
            print(f"Error updating market data: {e}")
            return False

    def _get_indian_market_data_direct(self):
        """Direct API call for Indian market data (without cache)"""
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

    def _get_global_market_data_direct(self):
        """Direct API call for global market data (without cache)"""
        try:
            return {
                'timestamp': datetime.now().isoformat(),
                'indices': self._get_global_indices(),
                'cryptocurrencies': self._get_top_crypto(),
                'commodities': self._get_commodities()
            }
        except Exception as e:
            return {'error': str(e)}

    def _get_fii_dii_data_direct(self):
        """Direct API call for FII/DII data (without cache)"""
        year_month = datetime.now().strftime("%Y-%m")
        api_url = f"https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data?request_type=combined&year_month={year_month}"
        response = requests.get(api_url)
        return response.json() if response.status_code == 200 else {"error": "Failed to fetch data"}

    def _get_ipos_direct(self):
        """Direct API call for IPOs data (without cache)"""
        response = requests.get('https://webapi.niftytrader.in/webapi/Other/ipo-company-list')
        return response.json().get('resultData', []) if response.status_code == 200 else {"error": "Failed to fetch IPOs"}

    def get_global_market_data(self):
        if self.database:
            cached = self.database.get_market_data('global_market')
            if cached:
                return cached
        return self._get_global_market_data_direct()

    def get_indian_market_data(self):
        if self.database:
            cached = self.database.get_market_data('indian_market')
            if cached:
                return cached
        return self._get_indian_market_data_direct()

    def get_fii_dii_data(self, year_month=None, request_type=None):
        if not year_month:
            year_month = datetime.now().strftime("%Y-%m")
        if not request_type:
            request_type = "combined"

        if self.database:
            cached = self.database.get_market_data('fii_dii')
            if cached:
                return cached
        return self._get_fii_dii_data_direct()

    def get_ipos(self):
        if self.database:
            cached = self.database.get_market_data('ipos')
            if cached:
                return cached
        return self._get_ipos_direct()

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

    def get_fno_stocks(self):
        """Get F&O stocks list from OptionChainService"""
        if self.option_chain_service:
            return self.option_chain_service.get_fno_stocks_with_symbols()
        return []  # Fallback if option_chain_service not available


    def _get_yfinance_top_movers(self):
        stocks = self.get_fno_stocks()
        if not stocks:
            return [], []  # Return empty lists if no stocks available

        changes = []
        for symbol in stocks:
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

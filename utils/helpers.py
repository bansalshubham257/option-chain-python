import re
import pytz
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from decimal import Decimal
from typing import Optional, Dict, List, Union

class Helpers:
    @staticmethod
    def parse_option_symbol(symbol: str) -> Optional[Dict[str, Union[str, int]]]:
        """Parse option symbol into its components"""
        match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)", symbol)
        if match:
            return {
                "stock": match.group(1),
                "expiry": match.group(2),
                "strike_price": int(match.group(3)),
                "option_type": match.group(4)
            }
        return None

    @staticmethod
    def get_ist_time() -> datetime:
        """Get current time in Indian Standard Time"""
        return datetime.now(pytz.timezone('Asia/Kolkata'))

    @staticmethod
    def format_ist_time(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
        """Format datetime object to IST string"""
        if not dt.tzinfo:
            dt = pytz.utc.localize(dt)
        return dt.astimezone(pytz.timezone('Asia/Kolkata')).strftime(fmt)

    @staticmethod
    def is_market_open() -> bool:
        """Check if Indian stock market is open"""
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()

        # Market is closed on weekends
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False

        return market_open <= now.time() <= market_close

    @staticmethod
    def get_yfinance_data(symbol: str, period: str = "1d", interval: str = "1m") -> Optional[pd.DataFrame]:
        """Get data from Yahoo Finance with error handling"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            return data if not data.empty else None
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            return None

    @staticmethod
    def convert_to_decimal(value: Union[str, float, int]) -> Decimal:
        """Convert value to Decimal safely"""
        try:
            return Decimal(str(value))
        except:
            return Decimal(0)

    @staticmethod
    def round_to_tick(price: float, tick_size: float = 0.05) -> float:
        """Round price to nearest tick size"""
        return round(price / tick_size) * tick_size

    @staticmethod
    def calculate_percentage_change(current: float, previous: float) -> float:
        """Calculate percentage change between two values"""
        if previous == 0:
            return 0.0
        return ((current - previous) / previous) * 100

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize stock symbol to consistent format"""
        return symbol.upper().replace('.NS', '').replace('.BO', '')

    @staticmethod
    def generate_unique_id() -> str:
        """Generate a unique ID for orders/trades"""
        return str(int(datetime.now().timestamp() * 1000))

    @staticmethod
    def validate_expiry_date(expiry: str) -> bool:
        """Validate option expiry date format (YYYY-MM-DD)"""
        try:
            datetime.strptime(expiry, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def get_next_expiry() -> str:
        """Get next Thursday's date (standard F&O expiry)"""
        today = datetime.now()
        days_ahead = (3 - today.weekday()) % 7  # 3 = Thursday
        if days_ahead <= 0:  # If today is Thursday, get next Thursday
            days_ahead += 7
        next_thursday = today + timedelta(days=days_ahead)
        return next_thursday.strftime("%Y-%m-%d")

    @staticmethod
    def calculate_lot_size(price: float, capital: float = 100000) -> int:
        """Calculate appropriate lot size based on price and capital"""
        return max(1, int(capital // price))

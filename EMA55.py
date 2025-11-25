import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf

# ✅ Make sure this import matches your project structure
from services.option_chain import OptionChainService


class EMATrendScanner:
    def __init__(
            self,
            ema_length: int = 55,
            yf_fetch_period: str = "30d",
            base_interval: str = "15m",
            max_symbols: Optional[int] = None,
    ):
        """
        ema_length: EMA period (55)
        yf_fetch_period: how much history to fetch from yfinance (e.g. '30d')
        base_interval: base timeframe for fetching data ('15m')
        max_symbols: limit number of symbols while testing
        """
        self.ema_length = ema_length
        self.yf_fetch_period = yf_fetch_period
        self.base_interval = base_interval
        self.max_symbols = max_symbols

    # ------------------------------------------------------------------
    # EMA calculation
    # ------------------------------------------------------------------
    @staticmethod
    def compute_ema(series: pd.Series, length: int) -> pd.Series:
        """
        Standard EMA using pandas ewm.
        We keep min_periods=1 so EMA is available as soon as data starts.
        """
        series = series.astype("float64")
        ema = series.ewm(span=length, adjust=False, min_periods=1).mean()
        return ema

    # ------------------------------------------------------------------
    # F&O symbols
    # ------------------------------------------------------------------
    def get_fno_symbols(self) -> List[str]:
        """
        Uses OptionChainService().get_fno_stocks_with_symbols().
        """
        svc = OptionChainService()
        symbols = svc.get_fno_stocks_with_symbols()
        if not isinstance(symbols, list):
            raise ValueError("OptionChainService().get_fno_stocks_with_symbols() must return a list of symbol strings")
        if self.max_symbols:
            symbols = symbols[: self.max_symbols]
        return symbols

    # ------------------------------------------------------------------
    # 15m OHLCV from yfinance
    # ------------------------------------------------------------------
    def fetch_15m_ohlcv(self, symbol: str) -> pd.DataFrame:
        """
        Fetch base-interval (15m) OHLCV data using yfinance.
        Handles possible MultiIndex columns from yfinance.
        """
        try:
            df = yf.download(
                tickers=symbol,
                period=self.yf_fetch_period,
                interval=self.base_interval,
                progress=False,
                threads=False,
                auto_adjust=False,  # explicitly set to avoid FutureWarning
            )
        except Exception as e:
            print(f"yfinance error for {symbol}: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # Handle MultiIndex columns (can happen in some yfinance versions)
        if isinstance(df.columns, pd.MultiIndex):
            try:
                if symbol in df.columns.get_level_values(1):
                    df = df.xs(symbol, axis=1, level=1)
            except Exception:
                if df.columns.nlevels > 1:
                    df = df.droplevel(0, axis=1)

        # Clean column names & index
        df = df.rename(columns=lambda s: str(s).strip())
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert(None)

        # Ensure we have standard OHLCV columns; otherwise skip this symbol
        required_cols = {"Open", "High", "Low", "Close", "Volume"}
        if not required_cols.issubset(set(df.columns)):
            print(f"Skipping {symbol}: missing OHLCV columns. Got: {list(df.columns)}")
            return pd.DataFrame()

        return df

    # ------------------------------------------------------------------
    # Build higher timeframe candles from 15m via resampling
    # ------------------------------------------------------------------
    @staticmethod
    def build_timeframe_from_15m(df_15m: pd.DataFrame, rule: str) -> pd.DataFrame:
        """
        Resample 15m data to higher timeframe (e.g. '45min', '90min', '180min').
        Returns OHLCV DataFrame.
        """
        if df_15m.empty:
            return pd.DataFrame()

        required_cols = {"Open", "High", "Low", "Close", "Volume"}
        if not required_cols.issubset(set(df_15m.columns)):
            return pd.DataFrame()

        df_tf = df_15m.resample(rule).agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        df_tf = df_tf.dropna(subset=["Open", "High", "Low", "Close"])
        return df_tf

    # ------------------------------------------------------------------
    # Check if price crossed ABOVE EMA on last bar (bullish cross)
    # ------------------------------------------------------------------
    def _is_bullish_cross(self, closes: pd.Series, ema: pd.Series) -> bool:
        # Ensure Series (not 1-col DataFrame)
        if isinstance(closes, pd.DataFrame):
            closes = closes.iloc[:, 0]
        if isinstance(ema, pd.DataFrame):
            ema = ema.iloc[:, 0]

        if len(closes) < 2 or len(ema) < 2:
            return False

        prev_close = closes.iloc[-2]
        last_close = closes.iloc[-1]
        prev_ema = ema.iloc[-2]
        last_ema = ema.iloc[-1]

        # Bullish cross from below to above
        return (prev_close <= prev_ema) and (last_close > last_ema)

    # ------------------------------------------------------------------
    # Check if price crossed BELOW EMA on last bar (bearish cross)
    # ------------------------------------------------------------------
    def _is_bearish_cross(self, closes: pd.Series, ema: pd.Series) -> bool:
        # Ensure Series (not 1-col DataFrame)
        if isinstance(closes, pd.DataFrame):
            closes = closes.iloc[:, 0]
        if isinstance(ema, pd.DataFrame):
            ema = ema.iloc[:, 0]

        if len(closes) < 2 or len(ema) < 2:
            return False

        prev_close = closes.iloc[-2]
        last_close = closes.iloc[-1]
        prev_ema = ema.iloc[-2]
        last_ema = ema.iloc[-1]

        # Bearish cross from above to below
        return (prev_close >= prev_ema) and (last_close < last_ema)

    # ------------------------------------------------------------------
    # Scan a single symbol
    # ------------------------------------------------------------------
    def scan_symbol(self, symbol: str) -> Optional[Dict]:
        """
        Conditions:
        - On 15m: price has JUST crossed 55 EMA (bullish OR bearish).
        - Bullish: 45m, 90m, 180m closes > 55 EMA (uptrend alignment).
        - Bearish: 45m, 90m, 180m closes < 55 EMA (downtrend alignment).
        """
        df_15m = self.fetch_15m_ohlcv(symbol)
        if df_15m.empty:
            return None

        if len(df_15m) < self.ema_length + 2:
            return None

        # 15m EMA & cross detection
        ema_15 = self.compute_ema(df_15m["Close"], self.ema_length)
        bullish_cross = self._is_bullish_cross(df_15m["Close"], ema_15)
        bearish_cross = self._is_bearish_cross(df_15m["Close"], ema_15)

        # If no fresh cross, skip
        if not (bullish_cross or bearish_cross):
            return None

        last_close_15 = df_15m["Close"].iloc[-1]
        last_ema_15 = ema_15.iloc[-1]

        # Build higher timeframes from the same 15m data
        timeframes = {
            "45m": "45min",
            "90m": "90min",
            "180m": "180min",
        }

        tf_status: Dict[str, Dict] = {}
        all_tf_above = True
        all_tf_below = True

        for tf_name, rule in timeframes.items():
            df_tf = self.build_timeframe_from_15m(df_15m, rule)
            if df_tf.empty:
                tf_status[tf_name] = {
                    "available": False,
                    "close": None,
                    "ema": None,
                    "above_ema": False,
                    "below_ema": False,
                }
                all_tf_above = False
                all_tf_below = False
                continue

            ema_tf = self.compute_ema(df_tf["Close"], self.ema_length)
            last_close_tf = df_tf["Close"].iloc[-1]
            last_ema_tf = ema_tf.iloc[-1]

            above_ema = last_close_tf > last_ema_tf
            below_ema = last_close_tf < last_ema_tf

            tf_status[tf_name] = {
                "available": True,
                "close": last_close_tf,
                "ema": last_ema_tf,
                "above_ema": above_ema,
                "below_ema": below_ema,
            }

            if not above_ema:
                all_tf_above = False
            if not below_ema:
                all_tf_below = False

        # Direction-specific alignment check
        direction: Optional[str] = None

        if bullish_cross and all_tf_above:
            direction = "bullish"
        elif bearish_cross and all_tf_below:
            direction = "bearish"
        else:
            # Cross happened, but higher timeframes are not aligned
            return None

        return {
            "symbol": symbol,
            "direction": direction,  # "bullish" or "bearish"
            "latest_time": df_15m.index[-1],
            "current_price_15m": last_close_15,
            "ema_15m": last_ema_15,
            "timeframes": tf_status,
        }

    # ------------------------------------------------------------------
    # Run scanner over all F&O symbols once
    # ------------------------------------------------------------------
    def run(self) -> List[Dict]:
        symbols = self.get_fno_symbols()
        results: List[Dict] = []

        for i, sym in enumerate(symbols, start=1):
            try:
                res = self.scan_symbol(sym)
                if res:
                    results.append(res)
                    direction = res["direction"].upper()
                    trend_side = "above" if res["direction"] == "bullish" else "below"
                    print(
                        f"[{i}/{len(symbols)}] {res['symbol']}: "
                        f"{direction} cross 15m | "
                        f"15m close={res['current_price_15m']:.2f}, "
                        f"15m EMA={res['ema_15m']:.2f} | "
                        f"45m/90m/180m all {trend_side} 55EMA"
                    )
            except Exception as e:
                print(f"[{i}/{len(symbols)}] Error scanning {sym}: {e}")

        return results

    # ------------------------------------------------------------------
    # Run continuously every N seconds
    # ------------------------------------------------------------------
    def run_continuous(self, interval_seconds: int = 900):
        """
        Run the scanner in an infinite loop.
        interval_seconds = how often to rescan (default 900s = 15 minutes).
        Stop with Ctrl + C.
        """
        while True:
            start = datetime.now()
            print(f"\n[{start}] Starting EMA scan...")
            try:
                results = self.run()
                total_matches = len(results)
                print(f"[{datetime.now()}] Scan completed. Matches found: {total_matches}")
            except Exception as e:
                print(f"Unexpected error in scan loop: {e}")
            time.sleep(interval_seconds)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
if __name__ == "__main__":
    scanner = EMATrendScanner(
        ema_length=55,
        yf_fetch_period="30d",
        base_interval="15m",
        max_symbols=None,  # e.g. 50 while testing
    )

    # OPTION 1: run once
    # scanner.run()

    # OPTION 2: run continuously (default ~15 minutes)
    scanner.run_continuous(interval_seconds=30)

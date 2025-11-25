import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

# ✅ Make sure this import matches your project structure
# from your_module import OptionChainService
# or, if it's in the same folder:
from services.option_chain import OptionChainService


class FNOScanner:
    def __init__(
            self,
            rsi_length: int = 21,
            rsi_target_high: int = 65,
            rsi_target_low: int = 35,
            rsi_tolerance: float = 2.0,
            price_pct_tolerance: float = 0.0025,  # 0.25% "near" threshold
            yf_fetch_period: str = "5d",
            yf_interval: str = "15m",
            max_symbols: Optional[int] = None,
    ):
        self.rsi_length = rsi_length
        self.rsi_target_high = rsi_target_high
        self.rsi_target_low = rsi_target_low
        self.rsi_tolerance = rsi_tolerance
        self.price_pct_tolerance = price_pct_tolerance
        self.yf_fetch_period = yf_fetch_period
        self.yf_interval = yf_interval
        self.max_symbols = max_symbols

    # ------------------------------------------------------------------
    # RSI (Wilder-style using EMA smoothing)
    # ------------------------------------------------------------------
    @staticmethod
    def compute_rsi(series: pd.Series, length: int) -> pd.Series:
        """
        Wilder RSI implementation:
        - delta = price changes
        - gains = positive deltas
        - losses = negative deltas (as positive)
        - EMA smoothing with alpha = 1/length
        """
        series = series.astype("float64")
        delta = series.diff()

        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
        avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    # ------------------------------------------------------------------
    # Camarilla Pivots
    # ------------------------------------------------------------------
    @staticmethod
    def compute_camarilla_pivots(prev_high: float, prev_low: float, prev_close: float) -> Dict[str, float]:
        """
        Camarilla pivot levels using common multipliers:
        R1 = C + (H - L) * 1.1/12
        R2 = C + (H - L) * 1.1/6
        R3 = C + (H - L) * 1.1/4
        R4 = C + (H - L) * 1.1/2
        S1 = C - (H - L) * 1.1/12
        S2 = C - (H - L) * 1.1/6
        S3 = C - (H - L) * 1.1/4
        S4 = C - (H - L) * 1.1/2
        """
        rng = prev_high - prev_low
        factor = 1.1

        l1 = (rng * factor) / 12.0
        l2 = (rng * factor) / 6.0
        l3 = (rng * factor) / 4.0
        l4 = (rng * factor) / 2.0

        return {
            "R1": prev_close + l1,
            "R2": prev_close + l2,
            "R3": prev_close + l3,
            "R4": prev_close + l4,
            "S1": prev_close - l1,
            "S2": prev_close - l2,
            "S3": prev_close - l3,
            "S4": prev_close - l4,
        }

    @staticmethod
    def pct_diff(a: float, b: float) -> float:
        if b == 0:
            return float("inf")
        return abs(a - b) / b

    # ------------------------------------------------------------------
    # F&O symbols
    # ------------------------------------------------------------------
    def get_fno_symbols(self) -> List[str]:
        """
        Uses OptionChainService().get_fno_stocks() as requested.
        No static/fallback data.
        Assumes symbols already have .NS if needed (no explicit adding).
        """
        svc = OptionChainService()
        symbols = svc.get_fno_stocks_with_symbols()
        if not isinstance(symbols, list):
            raise ValueError("OptionChainService().get_fno_stocks() must return a list of symbol strings")
        if self.max_symbols:
            symbols = symbols[: self.max_symbols]
        return symbols

    # ------------------------------------------------------------------
    # 15m OHLCV from yfinance
    # ------------------------------------------------------------------
    def fetch_15m_ohlcv(self, symbol: str) -> pd.DataFrame:
        """
        Fetch 15m OHLCV data for given symbol using yfinance.
        No symbol suffix manipulation here (respecting your requirement).
        """
        try:
            df = yf.download(
                tickers=symbol,
                period=self.yf_fetch_period,
                interval=self.yf_interval,
                progress=False,
                threads=False,
                auto_adjust=False,  # explicitly set to avoid FutureWarning in new yfinance
            )
        except Exception as e:
            print(f"yfinance error for {symbol}: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns=lambda s: s.strip())
        return df

    # ------------------------------------------------------------------
    # Previous trading day OHLC from 15m data
    # ------------------------------------------------------------------
    def _get_previous_trading_day_ohlc(self, df_15m: pd.DataFrame) -> Optional[Tuple[float, float, float]]:
        if df_15m.empty:
            return None

        df = df_15m.copy()

        # Handle tz-aware index if present
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert(None)

        df["date"] = df.index.date
        dates = sorted(df["date"].unique())
        if not dates:
            return None

        if len(dates) < 2:
            # only one date available; use that
            target_date = dates[0]
        else:
            # use previous full day (second last date)
            target_date = dates[-2]

        subset = df[df["date"] == target_date]
        if subset.empty:
            return None

        prev_high = subset["High"].max()
        prev_low = subset["Low"].min()
        prev_close = subset["Close"].iloc[-1]

        # Ensure plain Python floats
        prev_high = prev_high.item() if hasattr(prev_high, "item") else float(prev_high)
        prev_low = prev_low.item() if hasattr(prev_low, "item") else float(prev_low)
        prev_close = prev_close.item() if hasattr(prev_close, "item") else float(prev_close)

        return prev_high, prev_low, prev_close

    # ------------------------------------------------------------------
    # Scan a single symbol
    # ------------------------------------------------------------------
    def scan_symbol(self, symbol: str) -> Optional[Dict]:
        df = self.fetch_15m_ohlcv(symbol)
        if df.empty or len(df) < self.rsi_length + 2:
            return None

        # RSI
        rsi_series = self.compute_rsi(df["Close"], self.rsi_length)
        rsi_clean = rsi_series.dropna()
        if rsi_clean.empty:
            return None

        last_rsi = rsi_clean.iloc[-1]
        last_rsi = last_rsi.item() if hasattr(last_rsi, "item") else float(last_rsi)

        # Current price
        current_price = df["Close"].iloc[-1]
        current_price = current_price.item() if hasattr(current_price, "item") else float(current_price)

        # Previous day OHLC for pivots
        prev_ohlc = self._get_previous_trading_day_ohlc(df)
        if prev_ohlc is None:
            return None
        prev_high, prev_low, prev_close = prev_ohlc

        pivots = self.compute_camarilla_pivots(prev_high, prev_low, prev_close)

        matches = []
        tol = self.price_pct_tolerance

        # Check near R2 / R4 with RSI near 65
        for level_name in ("R2", "R4"):
            level_price = pivots[level_name]
            if (
                    self.pct_diff(current_price, level_price) <= tol
                    and abs(last_rsi - self.rsi_target_high) <= self.rsi_tolerance
            ):
                matches.append(
                    {
                        "symbol": symbol,
                        "side": "resistance",
                        "level": level_name,
                        "level_price": level_price,
                        "current_price": current_price,
                        "rsi": last_rsi,
                        "prev_high": prev_high,
                        "prev_low": prev_low,
                        "prev_close": prev_close,
                    }
                )

        # Check near S2 / S4 with RSI near 35
        for level_name in ("S2", "S4"):
            level_price = pivots[level_name]
            if (
                    self.pct_diff(current_price, level_price) <= tol
                    and abs(last_rsi - self.rsi_target_low) <= self.rsi_tolerance
            ):
                matches.append(
                    {
                        "symbol": symbol,
                        "side": "support",
                        "level": level_name,
                        "level_price": level_price,
                        "current_price": current_price,
                        "rsi": last_rsi,
                        "prev_high": prev_high,
                        "prev_low": prev_low,
                        "prev_close": prev_close,
                    }
                )

        if not matches:
            return None

        return {
            "symbol": symbol,
            "matches": matches,
            "pivots": pivots,
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
                    for m in res["matches"]:
                        print(
                            f"[{i}/{len(symbols)}] {m['symbol']}: "
                            #f"{m['side'].upper()} {m['level']} @ {m['level_price']:.2f} | "
                            #f"price={m['current_price']:.2f} | RSI={m['rsi']:.2f}"
                        )
            except Exception as e:
                print(f"[{i}/{len(symbols)}] Error scanning {sym}: {e}")

        return results

    # ------------------------------------------------------------------
    # Run continuously every N seconds
    # ------------------------------------------------------------------
    def run_continuous(self, interval_seconds: int = 5):
        """
        Run the scanner in an infinite loop.
        interval_seconds = how often to rescan (default 900s = 15 minutes).
        Stop with Ctrl + C.
        """
        while True:

            try:
                self.run()
            except Exception as e:
                print(f"Unexpected error in scan loop: {e}")

            time.sleep(interval_seconds)


# ----------------------------------------------------------------------
# MAIN METHOD
# ----------------------------------------------------------------------
if __name__ == "__main__":
    scanner = FNOScanner(
        rsi_length=21,
        rsi_target_high=65,
        rsi_target_low=35,
        rsi_tolerance=2.0,           # RSI +/- 2 around 65 / 35
        price_pct_tolerance=0.0025,  # 0.25% around pivot level
        yf_fetch_period="5d",
        yf_interval="15m",
        max_symbols=None,            # set e.g. 50 while testing
    )

    # OPTION 1: run once
    # scanner.run()

    # OPTION 2: run continuously every 15 minutes
    scanner.run_continuous(interval_seconds=5)

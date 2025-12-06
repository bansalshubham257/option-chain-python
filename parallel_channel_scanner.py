#!/usr/bin/env python3
"""
Parallel Channel Scanner for Indian Stocks
This script scans all Indian stocks to find those trading in parallel channels
and are below the middle line or near support.
"""

import pandas as pd
import numpy as np
import yfinance as yf
import concurrent.futures
from datetime import datetime
import time
from tqdm import tqdm
import matplotlib.pyplot as plt
from scipy.stats import linregress


def read_stock_symbols():
    """Read stock symbols from the NSE stocks CSV file"""
    try:
        stocks_df = pd.read_csv("nse_stocks.csv")
        # Add .NS suffix for NSE stocks in Yahoo Finance
        symbols = [f"{symbol}.NS" for symbol in stocks_df['SYMBOL'].tolist()]
        return symbols
    except Exception as e:
        print(f"Error reading stock symbols: {e}")
        return []


def download_stock_data(symbol, period="1y", interval="1d"):
    """Download historical data for a given stock symbol"""
    try:
        stock = yf.Ticker(symbol)
        history = stock.history(period=period, interval=interval)
        if len(history) > 30:  # Ensure we have enough data
            return symbol, history
        return None
    except Exception as e:
        return None


def detect_parallel_channel(df):
    """
    Detect if a stock is trading in a parallel channel

    Returns:
        dict: Channel information if detected, None otherwise
    """
    # We need enough data points
    if len(df) < 30:
        return None

    # Get high and low prices
    highs = df['High'].values
    lows = df['Low'].values
    closes = df['Close'].values

    # Create x-axis (time index)
    x = np.array(range(len(df)))

    # Find local maxima for resistance line
    window = 10  # Window to look for local maxima
    high_points_x = []
    high_points_y = []

    for i in range(window, len(highs) - window):
        if highs[i] == max(highs[i-window:i+window+1]):
            high_points_x.append(i)
            high_points_y.append(highs[i])

    # Find local minima for support line
    low_points_x = []
    low_points_y = []

    for i in range(window, len(lows) - window):
        if lows[i] == min(lows[i-window:i+window+1]):
            low_points_x.append(i)
            low_points_y.append(lows[i])

    # We need at least 2 points for each line
    if len(high_points_x) < 2 or len(low_points_x) < 2:
        return None

    # Calculate resistance line
    resistance_slope, resistance_intercept, r_value_resistance, _, _ = linregress(high_points_x, high_points_y)

    # Calculate support line
    support_slope, support_intercept, r_value_support, _, _ = linregress(low_points_x, low_points_y)

    # Check if lines are roughly parallel (slopes are similar)
    slope_diff = abs(resistance_slope - support_slope)
    avg_slope = (abs(resistance_slope) + abs(support_slope)) / 2

    # Parallel channel criteria:
    # 1. Lines have good fit (r_value)
    # 2. Slopes are similar (parallel)
    # 3. Channel has meaningful width
    if (r_value_resistance > 0.7 and r_value_support > 0.7 and
            slope_diff / max(0.0001, avg_slope) < 0.3):

        # Calculate channel width and middle line
        last_x = len(df) - 1
        resistance_last = resistance_slope * last_x + resistance_intercept
        support_last = support_slope * last_x + support_intercept
        channel_width = resistance_last - support_last

        # If channel is too narrow relative to price, reject it
        if channel_width / closes[-1] < 0.03:  # Less than 3% channel width
            return None

        # Middle line
        middle_slope = (resistance_slope + support_slope) / 2
        middle_intercept = (resistance_intercept + support_intercept) / 2

        # Calculate current position within channel
        last_close = closes[-1]
        last_resistance = resistance_slope * last_x + resistance_intercept
        last_support = support_slope * last_x + support_intercept
        last_middle = middle_slope * last_x + middle_intercept

        # Position in channel (0 = support, 0.5 = middle, 1 = resistance)
        if last_resistance > last_support:  # Avoid division by zero or negative
            position = (last_close - last_support) / (last_resistance - last_support)
        else:
            position = 0.5

        return {
            "channel_detected": True,
            "position_in_channel": position,
            "support": last_support,
            "middle": last_middle,
            "resistance": last_resistance,
            "r_value_support": r_value_support,
            "r_value_resistance": r_value_resistance,
            "channel_width_percent": channel_width / closes[-1] * 100,
            "support_slope": support_slope,
            "resistance_slope": resistance_slope
        }

    return None


def is_below_middle_or_near_support(channel_info, threshold=0.2):
    """
    Check if stock is below middle line or near support

    Args:
        channel_info: Dictionary with channel data
        threshold: How close to support to be considered "near" (0-1)

    Returns:
        bool: True if stock is below middle or near support
    """
    if not channel_info:
        return False

    position = channel_info["position_in_channel"]

    # Below middle line or within threshold of support line
    return position < 0.5 or position < threshold


def analyze_stock(symbol):
    """Analyze a stock for parallel channel pattern"""
    result = download_stock_data(symbol)
    if not result:
        return None

    symbol, data = result
    channel_info = detect_parallel_channel(data)

    if channel_info and is_below_middle_or_near_support(channel_info):
        # Add stock price and some basic stats
        channel_info["symbol"] = symbol.replace(".NS", "")
        channel_info["current_price"] = data["Close"].iloc[-1]
        channel_info["distance_to_support_percent"] = (
            (channel_info["current_price"] - channel_info["support"]) /
            channel_info["current_price"] * 100
        )
        return channel_info

    return None


def scan_all_stocks():
    """Scan all stocks in the NSE list"""
    symbols = read_stock_symbols()
    print(f"Scanning {len(symbols)} Indian stocks for parallel channels...")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, symbol): symbol for symbol in symbols}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(symbols)):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                symbol = futures[future]
                print(f"Error processing {symbol}: {e}")

    return results


def display_results(results):
    """Display the filtered stocks"""
    if not results:
        print("No stocks found trading in parallel channels below middle line or near support.")
        return

    # Sort by distance to support (ascending)
    results.sort(key=lambda x: x["distance_to_support_percent"])

    print(f"\nFound {len(results)} stocks in parallel channels trading below middle line or near support:")
    print("\n{:<12} {:<10} {:<10} {:<12} {:<12} {:<12} {:<15}".format(
        "Symbol", "Price", "Support", "Resistance", "Position", "To Support%", "Channel Width%"))
    print("-" * 85)

    for stock in results:
        print("{:<12} {:<10.2f} {:<10.2f} {:<12.2f} {:<12.2f} {:<12.2f} {:<15.2f}".format(
            stock["symbol"],
            stock["current_price"],
            stock["support"],
            stock["resistance"],
            stock["position_in_channel"],
            stock["distance_to_support_percent"],
            stock["channel_width_percent"]
        ))


if __name__ == "__main__":
    start_time = time.time()
    results = scan_all_stocks()
    display_results(results)
    print(f"\nScan completed in {time.time() - start_time:.2f} seconds")

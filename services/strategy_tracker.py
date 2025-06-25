import asyncio
import json
import time
import os
import logging
from datetime import datetime, timedelta, date
import calendar
import pytz
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from decimal import Decimal
import MarketDataFeed_pb2 as pb
import requests
import websockets
from concurrent.futures import ThreadPoolExecutor
import threading

from services.database import DatabaseService
from services.option_chain import OptionChainService

# Configure logging - only console output, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only log to console, no file logging
    ]
)
logger = logging.getLogger("strategy_tracker")

class StrategyTracker:
    """
    Service to track and execute option trading strategy:
    - Enter when price change > 5%
    - Take profit at 95% gain
    - Stop loss at 15% loss
    - Track monthly performance
    """

    def __init__(self, db_service=None, strategy_config=None):
        self.db = db_service or DatabaseService()

        # Default strategy configuration
        default_config = {
            "name": "percent_change_tracker",
            "entry_threshold": 5.0,  # Enter when price change > 5%
            "profit_target": 95.0,   # Take profit at 95% gain
            "stop_loss": -15.0,      # Stop loss at 15% loss
            "lot_size_multiplier": 1  # Number of lots to buy
        }

        # Override with custom config if provided
        if strategy_config:
            default_config.update(strategy_config)

        self.strategy_name = default_config["name"]
        self.entry_threshold = default_config["entry_threshold"]
        self.profit_target = default_config["profit_target"]
        self.stop_loss = default_config["stop_loss"]
        self.lot_size_multiplier = default_config["lot_size_multiplier"]

        self.is_running = False
        self.monitoring_instruments = {}  # Track which instruments we're monitoring
        self.active_orders = {}  # Track active orders
        self.tz = pytz.timezone('Asia/Kolkata')

        # Performance tracking
        self.today_capital = 0.0
        self.max_capital = 0.0

        # Worker API base URL for fetching live market data
        # Use localhost instead of 0.0.0.0 for better reliability
        self.worker_api_url = "http://0.0.0.0:8000"

        # WebSocket for fetching live prices
        self.market_data = {}  # Cache for market data received from WebSocket
        self.active_subscription = []  # Instruments currently subscribed to

        # Cache for prices - to avoid hitting API too frequently
        self.price_cache = {}
        self.cache_time = {}
        self.cache_ttl = 1  # Cache TTL in seconds

        # Initialize OptionChainService
        self.option_chain_service = OptionChainService()

        # Tables are already created, no need to initialize

    async def start(self):
        """Start the strategy tracker service"""
        if self.is_running:
            logger.warning("Strategy tracker is already running")
            return

        self.is_running = True
        logger.info("Starting strategy tracker service")

        # Load existing active orders from database
        await self._load_active_orders()

        try:
            # Run the main monitoring loop
            await self._run_monitor_loop()
        except Exception as e:
            logger.error(f"Error in strategy tracker: {str(e)}")
            self.is_running = False

    async def stop(self):
        """Stop the strategy tracker service"""
        logger.info("Stopping strategy tracker service")
        self.is_running = False

    async def _load_active_orders(self):
        """Load active orders from the database"""
        try:
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT id, symbol, strike_price, option_type, entry_price, target_price, 
                           stop_loss, status, instrument_key, expiry_date
                    FROM strategy_orders
                    WHERE status = 'OPEN' AND strategy_name = %s
                """, (self.strategy_name,))

                rows = cur.fetchall()

                for row in rows:
                    order_id = row[0]
                    symbol = row[1]
                    strike = row[2]
                    option_type = row[3]
                    entry_price = float(row[4])
                    target_price = float(row[5])
                    stop_loss_price = float(row[6])
                    instrument_key = row[8]
                    expiry_date = row[9]

                    # Add to active orders
                    key = f"{symbol}_{strike}_{option_type}"
                    self.active_orders[key] = {
                        'id': order_id,
                        'symbol': symbol,
                        'strike_price': strike,
                        'option_type': option_type,
                        'entry_price': entry_price,
                        'target_price': target_price,
                        'stop_loss': stop_loss_price,
                        'instrument_key': instrument_key,
                        'expiry_date': expiry_date
                    }

                    # Add to monitoring instruments
                    if instrument_key:
                        self.monitoring_instruments[instrument_key] = key

                logger.info(f"Loaded {len(rows)} active orders from database")
        except Exception as e:
            logger.error(f"Error loading active orders: {str(e)}")

    async def _run_monitor_loop(self):
        """Main monitoring loop to track prices and manage orders with parallel processing"""
        while self.is_running:
            try:
                # Check if market is open
                if not self._is_market_open():
                    logger.info("Market is closed. Waiting...")
                    await asyncio.sleep(60)  # Check every minute if market is open
                    continue

                start_time = time.time()

                # Create tasks for parallel execution
                with ThreadPoolExecutor(max_workers=3) as executor:
                    # Run potential entries check and active orders management in parallel
                    entries_future = executor.submit(self._fetch_potential_entries_sync)

                    # Only check active orders if we have any
                    if self.active_orders:
                        orders_future = executor.submit(self._update_active_orders_sync)
                    else:
                        orders_future = None

                    # Process potential new entries
                    potential_entries = await asyncio.to_thread(lambda: entries_future.result())

                    for option in potential_entries:
                        await self._process_potential_entry(option)

                    # Wait for orders update to complete if it was running
                    if orders_future:
                        await asyncio.to_thread(lambda: orders_future.result())

                # Update daily capital only every 5 minutes instead of every cycle
                current_minute = datetime.now(self.tz).minute
                if current_minute % 5 == 0 and datetime.now(self.tz).second < 3:
                    await asyncio.to_thread(lambda: self._update_daily_capital_sync())

                # Update monthly performance only on the first day of month at midnight
                today = datetime.now(self.tz).date()
                if today.day == 1 and datetime.now(self.tz).hour == 0 and datetime.now(self.tz).minute < 5:
                    # It's the first day of the month, update previous month's performance
                    prev_month = (today.replace(day=1) - timedelta(days=1))
                    await self._update_monthly_performance(prev_month.month, prev_month.year)

                elapsed = time.time() - start_time

                # Only log if processing takes too long (over 1 second)
                if elapsed > 1.0:
                    logger.info(f"Monitor loop completed in {elapsed:.2f} seconds")

                # Adjust sleep time to maintain roughly 3-second intervals
                sleep_time = max(0.1, 3.0 - elapsed)
                await asyncio.sleep(sleep_time)  # Dynamic sleep based on processing time

            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                await asyncio.sleep(5)  # Sleep longer on error

    def _is_market_open(self) -> bool:
        """Check if the market is currently open"""
        now = datetime.now(self.tz)

        # Market is open Monday to Friday, 9:15 AM to 3:30 PM
        if now.weekday() >= 5:  # Saturday or Sunday
            return False

        market_open = now.replace(hour=9, minute=14, second=0, microsecond=0)
        market_close = now.replace(hour=23, minute=32, second=0, microsecond=0)

        return market_open <= now <= market_close

    async def _get_potential_entries(self) -> List[Dict]:
        """Get options with price change > 5% as potential entries and check every 3-4 seconds"""
        potential_entries = []

        try:
            # Query options with significant price change
            with self.db._get_cursor() as cur:
                # Get options with stored ltp (the 'ltp' column in the database)
                cur.execute("""
                    SELECT o.symbol, o.strike_price, o.option_type, o.ltp, 
                           i.instrument_key, o.lot_size, i.expiry_date, o.oi, o.volume
                    FROM options_orders o
                    JOIN instrument_keys i ON o.symbol = i.symbol 
                                          AND o.strike_price = i.strike_price 
                                          AND o.option_type = i.option_type
                    WHERE o.status = 'Open'
                    AND o.ltp > 4  -- Minimum price filter to avoid very cheap options
                """)

                rows = cur.fetchall()
                logger.info(f"Found {len(rows)} potential options to check for entry conditions")

                # Fetch live prices for all instruments
                instrument_keys = [row[4] for row in rows if row[4]]
                if not instrument_keys:
                    logger.info("No instrument keys found for potential entries")
                    return []

                # Get live prices using the new get_live_ltp method instead of WebSocket API
                live_prices = await self._get_live_ltp_data(instrument_keys)
                logger.info(f"Fetched live prices for {len(live_prices)} instruments")

                # Track how many instruments meet the price change criteria
                criteria_met_count = 0

                for row in rows:
                    symbol = row[0]
                    strike = row[1]
                    option_type = row[2]
                    stored_price = float(row[3] or 0)  # This is ltp from the database
                    instrument_key = row[4]
                    lot_size = int(row[5] or 1)
                    expiry_date = row[6]
                    oi = row[7]
                    volume = row[8]

                    # Skip if no instrument key or no stored price
                    if not instrument_key or stored_price <= 0:
                        continue

                    # Get live price
                    current_price = self._get_price_from_live_data(instrument_key, live_prices)
                    if current_price <= 0:
                        continue

                    # Calculate percent change
                    percent_change = ((current_price - stored_price) / stored_price) * 100

                    # Count instruments meeting criteria before filtering
                    if percent_change > self.entry_threshold:
                        criteria_met_count += 1

                    # Debug log for price change - only log significant changes to avoid log spam
                    if abs(percent_change) > 2:
                        logger.info(f"Option {symbol} {strike} {option_type}: stored={stored_price}, current={current_price}, change={percent_change:.2f}%")

                    # Skip if percent change is below threshold
                    if percent_change <= self.entry_threshold:
                        continue

                    logger.info(f"Found potential entry: {symbol} {strike} {option_type} with {percent_change:.2f}% change")

                    # Skip if already an active order for this option
                    key = f"{symbol}_{strike}_{option_type}"
                    if key in self.active_orders:
                        logger.info(f"Skipping {symbol} {strike} {option_type} - already an active order")
                        continue

                    # Check if we've already processed this option
                    has_previous = await self._check_for_previous_order(symbol, strike, option_type)
                    if has_previous:
                        logger.info(f"Skipping {symbol} {strike} {option_type} - already processed in the last 30 days")
                        continue

                    # Add to potential entries
                    potential_entries.append({
                        'symbol': symbol,
                        'strike_price': strike,
                        'option_type': option_type,
                        'current_price': current_price,
                        'stored_price': stored_price,
                        'percent_change': percent_change,
                        'instrument_key': instrument_key,
                        'lot_size': lot_size,
                        'expiry_date': expiry_date,
                        'oi': oi,
                        'volume': volume
                    })

                logger.info(f"Found {len(potential_entries)} options that meet entry criteria from {criteria_met_count} that crossed threshold")

        except Exception as e:
            logger.error(f"Error getting potential entries: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return potential_entries

    async def _check_for_previous_order(self, symbol, strike, option_type) -> bool:
        """Check if we already processed this option in this expiry cycle"""
        try:
            # First check for any open order with these parameters
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM strategy_orders
                    WHERE symbol = %s AND strike_price = %s AND option_type = %s
                    AND strategy_name = %s
                    AND status = 'OPEN'
                """, (symbol, strike, option_type, self.strategy_name))

                open_count = cur.fetchone()[0]
                if open_count > 0:
                    logger.info(f"Found existing OPEN order for {symbol} {strike} {option_type}")
                    return True

                # Check if there was a recent closed order (stop loss hit)
                cur.execute("""
                    SELECT exit_time, status 
                    FROM strategy_orders
                    WHERE symbol = %s AND strike_price = %s AND option_type = %s
                    AND strategy_name = %s
                    AND status != 'OPEN'
                    ORDER BY exit_time DESC
                    LIMIT 1
                """, (symbol, strike, option_type, self.strategy_name))

                row = cur.fetchone()
                if row:
                    exit_time = row[0]
                    status = row[1]

                    # For orders closed due to stop loss or expiry, require a cooldown period
                    # before creating a new order for the same option
                    if status in ('LOSS', 'EXPIRED'):
                        cooldown_period = timedelta(days=3)  # 3-day cooldown for loss/expired
                        can_reenter_after = exit_time + cooldown_period

                        # Use timezone-aware datetime.now() for comparison
                        current_time = datetime.now(self.tz)
                        if current_time < can_reenter_after:
                            logger.info(f"Skipping {symbol} {strike} {option_type} - in cooldown period after {status} until {can_reenter_after}")
                            return True

                    # If the order was closed with profit, don't reenter for a longer period
                    elif status == 'PROFIT':
                        cooldown_period = timedelta(days=7)  # 7-day cooldown for profit orders
                        can_reenter_after = exit_time + cooldown_period

                        # Use timezone-aware datetime.now() for comparison
                        current_time = datetime.now(self.tz)
                        if current_time < can_reenter_after:
                            logger.info(f"Skipping {symbol} {strike} {option_type} - in cooldown period after PROFIT until {can_reenter_after}")
                            return True

                # No conflicts found, can proceed with order
                return False

        except Exception as e:
            logger.error(f"Error checking for previous order: {str(e)}")
            return False

    async def _process_potential_entry(self, option):
        """Process a potential entry option"""
        try:
            symbol = option['symbol']
            strike = option['strike_price']
            option_type = option['option_type']
            current_price = option['current_price']
            stored_price = option['stored_price']  # Get the stored LTP price
            instrument_key = option['instrument_key']
            lot_size = option['lot_size']
            expiry_date = option['expiry_date']

            # Calculate target and stop loss
            # Target price is now based on stored price instead of current price
            target_price = stored_price * (1 + self.profit_target / 100)
            stop_loss_price = stored_price * (1 + self.stop_loss / 100)

            # Determine quantity based on lot size
            quantity = lot_size * self.lot_size_multiplier

            # Create new order in the database
            order_id = await self._create_order(
                symbol, strike, option_type, current_price,
                target_price, stop_loss_price, quantity, lot_size,
                instrument_key, expiry_date
            )

            if order_id:
                # Add to active orders
                key = f"{symbol}_{strike}_{option_type}"
                self.active_orders[key] = {
                    'id': order_id,
                    'symbol': symbol,
                    'strike_price': strike,
                    'option_type': option_type,
                    'entry_price': current_price,
                    'target_price': target_price,
                    'stop_loss': stop_loss_price,
                    'instrument_key': instrument_key,
                    'expiry_date': expiry_date,
                    'quantity': quantity,
                    'lot_size': lot_size
                }

                # Add to monitoring instruments
                if instrument_key:
                    self.monitoring_instruments[instrument_key] = key

                logger.info(f"Created new order: {symbol} {strike} {option_type} at {current_price}")

        except Exception as e:
            logger.error(f"Error processing potential entry: {str(e)}")

    async def _create_order(self, symbol, strike, option_type, entry_price,
                           target_price, stop_loss, quantity, lot_size,
                           instrument_key, expiry_date):
        """Create a new order in the database"""
        try:
            with self.db._get_cursor() as cur:
                cur.execute("""
                    INSERT INTO strategy_orders (
                        symbol, strike_price, option_type, entry_price, current_price,
                        target_price, stop_loss, lot_size, quantity, entry_time,
                        status, strategy_name, expiry_date, instrument_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(),
                        'OPEN', %s, %s, %s
                    ) RETURNING id
                """, (
                    symbol, strike, option_type, entry_price, entry_price,
                    target_price, stop_loss, lot_size, quantity,
                    self.strategy_name, expiry_date, instrument_key
                ))

                order_id = cur.fetchone()[0]
                return order_id

        except Exception as e:
            logger.error(f"Error creating order: {str(e)}")
            return None

    async def _update_active_orders(self):
        """Update and manage all active orders"""
        if not self.active_orders:
            return

        # Get current prices for all active orders
        instrument_keys = [order['instrument_key'] for order in self.active_orders.values() if order['instrument_key']]
        if not instrument_keys:
            return

        # Get live prices using get_live_ltp_data instead of WebSocket API
        live_prices = await self._get_live_ltp_data(instrument_keys)

        # Process each active order
        orders_to_remove = []

        for key, order in self.active_orders.items():
            try:
                instrument_key = order['instrument_key']

                # Skip if no instrument key
                if not instrument_key:
                    continue

                # Get current price from live data
                current_price = self._get_price_from_live_data(instrument_key, live_prices)
                if current_price <= 0:
                    continue

                entry_price = order['entry_price']
                target_price = order['target_price']
                stop_loss_price = order['stop_loss']

                # Calculate percent change
                percent_change = ((current_price - entry_price) / entry_price) * 100

                # Update current price in database
                await self._update_order_price(order['id'], current_price)

                # Check if target or stop loss hit
                if current_price >= target_price:
                    # Take profit - exit at target
                    await self._close_order(order['id'], current_price, 'PROFIT', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Target hit: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                elif percent_change >= 90.0:
                    # Take profit when percentage change exceeds 90%, even if target price hasn't been hit
                    await self._close_order(order['id'], current_price, 'PROFIT', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"High profit (>90%) hit: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                elif current_price <= stop_loss_price:
                    # Stop loss hit - exit to limit loss
                    await self._close_order(order['id'], current_price, 'LOSS', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Stop loss hit: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                # Check for expiry
                expiry_date = order['expiry_date']
                if expiry_date and expiry_date <= date.today():
                    await self._close_order(order['id'], current_price, 'EXPIRED', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Position expired: {order['symbol']} {order['strike_price']} {order['option_type']}")

            except Exception as e:
                logger.error(f"Error updating order {key}: {str(e)}")

        # Remove closed orders from tracking
        for key in orders_to_remove:
            order = self.active_orders.pop(key, None)
            if order and order['instrument_key'] in self.monitoring_instruments:
                del self.monitoring_instruments[order['instrument_key']]

    def _update_active_orders_sync(self):
        """Synchronous version of _update_active_orders for use with ThreadPoolExecutor"""
        if not self.active_orders:
            return

        # Get current prices for all active orders
        instrument_keys = [order['instrument_key'] for order in self.active_orders.values() if order['instrument_key']]
        if not instrument_keys:
            return

        # Get live prices using _get_live_ltp_data_sync instead of WebSocket API
        try:
            # Get LTP data for all keys
            live_prices = self._get_live_ltp_data_sync(instrument_keys)

            # Process each active order
            orders_to_remove = []

            for key, order in self.active_orders.items():
                try:
                    instrument_key = order['instrument_key']

                    # Skip if no instrument key
                    if not instrument_key:
                        continue

                    # Get current price from live data
                    current_price = self._get_price_from_live_data(instrument_key, live_prices)
                    if current_price <= 0:
                        continue

                    entry_price = order['entry_price']
                    target_price = order['target_price']
                    stop_loss_price = order['stop_loss']

                    # Calculate percent change
                    percent_change = ((current_price - entry_price) / entry_price) * 100

                    # Update current price in database
                    with self.db._get_cursor() as cur:
                        cur.execute("""
                            UPDATE strategy_orders
                            SET current_price = %s
                            WHERE id = %s
                        """, (current_price, order['id']))

                    # Check if target or stop loss hit - these will be processed in the main loop
                    # after this function returns
                    if current_price >= target_price:
                        logger.info(f"Target hit detected: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                        # Immediately close the order with profit status
                        with self.db._get_cursor() as cur:
                            cur.execute("""
                                UPDATE strategy_orders
                                SET exit_price = %s, exit_time = NOW(), status = 'PROFIT', 
                                    pnl = (exit_price - entry_price) * quantity,
                                    pnl_percentage = ((exit_price - entry_price) / entry_price) * 100
                                WHERE id = %s
                            """, (current_price, order['id']))

                        orders_to_remove.append((key, current_price, 'PROFIT', percent_change))

                    elif percent_change >= 90.0:
                        logger.info(f"High profit (>90%) hit detected: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                        # Immediately close the order with profit status
                        with self.db._get_cursor() as cur:
                            cur.execute("""
                                UPDATE strategy_orders
                                SET exit_price = %s, exit_time = NOW(), status = 'PROFIT', 
                                    pnl = (exit_price - entry_price) * quantity,
                                    pnl_percentage = ((exit_price - entry_price) / entry_price) * 100
                                WHERE id = %s
                            """, (current_price, order['id']))

                        orders_to_remove.append((key, current_price, 'PROFIT', percent_change))

                    elif current_price <= stop_loss_price:
                        logger.info(f"Stop loss hit detected: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                        # Immediately close the order with loss status
                        with self.db._get_cursor() as cur:
                            cur.execute("""
                                UPDATE strategy_orders
                                SET exit_price = %s, exit_time = NOW(), status = 'LOSS', 
                                    pnl = (exit_price - entry_price) * quantity,
                                    pnl_percentage = ((exit_price - entry_price) / entry_price) * 100
                                WHERE id = %s
                            """, (current_price, order['id']))

                        orders_to_remove.append((key, current_price, 'LOSS', percent_change))

                    # Check for expiry
                    expiry_date = order['expiry_date']
                    if expiry_date and expiry_date <= date.today():
                        logger.info(f"Position expiry detected: {order['symbol']} {order['strike_price']} {order['option_type']}")

                        # Immediately close the order with expired status
                        with self.db._get_cursor() as cur:
                            cur.execute("""
                                UPDATE strategy_orders
                                SET exit_price = %s, exit_time = NOW(), status = 'EXPIRED', 
                                    pnl = (exit_price - entry_price) * quantity,
                                    pnl_percentage = ((exit_price - entry_price) / entry_price) * 100
                                WHERE id = %s
                            """, (current_price, order['id']))

                        orders_to_remove.append((key, current_price, 'EXPIRED', percent_change))

                except Exception as e:
                    logger.error(f"Error updating order {key}: {str(e)}")

            # Return orders that need to be removed for processing in the main thread
            return orders_to_remove

        except Exception as e:
            logger.error(f"Error in _update_active_orders_sync: {str(e)}")
            return []

    async def _get_live_prices_websocket(self, instrument_keys):
        """Get current prices for a list of instrument keys using the WebSocket API"""
        if not instrument_keys:
            return {}

        # First check cache for recent prices
        current_time = time.time()
        cached_prices = {}
        missing_keys = []

        for key in instrument_keys:
            if key in self.price_cache and current_time - self.cache_time.get(key, 0) < self.cache_ttl:
                cached_prices[key] = self.price_cache[key]
            else:
                missing_keys.append(key)

        # If all prices are in cache, return immediately
        if not missing_keys:
            return cached_prices

        # Use WebSocket API to fetch prices that are not in cache
        try:
            # Format the keys as needed for the API
            keys_param = ','.join(missing_keys)
            logger.info(f"Fetching LTP for {len(missing_keys)} instruments via WebSocket API")

            # Make a request to the WebSocket API endpoint with increased timeout and retry logic
            max_retries = 2
            retry_count = 0
            timeout = 3  # Reduced from 5 seconds to 3 seconds

            while retry_count < max_retries:
                try:
                    async with requests.Session() as session:
                        response = await asyncio.wait_for(
                            asyncio.to_thread(
                                session.get,
                                f"{self.worker_api_url}/api/market_data?keys={keys_param}",
                                timeout=timeout
                            ),
                            timeout=timeout + 1  # Add 1 second buffer for asyncio timeout
                        )

                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"WebSocket API returned data for {len(result)} instruments")

                        # Format the response to match expected structure
                        prices = {}
                        for key, data in result.items():
                            if data and 'ltp' in data:
                                # Safely handle None values in data
                                ltp = data.get('ltp')
                                if ltp is not None:
                                    try:
                                        float_ltp = float(ltp)
                                        prices[key] = {"ltp": float_ltp}
                                        logger.info(f"LTP for {key}: {float_ltp}")

                                        # Update cache
                                        self.price_cache[key] = prices[key]
                                        self.cache_time[key] = current_time
                                    except (TypeError, ValueError) as e:
                                        logger.error(f"Error converting LTP to float for {key}: {e}, value: {ltp}")
                                else:
                                    logger.warning(f"LTP is None for {key}")

                        # Combine with cached prices and return
                        combined_prices = {**cached_prices, **prices}
                        logger.info(f"Returning {len(combined_prices)} prices (cached: {len(cached_prices)}, fresh: {len(prices)})")
                        return combined_prices
                    elif response.status_code >= 500:
                        # Server error, retry
                        retry_count += 1
                        logger.warning(f"Server error {response.status_code} fetching live prices, retrying ({retry_count}/{max_retries})")
                        await asyncio.sleep(0.5)  # Short delay before retry
                    else:
                        logger.error(f"Error fetching live prices: HTTP {response.status_code}")
                        return cached_prices

                except (asyncio.TimeoutError, requests.exceptions.Timeout, requests.exceptions.ReadTimeout) as e:
                    retry_count += 1
                    logger.warning(f"Timeout fetching live prices, retrying ({retry_count}/{max_retries}): {str(e)}")
                    # For timeout errors, try again with a short delay
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error fetching live prices with WebSocket API: {str(e)}")
                    return cached_prices

            # If we've exhausted retries, return what we have in cache
            logger.error(f"Failed to fetch live prices after {max_retries} retries")
            return cached_prices

        except Exception as e:
            logger.error(f"Unexpected error fetching live prices with WebSocket API: {str(e)}")
            return cached_prices

    def _get_live_prices_websocket_sync(self, instrument_keys):
        """Synchronous version of _get_live_prices_websocket"""
        if not instrument_keys:
            return {}

        # First check cache for recent prices
        current_time = time.time()
        cached_prices = {}
        missing_keys = []

        for key in instrument_keys:
            if key in self.price_cache and current_time - self.cache_time.get(key, 0) < self.cache_ttl:
                cached_prices[key] = self.price_cache[key]
            else:
                missing_keys.append(key)

        # If all prices are in cache, return immediately
        if not missing_keys:
            return cached_prices

        # Use WebSocket API to fetch prices that are not in cache
        try:
            # Format the keys as needed for the API
            keys_param = ','.join(missing_keys)
            logger.info(f"Sync fetching LTP for {len(missing_keys)} instruments via WebSocket API")

            # Implement retry logic for synchronous calls too
            max_retries = 2
            retry_count = 0
            timeout = 3  # Reduced from 5 seconds to 3 seconds

            while retry_count < max_retries:
                try:
                    # Make a synchronous request to the WebSocket API endpoint
                    response = requests.get(
                        f"{self.worker_api_url}/api/market_data?keys={keys_param}",
                        timeout=timeout
                    )

                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"WebSocket API returned data for {len(result)} instruments (sync)")

                        # Format the response to match expected structure
                        prices = {}
                        for key, data in result.items():
                            if data and 'ltp' in data:
                                # Safely handle None values in data
                                ltp = data.get('ltp')
                                if ltp is not None:
                                    try:
                                        float_ltp = float(ltp)
                                        prices[key] = {"ltp": float_ltp}
                                        logger.info(f"Sync LTP for {key}: {float_ltp}")

                                        # Update cache
                                        self.price_cache[key] = prices[key]
                                        self.cache_time[key] = current_time
                                    except (TypeError, ValueError) as e:
                                        logger.error(f"Error converting LTP to float for {key}: {e}, value: {ltp}")
                                else:
                                    logger.warning(f"Sync LTP is None for {key}")

                        # Combine with cached prices and return
                        combined_prices = {**cached_prices, **prices}
                        logger.info(f"Returning {len(combined_prices)} prices (cached: {len(cached_prices)}, fresh: {len(prices)}) (sync)")
                        return combined_prices
                    elif response.status_code >= 500:
                        # Server error, retry
                        retry_count += 1
                        logger.warning(f"Server error {response.status_code} fetching live prices, retrying ({retry_count}/{max_retries}) (sync)")
                        time.sleep(0.5)  # Short delay before retry
                    else:
                        logger.error(f"Error fetching live prices: HTTP {response.status_code} (sync)")
                        return cached_prices

                except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout) as e:
                    retry_count += 1
                    logger.warning(f"Timeout fetching live prices, retrying ({retry_count}/{max_retries}): {str(e)} (sync)")
                    # For timeout errors, try again with a short delay
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error fetching live prices with WebSocket API: {str(e)} (sync)")
                    return cached_prices

            # If we've exhausted retries, return what we have in cache
            logger.error(f"Failed to fetch live prices after {max_retries} retries (sync)")
            return cached_prices

        except Exception as e:
            logger.error(f"Unexpected error fetching live prices with WebSocket API: {str(e)} (sync)")
            return cached_prices

    def _get_price_from_live_data(self, instrument_key, live_data):
        """Extract price from live data"""
        if not instrument_key or not live_data or instrument_key not in live_data:
            logger.warning(f"No live data found for {instrument_key}")
            return 0.0

        data = live_data[instrument_key]

        # Safely handle None values in 'ltp'
        ltp = data.get('ltp')
        if ltp is None:
            logger.warning(f"LTP is None for {instrument_key}")
            return 0.0

        try:
            float_ltp = float(ltp)
            logger.info(f"Extracted LTP for {instrument_key}: {float_ltp}")
            return float_ltp
        except (TypeError, ValueError):
            logger.error(f"Error converting LTP to float for {instrument_key}, value: {ltp}")
            return 0.0

    async def _get_live_ltp_data(self, instrument_keys):
        """Get current LTP (Last Traded Price) for a list of instrument keys using get_options_orders_analysis API"""
        if not instrument_keys:
            return {}

        # First check cache for recent prices
        current_time = time.time()
        cached_prices = {}
        missing_keys = []

        for key in instrument_keys:
            if key in self.price_cache and current_time - self.cache_time.get(key, 0) < self.cache_ttl:
                cached_prices[key] = self.price_cache[key]
            else:
                missing_keys.append(key)

        # If all prices are in cache, return immediately
        if not missing_keys:
            return cached_prices

        # Use the get_options_orders_analysis API to fetch live prices
        try:
            logger.info(f"Fetching LTP for {len(missing_keys)} instruments via get_options_orders_analysis API")

            # Make a request to the options-orders-analysis endpoint
            url = f"{self.worker_api_url}/api/options-orders-analysis"

            # This endpoint doesn't take any parameters - it automatically fetches data for all instruments
            async with requests.Session() as session:
                response = await asyncio.to_thread(session.get, url, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get("success") and result.get("data"):
                    data = result.get("data", [])
                    logger.info(f"get_options_orders_analysis returned data for {len(data)} instruments")

                    # Create a mapping of instrument_key to current_ltp
                    live_prices = {}
                    for item in data:
                        if item.get("instrument_key") and item.get("instrument_key") in missing_keys:
                            instrument_key = item.get("instrument_key")
                            current_ltp = item.get("current_ltp", 0)

                            # Add to live_prices
                            live_prices[instrument_key] = {"ltp": current_ltp}

                            # Update cache
                            self.price_cache[instrument_key] = {"ltp": current_ltp}
                            self.cache_time[instrument_key] = current_time

                # Combine with cached prices and return
                combined_prices = {**cached_prices, **live_prices}
                logger.info(f"Returning {len(combined_prices)} prices (cached: {len(cached_prices)}, fresh: {len(live_prices)})")
                return combined_prices
            else:
                logger.error(f"HTTP error from get_options_orders_analysis: {response.status_code}")
                return cached_prices

        except Exception as e:
            logger.error(f"Error fetching live prices with get_options_orders_analysis: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return cached_prices

    def _get_live_ltp_data_sync(self, instrument_keys):
        """Synchronous version of _get_live_ltp_data for use with ThreadPoolExecutor"""
        if not instrument_keys:
            return {}

        # First check cache for recent prices
        current_time = time.time()
        cached_prices = {}
        missing_keys = []

        for key in instrument_keys:
            if key in self.price_cache and current_time - self.cache_time.get(key, 0) < self.cache_ttl:
                cached_prices[key] = self.price_cache[key]
            else:
                missing_keys.append(key)

        # If all prices are in cache, return immediately
        if not missing_keys:
            return cached_prices

        # Use the get_options_orders_analysis API to fetch live prices
        try:
            logger.info(f"Sync fetching LTP for {len(missing_keys)} instruments via get_options_orders_analysis API")

            # Make a request to the options-orders-analysis endpoint
            url = f"{self.worker_api_url}/api/options-orders-analysis"

            # This endpoint doesn't take any parameters - it automatically fetches data for all instruments
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get("success") and result.get("data"):
                    data = result.get("data", [])
                    logger.info(f"get_options_orders_analysis returned data for {len(data)} instruments (sync)")

                    # Create a mapping of instrument_key to current_ltp
                    live_prices = {}
                    for item in data:
                        if item.get("instrument_key") and item.get("instrument_key") in missing_keys:
                            instrument_key = item.get("instrument_key")
                            current_ltp = item.get("current_ltp", 0)

                            # Add to live_prices
                            live_prices[instrument_key] = {"ltp": current_ltp}

                            # Update cache
                            self.price_cache[instrument_key] = {"ltp": current_ltp}
                            self.cache_time[instrument_key] = current_time

                # Combine with cached prices and return
                combined_prices = {**cached_prices, **live_prices}
                logger.info(f"Returning {len(combined_prices)} prices (cached: {len(cached_prices)}, fresh: {len(live_prices)}) (sync)")
                return combined_prices
            else:
                logger.error(f"HTTP error from get_options_orders_analysis (sync): {response.status_code}")
                return cached_prices

        except Exception as e:
            logger.error(f"Error fetching live prices with get_options_orders_analysis (sync): {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return cached_prices

    def _fetch_potential_entries_sync(self):
        """Synchronous version of _get_potential_entries for use with ThreadPoolExecutor"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            potential_entries = loop.run_until_complete(self._get_potential_entries())
            return potential_entries
        finally:
            loop.close()

# Function to run the strategy tracker as a background process
async def run_strategy_tracker():
    """Run multiple strategy trackers as parallel background processes"""
    db_service = DatabaseService()

    # Create both strategy trackers with different configurations
    strategy1 = StrategyTracker(db_service, {
        "name": "percent_change_tracker",  # Original strategy: 5% entry, 95% profit, -15% stop loss
        "entry_threshold": 5.0,
        "profit_target": 92.0,
        "stop_loss": -15.0,
        "lot_size_multiplier": 1
    })

    strategy2 = StrategyTracker(db_service, {
        "name": "aggressive_tracker",  # New strategy: 10% entry, 95% profit, -30% stop loss
        "entry_threshold": 10.0,
        "profit_target": 90.0,
        "stop_loss": -30.0,
        "lot_size_multiplier": 1
    })

    # Run both strategies in parallel
    tasks = [
        asyncio.create_task(run_single_strategy(strategy1)),
        asyncio.create_task(run_single_strategy(strategy2))
    ]

    try:
        logger.info("Starting multiple strategy tracker background processes")
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Stopping strategy tracker background processes due to KeyboardInterrupt")
        for strategy in [strategy1, strategy2]:
            if strategy.is_running:
                await strategy.stop()
    except Exception as e:
        logger.error(f"Error in strategy tracker background processes: {str(e)}")
        for strategy in [strategy1, strategy2]:
            if strategy.is_running:
                await strategy.stop()
    finally:
        # Make sure to stop all trackers even if there's an unhandled exception
        for strategy in [strategy1, strategy2]:
            if strategy.is_running:
                await strategy.stop()
        logger.info("All strategy tracker processes have been stopped")

async def run_single_strategy(strategy):
    """Run a single strategy tracker"""
    try:
        await strategy.start()
    except Exception as e:
        logger.error(f"Error in strategy {strategy.strategy_name}: {str(e)}")
        if strategy.is_running:
            await strategy.stop()


if __name__ == "__main__":
    # Set up proper signal handling for Ctrl+C
    import signal
    import sys

    loop = asyncio.get_event_loop()

    # Define signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Received termination signal, shutting down...")
        if not loop.is_closed():
            loop.stop()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    try:
        # Run the tracker
        loop.run_until_complete(run_strategy_tracker())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, shutting down...")
    finally:
        # Clean up resources
        if not loop.is_closed():
            loop.close()
        logger.info("Event loop closed, exiting program")

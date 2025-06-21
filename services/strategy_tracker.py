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

        # Worker API base URL for fetching live market data (kept for backward compatibility)
        self.worker_api_url = "http://0.0.0.0:8000"  # Updated from 0.0.0.0 to 127.0.0.1

        # New Upstox feed direct connection parameters
        self.access_token = None  # Will be fetched from database

        # Websocket connection management
        self.websocket = None
        self.is_websocket_connected = False
        self.max_retries = 3
        self.retry_delay = 2

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

        # Close websocket if connected
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()

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
        market_close = now.replace(hour=15, minute=32, second=0, microsecond=0)

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

                # Get live prices using the LTP API - this is now called every 3-4 seconds from the monitor loop
                live_prices = await self._get_live_prices(instrument_keys)
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
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM strategy_orders
                    WHERE symbol = %s AND strike_price = %s AND option_type = %s
                    AND strategy_name = %s
                    AND entry_time > (CURRENT_DATE - INTERVAL '30 days')
                """, (symbol, strike, option_type, self.strategy_name))

                count = cur.fetchone()[0]
                return count > 0

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
            instrument_key = option['instrument_key']
            lot_size = option['lot_size']
            expiry_date = option['expiry_date']

            # Calculate target and stop loss
            target_price = current_price * (1 + self.profit_target / 100)
            stop_loss_price = current_price * (1 + self.stop_loss / 100)

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

        # Get live prices from worker API
        live_prices = await self._get_live_prices(instrument_keys)

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

    async def _get_live_prices(self, instrument_keys):
        """Get current prices for a list of instrument keys using the LTP API endpoint"""
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

        # Use LTP API to fetch prices that are not in cache
        prices = {}

        try:
            # Get LTP data for all missing keys in one call
            ltp_data = self.option_chain_service.fetch_ltp(missing_keys)

            # Format the data to match expected structure
            for instrument_key, ltp in ltp_data.items():
                prices[instrument_key] = {"ltp": float(ltp)}

            print(f"Retrieved LTP for {len(prices)} instruments")

        except Exception as e:
            logger.error(f"Error fetching live prices: {str(e)}")

        # Update cache with new prices
        for key, value in prices.items():
            self.price_cache[key] = value
            self.cache_time[key] = current_time

        # Combine cached and new prices
        return {**cached_prices, **prices}

    async def _connect_to_feed(self):
        """Connect to Upstox feed"""
        try:
            # Get access token from database if not already loaded
            if not self.access_token:
                self.access_token = self.db.get_access_token(account_id=4)  # Use account ID 4 for strategy tracker
                if not self.access_token:
                    logger.error("No access token found in database for account ID 4")
                    return False

            # Get authorization
            auth_response = await self._get_market_data_feed_authorize()
            if not auth_response or not auth_response.get('data', {}).get('authorized_redirect_uri'):
                logger.error("Failed to get authorization for market data feed")
                return False

            uri = auth_response['data']['authorized_redirect_uri']

            # Close existing connection if any
            if self.websocket and not self.websocket.closed:
                await self.websocket.close()

            # Connect to websocket
            self.websocket = await websockets.connect(uri)
            self.is_websocket_connected = True
            logger.info("Successfully connected to Upstox feed")

            return True

        except Exception as e:
            logger.error(f"Error connecting to Upstox feed: {str(e)}")
            self.is_websocket_connected = False
            return False

    async def _get_market_data_feed_authorize(self):
        """Get authorization for market data feed"""
        if not self.access_token:
            logger.error("No access token available")
            return None

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

        try:
            response = await asyncio.to_thread(
                requests.get,
                url=self.auth_url,
                headers=headers,
                timeout=5
            )
            return response.json()
        except Exception as e:
            logger.error(f"Error getting authorization: {str(e)}")
            return None

    async def _subscribe_to_instruments(self, instrument_keys):
        """Subscribe to instruments for price updates"""
        if not self.websocket or self.websocket.closed:
            logger.error("Cannot subscribe - websocket not connected")
            return False

        try:
            # Send subscription message
            subscription_data = {
                "guid": str(time.time()),
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": instrument_keys
                }
            }

            await self.websocket.send(json.dumps(subscription_data).encode('utf-8'))
            logger.info(f"Sent subscription request for {len(instrument_keys)} instruments")
            return True

        except Exception as e:
            logger.error(f"Error subscribing to instruments: {str(e)}")
            self.is_websocket_connected = False
            return False

    def _decode_protobuf(self, buffer):
        """Decode protobuf message."""
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response

    def _get_price_from_live_data(self, instrument_key, live_data):
        """Extract price from live data"""
        if not instrument_key or not live_data or instrument_key not in live_data:
            return 0.0

        data = live_data[instrument_key]
        return float(data.get('ltp', 0) or 0)

    async def _close_order(self, order_id, exit_price, status, pnl_percentage):
        """Close an order with the given status and exit price"""
        try:
            with self.db._get_cursor() as cur:
                # Get order details first
                cur.execute("""
                    SELECT entry_price, quantity
                    FROM strategy_orders
                    WHERE id = %s
                """, (order_id,))

                row = cur.fetchone()
                if not row:
                    logger.error(f"Order {order_id} not found for closing")
                    return

                entry_price = float(row[0])
                quantity = int(row[1])

                # Calculate PnL
                pnl = (exit_price - entry_price) * quantity

                # Update the order
                cur.execute("""
                    UPDATE strategy_orders
                    SET exit_price = %s,
                        exit_time = NOW(),
                        status = %s,
                        pnl = %s,
                        pnl_percentage = %s
                    WHERE id = %s
                """, (exit_price, status, pnl, pnl_percentage, order_id))

        except Exception as e:
            logger.error(f"Error closing order: {str(e)}")

    async def _update_order_price(self, order_id, current_price):
        """Update the current price of an order"""
        try:
            with self.db._get_cursor() as cur:
                cur.execute("""
                    UPDATE strategy_orders
                    SET current_price = %s
                    WHERE id = %s
                """, (current_price, order_id))
        except Exception as e:
            logger.error(f"Error updating order price: {str(e)}")

    async def _update_daily_capital(self):
        """Update daily capital statistics"""
        try:
            today = datetime.now(self.tz).date()
            total_capital = 0.0
            unrealized_pnl = 0.0
            open_positions = len(self.active_orders)

            # Calculate current capital deployed and unrealized PnL
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT SUM(entry_price * quantity), 
                           SUM((current_price - entry_price) * quantity)
                    FROM strategy_orders
                    WHERE status = 'OPEN' AND strategy_name = %s
                """, (self.strategy_name,))

                row = cur.fetchone()
                if row and row[0]:
                    total_capital = float(row[0])
                    unrealized_pnl = float(row[1] or 0)

            # Update max capital if needed
            self.today_capital = total_capital
            if total_capital > self.max_capital:
                self.max_capital = total_capital

            # Update or insert daily capital record
            with self.db._get_cursor() as cur:
                cur.execute("""
                    INSERT INTO strategy_daily_capital
                    (strategy_name, date, open_positions, capital_deployed, unrealized_pnl, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (strategy_name, date)
                    DO UPDATE SET
                        open_positions = EXCLUDED.open_positions,
                        capital_deployed = EXCLUDED.capital_deployed,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        updated_at = NOW()
                """, (self.strategy_name, today, open_positions, total_capital, unrealized_pnl))

        except Exception as e:
            logger.error(f"Error updating daily capital: {str(e)}")

    async def _update_monthly_performance(self, month, year):
        """Update monthly performance metrics"""
        try:
            # Get all closed orders for the month
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_orders,
                        SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END) as profit_orders,
                        SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) as loss_orders,
                        SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) as expired_orders,
                        SUM(pnl) as total_pnl,
                        AVG(CASE WHEN status = 'PROFIT' THEN pnl_percentage ELSE NULL END) as avg_profit_pct,
                        AVG(CASE WHEN status = 'LOSS' THEN pnl_percentage ELSE NULL END) as avg_loss_pct
                    FROM strategy_orders
                    WHERE strategy_name = %s
                    AND EXTRACT(MONTH FROM exit_time) = %s
                    AND EXTRACT(YEAR FROM exit_time) = %s
                    AND status != 'OPEN'
                """, (self.strategy_name, month, year))

                stats = cur.fetchone()

                if not stats or stats[0] == 0:
                    logger.info(f"No closed orders for {month}/{year}, skipping performance update")
                    return

                total_orders = stats[0]
                profit_orders = stats[1] or 0
                loss_orders = stats[2] or 0
                expired_orders = stats[3] or 0
                total_pnl = float(stats[4] or 0)
                avg_profit_pct = float(stats[5] or 0)
                avg_loss_pct = float(stats[6] or 0)

                # Calculate win rate
                win_rate = (profit_orders / total_orders * 100) if total_orders > 0 else 0

                # Get maximum drawdown for the month
                cur.execute("""
                    SELECT MAX(capital_deployed)
                    FROM strategy_daily_capital
                    WHERE strategy_name = %s
                    AND EXTRACT(MONTH FROM date) = %s
                    AND EXTRACT(YEAR FROM date) = %s
                """, (self.strategy_name, month, year))

                max_capital = cur.fetchone()[0] or 0

                # Insert or update monthly performance
                cur.execute("""
                    INSERT INTO strategy_monthly_performance
                    (month, year, strategy_name, total_orders, profit_orders, loss_orders,
                     expired_orders, total_pnl, win_rate, avg_profit_percentage, avg_loss_percentage,
                     max_capital_required, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (month, year, strategy_name)
                    DO UPDATE SET
                        total_orders = EXCLUDED.total_orders,
                        profit_orders = EXCLUDED.profit_orders,
                        loss_orders = EXCLUDED.loss_orders,
                        expired_orders = EXCLUDED.expired_orders,
                        total_pnl = EXCLUDED.total_pnl,
                        win_rate = EXCLUDED.win_rate,
                        avg_profit_percentage = EXCLUDED.avg_profit_percentage,
                        avg_loss_percentage = EXCLUDED.avg_loss_percentage,
                        max_capital_required = EXCLUDED.max_capital_required,
                        updated_at = NOW()
                """, (
                    month, year, self.strategy_name, total_orders, profit_orders, loss_orders,
                    expired_orders, total_pnl, win_rate, avg_profit_pct, avg_loss_pct,
                    max_capital,
                ))

                logger.info(f"Updated performance for {month}/{year}: {profit_orders}/{total_orders} profitable, PnL: {total_pnl}")

        except Exception as e:
            logger.error(f"Error updating monthly performance: {str(e)}")

    async def generate_monthly_report(self, month=None, year=None):
        """Generate a monthly performance report"""
        if not month or not year:
            # Use previous month if not specified
            today = datetime.now(self.tz)
            first_day_of_month = today.replace(day=1)
            last_month = first_day_of_month - timedelta(days=1)
            month = last_month.month
            year = last_month.year

        try:
            # Get monthly performance
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT * FROM strategy_monthly_performance
                    WHERE month = %s AND year = %s AND strategy_name = %s
                """, (month, year, self.strategy_name))

                performance = cur.fetchone()

                if not performance:
                    logger.warning(f"No performance data for {month}/{year}")
                    return None

                # Get all orders for the month
                cur.execute("""
                    SELECT 
                        symbol, strike_price, option_type, 
                        entry_price, exit_price, entry_time, exit_time,
                        status, pnl, pnl_percentage
                    FROM strategy_orders
                    WHERE strategy_name = %s
                    AND EXTRACT(MONTH FROM entry_time) = %s
                    AND EXTRACT(YEAR FROM entry_time) = %s
                    ORDER BY entry_time
                """, (self.strategy_name, month, year))

                orders = cur.fetchall()

                # Format the report
                month_name = calendar.month_name[month]
                report = f"# Performance Report: {month_name} {year}\n\n"

                # Summary statistics
                report += "## Summary\n"
                report += f"- Total Orders: {performance[3]}\n"
                report += f"- Profit Orders: {performance[4]}\n"
                report += f"- Loss Orders: {performance[5]}\n"
                report += f"- Expired Orders: {performance[6]}\n"
                report += f"- Total P&L: ₹{float(performance[7]):.2f}\n"
                report += f"- Win Rate: {float(performance[8]):.2f}%\n"
                report += f"- Average Profit: {float(performance[9]):.2f}%\n"
                report += f"- Average Loss: {float(performance[10]):.2f}%\n"
                report += f"- Maximum Capital Required: ₹{float(performance[12]):.2f}\n\n"

                # Orders table
                report += "## Orders\n\n"
                report += "| Symbol | Strike | Type | Entry Price | Exit Price | Entry Time | Exit Time | Status | P&L | P&L % |\n"
                report += "|--------|--------|------|------------|------------|------------|-----------|--------|-----|-------|\n"

                for order in orders:
                    symbol = order[0]
                    strike = order[1]
                    option_type = order[2]
                    entry_price = float(order[3])
                    exit_price = float(order[4] or 0)
                    entry_time = order[5].strftime("%Y-%m-%d %H:%M")
                    exit_time = order[6].strftime("%Y-%m-%d %H:%M") if order[6] else "Open"
                    status = order[7]
                    pnl = float(order[8] or 0)
                    pnl_pct = float(order[9] or 0)

                    report += f"| {symbol} | {strike} | {option_type} | {entry_price:.2f} | {exit_price:.2f} | {entry_time} | {exit_time} | {status} | {pnl:.2f} | {pnl_pct:.2f}% |\n"

                return report

        except Exception as e:
            logger.error(f"Error generating monthly report: {str(e)}")
            return None

    def _fetch_potential_entries_sync(self) -> List[Dict]:
        """Synchronous version of _get_potential_entries for use with ThreadPoolExecutor"""
        potential_entries = []
        start_time = time.time()

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

                # Get live prices directly from the option chain service
                # This is a synchronous call to fetch_ltp
                ltp_data = self.option_chain_service.fetch_ltp(instrument_keys)
                live_prices = {}
                for instrument_key, ltp in ltp_data.items():
                    live_prices[instrument_key] = {"ltp": float(ltp)}

                logger.info(f"Fetched live prices for {len(live_prices)} instruments in {time.time() - start_time:.2f}s")

                # Track how many instruments meet the price change criteria
                criteria_met_count = 0
                start_processing = time.time()

                # Process in smaller batches if there are many rows
                batch_size = 100
                total_rows = len(rows)

                for i in range(0, total_rows, batch_size):
                    batch = rows[i:i+batch_size]

                    for row in batch:
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

                        # Check if we've already processed this option (synchronous DB check)
                        has_previous = self._check_for_previous_order_sync(symbol, strike, option_type)
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
                logger.info(f"Processing took {time.time() - start_processing:.2f}s, total time: {time.time() - start_time:.2f}s")

        except Exception as e:
            logger.error(f"Error getting potential entries: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return potential_entries

    def _check_for_previous_order_sync(self, symbol, strike, option_type) -> bool:
        """Synchronous version of _check_for_previous_order"""
        try:
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM strategy_orders
                    WHERE symbol = %s AND strike_price = %s AND option_type = %s
                    AND strategy_name = %s
                    AND entry_time > (CURRENT_DATE - INTERVAL '30 days')
                """, (symbol, strike, option_type, self.strategy_name))

                count = cur.fetchone()[0]
                return count > 0

        except Exception as e:
            logger.error(f"Error checking for previous order: {str(e)}")
            return False

    def _update_active_orders_sync(self) -> List[str]:
        """Synchronous version of _update_active_orders for use with ThreadPoolExecutor"""
        if not self.active_orders:
            return []

        start_time = time.time()
        orders_to_remove = []

        # Get current prices for all active orders
        instrument_keys = [order['instrument_key'] for order in self.active_orders.values() if order['instrument_key']]
        if not instrument_keys:
            return []

        # Get live prices directly using the option chain service
        ltp_data = self.option_chain_service.fetch_ltp(instrument_keys)
        live_prices = {}
        for instrument_key, ltp in ltp_data.items():
            live_prices[instrument_key] = {"ltp": float(ltp)}

            # Update cache at the same time
            self.price_cache[instrument_key] = {"ltp": float(ltp)}
            self.cache_time[instrument_key] = time.time()

        logger.info(f"Fetched live prices for {len(live_prices)} active orders in {time.time() - start_time:.2f}s")

        # Process each active order
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
                self._update_order_price_sync(order['id'], current_price)

                # Check if target or stop loss hit
                if current_price >= target_price:
                    # Take profit - exit at target
                    self._close_order_sync(order['id'], current_price, 'PROFIT', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Target hit: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                elif current_price <= stop_loss_price:
                    # Stop loss hit - exit to limit loss
                    self._close_order_sync(order['id'], current_price, 'LOSS', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Stop loss hit: {order['symbol']} {order['strike_price']} {order['option_type']} at {current_price} ({percent_change:.2f}%)")

                # Check for expiry
                expiry_date = order['expiry_date']
                if expiry_date and expiry_date <= date.today():
                    self._close_order_sync(order['id'], current_price, 'EXPIRED', percent_change)
                    orders_to_remove.append(key)
                    logger.info(f"Position expired: {order['symbol']} {order['strike_price']} {order['option_type']}")

            except Exception as e:
                logger.error(f"Error updating order {key}: {str(e)}")

        logger.info(f"Updated {len(self.active_orders)} active orders in {time.time() - start_time:.2f}s, closing {len(orders_to_remove)} orders")

        # Note: The actual removal from self.active_orders is done in the main thread
        # to avoid concurrent modification issues
        return orders_to_remove

    def _update_order_price_sync(self, order_id, current_price):
        """Synchronous version of _update_order_price"""
        try:
            with self.db._get_cursor() as cur:
                cur.execute("""
                    UPDATE strategy_orders
                    SET current_price = %s
                    WHERE id = %s
                """, (current_price, order_id))
        except Exception as e:
            logger.error(f"Error updating order price: {str(e)}")

    def _close_order_sync(self, order_id, exit_price, status, pnl_percentage):
        """Synchronous version of _close_order"""
        try:
            with self.db._get_cursor() as cur:
                # Get order details first
                cur.execute("""
                    SELECT entry_price, quantity
                    FROM strategy_orders
                    WHERE id = %s
                """, (order_id,))

                row = cur.fetchone()
                if not row:
                    logger.error(f"Order {order_id} not found for closing")
                    return

                entry_price = float(row[0])
                quantity = int(row[1])

                # Calculate PnL
                pnl = (exit_price - entry_price) * quantity

                # Update the order
                cur.execute("""
                    UPDATE strategy_orders
                    SET exit_price = %s,
                        exit_time = NOW(),
                        status = %s,
                        pnl = %s,
                        pnl_percentage = %s
                    WHERE id = %s
                """, (exit_price, status, pnl, pnl_percentage, order_id))

        except Exception as e:
            logger.error(f"Error closing order: {str(e)}")

    def _update_daily_capital_sync(self):
        """Synchronous version of _update_daily_capital"""
        try:
            today = datetime.now(self.tz).date()
            total_capital = 0.0
            unrealized_pnl = 0.0
            open_positions = len(self.active_orders)

            # Calculate current capital deployed and unrealized PnL
            with self.db._get_cursor() as cur:
                cur.execute("""
                    SELECT SUM(entry_price * quantity), 
                           SUM((current_price - entry_price) * quantity)
                    FROM strategy_orders
                    WHERE status = 'OPEN' AND strategy_name = %s
                """, (self.strategy_name,))

                row = cur.fetchone()
                if row and row[0]:
                    total_capital = float(row[0])
                    unrealized_pnl = float(row[1] or 0)

            # Update max capital if needed
            self.today_capital = total_capital
            if total_capital > self.max_capital:
                self.max_capital = total_capital

            # Update or insert daily capital record
            with self.db._get_cursor() as cur:
                cur.execute("""
                    INSERT INTO strategy_daily_capital
                    (strategy_name, date, open_positions, capital_deployed, unrealized_pnl, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (strategy_name, date)
                    DO UPDATE SET
                        open_positions = EXCLUDED.open_positions,
                        capital_deployed = EXCLUDED.capital_deployed,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        updated_at = NOW()
                """, (self.strategy_name, today, open_positions, total_capital, unrealized_pnl))

            return {
                'capital': total_capital,
                'pnl': unrealized_pnl,
                'positions': open_positions
            }

        except Exception as e:
            logger.error(f"Error updating daily capital: {str(e)}")
            return None


# Function to run the strategy tracker as a background process
async def run_strategy_tracker():
    """Run multiple strategy trackers as parallel background processes"""
    db_service = DatabaseService()

    # Create both strategy trackers with different configurations
    strategy1 = StrategyTracker(db_service, {
        "name": "percent_change_tracker",  # Original strategy: 5% entry, 95% profit, -15% stop loss
        "entry_threshold": 5.0,
        "profit_target": 95.0,
        "stop_loss": -15.0,
        "lot_size_multiplier": 1
    })

    strategy2 = StrategyTracker(db_service, {
        "name": "aggressive_tracker",  # New strategy: 10% entry, 95% profit, -30% stop loss
        "entry_threshold": 10.0,
        "profit_target": 95.0,
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

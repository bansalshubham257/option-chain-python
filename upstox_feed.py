# Add this to app.py (after the existing imports)
import asyncio
import json
import ssl
import uuid
from datetime import datetime
import random
import concurrent.futures
import time

import pandas as pd
import pytz
import requests
import websockets
from google.protobuf.json_format import MessageToDict
import MarketDataFeed_pb2 as pb
from config import Config

# Add this class before the Flask app routes
class UpstoxFeedWorker:
    def __init__(self, database_service):
        self.db = database_service
        self.access_token = Config.ACCESS_TOKEN
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.running = False
        # Constants for connection limits
        self.MAX_CONNECTIONS = 2
        self.MAX_KEYS_PER_CONNECTION = 2650
        # Threshold values for options and futures
        self.OPTIONS_THRESHOLD = 87  # For options orders
        self.FUTURES_THRESHOLD = 36  # For futures orders
        # For parallel processing
        self.MAX_WORKERS = 10  # Maximum number of workers for parallel processing
        self.processing_stats = {
            'start_time': None,
            'total_instruments': 0,
            'processed_instruments': 0,
            'last_update_time': 0
        }
        # Cache for instruments to reduce DB calls
        self.instrument_cache = {}
        self.cache_refresh_time = 0
        self.CACHE_TTL = 3600  # Cache time to live in seconds (1 hour)
        self.current_batch = {
            'feeds': {},
            'start_time': None,
            'instrument_count': 0
        }
        self.BATCH_INTERVAL = 10  # seconds between batch processing
        self.last_batch_time = time.time()

    async def get_market_data_feed_authorize(self):
        """Get authorization for market data feed with token refresh handling."""
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'

        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()  # Raises exception for 4XX/5XX status codes
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:  # Unauthorized
                raise  # Re-raise if we can't handle it

    def decode_protobuf(self, buffer):
        """Decode protobuf message."""
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response

    def get_instrument_details_cached(self, instrument_keys):
        """Get instrument details from cache or database for a batch of keys."""
        current_time = time.time()

        # Check if we need to refresh the cache
        if current_time - self.cache_refresh_time > self.CACHE_TTL:
            # Fetch all instruments and cache them by instrument_key
            all_instruments = self.db.get_instrument_keys()
            self.instrument_cache = {inst['instrument_key']: inst for inst in all_instruments}
            self.cache_refresh_time = current_time
            print(f"Refreshed instrument cache with {len(self.instrument_cache)} instruments")

        # Get instruments from cache
        result = {}
        missing_keys = []

        for key in instrument_keys:
            if key in self.instrument_cache:
                result[key] = self.instrument_cache[key]
            else:
                missing_keys.append(key)

        # Fetch any missing instruments from DB and update cache
        if missing_keys:
            missing_instruments = self.db.get_instruments_by_keys(missing_keys)
            for inst in missing_instruments:
                self.instrument_cache[inst['instrument_key']] = inst
                result[inst['instrument_key']] = inst

        return result

    async def process_instrument_data(self, instrument_key, feed_data, current_time, instruments_dict):
        """Process market data for a single instrument."""
        try:
            oi_record = None
            stock_data = None
            option_order = None
            future_order = None

            # Handle firstLevelWithGreeks format (new structure)
            if 'firstLevelWithGreeks' in feed_data:
                feed_struct = feed_data['firstLevelWithGreeks']
                instrument = instruments_dict.get(instrument_key)
                if not instrument:
                    return None, None, None, None

                # Extract data from the new structure
                ltpc = feed_struct.get('ltpc', {})
                ltp = ltpc.get('ltp')
                oi = feed_struct.get('oi')
                volume = feed_struct.get('vtt')
                greeks = feed_struct.get('optionGreeks', {})
                first_depth = feed_struct.get('firstDepth', {})
                iv = feed_struct.get('iv')

                # Process based on instrument type
                if instrument['instrument_type'] == 'FO':
                    lot_size = instrument.get('lot_size', 0) or 1  # Default to 1 if not available

                    if instrument['option_type'] in ['CE', 'PE']:  # Option
                        # Only add to options_orders if bid_qty or ask_qty meets the threshold
                        bid_qty = first_depth.get('bidQ')
                        ask_qty = first_depth.get('askQ')

                        # Convert bid_qty and ask_qty to integers or set to 0 if not available
                        try:
                            bid_qty = int(bid_qty) if bid_qty else 0
                            ask_qty = int(ask_qty) if ask_qty else 0
                        except ValueError:
                            bid_qty = 0
                            ask_qty = 0

                        # Skip records where price is less than 1 or bid/ask quantity is 0
                        if ltp is None or float(ltp) < 1 or bid_qty == 0 or ask_qty == 0:
                            # Skip this record for options_orders
                            pass
                        else:
                            # Check if either bid_qty or ask_qty meets the threshold
                            threshold_val = self.OPTIONS_THRESHOLD * lot_size
                            if bid_qty >= threshold_val or ask_qty >= threshold_val:
                                option_order = {
                                    'stock': instrument['symbol'],
                                    'strike_price': instrument['strike_price'],
                                    'type': instrument['option_type'],
                                    'ltp': ltp,
                                    'bid_qty': bid_qty,
                                    'ask_qty': ask_qty,
                                    'lot_size': lot_size,
                                    'timestamp': current_time
                                }

                        # Add to OI volume history regardless of thresholds
                        if oi is not None and volume is not None:
                            oi_record = {
                                'symbol': instrument['symbol'],
                                'expiry': instrument['expiry_date'],
                                'strike': instrument['strike_price'],
                                'option_type': instrument['option_type'],
                                'oi': oi,
                                'volume': volume,
                                'price': ltp,
                                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%H:%M"),
                                'pct_change': 0,  # Will be calculated later
                                'vega': greeks.get('vega'),
                                'theta': greeks.get('theta'),
                                'gamma': greeks.get('gamma'),
                                'delta': greeks.get('delta'),
                                'iv': iv,
                                'pop': greeks.get('pop', 0)  # Probability of profit
                            }

                    elif instrument['option_type'] == 'FU':  # Future
                        bid_qty = first_depth.get('bidQ')
                        ask_qty = first_depth.get('askQ')

                        # Convert bid_qty and ask_qty to integers or set to 0 if not available
                        try:
                            bid_qty = int(bid_qty) if bid_qty else 0
                            ask_qty = int(ask_qty) if ask_qty else 0
                        except ValueError:
                            bid_qty = 0
                            ask_qty = 0

                        # Check if either bid_qty or ask_qty meets the threshold
                        threshold_val = self.FUTURES_THRESHOLD * lot_size
                        if bid_qty >= threshold_val or ask_qty >= threshold_val:
                            future_order = {
                                'stock': instrument['symbol'],
                                'ltp': ltp,
                                'bid_qty': bid_qty,
                                'ask_qty': ask_qty,
                                'lot_size': lot_size,
                                'timestamp': current_time
                            }

                        # Add to OI volume history for futures regardless of thresholds
                        if oi is not None and volume is not None:
                            oi_record = {
                                'symbol': instrument['symbol'],
                                'expiry': instrument['expiry_date'],
                                'strike': 0,  # No strike price for futures
                                'option_type': 'FU',
                                'oi': oi,
                                'volume': volume,
                                'price': ltp,
                                'timestamp': current_time,
                                'pct_change': 0,  # Will be calculated later
                                'vega': None,
                                'theta': None,
                                'gamma': None,
                                'delta': None,
                                'iv': None,
                                'pop': None
                            }

                elif instrument['instrument_type'] in ['EQUITY', 'INDEX']:
                    # For EQUITY and INDEX, add .NS suffix to symbol for database compatibility
                    symbol = instrument['symbol']
                    if not symbol.endswith('.NS'):
                        symbol = f"{symbol}.NS"

                    stock_data = {
                        'symbol': symbol,
                        'close': ltp,
                        'timestamp': current_time,
                    }

            return oi_record, stock_data, option_order, future_order

        except Exception as e:
            print(f"Error processing instrument {instrument_key}: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None, None

    async def process_market_data(self, data_dict):
        """Process market data and accumulate for batch processing."""
        if 'feeds' not in data_dict:
            return

        # Initialize batch timing
        if not self.current_batch['start_time']:
            self.current_batch['start_time'] = time.time()

        # Accumulate feeds
        self.current_batch['feeds'].update(data_dict['feeds'])
        self.current_batch['instrument_count'] = len(self.current_batch['feeds'])

        # Check if we should process the batch
        current_time = time.time()
        if current_time - self.last_batch_time >= self.BATCH_INTERVAL:
            await self.process_accumulated_batch()

    async def process_accumulated_batch(self):
        """Process all accumulated market data in one batch."""
        if not self.current_batch['feeds']:
            return

        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        total_instruments = len(self.current_batch['feeds'])

        print(f"\nStarting FULL batch processing of {total_instruments} instruments at {current_time.strftime('%H:%M:%S')}")

        # Pre-fetch all instrument details
        start_fetch = time.time()
        instrument_keys = list(self.current_batch['feeds'].keys())
        instruments_dict = self.get_instrument_details_cached(instrument_keys)
        fetch_time = time.time() - start_fetch
        print(f"Pre-fetched {len(instruments_dict)} instrument details in {fetch_time:.2f}s")

        # Process all instruments in parallel
        tasks = []
        for instrument_key, feed_data in self.current_batch['feeds'].items():
            tasks.append(self.process_instrument_data(instrument_key, feed_data, current_time, instruments_dict))

        # Prepare batch collections
        oi_volume_records = []
        stock_data_cache = []
        options_orders = []
        futures_orders = []

        # Process results
        for future in asyncio.as_completed(tasks):
            oi_record, stock_data, option_order, future_order = await future

            if oi_record:
                oi_volume_records.append(oi_record)
            if stock_data:
                stock_data_cache.append(stock_data)
            if option_order:
                options_orders.append(option_order)
            if future_order:
                futures_orders.append(future_order)

        # Batch save to database
        start_save = time.time()

        if oi_volume_records:
            print(f"Saving {len(oi_volume_records)} OI volume records to database")
            self.db.save_oi_volume_batch_feed(oi_volume_records)

        if stock_data_cache:
            print(f"Saving {len(stock_data_cache)} stock data to database")
            self.db.update_stock_prices_batch(stock_data_cache)

        if options_orders:
            print(f"Saving {len(options_orders)} options orders to database")
            self.db.save_options_data('options', options_orders)

        if futures_orders:
            print(f"Saving {len(futures_orders)} futures orders to database")
            self.db.save_futures_data('futures', futures_orders)

        # Log completion
        total_elapsed = time.time() - self.current_batch['start_time']
        save_time = time.time() - start_save
        print(f"Completed FULL batch processing of {total_instruments} instruments in {total_elapsed:.1f} seconds (Save time: {save_time:.1f}s\n")

        # Reset batch
        self.current_batch = {
            'feeds': {},
            'start_time': None,
            'instrument_count': 0
        }
        self.last_batch_time = time.time()

    async def run_feed(self):
        """Run the market data feed worker with persistent connection management."""
        self.running = True

        while self.running:
            try:
                # Get instrument keys from database
                instruments = self.db.get_instrument_keys(limit=6000)

                if not instruments:
                    print("No instruments found, waiting 60 seconds before retry")
                    await asyncio.sleep(60)
                    continue

                # Prepare instrument keys
                instrument_keys = [inst['instrument_key'] for inst in instruments]
                max_total_keys = self.MAX_CONNECTIONS * self.MAX_KEYS_PER_CONNECTION

                if len(instrument_keys) > max_total_keys:
                    print(f"Limiting instruments from {len(instrument_keys)} to {max_total_keys}")
                    instrument_keys = instrument_keys[:max_total_keys]

                # Split into batches for connections
                key_batches = [
                    instrument_keys[i:i+self.MAX_KEYS_PER_CONNECTION]
                    for i in range(0, len(instrument_keys), self.MAX_KEYS_PER_CONNECTION)
                    if instrument_keys[i:i+self.MAX_KEYS_PER_CONNECTION]
                ]

                print(f"Preparing {len(key_batches)} connections with batch sizes: {[len(b) for b in key_batches]}")

                # Create and manage connections with retry logic
                connection_tasks = []
                for batch_idx, key_batch in enumerate(key_batches):
                    # Wrap connection in a persistent task
                    task = asyncio.create_task(
                        self.persistent_connection_manager(key_batch, batch_idx)
                    )
                    connection_tasks.append(task)

                # Wait for all connections to complete (they shouldn't unless there's an error)
                await asyncio.gather(*connection_tasks)

            except Exception as e:
                print(f"Main feed loop error: {e}")
                await asyncio.sleep(10)  # Brief pause before restarting


    async def persistent_connection_manager(self, key_batch, batch_idx):
        """Manage a persistent connection with automatic reconnection."""
        retry_count = 0
        max_retries = 5
        base_delay = 5  # seconds

        while self.running and retry_count < max_retries:
            try:
                print(f"Connection {batch_idx}: Attempting to establish (attempt {retry_count + 1})")
                await self.manage_connection(key_batch, batch_idx)
                retry_count = 0  # Reset on successful connection
            except Exception as e:
                retry_count += 1
                delay = base_delay * retry_count
                print(f"Connection {batch_idx}: Error in connection (attempt {retry_count}): {e}. Retrying in {delay}s")
                await asyncio.sleep(delay)

        if retry_count >= max_retries:
            print(f"Connection {batch_idx}: Max retries reached. Waiting before new attempt.")
            await asyncio.sleep(30)

    async def manage_connection(self, key_batch, batch_idx):
        """Manage a single websocket connection with a batch of instrument keys."""
        try:
            # Get authorization
            auth_response = await self.get_market_data_feed_authorize()
            if not auth_response.get('data', {}).get('authorized_redirect_uri'):
                print(f"Connection {batch_idx}: Authorization failed")
                raise ConnectionError("Failed to authorize feed")

            print(f"Connection {batch_idx}: Establishing WebSocket connection")

            connect_kwargs = {
                'ssl': self.ssl_context,
                'ping_interval': 20,
                'ping_timeout': 20,
                'close_timeout': 10
            }

            async with websockets.connect(
                    auth_response['data']['authorized_redirect_uri'],
                    **connect_kwargs
            ) as websocket:
                print(f'Connection {batch_idx}: WebSocket connected, subscribing to {len(key_batch)} instruments')

                # Subscribe to instruments
                subscribe_msg = {
                    "guid": str(uuid.uuid4()),
                    "method": "sub",
                    "data": {
                        "mode": "option_greeks",
                        "instrumentKeys": key_batch
                    }
                }
                await websocket.send(json.dumps(subscribe_msg).encode('utf-8'))

                # Continuous message processing
                while self.running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        decoded = self.decode_protobuf(message)
                        data_dict = MessageToDict(decoded)
                        await self.process_market_data(data_dict)
                    except asyncio.TimeoutError:
                        # Send ping to keep connection alive
                        try:
                            await websocket.ping()
                            print(f"Connection {batch_idx}: Sent keepalive ping")
                        except Exception as e:
                            print(f"Connection {batch_idx}: Ping failed: {e}")
                            break  # Will trigger reconnection
                    except Exception as e:
                        print(f"Connection {batch_idx}: Message processing error: {e}")
                        raise  # Will trigger reconnection

        except Exception as e:
            print(f"Connection {batch_idx}: Connection error: {e}")
            raise  # Propagate to trigger reconnection logic

    async def stop(self):
        """Stop the feed worker gracefully."""
        self.running = False
        print("Feed worker stopping...")

import asyncio
import json
import ssl
import uuid
from datetime import datetime
import time
import numpy as np
from typing import Dict, List, Optional, Tuple
import concurrent.futures
import requests
import pandas as pd
import pytz
import websockets
from google.protobuf.json_format import MessageToDict
import MarketDataFeed_pb2 as pb
from config import Config
from services.database import DatabaseService
import threading
from queue import Queue, Empty  # Import Empty exception from queue module
import multiprocessing
from functools import partial


class UpstoxFeedWorker:
    def __init__(self, database_service):
        self.db = database_service
        # Fetch access tokens from database with different IDs
        self.access_tokens = []
        self.access_token_1 = self.db.get_access_token(account_id=1)  # Get first token with ID=1
        self.access_token_3 = self.db.get_access_token(account_id=3)  # Get second token with ID=3

        if self.access_token_1:
            print(f"Using first access token ending with ...{self.access_token_1[-4:]} from database (ID=1)")
            self.access_tokens.append(self.access_token_1)

        if self.access_token_3:
            print(f"Using second access token ending with ...{self.access_token_3[-4:]} from database (ID=3)")
            self.access_tokens.append(self.access_token_3)

        if not self.access_tokens:
            print("Warning: No access tokens found in database, falling back to Config")
            self.access_tokens.append(Config.ACCESS_TOKEN)
        
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.running = False

        # Connection settings - Updated for multiple connections
        self.MAX_CONNECTIONS = 2  # Two connections to handle 6000 instruments
        self.MAX_KEYS_PER_CONNECTION = 3000  # Maximum of 3000 keys per connection
        self.RECONNECT_DELAY = 1  # seconds
        self.CONNECTION_DELAY = 2  # seconds between connections

        # Keep track of connections
        self.connections = []
        self.connection_tasks = []

        # Processing thresholds
        self.OPTIONS_THRESHOLD = 87
        self.FUTURES_THRESHOLD = 36

        # Parallel processing settings
        self.MAX_WORKERS = 20
        self.CHUNK_SIZE = 300  # Instruments per processing chunk

        # DB worker pool settings
        self.DB_WORKERS = min(8, multiprocessing.cpu_count())  # Number of database worker threads
        self.DB_CHUNK_SIZE = 100  # Number of records per database batch
        self.db_workers = []
        self.db_task_queues = []
        self.db_workers_running = False

        # Data processing pipeline
        self.data_queue = asyncio.Queue(maxsize=10000)
        self.processing_task = None
        self.refresh_request_queue = asyncio.Queue(maxsize=1)

        # Database distribution queue
        self.db_distribution_queue = Queue(maxsize=10000)
        self.db_distributor_thread = None

        # Instrument cache and keys
        self.instrument_cache = {}
        self.cache_refresh_time = 0
        self.CACHE_TTL = 3600  # 1 hour cache
        self.instrument_keys = []  # Store all instrument keys here
        self.connection_key_batches = []  # Store key batches for each connection

        # Performance monitoring
        self.last_processed_time = time.time()
        self.processed_count = 0
        self.db_stats = {
            'oi_volume': {'count': 0, 'time': 0},
            'stock_prices': {'count': 0, 'time': 0},
            'options': {'count': 0, 'time': 0},
            'futures': {'count': 0, 'time': 0}
        }
        self.last_stats_time = time.time()
        
        # Market hours check interval
        self.MARKET_CHECK_INTERVAL = 60  # Check market hours every 60 seconds

    async def start(self):
        """Start the feed worker and processing pipeline."""
        self.running = True
        self.db_workers_running = True

        # Check if market is open before starting
        if not self.is_market_open():
            print("Market is closed. Feed worker will wait until market opens.")
            await self.wait_for_market_open()

        print("Market is open. Starting feed worker.")

        # Start database worker threads
        print(f"Starting {self.DB_WORKERS} database worker threads")
        self._setup_db_workers()

        # Start database distributor thread
        self.db_distributor_thread = threading.Thread(target=self._db_distributor_thread)
        self.db_distributor_thread.daemon = True
        self.db_distributor_thread.start()

        # Fetch instrument keys once at startup in parallel with other tasks
        fetch_keys_task = asyncio.create_task(self.fetch_instrument_keys())
        self.processing_task = asyncio.create_task(self._process_queue_continuously())

        # Wait only for keys to be fetched, then start feed
        await fetch_keys_task

        # Start market hours checker task
        market_checker_task = asyncio.create_task(self.check_market_hours())

        # Run feed (this will loop until self.running is False)
        await self.run_feed()

    def is_market_open(self) -> bool:
        """Check if the market is currently open based on config settings."""
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        current_time = now.time()
        current_weekday = now.weekday()

        # Check if today is a trading day
        if current_weekday not in Config.TRADING_DAYS:
            return False

        # Check if current time is within market hours
        return Config.MARKET_OPEN <= current_time <= Config.MARKET_CLOSE

    async def wait_for_market_open(self):
        """Wait until the market opens."""
        while not self.is_market_open() and self.running:
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            current_time = now.time()
            current_weekday = now.weekday()

            if current_weekday not in Config.TRADING_DAYS:
                # Calculate time until next trading day
                days_until_next = min((day - current_weekday) % 7 for day in Config.TRADING_DAYS)
                if days_until_next == 0:  # Already past market hours on a trading day
                    days_until_next = min((day + 7 - current_weekday) % 7 for day in Config.TRADING_DAYS)
                    
                print(f"Not a trading day. Waiting until next trading day ({days_until_next} days from now)")
                await asyncio.sleep(3600)  # Check again in an hour
            elif current_time < Config.MARKET_OPEN:
                # Calculate seconds until market open
                market_open_today = datetime.combine(now.date(), Config.MARKET_OPEN)
                market_open_today = pytz.timezone('Asia/Kolkata').localize(market_open_today)
                seconds_until_open = (market_open_today - now).total_seconds()
                
                print(f"Market not open yet. Opening in {seconds_until_open/60:.1f} minutes")
                # Sleep until market opens (with a small buffer)
                await asyncio.sleep(min(seconds_until_open, 300))
            else:
                # Past market close, wait until tomorrow
                print("Market closed for today. Waiting until next trading day")
                await asyncio.sleep(3600)  # Check again in an hour

    async def check_market_hours(self):
        """Periodically check if market is open and stop feed if closed."""
        while self.running:
            if not self.is_market_open():
                print("Market has closed. Stopping feed connections.")
                # Keep the worker running but stop the connections
                await self.stop_connections()
                # Wait for market to open again
                await self.wait_for_market_open()
                # Restart feed connections when market opens
                print("Market has reopened. Restarting feed connections.")
                # Refresh instrument keys
                await self.fetch_instrument_keys()
            
            # Check market status periodically
            await asyncio.sleep(self.MARKET_CHECK_INTERVAL)

    async def stop_connections(self):
        """Stop all feed connections but keep the worker running."""
        # Cancel all connection tasks if active
        for task in self.connection_tasks:
            if task and not task.done():
                task.cancel()

        # Reset connection state
        self.connections = []
        self.connection_tasks = []

        print("All feed connections have been stopped")

    def _setup_db_workers(self):
        """Setup multiple database worker threads for parallel processing."""
        for i in range(self.DB_WORKERS):
            # Create a queue for this worker
            task_queue = Queue(maxsize=1000)
            self.db_task_queues.append(task_queue)

            # Create and start a worker thread
            worker = threading.Thread(
                target=self._db_worker_thread,
                args=(i, task_queue)
            )
            worker.daemon = True
            worker.start()
            self.db_workers.append(worker)

    def _db_distributor_thread(self):
        """Thread that distributes database tasks to worker threads."""
        print("Database distributor thread started")

        # Initialize buffers for different data types
        buffers = {
            'oi_volume': [],
            'stock_prices': [],
            'options': [],
            'futures': []
        }

        # Define max buffer sizes for different data types
        max_buffer_sizes = {
            'oi_volume': self.DB_CHUNK_SIZE * 2,
            'stock_prices': self.DB_CHUNK_SIZE,
            'options': self.DB_CHUNK_SIZE,
            'futures': self.DB_CHUNK_SIZE
        }

        # Keep track of last flush time for each buffer
        last_flush_time = {k: time.time() for k in buffers}
        flush_interval = 1.0  # Max seconds to hold data before flushing

        while self.db_workers_running:
            try:
                # Try to get a batch from the queue with a short timeout
                try:
                    batch = self.db_distribution_queue.get(timeout=0.1)
                except Empty:
                    # No new data, check if any buffers need time-based flushing
                    current_time = time.time()
                    for data_type, buffer in buffers.items():
                        if buffer and current_time - last_flush_time[data_type] > flush_interval:
                            self._dispatch_buffer(data_type, buffer)
                            buffers[data_type] = []
                            last_flush_time[data_type] = current_time
                    continue

                batch_type = batch['type']
                data = batch['data']

                if not data:
                    self.db_distribution_queue.task_done()
                    continue

                # Add data to appropriate buffer
                buffers[batch_type].extend(data)

                # If buffer reaches threshold size, distribute it to a worker
                if len(buffers[batch_type]) >= max_buffer_sizes[batch_type]:
                    self._dispatch_buffer(batch_type, buffers[batch_type])
                    buffers[batch_type] = []
                    last_flush_time[batch_type] = time.time()

                self.db_distribution_queue.task_done()

            except Exception as e:
                print(f"Error in database distributor thread: {e}")
                time.sleep(0.1)

        # Flush any remaining data before shutting down
        for data_type, buffer in buffers.items():
            if buffer:
                try:
                    self._dispatch_buffer(data_type, buffer)
                except Exception as e:
                    print(f"Error flushing buffer {data_type} during shutdown: {e}")

        print("Database distributor thread stopped")

    def _dispatch_buffer(self, data_type, buffer):
        """Dispatch a buffer to the least busy worker thread."""
        if not buffer:
            return

        # Find the worker with the smallest queue
        min_size = float('inf')
        min_index = 0

        for i, queue in enumerate(self.db_task_queues):
            size = queue.qsize()
            if size < min_size:
                min_size = size
                min_index = i

        # Send the task to the worker
        self.db_task_queues[min_index].put({
            'type': data_type,
            'data': buffer
        })

    def _db_worker_thread(self, worker_id, task_queue):
        """Worker thread function to save data to the database."""
        print(f"Database worker {worker_id} started")

        while self.db_workers_running:
            try:
                # Try to get a task with timeout
                try:
                    task = task_queue.get(timeout=0.5)
                except Empty:
                    continue

                task_type = task['type']
                data = task['data']

                if not data:
                    task_queue.task_done()
                    continue

                # Measure performance
                start_time = time.time()

                # Execute database operation based on task type
                if task_type == 'oi_volume':
                    # Aggregate call and put OI data by symbol
                    symbol_oi_data = {}

                    # Create time bucket (rounded to nearest 5 minutes for consistent aggregation)
                    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
                    current_minute = current_time.minute
                    rounded_minute = 5 * (current_minute // 5)  # Round to nearest 5 minutes
                    display_time = f"{current_time.hour:02d}:{rounded_minute:02d}"

                    # Process all records to aggregate total call and put OI by symbol
                    for record in data:
                        symbol = record.get('symbol')
                        option_type = record.get('option_type')
                        oi = float(record.get('oi', 0) or 0)

                        if not symbol or not option_type or option_type not in ['CE', 'PE']:
                            continue

                        if symbol not in symbol_oi_data:
                            symbol_oi_data[symbol] = {'call_oi': 0, 'put_oi': 0}

                        if option_type == 'CE':
                            symbol_oi_data[symbol]['call_oi'] += oi
                        elif option_type == 'PE':
                            symbol_oi_data[symbol]['put_oi'] += oi

                    # Save aggregated OI data to total_oi_history table
                    if symbol_oi_data:
                        total_oi_records = []
                        for symbol, oi_data in symbol_oi_data.items():
                            call_oi = oi_data['call_oi']
                            put_oi = oi_data['put_oi']
                            call_put_ratio = call_oi / put_oi if put_oi > 0 else 0

                            total_oi_records.append({
                                'symbol': symbol,
                                'display_time': display_time,
                                'call_oi': call_oi,
                                'put_oi': put_oi,
                                'call_put_ratio': call_put_ratio,
                                'timestamp': current_time
                            })

                        # Save to database with upsert logic to handle 5-minute intervals
                        try:
                            self.db.save_total_oi_data(total_oi_records)
                        except Exception as e:
                            print(f"Error saving total OI data: {e}")

                    # Continue processing individual OI records
                    #self.db.save_oi_volume_batch_feed(data)
                    records_count = len(data)
                elif task_type == 'stock_prices':
                    self.db.update_stock_prices_batch(data)
                    records_count = len(data)
                elif task_type == 'options':
                    self.db.save_options_data('options', data)
                    records_count = len(data)
                elif task_type == 'futures':
                    self.db.save_futures_data('futures', data)
                    records_count = len(data)
                else:
                    records_count = 0

                # Update performance stats
                elapsed = time.time() - start_time
                with threading.Lock():
                    self.db_stats[task_type]['count'] += records_count
                    self.db_stats[task_type]['time'] += elapsed

                # Log worker performance occasionally
                if worker_id == 0 and time.time() - self.last_stats_time > 30:
                    self._log_db_stats()

                task_queue.task_done()

            except Exception as e:
                print(f"Error in database worker {worker_id}: {e}")
                time.sleep(0.1)

        print(f"Database worker {worker_id} stopped")

    def _log_db_stats(self):
        """Log database performance statistics."""
        current_time = time.time()
        elapsed = current_time - self.last_stats_time
        if elapsed < 1:
            return

        stats_str = "Database stats: "
        for data_type, stats in self.db_stats.items():
            if stats['count'] > 0:
                rate = stats['count'] / elapsed
                avg_time = (stats['time'] * 1000 / stats['count']) if stats['count'] > 0 else 0
                stats_str += f"{data_type}: {rate:.1f}/s ({avg_time:.1f}ms/rec), "

        print(stats_str)

        # Reset statistics
        self.db_stats = {
            'oi_volume': {'count': 0, 'time': 0},
            'stock_prices': {'count': 0, 'time': 0},
            'options': {'count': 0, 'time': 0},
            'futures': {'count': 0, 'time': 0}
        }
        self.last_stats_time = current_time

    async def stop(self):
        """Stop the feed worker gracefully."""
        self.running = False

        # Stop database workers
        self.db_workers_running = False

        # Wait for database threads to finish
        if self.db_distributor_thread:
            self.db_distributor_thread.join(timeout=2)

        for worker in self.db_workers:
            worker.join(timeout=1)

        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        # Cancel all connection tasks
        await self.stop_connections()

        print("Feed worker stopped")

    async def fetch_instrument_keys(self):
        """Fetch up to 6000 instrument keys."""
        try:
            # Set a timeout for fetching keys to prevent long delays
            instruments = await asyncio.wait_for(
                self.db.get_instrument_keys_async(limit=6000),  # Increased to 6000 instruments
                timeout=20  # 20 second timeout for database query
            )

            if not instruments:
                print("No instruments found during initial fetch")
                return False

            print(f"Found {len(instruments)} instruments during initial fetch")

            # Store keys and create instrument cache at the same time
            self.instrument_keys = []
            self.instrument_cache = {}

            for instrument in instruments:
                self.instrument_keys.append(instrument['instrument_key'])
                self.instrument_cache[instrument['instrument_key']] = instrument

            self.cache_refresh_time = time.time()

            # Ensure we don't exceed total capacity (MAX_CONNECTIONS * MAX_KEYS_PER_CONNECTION)
            max_total_keys = self.MAX_CONNECTIONS * self.MAX_KEYS_PER_CONNECTION
            if len(self.instrument_keys) > max_total_keys:
                print(f"Limiting instruments from {len(self.instrument_keys)} to {max_total_keys}")
                self.instrument_keys = self.instrument_keys[:max_total_keys]

            # Split keys into batches for each connection
            self.connection_key_batches = self._split_keys_for_connections(self.instrument_keys)

            print(f"Prepared {len(self.instrument_keys)} instruments across {len(self.connection_key_batches)} connections")
            return True

        except asyncio.TimeoutError:
            print("Timeout while fetching instrument keys")
            return False
        except Exception as e:
            print(f"Error fetching instrument keys: {e}")
            return False

    def _split_keys_for_connections(self, keys):
        """Split instrument keys into batches for each connection."""
        if not keys:
            return []

        # Calculate how many connections we need
        num_connections = min(self.MAX_CONNECTIONS,
                             (len(keys) + self.MAX_KEYS_PER_CONNECTION - 1) // self.MAX_KEYS_PER_CONNECTION)

        # Split keys into roughly equal batches
        batches = []
        for i in range(num_connections):
            start_idx = i * self.MAX_KEYS_PER_CONNECTION
            end_idx = min(start_idx + self.MAX_KEYS_PER_CONNECTION, len(keys))
            batch = keys[start_idx:end_idx]
            if batch:  # Only add non-empty batches
                batches.append(batch)

        return batches

    async def run_feed(self):
        """Main feed running loop with multiple connections."""
        while self.running:
            try:
                # Check if market is open before trying to connect
                if not self.is_market_open():
                    await asyncio.sleep(1)
                    continue

                # If no connection batches available, try to fetch them
                if not self.connection_key_batches:
                    print("No instrument key batches available, retrying fetch")
                    success = await asyncio.wait_for(
                        self.fetch_instrument_keys(),
                        timeout=10
                    )
                    if not success:
                        await asyncio.sleep(0.5)
                        continue

                # Calculate how many active connections we have
                active_connections = sum(1 for task in self.connection_tasks if task and not task.done())

                # If we have all the connections we need, just monitor them
                if active_connections >= len(self.connection_key_batches):
                    await asyncio.sleep(0.5)
                    continue
                
                # Start connections for any missing batches
                for i, key_batch in enumerate(self.connection_key_batches):
                    # Skip if we already have a connection for this batch
                    if i < len(self.connection_tasks) and self.connection_tasks[i] and not self.connection_tasks[i].done():
                        continue

                    # Select the appropriate token for this connection
                    # For even connection indices, use the first token; for odd, use the second token
                    token_index = min(i % len(self.access_tokens), len(self.access_tokens) - 1)
                    token = self.access_tokens[token_index]
                    token_id = 1 if token == self.access_token_1 else 3

                    print(f"Starting feed connection {i+1}/{len(self.connection_key_batches)} with {len(key_batch)} instruments using token from ID={token_id}")

                    # Create a new connection task
                    connection_task = asyncio.create_task(
                        self.persistent_connection_manager(key_batch, token, i)
                    )

                    # Add to our list of tasks
                    if i < len(self.connection_tasks):
                        self.connection_tasks[i] = connection_task
                    else:
                        self.connection_tasks.append(connection_task)

                    # Wait between starting connections to avoid overwhelming the server
                    await asyncio.sleep(self.CONNECTION_DELAY)

                # Sleep before checking connections again
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"Main feed loop error: {e}")
                await asyncio.sleep(0.5)

    async def persistent_connection_manager(self, key_batch, token, connection_id):
        """Manage a persistent connection with automatic reconnection."""
        retry_count = 0
        max_retries = 5

        while self.running and retry_count < max_retries:
            try:
                print(f"Connection {connection_id}: Attempting to establish (attempt {retry_count + 1}) with token ...{token[-4:]}")

                # Use timeouts to prevent hanging
                await asyncio.wait_for(
                    self.manage_connection(key_batch, token, connection_id),
                    timeout=25  # 25 second timeout
                )

                retry_count = 0  # Reset on successful connection
            except asyncio.TimeoutError:
                retry_count += 1
                delay = min(0.5 * retry_count, 1)  # Cap delay at 1s, start with 0.5s
                print(f"Connection {connection_id}: Timed out (attempt {retry_count}). Retrying in {delay}s")
                await asyncio.sleep(delay)
            except Exception as e:
                retry_count += 1
                delay = min(0.5 * retry_count, 1)
                print(f"Connection {connection_id}: Error (attempt {retry_count}): {e}. Retrying in {delay}s")
                await asyncio.sleep(delay)

        print(f"Connection {connection_id} with token ...{token[-4:]} terminated")

    async def manage_connection(self, key_batch, token, connection_id):
        """Manage a single websocket connection."""
        # Set a timeout for authorization to prevent hanging
        auth_response = await asyncio.wait_for(
            self.get_market_data_feed_authorize(token),
            timeout=5  # 5 second timeout
        )

        if not auth_response.get('data', {}).get('authorized_redirect_uri'):
            print(f"Connection {connection_id}: Authorization failed for token ...{token[-4:]}")
            raise ConnectionError("Failed to authorize feed")

        print(f"Connection {connection_id}: Establishing WebSocket connection with token ...{token[-4:]}")

        # Reduced timeouts
        connect_kwargs = {
            'ssl': self.ssl_context,
            'ping_interval': 10,  # Reduced from 20
            'ping_timeout': 10,  # Reduced from 20
            'close_timeout': 5    # Reduced from 10
        }

        # Set a timeout for the websocket connection establishment
        try:
            websocket = await asyncio.wait_for(
                websockets.connect(
                    auth_response['data']['authorized_redirect_uri'],
                    **connect_kwargs
                ),
                timeout=5  # 5 second timeout for connection
            )
        except asyncio.TimeoutError:
            print(f"Connection {connection_id}: WebSocket connection timed out")
            raise

        print(f'Connection {connection_id}: WebSocket connected, subscribing to {len(key_batch)} instruments')

        async with websocket:
            # Start subscription immediately to reduce wait time
            subscription_task = asyncio.create_task(
                self._subscription_manager(websocket, key_batch, connection_id)
            )

            # Start reader task after subscription is sent
            reader_task = asyncio.create_task(
                self._websocket_reader(websocket, key_batch, connection_id)
            )

            # Wait for first task to complete with a timeout
            done, pending = await asyncio.wait(
                [reader_task, subscription_task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=15  # 15 second overall timeout
            )

            # Cancel all pending tasks
            for task in pending:
                task.cancel()

            # Wait for cancellations to complete
            try:
                await asyncio.gather(*pending, return_exceptions=True)
            except:
                pass

    async def _websocket_reader(self, websocket, key_batch, connection_id):
        """Dedicated task for reading from websocket."""
        try:
            while self.running:
                try:
                    # Reduced timeout for faster detection of connection issues
                    message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                    decoded = self.decode_protobuf(message)
                    data_dict = MessageToDict(decoded)

                    # Put message in queue for processing
                    await self.data_queue.put(data_dict)

                except asyncio.TimeoutError:
                    # More frequent but lightweight pings
                    try:
                        pong_waiter = await websocket.ping()
                        await asyncio.wait_for(pong_waiter, timeout=0.5)
                    except:
                        print(f"Connection {connection_id}: Ping failed, reconnecting")
                        break
                except Exception as e:
                    print(f"Connection {connection_id}: Reader error: {e}")
                    break
        except Exception as e:
            print(f"Connection {connection_id}: Reader task error: {e}")

    async def _subscription_manager(self, websocket, key_batch, connection_id):
        """Dedicated task for handling subscriptions."""
        last_subscription_time = time.time()
        subscription_interval = 30  # Refresh subscription every 30 seconds

        # Initial subscription with a timeout
        try:
            await asyncio.wait_for(
                self.send_subscription(websocket, key_batch, connection_id),
                timeout=5  # 5 second timeout for subscription
            )
        except asyncio.TimeoutError:
            print(f"Connection {connection_id}: Initial subscription timed out")
            return
        except Exception as e:
            print(f"Connection {connection_id}: Initial subscription error: {e}")
            return

        try:
            while self.running:
                current_time = time.time()

                # Handle periodic subscription refresh
                if current_time - last_subscription_time > subscription_interval:
                    try:
                        await asyncio.wait_for(
                            self.send_subscription(websocket, key_batch, connection_id, refresh=True),
                            timeout=5  # 5 second timeout for refresh
                        )
                        last_subscription_time = current_time
                    except Exception as e:
                        print(f"Connection {connection_id}: Refresh subscription error: {e}")
                        break

                # Check for refresh requests (with non-blocking check)
                if not self.refresh_request_queue.empty():
                    try:
                        await self.refresh_request_queue.get()
                        await asyncio.wait_for(
                            self.send_subscription(websocket, key_batch, connection_id, refresh=True),
                            timeout=3
                        )
                        self.refresh_request_queue.task_done()
                        last_subscription_time = current_time
                    except Exception as e:
                        print(f"Connection {connection_id}: Requested refresh error: {e}")

                await asyncio.sleep(0.5)  # Check more frequently
        except Exception as e:
            print(f"Connection {connection_id}: Subscription manager error: {e}")

    async def _process_queue_continuously(self):
        """Continuously process data from the queue."""
        while self.running:
            try:
                # Process in chunks for efficiency
                chunk = []
                start_time = time.time()

                # Gather up to CHUNK_SIZE messages or wait for 0.2 seconds (reduced from 0.5)
                while len(chunk) < self.CHUNK_SIZE and (time.time() - start_time) < 0.2:
                    try:
                        data = await asyncio.wait_for(self.data_queue.get(), timeout=0.1)
                        chunk.append(data)
                        self.data_queue.task_done()
                    except asyncio.TimeoutError:
                        if chunk:  # If we have some data, process it
                            break

                if chunk:
                    await self._process_chunk(chunk)

            except Exception as e:
                print(f"Error in processing queue: {e}")
                await asyncio.sleep(0.1)  # reduced from 1

    async def _process_chunk(self, chunk):
        """Process a chunk of market data messages."""
        if not chunk:
            return

        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

        # Combine feeds from all messages in the chunk
        combined_feeds = {}
        for data_dict in chunk:
            if 'feeds' in data_dict:
                combined_feeds.update(data_dict['feeds'])

        if not combined_feeds:
            return

        instrument_keys = list(combined_feeds.keys())
        instruments_dict = await self.get_instrument_details_cached_async(instrument_keys)

        # Process instruments in parallel
        tasks = []
        for instrument_key, feed_data in combined_feeds.items():
            tasks.append(
                self.process_instrument_data(instrument_key, feed_data, current_time, instruments_dict)
            )

        # Prepare batch collections
        oi_volume_records = []
        stock_data_cache = []
        options_orders = []
        futures_orders = []

        # Process results as they complete
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

        # Queue data for the database distributor thread instead of direct saving
        if oi_volume_records:
            self.db_distribution_queue.put({'type': 'oi_volume', 'data': oi_volume_records})
        if stock_data_cache:
            self.db_distribution_queue.put({'type': 'stock_prices', 'data': stock_data_cache})
        if options_orders:
            self.db_distribution_queue.put({'type': 'options', 'data': options_orders})
        if futures_orders:
            self.db_distribution_queue.put({'type': 'futures', 'data': futures_orders})

        # Update performance metrics
        self.processed_count += len(combined_feeds)
        if time.time() - self.last_processed_time >= 10:  # Log every 10 seconds
            print(f"Processing rate: {self.processed_count / 10:.1f} instruments/sec")
            self.last_processed_time = time.time()
            self.processed_count = 0

    async def get_market_data_feed_authorize(self, token=None):
        """Get authorization for market data feed."""
        if not token:
            # If no token provided, use the first available token
            if self.access_tokens:
                token = self.access_tokens[0]
            else:
                print("Warning: No access tokens available")
                return None
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'

        try:
            # Use a faster executor with a reduced timeout
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(url=url, headers=headers, timeout=4)  # Reduced from 10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Authorization failed with token ...{token[-4:]}: {e}")
            raise

    def decode_protobuf(self, buffer):
        """Decode protobuf message."""
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response

    async def get_instrument_details_cached_async(self, instrument_keys):
        """Get instrument details from cache (async)."""
        result = {}

        # Use the pre-populated cache, no need to hit the database
        for key in instrument_keys:
            if key in self.instrument_cache:
                result[key] = self.instrument_cache[key]

        # If somehow keys are missing from cache (shouldn't happen, but just in case)
        missing_keys = [key for key in instrument_keys if key not in self.instrument_cache]
        if missing_keys:
            print(f"Warning: {len(missing_keys)} instrument keys missing from cache")
            missing_instruments = await self.db.get_instruments_by_keys_async(missing_keys)
            for inst in missing_instruments:
                self.instrument_cache[inst['instrument_key']] = inst
                result[inst['instrument_key']] = inst

        return result

    async def process_instrument_data(self, instrument_key, feed_data, current_time, instruments_dict):
        """Process market data for a single instrument."""
        try:
            instrument = instruments_dict.get(instrument_key)
            if not instrument:
                return None, None, None, None

            # Handle firstLevelWithGreeks format
            if 'firstLevelWithGreeks' not in feed_data:
                return None, None, None, None

            feed_struct = feed_data['firstLevelWithGreeks']
            ltpc = feed_struct.get('ltpc', {})
            ltp = ltpc.get('ltp')
            oi = feed_struct.get('oi')
            volume = feed_struct.get('vtt')
            greeks = feed_struct.get('optionGreeks', {})
            first_depth = feed_struct.get('firstDepth', {})
            iv = feed_struct.get('iv')

            prev_close = instrument.get('prev_close')
            price_change = (ltp - prev_close) if ltp and prev_close else 0
            pct_change = ((ltp - prev_close) / prev_close * 100) if ltp and prev_close else 0

            oi_record = None
            stock_data = None
            option_order = None
            future_order = None

            # Process based on instrument type
            if instrument['instrument_type'] == 'FO':
                lot_size = instrument.get('lot_size', 1)

                if instrument['option_type'] in ['CE', 'PE']:  # Option
                    bid_qty = first_depth.get('bidQ', 0)
                    ask_qty = first_depth.get('askQ', 0)

                    try:
                        bid_qty = int(bid_qty) if bid_qty else 0
                        ask_qty = int(ask_qty) if ask_qty else 0
                    except (ValueError, TypeError):
                        bid_qty, ask_qty = 0, 0

                    # Only process if price is valid and quantities meet threshold
                    if ltp and ltp >= 1 and (bid_qty > 0 or ask_qty > 0):
                        threshold = self.OPTIONS_THRESHOLD * lot_size
                        if bid_qty >= threshold or ask_qty >= threshold:
                            option_order = {
                                'stock': instrument['symbol'],
                                'strike_price': instrument['strike_price'],
                                'type': instrument['option_type'],
                                'ltp': ltp,
                                'bid_qty': bid_qty,
                                'ask_qty': ask_qty,
                                'lot_size': lot_size,
                                'timestamp': current_time,
                                'oi': oi,
                                'volume': volume,
                                'vega': greeks.get('vega'),
                                'theta': greeks.get('theta'),
                                'gamma': greeks.get('gamma'),
                                'delta': greeks.get('delta'),
                                'iv': iv,
                                'pop': greeks.get('pop', 0)
                            }

                        # Always record OI data for options
                        if oi is not None and volume is not None:
                            oi_record = {
                                'symbol': instrument['symbol'],
                                'expiry': instrument['expiry_date'],
                                'strike': instrument['strike_price'],
                                'option_type': instrument['option_type'],
                                'oi': oi,
                                'volume': volume,
                                'price': ltp,
                                'timestamp': current_time.strftime("%H:%M"),
                                'pct_change': pct_change,
                                'vega': greeks.get('vega'),
                                'theta': greeks.get('theta'),
                                'gamma': greeks.get('gamma'),
                                'delta': greeks.get('delta'),
                                'iv': iv,
                                'pop': greeks.get('pop', 0)
                            }

                elif instrument['option_type'] == 'FU':  # Future
                    bid_qty = first_depth.get('bidQ', 0)
                    ask_qty = first_depth.get('askQ', 0)

                    try:
                        bid_qty = int(bid_qty) if bid_qty else 0
                        ask_qty = int(ask_qty) if ask_qty else 0
                    except (ValueError, TypeError):
                        bid_qty, ask_qty = 0, 0

                    threshold = self.FUTURES_THRESHOLD * lot_size
                    if bid_qty >= threshold or ask_qty >= threshold:
                        future_order = {
                            'stock': instrument['symbol'],
                            'ltp': ltp,
                            'bid_qty': bid_qty,
                            'ask_qty': ask_qty,
                            'lot_size': lot_size,
                            'timestamp': current_time
                        }

                    # OI data for futures
                    if oi is not None and volume is not None:
                        oi_record = {
                            'symbol': instrument['symbol'],
                            'expiry': instrument['expiry_date'],
                            'strike': 0,
                            'option_type': 'FU',
                            'oi': oi,
                            'volume': volume,
                            'price': ltp,
                            'timestamp': current_time,
                            'pct_change': pct_change,
                            'vega': None,
                            'theta': None,
                            'gamma': None,
                            'delta': None,
                            'iv': None,
                            'pop': None
                        }

            elif instrument['instrument_type'] in ['EQUITY', 'INDEX']:
                symbol = f"{instrument['symbol']}.NS" if not instrument['symbol'].endswith('.NS') else instrument['symbol']
                stock_data = {
                    'symbol': symbol,
                    'close': ltp,
                    'price_change': price_change,
                    'percent_change': pct_change,
                    'timestamp': current_time,
                }

            return oi_record, stock_data, option_order, future_order

        except Exception as e:
            print(f"Error processing instrument {instrument_key}: {e}")
            return None, None, None, None

    async def send_subscription(self, websocket, key_batch, connection_id, refresh=False):
        """Send subscription message to websocket."""
        # Split large batches to reduce subscription time
        max_batch_size = 300  # Process in smaller batches of 300 keys
        sub_batches = [key_batch[i:i+max_batch_size] for i in range(0, len(key_batch), max_batch_size)]

        if refresh:
            try:
                for sub_batch in sub_batches:
                    unsubscribe_msg = {
                        "guid": str(uuid.uuid4()),
                        "method": "unsub",
                        "data": {
                            "mode": "option_greeks",
                            "instrumentKeys": sub_batch
                        }
                    }
                    await websocket.send(json.dumps(unsubscribe_msg).encode('utf-8'))
                    await asyncio.sleep(0.05)  # Reduced from 0.1
            except Exception as e:
                print(f"Error during unsubscribe for connection {connection_id}: {e}")

        # Subscribe in smaller batches
        for i, sub_batch in enumerate(sub_batches):
            subscribe_msg = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {
                    "mode": "option_greeks",
                    "instrumentKeys": sub_batch
                }
            }
            await websocket.send(json.dumps(subscribe_msg).encode('utf-8'))
            print(f"Connection {connection_id}: Subscribed to batch {i+1}/{len(sub_batches)} ({len(sub_batch)} instruments)")
            await asyncio.sleep(0.05)  # Small delay between batches

async def main():
    db_service = DatabaseService()
    feed_worker = UpstoxFeedWorker(db_service)

    try:
        await feed_worker.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await feed_worker.stop()

if __name__ == "__main__":
    asyncio.run(main())

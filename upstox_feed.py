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
        self.access_token = Config.ACCESS_TOKEN
        self.access_token2 = Config.ACCESS_TOKEN2
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.running = False

        # Connection settings - Modified for 1 connection per token
        self.MAX_CONNECTIONS = 2  # Total connections across all tokens
        self.MAX_CONNECTIONS_PER_TOKEN = 1  # Maximum 1 connection per token
        self.MAX_KEYS_PER_CONNECTION = 2800
        self.RECONNECT_DELAY = 1  # seconds

        # Keep track of connections per token
        self.token_connections = {
            self.access_token: 0,
            self.access_token2: 0
        }

        # Processing thresholds
        self.OPTIONS_THRESHOLD = 87
        self.FUTURES_THRESHOLD = 36

        # Parallel processing settings
        self.MAX_WORKERS = 20
        self.CHUNK_SIZE = 3000  # Instruments per processing chunk

        # DB worker pool settings
        self.DB_WORKERS = min(8, multiprocessing.cpu_count())  # Number of database worker threads
        self.DB_CHUNK_SIZE = 100  # Number of records per database batch
        self.db_workers = []
        self.db_task_queues = []
        self.db_workers_running = False

        # Data processing pipeline
        self.data_queue = asyncio.Queue(maxsize=10000)
        self.processing_task = None
        self.connection_tasks = {}
        self.refresh_request_queues = {}

        # Database distribution queue
        self.db_distribution_queue = Queue(maxsize=10000)
        self.db_distributor_thread = None

        # Instrument cache and keys
        self.instrument_cache = {}
        self.cache_refresh_time = 0
        self.CACHE_TTL = 3600  # 1 hour cache
        self.instrument_keys = []  # Store instrument keys here
        self.key_batches = []  # Store pre-calculated batches here

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

    async def start(self):
        """Start the feed worker and processing pipeline."""
        self.running = True
        self.db_workers_running = True

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
        await self.run_feed()

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
                    self.db.save_oi_volume_batch_feed(data)
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
        for task in self.connection_tasks.values():
            task.cancel()

        print("Feed worker stopped")

    async def fetch_instrument_keys(self):
        """Fetch instrument keys once and prepare batches."""
        try:
            # Set a timeout for fetching keys to prevent long delays
            instruments = await asyncio.wait_for(
                self.db.get_instrument_keys_async(limit=6000),
                timeout=10  # 10 second timeout for database query
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

            # Calculate how many keys we can handle with our available tokens
            max_total_keys = self.MAX_CONNECTIONS * self.MAX_KEYS_PER_CONNECTION
            if len(self.instrument_keys) > max_total_keys:
                print(f"Limiting instruments from {len(self.instrument_keys)} to {max_total_keys}")
                self.instrument_keys = self.instrument_keys[:max_total_keys]

            # Distribute keys based on available tokens
            available_tokens = [self.access_token, self.access_token2]
            valid_tokens = [token for token in available_tokens if token]
            tokens_count = len(valid_tokens)

            if tokens_count == 0:
                print("No valid tokens available!")
                return False

            print(f"Distributing keys across {tokens_count} available tokens")

            # Evenly split keys based on the number of available tokens
            keys_per_token = len(self.instrument_keys) // tokens_count
            remainder = len(self.instrument_keys) % tokens_count

            # Create batches based on token distribution
            self.key_batches = []
            start_idx = 0

            for i, token in enumerate(valid_tokens):
                # Add an extra key for the first 'remainder' tokens
                token_keys_count = keys_per_token + (1 if i < remainder else 0)
                if token_keys_count > 0:
                    end_idx = start_idx + token_keys_count
                    token_keys = self.instrument_keys[start_idx:end_idx]
                    # One batch per token with maximum of MAX_KEYS_PER_CONNECTION keys
                    self.key_batches.append(token_keys[:self.MAX_KEYS_PER_CONNECTION])
                    start_idx = end_idx

            print(f"Prepared {len(self.key_batches)} connection batches with sizes: {[len(b) for b in self.key_batches]}")
            return True

        except asyncio.TimeoutError:
            print("Timeout while fetching instrument keys, using default batches")
            self.key_batches = []
            return False
        except Exception as e:
            print(f"Error fetching instrument keys: {e}")
            self.key_batches = []
            return False

    async def run_feed(self):
        """Main feed running loop with connection management."""
        while self.running:
            try:
                print("Starting feed connections with cached instrument keys")

                # Reset token connection counts at the start
                for token in self.token_connections:
                    self.token_connections[token] = 0

                # If we don't have any instrument keys yet, continue with empty list
                if not self.key_batches:
                    print("No instrument keys available, retrying fetch")
                    success = await asyncio.wait_for(
                        self.fetch_instrument_keys(),
                        timeout=5
                    )
                    if not success:
                        await asyncio.sleep(0.5)
                        continue

                # Create tasks for establishing connections
                connection_tasks = []
                connection_futures = []

                # Prepare all connection tasks in advance
                available_tokens = [self.access_token, self.access_token2]
                valid_tokens = [token for token in available_tokens if token]

                for batch_idx, key_batch in enumerate(self.key_batches):
                    if batch_idx not in self.refresh_request_queues:
                        self.refresh_request_queues[batch_idx] = asyncio.Queue(maxsize=1)

                    if batch_idx in self.connection_tasks and not self.connection_tasks[batch_idx].done():
                        continue  # Existing connection is still active

                    # Select token for this batch - ensure not exceeding MAX_CONNECTIONS_PER_TOKEN
                    token = valid_tokens[batch_idx % len(valid_tokens)] if batch_idx < len(valid_tokens) else None

                    if not token:
                        print(f"No token available for batch {batch_idx}")
                        continue

                    # Check if we've reached max connections for this token
                    if self.token_connections[token] >= self.MAX_CONNECTIONS_PER_TOKEN:
                        print(f"Max connections ({self.MAX_CONNECTIONS_PER_TOKEN}) reached for token ending with ...{token[-4:]}")
                        continue

                    # Increment the connection count for this token
                    self.token_connections[token] += 1

                    print(f"Creating connection {batch_idx} with token ending ...{token[-4:]} (count: {self.token_connections[token]})")

                    # Use shorter timeouts for connection authorization
                    task = asyncio.create_task(
                        self.persistent_connection_manager(key_batch, batch_idx, token)
                    )
                    self.connection_tasks[batch_idx] = task
                    connection_tasks.append(task)
                    connection_futures.append(asyncio.create_task(
                        asyncio.wait_for(
                            asyncio.shield(task),
                            timeout=30  # 30 second timeout per connection
                        )
                    ))

                # If no connections need to be established, just wait a bit
                if not connection_tasks:
                    await asyncio.sleep(0.5)
                    continue

                # Wait for connections with timeout
                try:
                    done, pending = await asyncio.wait(
                        connection_futures,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=1  # Check status every 1 second
                    )

                    # Handle any completed connections (likely due to errors)
                    for future in done:
                        try:
                            await future
                        except asyncio.TimeoutError:
                            print("Connection timed out, will retry")
                        except Exception as e:
                            print(f"Connection error: {e}")

                    # Continue monitoring remaining connections
                    if pending:
                        continue

                except Exception as e:
                    print(f"Error waiting for connections: {e}")

            except Exception as e:
                print(f"Main feed loop error: {e}")
                await asyncio.sleep(0.5)

    async def persistent_connection_manager(self, key_batch, batch_idx, token):
        """Manage a persistent connection with automatic reconnection."""
        retry_count = 0
        max_retries = 5

        while self.running and retry_count < max_retries:
            try:
                print(f"Connection {batch_idx}: Attempting to establish (attempt {retry_count + 1}) with token ...{token[-4:]}")

                # Use timeouts to prevent hanging
                await asyncio.wait_for(
                    self.manage_connection(key_batch, batch_idx, token),
                    timeout=25  # 25 second timeout
                )

                retry_count = 0  # Reset on successful connection
            except asyncio.TimeoutError:
                retry_count += 1
                delay = min(0.5 * retry_count, 1)  # Cap delay at 1s, start with 0.5s
                print(f"Connection {batch_idx}: Timed out (attempt {retry_count}). Retrying in {delay}s")
                await asyncio.sleep(delay)
            except Exception as e:
                retry_count += 1
                delay = min(0.5 * retry_count, 1)
                print(f"Connection {batch_idx}: Error in connection (attempt {retry_count}): {e}. Retrying in {delay}s")
                await asyncio.sleep(delay)

        # When we exit this loop, update the token connection count
        self.token_connections[token] -= 1
        print(f"Connection {batch_idx} with token ...{token[-4:]} terminated (remaining: {self.token_connections[token]})")

    async def manage_connection(self, key_batch, batch_idx, token):
        """Manage a single websocket connection."""
        # Set a timeout for authorization to prevent hanging
        auth_response = await asyncio.wait_for(
            self.get_market_data_feed_authorize(token),
            timeout=5  # 5 second timeout
        )

        if not auth_response.get('data', {}).get('authorized_redirect_uri'):
            print(f"Connection {batch_idx}: Authorization failed for token ...{token[-4:]}")
            raise ConnectionError("Failed to authorize feed")

        print(f"Connection {batch_idx}: Establishing WebSocket connection with token ...{token[-4:]}")

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
            print(f"Connection {batch_idx}: WebSocket connection timed out")
            raise

        print(f'Connection {batch_idx}: WebSocket connected, subscribing to {len(key_batch)} instruments')

        async with websocket:
            # Start subscription immediately to reduce wait time
            subscription_task = asyncio.create_task(
                self._subscription_manager(websocket, key_batch, batch_idx)
            )

            # Start reader task after subscription is sent
            reader_task = asyncio.create_task(
                self._websocket_reader(websocket, key_batch, batch_idx)
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

    async def _websocket_reader(self, websocket, key_batch, batch_idx):
        """Dedicated task for reading from websocket."""
        try:
            # We don't need to create subscription_task here anymore
            # as it's handled in manage_connection

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
                        print(f"Connection {batch_idx}: Ping failed, reconnecting")
                        break
                except Exception as e:
                    print(f"Connection {batch_idx}: Reader error: {e}")
                    break
        except Exception as e:
            print(f"Connection {batch_idx}: Reader task error: {e}")

    async def _subscription_manager(self, websocket, key_batch, batch_idx):
        """Dedicated task for handling subscriptions."""
        last_subscription_time = time.time()
        subscription_interval = 30  # Refresh subscription every 30 seconds

        # Initial subscription with a timeout
        try:
            await asyncio.wait_for(
                self.send_subscription(websocket, key_batch),
                timeout=5  # 5 second timeout for subscription
            )
        except asyncio.TimeoutError:
            print(f"Connection {batch_idx}: Initial subscription timed out")
            return
        except Exception as e:
            print(f"Connection {batch_idx}: Initial subscription error: {e}")
            return

        try:
            while self.running:
                current_time = time.time()

                # Handle periodic subscription refresh
                if current_time - last_subscription_time > subscription_interval:
                    try:
                        await asyncio.wait_for(
                            self.send_subscription(websocket, key_batch, refresh=True),
                            timeout=5  # 5 second timeout for refresh
                        )
                        last_subscription_time = current_time
                    except Exception as e:
                        print(f"Connection {batch_idx}: Refresh subscription error: {e}")
                        break

                # Check for refresh requests (with non-blocking check)
                refresh_queue = self.refresh_request_queues.get(batch_idx)
                if refresh_queue and not refresh_queue.empty():
                    try:
                        await refresh_queue.get()
                        await asyncio.wait_for(
                            self.send_subscription(websocket, key_batch, refresh=True),
                            timeout=3
                        )
                        refresh_queue.task_done()
                        last_subscription_time = current_time
                    except Exception as e:
                        print(f"Connection {batch_idx}: Requested refresh error: {e}")

                await asyncio.sleep(0.5)  # Check more frequently
        except Exception as e:
            print(f"Connection {batch_idx}: Subscription manager error: {e}")

    async def _process_messages(self, websocket, key_batch, batch_idx):
        """Dedicated task for connection maintenance."""
        while self.running:
            try:
                # No need to check for refresh requests - subscription_manager does that now
                await asyncio.sleep(0.1)  # Very small wait to prevent CPU spinning
            except Exception as e:
                print(f"Connection {batch_idx}: Processor error: {e}")
                break

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
                                'timestamp': current_time
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

    async def send_subscription(self, websocket, key_batch, refresh=False):
        """Send subscription message to websocket."""
        # Split large batches to reduce subscription time
        max_batch_size = 1000
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
                print(f"Error during unsubscribe: {e}")

        # Subscribe in smaller batches
        for sub_batch in sub_batches:
            subscribe_msg = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {
                    "mode": "option_greeks",
                    "instrumentKeys": sub_batch
                }
            }
            await websocket.send(json.dumps(subscribe_msg).encode('utf-8'))
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

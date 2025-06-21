#!/usr/bin/env python3
import asyncio
import os
import sys
import signal
import logging
import time
import multiprocessing
import traceback
from datetime import datetime, timedelta
import threading
import atexit

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.strategy_tracker import run_strategy_tracker
from services.database import DatabaseService

# Configure logging - only console output, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only log to console, no file logging
    ]
)
logger = logging.getLogger("strategy_background")

# Global variables for shutdown coordination
shutdown_event = multiprocessing.Event()
watchdog_event = threading.Event()
last_activity_time = time.time()

# Define maximum inactivity period before restart (10 minutes)
MAX_INACTIVITY_PERIOD = 600  # seconds

def update_activity_timestamp():
    """Update the last activity timestamp"""
    global last_activity_time
    last_activity_time = time.time()
    # Clear the watchdog event to indicate activity
    watchdog_event.clear()

async def start_strategy_tracker():
    """Start the strategy tracker in a separate process"""
    logger.info("Starting strategy tracker process")

    # Create an asyncio event loop for the strategy tracker
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Try to increase process priority (UNIX only)
        try:
            os.nice(-10)
        except Exception as e:
            logger.warning(f"Could not set process priority: {str(e)}")

        # Initialize database connection with dedicated settings for the strategy tracker
        db_service = DatabaseService()

        # Set up a periodic heartbeat to update activity timestamp
        heartbeat_task = asyncio.create_task(periodic_heartbeat())

        # Create a task for the strategy tracker
        tracker_task = asyncio.create_task(run_strategy_tracker())

        # Wait for any of the tasks to complete or for shutdown signal
        await asyncio.gather(
            wait_for_shutdown_signal(),
            tracker_task,
            heartbeat_task
        )
    except asyncio.CancelledError:
        logger.info("Strategy tracker tasks cancelled")
    except Exception as e:
        logger.error(f"Error in strategy tracker: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Clean up
        loop.close()
        logger.info("Strategy tracker process stopped")

async def periodic_heartbeat():
    """Send periodic heartbeats to indicate the process is alive"""
    try:
        while True:
            # Update activity timestamp
            update_activity_timestamp()

            # Sleep for a short interval - reduced from 5 seconds to 2
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("Heartbeat task cancelled")
    except Exception as e:
        logger.error(f"Error in heartbeat task: {str(e)}")

async def wait_for_shutdown_signal():
    """Wait for the shutdown event to be set"""
    while not shutdown_event.is_set():
        await asyncio.sleep(0.5)
    logger.info("Shutdown signal received in tracker process")
    raise asyncio.CancelledError("Shutdown requested")

def strategy_process_main():
    """Main function for the strategy process"""
    # Set process name for better identification
    try:
        import setproctitle
        setproctitle.setproctitle("strategy-worker")
    except ImportError:
        pass

    # Configure specific logging for strategy process - only console output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Only log to console
        ],
        force=True  # Force reconfiguration of the logger
    )
    strategy_logger = logging.getLogger("strategy_process")
    strategy_logger.info("Strategy process starting")

    # Define signal handler for strategy process
    def strategy_signal_handler(sig, frame):
        strategy_logger.info(f"Strategy process received signal {sig}, shutting down...")
        # The process will exit automatically when asyncio event loop is stopped
        # Just need to notify the main process
        shutdown_event.set()

    # Set up signal handlers
    signal.signal(signal.SIGINT, strategy_signal_handler)
    signal.signal(signal.SIGTERM, strategy_signal_handler)

    # Set up exception hook to catch unhandled exceptions
    def exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Call original handler for KeyboardInterrupt
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        strategy_logger.critical("Unhandled exception in strategy process:",
                               exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = exception_handler

    # Start the strategy tracker
    try:
        # Update activity timestamp before starting
        update_activity_timestamp()

        # Run the strategy tracker
        asyncio.run(start_strategy_tracker())
    except Exception as e:
        strategy_logger.error(f"Fatal error in strategy process: {str(e)}")
        strategy_logger.error(traceback.format_exc())
        # Raise the exception after logging to trigger process restart
        raise

def watchdog_monitor():
    """Monitor thread to check for strategy process activity"""
    logger.info("Started watchdog monitor thread")

    while not shutdown_event.is_set():
        # Wait for inactivity event or timeout
        inactivity_detected = watchdog_event.wait(timeout=30)

        if shutdown_event.is_set():
            logger.info("Shutdown event detected in watchdog, exiting")
            break

        # Check if the process has been inactive for too long
        if inactivity_detected:
            logger.warning("Watchdog detected inactivity in strategy process")
            # Signal the main thread to restart the process
            if 'strategy_process' in globals() and strategy_process.is_alive():
                logger.warning("Watchdog is requesting strategy process restart")
                restart_strategy_process()

            # Clear the event for next cycle
            watchdog_event.clear()

        # Check for inactivity based on timestamp
        current_time = time.time()
        if current_time - last_activity_time > MAX_INACTIVITY_PERIOD:
            logger.warning(f"No activity detected for {MAX_INACTIVITY_PERIOD} seconds")
            watchdog_event.set()

    logger.info("Watchdog monitor thread exiting")

def restart_strategy_process():
    """Restart the strategy process"""
    global strategy_process

    if 'strategy_process' in globals() and strategy_process.is_alive():
        logger.warning("Terminating unresponsive strategy process")
        strategy_process.terminate()

        # Wait for process to terminate
        strategy_process.join(5)

        # Force kill if it's still alive
        if strategy_process.is_alive():
            logger.warning("Forcing kill of strategy process")
            try:
                os.kill(strategy_process.pid, signal.SIGKILL)
            except:
                pass

    # Create and start a new process
    logger.info("Creating new strategy process")
    strategy_process = multiprocessing.Process(target=strategy_process_main, name="Strategy-Tracker")
    strategy_process.daemon = False
    strategy_process.start()
    logger.info(f"Restarted strategy tracker process (PID: {strategy_process.pid})")

    # Update activity timestamp after restart
    update_activity_timestamp()

def cleanup_resources():
    """Clean up resources before exiting"""
    logger.info("Cleaning up resources")

    # Set shutdown event to signal all threads to exit
    shutdown_event.set()

    # Terminate strategy process if it exists and is running
    if 'strategy_process' in globals() and strategy_process.is_alive():
        logger.info("Terminating strategy process during cleanup")
        strategy_process.terminate()
        strategy_process.join(2)

        # Force kill if still alive
        if strategy_process.is_alive():
            try:
                os.kill(strategy_process.pid, signal.SIGKILL)
            except:
                pass

if __name__ == "__main__":
    # Register cleanup function to be called on exit
    atexit.register(cleanup_resources)

    # Install recommended packages if not already installed
    try:
        import setproctitle
    except ImportError:
        logger.info("Installing recommended package: setproctitle")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "setproctitle"], check=False)
        logger.info("Please restart the application to use the newly installed packages")

    # Initialize multiprocessing with spawn method for better compatibility and isolation
    multiprocessing.set_start_method('spawn', force=True)

    # Create the strategy process
    strategy_process = multiprocessing.Process(target=strategy_process_main, name="Strategy-Tracker")
    strategy_process.daemon = False

    # Start the strategy process
    strategy_process.start()
    logger.info(f"Started strategy tracker process (PID: {strategy_process.pid})")

    # Update activity timestamp after start
    update_activity_timestamp()

    # Start watchdog monitor thread
    watchdog_thread = threading.Thread(target=watchdog_monitor, daemon=True)
    watchdog_thread.start()

    # Define main process signal handler
    def main_signal_handler(sig, frame):
        logger.info(f"Main process received signal {sig}, shutting down...")
        shutdown_event.set()

        # Give process time to shut down gracefully
        logger.info("Waiting for process to terminate...")

        # Set a timeout for graceful shutdown
        timeout = 5  # seconds
        strategy_process.join(timeout)

        # Force terminate if still running
        if strategy_process.is_alive():
            logger.warning("Strategy process did not terminate gracefully, forcing...")
            strategy_process.terminate()

        logger.info("All processes terminated, exiting main process")
        sys.exit(0)

    # Set up signal handlers for main process
    signal.signal(signal.SIGINT, main_signal_handler)
    signal.signal(signal.SIGTERM, main_signal_handler)

    try:
        # Monitor shutdown event, process health, and handle restarts
        restart_count = 0
        max_restarts_per_day = 10
        restart_timestamps = []

        while not shutdown_event.is_set():
            # Check if process died unexpectedly
            if not strategy_process.is_alive():
                logger.error("Strategy tracker process died unexpectedly!")

                # Check if we've exceeded restart limits
                current_time = time.time()
                restart_timestamps = [ts for ts in restart_timestamps if current_time - ts < 86400]  # Keep only last 24h

                if len(restart_timestamps) < max_restarts_per_day:
                    logger.info(f"Restarting strategy process (restart {len(restart_timestamps) + 1}/{max_restarts_per_day} in 24h)")
                    restart_strategy_process()
                    restart_timestamps.append(current_time)
                else:
                    logger.critical(f"Exceeded maximum restarts ({max_restarts_per_day}) in 24 hours. Shutting down.")
                    shutdown_event.set()
                    break

            # Check for watchdog trigger
            if watchdog_event.is_set():
                logger.warning("Watchdog event triggered, checking strategy process")

                # Update the activity timestamp to avoid immediate re-trigger
                update_activity_timestamp()

                # Check restart limits same as above
                current_time = time.time()
                restart_timestamps = [ts for ts in restart_timestamps if current_time - ts < 86400]

                if len(restart_timestamps) < max_restarts_per_day:
                    logger.info(f"Watchdog initiating restart (restart {len(restart_timestamps) + 1}/{max_restarts_per_day} in 24h)")
                    restart_strategy_process()
                    restart_timestamps.append(current_time)
                else:
                    logger.critical(f"Exceeded maximum restarts ({max_restarts_per_day}) in 24 hours. Shutting down.")
                    shutdown_event.set()
                    break

            # Log process health periodically (every 5 minutes)
            current_minute = datetime.now().minute
            if current_minute % 5 == 0 and datetime.now().second < 2:
                logger.info(f"Strategy process health check: Running={strategy_process.is_alive()}, "
                           f"PID={strategy_process.pid}, Restarts={len(restart_timestamps)}/{max_restarts_per_day} in 24h")

            # Sleep to reduce CPU usage (shorter interval for more responsive shutdown)
            time.sleep(0.5)

        # Shutdown initiated, terminate process
        logger.info("Shutdown event detected, terminating process...")

        if strategy_process.is_alive():
            strategy_process.terminate()

        # Wait for process to terminate
        strategy_process.join()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in main process")
        # This will be handled by the signal handler
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Ensure the process is terminated
        if strategy_process.is_alive():
            strategy_process.terminate()
            strategy_process.join(1)
            # If still alive after join timeout, kill with SIGKILL
            if strategy_process.is_alive():
                logger.warning("Forcing Strategy process to exit with SIGKILL")
                try:
                    os.kill(strategy_process.pid, signal.SIGKILL)
                except:
                    pass

        logger.info("Main process exiting")
        sys.exit(0)

#!/usr/bin/env python3
"""
Script to delete options_orders records where ltp (last traded price) is less than a threshold
"""

import os
import sys
from contextlib import contextmanager
import psycopg2

from config import Config

# Set threshold price for deletion (default 9.0)
PRICE_THRESHOLD = 9.0

# Database connection parameters (using environment variables with defaults)
conn_params = {
    'dbname': os.getenv('DB_NAME', 'your_db_name'),
    'user': os.getenv('DB_USER', 'your_db_user'),
    'password': os.getenv('DB_PASSWORD', 'your_db_password'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

@contextmanager
def get_cursor():
    """Get a database cursor with automatic cleanup"""
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def delete_low_price_options():
    """Delete options_orders records where ltp is less than the threshold"""
    try:
        with get_cursor() as cur:
            # First, count how many records will be deleted
            cur.execute(
                "SELECT COUNT(*) FROM options_orders WHERE ltp < %s",
                (PRICE_THRESHOLD,)
            )
            count_result = cur.fetchone()
            count = count_result[0] if count_result else 0

            # Execute the delete operation
            cur.execute(
                "DELETE FROM options_orders WHERE ltp < %s RETURNING id, symbol, strike_price, option_type, ltp",
                (PRICE_THRESHOLD,)
            )

            deleted_records = cur.fetchall()
            deleted_count = len(deleted_records)

            print(f"Found {count} records with price below {PRICE_THRESHOLD}")
            print(f"Deleted {deleted_count} options_orders records where price < {PRICE_THRESHOLD}")

            # Print some details about the deleted records (limit to first 10)
            if deleted_records:
                print("\nSample of deleted records:")
                for i, record in enumerate(deleted_records[:10]):
                    print(f"  {i+1}. ID: {record[0]}, Symbol: {record[1]}, Strike: {record[2]}, Type: {record[3]}, Price: {record[4]}")

                if deleted_count > 10:
                    print(f"  ... and {deleted_count - 10} more records")

            return deleted_count
    except Exception as e:
        print(f"Error: {str(e)}")
        return 0


def export_closed_trades_watchlist(output_file_base="closed_trades_watchlist"):
    """
    Exports closed trades to a watchlist format, splitting into multiple files with max 50 items per file
    Format: symbol 28 OCT strike_price CALL/PUT
    Each instrument is separated by a comma
    """
    try:
        # Create the watchlists directory if it doesn't exist
        watchlist_dir = "watchlists"
        os.makedirs(watchlist_dir, exist_ok=True)

        with get_cursor() as cur:
            # Query closed trades (any status other than 'Open')
            cur.execute("""
                SELECT symbol, strike_price, option_type
                FROM options_orders
                WHERE status != 'Open' OR status IS NULL
                ORDER BY symbol, strike_price, option_type
            """)

            closed_trades = cur.fetchall()

            # Format each trade
            watchlist_items = []
            for trade in closed_trades:
                symbol, strike_price, option_type = trade

                # Convert option_type from CE/PE to CALL/PUT
                option_type_full = "CALL" if option_type == "CE" else "PUT"

                # Format strike price: remove .0 if it's a whole number
                formatted_strike = str(int(strike_price)) if strike_price % 1 == 0 else str(strike_price)

                # Format: symbol 28 OCT strike_price CALL/PUT
                formatted_trade = f"{symbol} 28 OCT {formatted_strike} {option_type_full}"
                watchlist_items.append(formatted_trade)

            # Split into chunks of 50 and save to multiple files
            max_items_per_file = 50
            total_files = (len(watchlist_items) + max_items_per_file - 1) // max_items_per_file  # Ceiling division

            files_created = []

            for i in range(total_files):
                start_idx = i * max_items_per_file
                end_idx = min((i + 1) * max_items_per_file, len(watchlist_items))
                chunk = watchlist_items[start_idx:end_idx]

                # Generate filename with index
                if total_files > 1:
                    output_filename = f"{output_file_base}_{i+1}.txt"
                else:
                    output_filename = f"{output_file_base}.txt"

                # Create full path with directory
                output_file = os.path.join(watchlist_dir, output_filename)

                watchlist_string = ", ".join(chunk)

                # Write to file
                with open(output_file, 'w') as f:
                    f.write(watchlist_string)

                files_created.append(output_file)

            print(f"Closed trades watchlist exported to {len(files_created)} file(s) in '{watchlist_dir}' directory")
            print(f"Total closed trades: {len(closed_trades)}")

            # Print details about the files
            for i, file_path in enumerate(files_created):
                start_idx = i * max_items_per_file
                end_idx = min((i + 1) * max_items_per_file, len(watchlist_items))
                print(f"  File {i+1}: {file_path} ({end_idx - start_idx} items)")

            # Print sample of exported trades from the first file
            if closed_trades:
                print("\nSample from first file:")
                sample_count = min(5, len(watchlist_items[:max_items_per_file]))
                for i in range(sample_count):
                    print(f"  {i+1}. {watchlist_items[i]}")

                if len(watchlist_items[:max_items_per_file]) > 5:
                    print(f"  ... and {len(watchlist_items[:max_items_per_file]) - 5} more trades in first file")

            return len(closed_trades)

    except Exception as e:
        print(f"Error exporting closed trades: {str(e)}")
        return 0

def export_open_trades_watchlist(output_file_base="open_trades_watchlist"):
    """
    Exports open trades to a watchlist format, splitting into multiple files with max 50 items per file
    Format: symbol 28 OCT strike_price CALL/PUT
    Each instrument is separated by a comma
    """
    try:
        # Create the watchlists directory if it doesn't exist
        watchlist_dir = "watchlists"
        os.makedirs(watchlist_dir, exist_ok=True)

        with get_cursor() as cur:
            # Query open trades
            cur.execute("""
                SELECT symbol, strike_price, option_type 
                FROM options_orders 
                WHERE status = 'Open' 
                ORDER BY symbol, strike_price, option_type
            """)

            open_trades = cur.fetchall()

            # Format each trade
            watchlist_items = []
            for trade in open_trades:
                symbol, strike_price, option_type = trade

                # Convert option_type from CE/PE to CALL/PUT
                option_type_full = "CALL" if option_type == "CE" else "PUT"

                # Format strike price: remove .0 if it's a whole number
                formatted_strike = str(int(strike_price)) if strike_price % 1 == 0 else str(strike_price)

                # Format: symbol 28 OCT strike_price CALL/PUT
                formatted_trade = f"{symbol} 28 OCT {formatted_strike} {option_type_full}"
                watchlist_items.append(formatted_trade)

            # Split into chunks of 50 and save to multiple files
            max_items_per_file = 50
            total_files = (len(watchlist_items) + max_items_per_file - 1) // max_items_per_file  # Ceiling division

            files_created = []

            for i in range(total_files):
                start_idx = i * max_items_per_file
                end_idx = min((i + 1) * max_items_per_file, len(watchlist_items))
                chunk = watchlist_items[start_idx:end_idx]

                # Generate filename with index
                if total_files > 1:
                    output_filename = f"{output_file_base}_{i+1}.txt"
                else:
                    output_filename = f"{output_file_base}.txt"

                # Create full path with directory
                output_file = os.path.join(watchlist_dir, output_filename)

                watchlist_string = ", ".join(chunk)

                # Write to file
                with open(output_file, 'w') as f:
                    f.write(watchlist_string)

                files_created.append(output_file)

            print(f"Open trades watchlist exported to {len(files_created)} file(s) in '{watchlist_dir}' directory")
            print(f"Total open trades: {len(open_trades)}")

            # Print details about the files
            for i, file_path in enumerate(files_created):
                start_idx = i * max_items_per_file
                end_idx = min((i + 1) * max_items_per_file, len(watchlist_items))
                print(f"  File {i+1}: {file_path} ({end_idx - start_idx} items)")

            # Print sample of exported trades from the first file
            if open_trades:
                print("\nSample from first file:")
                sample_count = min(5, len(watchlist_items[:max_items_per_file]))
                for i in range(sample_count):
                    print(f"  {i+1}. {watchlist_items[i]}")

                if len(watchlist_items[:max_items_per_file]) > 5:
                    print(f"  ... and {len(watchlist_items[:max_items_per_file]) - 5} more trades in first file")

            return len(open_trades)

    except Exception as e:
        print(f"Error exporting open trades: {str(e)}")
        return 0


if __name__ == "__main__":
    print(f"Deleting options_orders records where ltp < {PRICE_THRESHOLD}")
    export_count = export_open_trades_watchlist()
    print(f"\nExport completed. Total open trades exported: {export_count}")

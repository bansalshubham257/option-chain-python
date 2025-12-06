#!/usr/bin/env python3
"""
Script to delete options_orders records where ltp (last traded price) is less than a threshold
"""

import os
import sys
from contextlib import contextmanager
import psycopg2
from datetime import datetime

from config import Config

# Set threshold price for deletion (default 9.0)
PRICE_THRESHOLD = 9.95

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
                formatted_trade = f"{symbol} 25 NOV {formatted_strike} {option_type_full}"
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


def export_recent_open_trades_watchlist(output_file_base="recent_open_trades_watchlist"):
    """
    Exports open trades from the last 2 days to a watchlist format, splitting into multiple files with max 50 items per file
    Format: symbol 28 OCT strike_price CALL/PUT
    Each instrument is separated by a comma
    """
    try:
        # Create the watchlists directory if it doesn't exist
        watchlist_dir = "watchlists"
        os.makedirs(watchlist_dir, exist_ok=True)

        with get_cursor() as cur:
            cur.execute("""
                SELECT symbol, strike_price, option_type 
                FROM options_orders 
                WHERE status = 'Open' 
                  AND timestamp >= (NOW() - INTERVAL '1 days')
                ORDER BY symbol, strike_price, option_type
            """)

            open_trades = cur.fetchall()

            # Format each trade
            watchlist_items = []
            for trade in open_trades:
                symbol, strike_price, option_type = trade
                option_type_full = "CALL" if option_type == "CE" else "PUT"
                formatted_strike = str(int(strike_price)) if strike_price % 1 == 0 else str(strike_price)
                item = f"{symbol} 25 NOV {formatted_strike} {option_type_full}"
                watchlist_items.append(item)

            # Split into files of max 50 items each
            max_items_per_file = 50
            for i in range(0, len(watchlist_items), max_items_per_file):
                file_items = watchlist_items[i:i+max_items_per_file]
                file_index = i // max_items_per_file + 1
                output_file = os.path.join(watchlist_dir, f"{output_file_base}_{file_index}.txt")
                with open(output_file, "w") as f:
                    f.write(", ".join(file_items))

            print(f"Exported {len(watchlist_items)} recent open trades to watchlist files.")
            return len(watchlist_items)

    except Exception as e:
        print(f"Error exporting recent open trades: {str(e)}")
        return 0


def delete_all_options_orders():
    """Delete all records from options_orders table"""
    try:
        with get_cursor() as cur:
            # Count total records before deletion
            cur.execute("SELECT COUNT(*) FROM options_orders")
            count_result = cur.fetchone()
            count = count_result[0] if count_result else 0

            # Delete all records
            cur.execute("DELETE FROM options_orders RETURNING id, symbol, strike_price, option_type, ltp")
            deleted_records = cur.fetchall()
            deleted_count = len(deleted_records)

            print(f"Found {count} records in options_orders table")
            print(f"Deleted {deleted_count} options_orders records (all records)")

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


def export_filtered_call_orders(output_file_base="filtered_call_orders"):
    """
    Identifies and exports orders that satisfy specific conditions:
    - Must be Call options (CE)
    - Delta between 0.28 and 0.40
    - For buyer side (bid_qty > ask_qty): volume >= 300,000
    - For seller side (bid_qty <= ask_qty): volume >= 900,000
    - IV <= 0.27
    """
    try:
        # Create the output directory if it doesn't exist
        output_dir = "data_test"
        os.makedirs(output_dir, exist_ok=True)

        with get_cursor() as cur:
            # Query for orders matching all conditions
            cur.execute("""
                SELECT *
                FROM options_orders
                WHERE option_type = 'CE'
                AND delta BETWEEN 0.28 AND 0.40
                AND iv <= 0.27
                AND (
                    (bid_qty > ask_qty AND volume >= 300000) OR
                    (bid_qty <= ask_qty AND volume >= 900000)
                )
                ORDER BY symbol, strike_price;
            """)

            matching_orders = cur.fetchall()

            # Generate the output filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"{output_file_base}_{timestamp}.txt"
            output_file = os.path.join(output_dir, output_filename)

            # Write the results to the file
            with open(output_file, 'w') as f:
                f.write("=== Filtered Call Orders Report ===\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("Conditions applied:\n")
                f.write("- Call options only (CE)\n")
                f.write("- Delta: 0.28 - 0.40\n")
                f.write("- Volume conditions:\n")
                f.write("  * Buyer side (bid_qty > ask_qty): ≥300,000\n")
                f.write("  * Seller side (bid_qty <= ask_qty): ≥900,000\n")
                f.write("- IV: ≤0.27\n\n")

                if matching_orders:
                    # Get column names
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='options_orders' ORDER BY ordinal_position;")
                    columns = [col[0] for col in cur.fetchall()]

                    f.write(f"Total matching orders found: {len(matching_orders)}\n\n")
                    f.write("=== Matching Orders ===\n")

                    for order in matching_orders:
                        f.write("\n---\n")
                        order_dict = dict(zip(columns, order))

                        # Add derived side information
                        side = "Buyer" if order_dict['bid_qty'] > order_dict['ask_qty'] else "Seller"
                        f.write(f"Trading Side: {side}\n")

                        # Write all other order details
                        for col_name, value in order_dict.items():
                            f.write(f"{col_name}: {value}\n")
                else:
                    f.write("No orders found matching the specified criteria.\n")

            print(f"Found {len(matching_orders)} matching orders")
            print(f"Results exported to: {output_file}")
            return len(matching_orders)

    except Exception as e:
        print(f"Error exporting filtered call orders: {str(e)}")
        return 0


def find_qualified_trades_to_csv(output_file_base="qualified_trades"):
    """
    Finds trades that meet specific quality criteria and exports them to CSV.

    Rules:
    - Median bid quantity (bid_qty) is at least 18,013
    - Median ask quantity (ask_qty) is at least 15,350
    - Delta is at least 0.244
    - Lot size is at least 400
    - Open interest (oi) typically above 277,600
    - Last traded price (ltp) is at least 20.75
    - Implied volatility (iv) around 0.248 (interpreted as >= 0.248)
    """
    import csv

    try:
        # Create the output directory if it doesn't exist
        output_dir = "data_exports"
        os.makedirs(output_dir, exist_ok=True)

        with get_cursor() as cur:
            # Query for trades matching all criteria
            cur.execute("""
                SELECT 
                    id, symbol, strike_price, option_type, ltp, bid_qty, ask_qty,
                    lot_size, oi, volume, delta, iv, gamma, theta, vega, pop, timestamp
                FROM options_orders
                WHERE 
                    bid_qty >= 18013
                    AND ask_qty >= 15350
                    AND delta >= 0.244
                    AND lot_size >= 400
                    AND oi > 277600
                    AND ltp >= 20.75
                    AND iv >= 0.248
                ORDER BY symbol, strike_price, option_type;
            """)

            matching_trades = cur.fetchall()

            # Column names for the CSV
            columns = [
                'id', 'symbol', 'strike_price', 'option_type', 'ltp', 'bid_qty', 'ask_qty',
                'lot_size', 'oi', 'volume', 'delta', 'iv', 'gamma', 'theta', 'vega', 'pop', 'timestamp'
            ]

            # Generate the output filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"{output_file_base}_{timestamp}.csv"
            output_file = os.path.join(output_dir, output_filename)

            # Write the results to CSV
            with open(output_file, 'w', newline='') as f:
                csv_writer = csv.writer(f)

                # Write header
                csv_writer.writerow(columns)

                # Write data rows
                csv_writer.writerows(matching_trades)

            print(f"\n=== Qualified Trades Report ===")
            print(f"Found {len(matching_trades)} trades matching qualification criteria")
            print(f"Results exported to: {output_file}")

            # Display summary statistics
            if matching_trades:
                # Calculate statistics
                total_trades = len(matching_trades)
                unique_symbols = len(set(trade[1] for trade in matching_trades))
                avg_ltp = sum(float(trade[4]) for trade in matching_trades if trade[4]) / total_trades
                avg_delta = sum(float(trade[10]) for trade in matching_trades if trade[10]) / total_trades
                avg_oi = sum(float(trade[8]) for trade in matching_trades if trade[8]) / total_trades
                avg_iv = sum(float(trade[11]) for trade in matching_trades if trade[11]) / total_trades
                avg_bid_qty = sum(int(trade[5]) for trade in matching_trades if trade[5]) / total_trades
                avg_ask_qty = sum(int(trade[6]) for trade in matching_trades if trade[6]) / total_trades

                print(f"\n=== Summary Statistics ===")
                print(f"Total qualified trades: {total_trades}")
                print(f"Unique symbols: {unique_symbols}")
                print(f"Average LTP: {avg_ltp:.2f}")
                print(f"Average Delta: {avg_delta:.4f}")
                print(f"Average OI: {avg_oi:,.0f}")
                print(f"Average IV: {avg_iv:.4f}")
                print(f"Average Bid Qty: {avg_bid_qty:,.0f}")
                print(f"Average Ask Qty: {avg_ask_qty:,.0f}")

                # Display first 10 records
                print(f"\n=== Sample Records (First 10) ===")
                for i, trade in enumerate(matching_trades[:10]):
                    print(f"{i+1}. Symbol: {trade[1]}, Strike: {trade[2]}, Type: {trade[3]}, "
                          f"LTP: {trade[4]}, Delta: {trade[10]}, OI: {trade[8]:,.0f}, IV: {trade[11]}")

                if total_trades > 10:
                    print(f"... and {total_trades - 10} more trades")
            else:
                print("\nNo trades found matching the qualification criteria.")

            return len(matching_trades)

    except Exception as e:
        print(f"Error finding qualified trades: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


if __name__ == "__main__":
    print(f"Deleting options_orders records where ltp < {PRICE_THRESHOLD}")
    #delete_all_options_orders()
    #export_count = export_open_trades_watchlist()
    #print(f"\nExport completed. Total open trades exported: {export_count}")

    # Run the filtered call orders export
    #filtered_count = export_filtered_call_orders()
    #print(f"\nFiltered call orders export completed. Found {filtered_count} matching orders.")

    # Find and export qualified trades to CSV
    qualified_count = find_qualified_trades_to_csv()
    print(f"\nQualified trades export completed. Found {qualified_count} matching trades.")

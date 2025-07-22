import os
import requests
import json
import time
import psycopg2
from psycopg2.extras import execute_batch
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple
from services.database import DatabaseService

class OptionOIService:
    """
    Service to fetch option interest data for instrument keys from Upstox API
    """

    def __init__(self):
        self.db_service = DatabaseService()
        self.base_url = "https://api.upstox.com/v3/market-quote/option-greek"

    def get_access_token(self) -> str:
        """
        Get the Upstox access token from environment variables or another source
        """
        # You might want to implement a better token management strategy
        # This is a placeholder implementation
        token = os.getenv("UPSTOX_ACCESS_TOKEN")
        if not token:
            raise ValueError("UPSTOX_ACCESS_TOKEN environment variable not set")
        return token

    def fetch_instrument_keys_from_db(self) -> List[str]:
        """
        Fetch instrument keys from the database
        """
        with self.db_service._get_cursor() as cursor:
            cursor.execute("SELECT instrument_key FROM instrument_keys")
            results = cursor.fetchall()
            return [row[0] for row in results]

    def process_instrument_keys_in_batches(self, keys: List[str], batch_size: int = 50) -> List[Dict[str, Any]]:
        """
        Process instrument keys in batches to avoid exceeding API limits
        Uses parallel processing for better performance
        """
        all_results = []
        total_keys = len(keys)

        print(f"Processing {total_keys} instrument keys in batches of {batch_size}")
        start_time = time.time()

        # Split keys into batches
        batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]

        # Use ThreadPoolExecutor for parallel API requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all batch jobs to the executor
            future_to_batch = {executor.submit(self.fetch_option_oi_data, batch): i for i, batch in enumerate(batches)}

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_index = future_to_batch[future]
                try:
                    batch_results = future.result()
                    if batch_results:
                        all_results.extend(batch_results)
                        print(f"Completed batch {batch_index+1}/{len(batches)} ({len(batch_results)} results)")
                except Exception as e:
                    print(f"Error processing batch {batch_index+1}: {str(e)}")

        elapsed_time = time.time() - start_time
        print(f"Processed all batches in {elapsed_time:.2f} seconds, found {len(all_results)} records")

        return all_results

    def fetch_option_oi_data(self, instrument_keys: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch option interest data for a list of instrument keys

        Args:
            instrument_keys: List of instrument keys to fetch data for

        Returns:
            List of dictionaries containing option OI data or None if request failed
        """
        if not instrument_keys:
            return None

        # Format the instrument_keys for the API request
        formatted_keys = ",".join(instrument_keys)

        try:
            token = self.get_access_token()
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {token}"
            }

            response = requests.get(
                f"{self.base_url}?instrument_key={formatted_keys}",
                headers=headers
            )

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                return self.transform_oi_data(data)
            else:
                print(f"Error fetching OI data: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"Exception occurred while fetching OI data: {str(e)}")
            return None

    def transform_oi_data(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform the raw API response to extract relevant OI data

        Args:
            response_data: Raw API response

        Returns:
            List of dictionaries with processed OI data
        """
        result = []

        # Check if response has the expected structure
        if not response_data.get("status") == "success":
            return result

        if not response_data.get("data"):
            return result

        for key, data in response_data['data'].items():
            option_type = self._get_option_type(key)
            symbol, strike = self._extract_symbol_and_strike(key)

            oi_value = data.get("oi", 0)

            result.append({
                "instrument_key": key,
                "symbol": symbol,
                "strike_price": strike,
                "option_type": option_type,
                "oi_value": oi_value,
                "last_price": data.get("last_price"),
                "instrument_token": data.get("instrument_token"),
                "volume": data.get("volume"),
                "iv": data.get("iv"),
                "delta": data.get("delta")
            })

        return result

    def _extract_symbol_and_strike(self, instrument_key: str) -> Tuple[str, float]:
        """
        Extract symbol and strike price from the instrument key
        Example: NSE_FO:TITAN25JUL3650PE -> ("TITAN", 3650.0)
        """
        try:
            # Splitting by colon to get the actual instrument part
            parts = instrument_key.split(':')
            if len(parts) > 1:
                key_part = parts[1]
            else:
                key_part = parts[0]

            # Remove PE or CE suffix for processing
            if key_part.endswith("PE") or key_part.endswith("CE"):
                key_part = key_part[:-2]

            # Find the last numeric sequence in the string which should be the strike price
            strike_str = ""
            numeric_found = False

            for i in range(len(key_part) - 1, -1, -1):
                if key_part[i].isdigit():
                    strike_str = key_part[i] + strike_str
                    numeric_found = True
                elif numeric_found:
                    # If we've started collecting digits and hit a non-digit, we're done
                    break

            # Convert strike price to float
            strike = float(strike_str) if strike_str else 0.0

            # Extract symbol - everything before any numeric characters or month codes
            symbol = ""
            month_codes = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

            # Find where the date/month part or numbers start
            for i in range(len(key_part)):
                if key_part[i].isdigit() or any(key_part[i:i+3] == month for month in month_codes):
                    symbol = key_part[:i]
                    break

            return symbol, strike
        except Exception as e:
            # For debugging, you might want to log the error
            return "UNKNOWN", 0.0

    def _get_option_type(self, instrument_key: str) -> str:
        """
        Determine the option type (CE/PE) from the instrument key
        """
        # Check for PE or CE in the instrument key
        if "PE" in instrument_key:
            return "PE"
        elif "CE" in instrument_key:
            return "CE"
        else:
            # If option type cannot be determined, check from the database
            with self.db_service._get_cursor() as cursor:
                cursor.execute(
                    "SELECT option_type FROM instrument_keys WHERE instrument_key = %s",
                    (instrument_key,)
                )
                result = cursor.fetchone()
                return result[0] if result else "UNKNOWN"

    def calculate_pcr(self, oi_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Put-Call Ratio for each symbol and strike price

        Args:
            oi_data: List of option OI data from transform_oi_data

        Returns:
            List of dictionaries with PCR calculations for each symbol and strike
        """
        # Group OI data by symbol and strike price
        grouped_data = {}
        pcr_updates = []  # Store PCR updates for batch processing

        for item in oi_data:
            symbol = item["symbol"]
            strike = item["strike_price"]
            option_type = item["option_type"]
            oi = item["oi_value"] or 0

            key = f"{symbol}_{strike}"
            if key not in grouped_data:
                grouped_data[key] = {
                    "symbol": symbol,
                    "strike": strike,
                    "CE": 0,
                    "PE": 0
                }

            grouped_data[key][option_type] = oi

        # Calculate PCR for each group
        pcr_results = []
        for key, data in grouped_data.items():
            call_oi = data["CE"]
            put_oi = data["PE"]

            # Calculate PCR, handle division by zero
            pcr = 0
            if call_oi > 0:
                pcr = put_oi / call_oi

            pcr_value = round(pcr, 2)
            pcr_results.append({
                "symbol": data["symbol"],
                "strike": data["strike"],
                "call_oi": call_oi,
                "put_oi": put_oi,
                "pcr": pcr_value
            })

            # Add to batch update list instead of updating individually
            pcr_updates.append((pcr_value, data["symbol"], data["strike"]))

        # Perform batch update if we have any PCR values to update
        if pcr_updates:
            self.batch_update_pcr_in_database(pcr_updates)

        return pcr_results

    def batch_update_pcr_in_database(self, pcr_updates: List[Tuple[float, str, float]]) -> None:
        """
        Batch update the PCR values in the options_orders table for multiple symbols and strike prices

        Args:
            pcr_updates: List of tuples containing (pcr, symbol, strike_price) for each update
        """
        if not pcr_updates:
            return

        try:
            # Use a single connection for all updates
            conn = psycopg2.connect(**self.db_service.conn_params)
            conn.autocommit = False  # Use explicit transaction

            with conn.cursor() as cursor:
                # Start a transaction
                cursor.execute("BEGIN")

                # Execute batch update using execute_batch for better performance
                execute_batch(
                    cursor,
                    """
                    UPDATE options_orders
                    SET pcr = %s
                    WHERE symbol = %s AND strike_price = %s
                    """,
                    pcr_updates,
                    page_size=1000  # Process 1000 records at a time
                )

                # Commit the transaction
                conn.commit()

                # Check how many rows were updated
                updated_rows = cursor.rowcount
                if updated_rows > 0:
                    print(f"Batch updated PCR for {len(pcr_updates)} updates ({updated_rows} rows affected)")

        except Exception as e:
            if conn:
                conn.rollback()  # Roll back on error
            print(f"Error batch updating PCR in database: {str(e)}")
        finally:
            if conn:
                conn.close()  # Ensure connection is closed


# Function to create and return the API service
def get_option_oi_service():
    return OptionOIService()

# Test function to demonstrate usage
def test_option_oi_api():
    print("Starting OI API test...")

    # Set access token for testing
    os.environ["UPSTOX_ACCESS_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyWEJSUFMiLCJqdGkiOiI2ODdkNzYxMWZhYzMxZTA2NDhhYzUzMzciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1MzA1MjY4OSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzUzMTM1MjAwfQ.2Td1eTxB7KVsXBWmehXit5ucMqgqVmVij03Rz-cIotA"

    service = get_option_oi_service()

    try:
        # Fetch instrument keys from the database
        keys = service.fetch_instrument_keys_from_db()

        # Process keys and get OI data
        oi_data = service.process_instrument_keys_in_batches(keys)

        if oi_data:
            # Calculate PCR for each symbol and strike
            pcr_results = service.calculate_pcr(oi_data)

            print(f"\nSymbol, Strike, Call OI, Put OI, PCR:")
            for result in pcr_results:
                print(f"{result['symbol']}, {result['strike']}, {result['call_oi']}, {result['put_oi']}, {result['pcr']}")
        else:
            print("No OI data retrieved.")

    except Exception as e:
        print(f"Error in test: {str(e)}")

def run_continuous_processing(interval_seconds=300):
    """
    Run the option chain data processing continuously in an infinite loop

    Args:
        interval_seconds: Time to wait between processing cycles (default: 5 minutes)
    """
    print(f"Starting continuous processing with {interval_seconds} seconds interval")
    service = get_option_oi_service()

    cycle_count = 0

    try:
        while True:
            cycle_count += 1
            cycle_start_time = time.time()
            print(f"\n{'='*80}")
            print(f"Starting processing cycle #{cycle_count} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}")

            try:
                # Fetch instrument keys from the database
                keys = service.fetch_instrument_keys_from_db()

                if not keys:
                    print("No instrument keys found in database. Waiting for next cycle.")
                    time.sleep(interval_seconds)
                    continue

                # Process keys and get OI data
                oi_data = service.process_instrument_keys_in_batches(keys)

                if oi_data:
                    # Calculate PCR for each symbol and strike
                    pcr_results = service.calculate_pcr(oi_data)
                    print(f"Updated PCR values for {len(pcr_results)} symbol/strike combinations")
                else:
                    print("No OI data retrieved in this cycle.")

            except Exception as e:
                print(f"Error in processing cycle #{cycle_count}: {str(e)}")

            # Calculate time taken and wait for next cycle
            cycle_duration = time.time() - cycle_start_time
            print(f"Cycle #{cycle_count} completed in {cycle_duration:.2f} seconds")

            # Calculate sleep time (ensure we wait at least 1 second)
            sleep_time = max(1, interval_seconds - cycle_duration)
            next_cycle_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                          time.localtime(time.time() + sleep_time))

            print(f"Waiting {sleep_time:.2f} seconds for next cycle at {next_cycle_time}")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nContinuous processing stopped by user.")
    except Exception as e:
        print(f"\nContinuous processing stopped due to error: {str(e)}")


# Main execution if this file is run directly
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Option Chain PCR Calculator")
    parser.add_argument("--run-once", action="store_true",
                        help="Run the processing once and exit")
    parser.add_argument("--interval", type=int, default=300,
                        help="Interval between processing cycles in seconds (default: 300)")

    args = parser.parse_args()

    if args.run_once:
        print("Running single processing cycle...")
        test_option_oi_api()
    else:
        print(f"Starting continuous processing with {args.interval} seconds interval...")
        run_continuous_processing(args.interval)

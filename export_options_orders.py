#!/usr/bin/env python
# Script to export all options orders data from the database
import os
import csv
import json
import datetime
import logging
import decimal
from services.database import DatabaseService

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

def main():
    """Export all options_orders data to CSV and JSON files"""
    try:
        print("Starting options_orders data export...")

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # Initialize database service
        db_service = DatabaseService()

        # Create exports directory if it doesn't exist
        export_dir = 'data_exports'
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        # Generate timestamp for filenames
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"{export_dir}/options_orders_{timestamp}.csv"
        json_filename = f"{export_dir}/options_orders_{timestamp}.json"

        # Fetch all data from options_orders table
        with db_service._get_cursor() as cur:
            cur.execute("SELECT * FROM options_orders")
            records = cur.fetchall()

            # Get column names
            column_names = [desc[0] for desc in cur.description]

            # Convert to list of dicts for easier manipulation
            records_dicts = []
            for record in records:
                record_dict = {}
                for i, column in enumerate(column_names):
                    # Convert Decimal to float for JSON serialization
                    if isinstance(record[i], decimal.Decimal):
                        record_dict[column] = float(record[i])
                    # Handle datetime objects by converting to string
                    elif isinstance(record[i], (datetime.date, datetime.datetime)):
                        record_dict[column] = record[i].isoformat()
                    else:
                        record_dict[column] = record[i]
                records_dicts.append(record_dict)

        # Export to CSV
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            writer.writeheader()
            for record in records_dicts:
                writer.writerow(record)

        # Export to JSON using the custom encoder
        with open(json_filename, 'w') as jsonfile:
            json.dump(records_dicts, jsonfile, indent=2, cls=DecimalEncoder)

        print(f"Successfully exported {len(records_dicts)} records.")
        print(f"CSV data saved to: {csv_filename}")
        print(f"JSON data saved to: {json_filename}")

        return {
            'status': 'success',
            'record_count': len(records_dicts),
            'csv_path': csv_filename,
            'json_path': json_filename
        }

    except Exception as e:
        logging.error(f"Error exporting options_orders data: {str(e)}")
        print(f"Error exporting data: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

if __name__ == "__main__":
    main()

import logging
import pytz
from datetime import datetime, timedelta
import time
import re

class DataConsolidationService:
    """Service to consolidate historical data in the database"""
    
    def __init__(self, database_service):
        self.database_service = database_service
        self.logger = logging.getLogger(__name__)
    
    def consolidate_oi_volume_history(self, retention_minutes=15, batch_size=1000):
        """
        Consolidate 5-minute OI volume history data that's older than retention_minutes.
        
        Args:
            retention_minutes (int): Minutes of recent data to keep intact (default 15 minutes)
            batch_size (int): Number of records to process in each batch
        
        Returns:
            dict: Statistics about the consolidation process
        """
        try:
            stats = {"start_time": datetime.now(pytz.timezone('Asia/Kolkata'))}
            
            # Calculate the cutoff time as a string in HH:MM format
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            cutoff_time = now - timedelta(minutes=retention_minutes)
            cutoff_time_str = cutoff_time.strftime('%H:%M')
            current_date_str = now.strftime('%Y-%m-%d')
            
            self.logger.info(f"Consolidating OI volume data older than {cutoff_time_str} for date {current_date_str}")
            
            # Use a more direct SQL approach with window functions to improve performance
            with self.database_service._get_cursor() as cur:
                # Step 1: Create a temporary table with grouped data
                # This directly groups all data by 5-minute intervals
                cur.execute("""
                    CREATE TEMPORARY TABLE temp_consolidated_data AS
                    WITH time_groups AS (
                        SELECT 
                            symbol,
                            expiry_date,
                            strike_price,
                            option_type,
                            -- Create a 5-minute interval group key using a different approach
                            -- Extract hour and minute parts from display_time
                            SUBSTRING(display_time FROM 1 FOR 3) || 
                            -- To avoid LPAD error, convert to text explicitly and use right-padded format
                            to_char((CAST(SUBSTRING(display_time FROM 4 FOR 2) AS INTEGER) / 5) * 5, 'FM00')
                                AS interval_key,
                            display_time
                        FROM oi_volume_history
                        WHERE display_time < %s
                    ),
                    grouped_data AS (
                        SELECT 
                            time_groups.symbol,
                            time_groups.expiry_date,
                            time_groups.strike_price,
                            time_groups.option_type,
                            time_groups.interval_key,
                            ROUND(AVG(oih.price)::numeric, 2) AS avg_price,
                            ROUND(AVG(oih.oi)::numeric, 2) AS avg_oi,
                            SUM(oih.volume) AS total_volume,
                            AVG(oih.pct_change) AS avg_pct_change,
                            AVG(oih.vega) AS avg_vega, 
                            AVG(oih.theta) AS avg_theta,
                            AVG(oih.gamma) AS avg_gamma,
                            AVG(oih.delta) AS avg_delta,
                            AVG(oih.iv) AS avg_iv,
                            AVG(oih.pop) AS avg_pop,
                            COUNT(*) AS record_count,
                            array_agg(time_groups.display_time) AS original_times
                        FROM time_groups
                        JOIN oi_volume_history oih ON
                            oih.symbol = time_groups.symbol AND
                            oih.expiry_date = time_groups.expiry_date AND
                            oih.strike_price = time_groups.strike_price AND
                            oih.option_type = time_groups.option_type AND
                            oih.display_time = time_groups.display_time
                        GROUP BY 
                            time_groups.symbol,
                            time_groups.expiry_date,
                            time_groups.strike_price,
                            time_groups.option_type,
                            time_groups.interval_key
                        HAVING COUNT(*) > 1  -- Only process intervals with multiple records
                    )
                    SELECT * FROM grouped_data
                """, (cutoff_time_str,))
                
                # Step 2: Get the count of consolidated records
                cur.execute("SELECT COUNT(*) FROM temp_consolidated_data")
                total_consolidation_groups = cur.fetchone()[0]
                
                stats["total_consolidation_groups"] = total_consolidation_groups
                self.logger.info(f"Found {total_consolidation_groups} groups to consolidate")
                
                # Skip further processing if no data needs consolidation
                if total_consolidation_groups == 0:
                    stats.update({
                        "end_time": datetime.now(pytz.timezone('Asia/Kolkata')),
                        "consolidated_count": 0,
                        "deleted_count": 0,
                        "error_count": 0,
                        "status": "No data to consolidate"
                    })
                    return stats
                
                # Step 3: Insert consolidated records
                cur.execute("""
                    INSERT INTO oi_volume_history (
                        symbol, expiry_date, strike_price, option_type,
                        oi, volume, price, display_time, pct_change,
                        vega, theta, gamma, delta, iv, pop
                    )
                    SELECT 
                        symbol, expiry_date, strike_price, option_type,
                        avg_oi, total_volume, avg_price, interval_key,
                        avg_pct_change, avg_vega, avg_theta, avg_gamma,
                        avg_delta, avg_iv, avg_pop
                    FROM temp_consolidated_data
                    ON CONFLICT (symbol, expiry_date, strike_price, option_type, display_time) 
                    DO UPDATE SET
                        oi = EXCLUDED.oi,
                        volume = EXCLUDED.volume,
                        price = EXCLUDED.price,
                        pct_change = EXCLUDED.pct_change,
                        vega = EXCLUDED.vega,
                        theta = EXCLUDED.theta,
                        gamma = EXCLUDED.gamma,
                        delta = EXCLUDED.delta,
                        iv = EXCLUDED.iv,
                        pop = EXCLUDED.pop
                """)
                
                consolidated_count = cur.rowcount
                
                # Step 4: Delete original records in a batch process
                # Fix: Use a join-based delete approach rather than a subquery that might return multiple rows
                cur.execute("""
                    WITH records_to_delete AS (
                        SELECT 
                            symbol, expiry_date, strike_price, option_type,
                            unnest(original_times) AS old_display_time,
                            interval_key
                        FROM temp_consolidated_data
                    )
                    DELETE FROM oi_volume_history oih
                    USING records_to_delete rtd
                    WHERE 
                        oih.symbol = rtd.symbol AND
                        oih.expiry_date = rtd.expiry_date AND
                        oih.strike_price = rtd.strike_price AND
                        oih.option_type = rtd.option_type AND
                        oih.display_time = rtd.old_display_time AND
                        oih.display_time != rtd.interval_key
                """)
                
                deleted_count = cur.rowcount
                
                # Clean up temporary table
                cur.execute("DROP TABLE temp_consolidated_data")
            
            stats.update({
                "end_time": datetime.now(pytz.timezone('Asia/Kolkata')),
                "consolidated_count": consolidated_count,
                "deleted_count": deleted_count,
                "error_count": 0,
                "duration_seconds": (datetime.now(pytz.timezone('Asia/Kolkata')) - stats["start_time"]).total_seconds()
            })
            
            return stats
        
        except Exception as e:
            self.logger.error(f"Error in consolidation process: {e}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

def run_consolidation_worker(database_service, consolidation_interval=3600, retention_minutes=15):
    """
    Background worker that runs the data consolidation at regular intervals
    
    Args:
        database_service: Database service instance
        consolidation_interval (int): Interval between consolidation runs in seconds
        retention_minutes (int): Minutes of recent data to keep intact
    """
    consolidation_service = DataConsolidationService(database_service)
    
    while True:
        try:
            # Always run consolidation regardless of market hours to ensure database size is managed
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            print(f"Starting data consolidation at {now}")
            
            stats = consolidation_service.consolidate_oi_volume_history(
                retention_minutes=retention_minutes
            )
            
            print(f"Data consolidation complete: {stats}")
            
            # Sleep for the configured interval
            time.sleep(consolidation_interval)
            
        except Exception as e:
            print(f"Error in consolidation worker: {e}")
            # On error, wait a shorter time before retry
            time.sleep(300)  # 5 minutes

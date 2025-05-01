import os
from datetime import time


class Config:
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://swingtrade_db_user:ZlewRq8aZKimqMwrP2LdRTuFsvhi9qDw@dpg-cvh8gfpu0jms73bj6gm0-a.oregon-postgres.render.com/swingtrade_db')

    # Upstox API configuration
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyWEJSUFMiLCJqdGkiOiI2N2VkZjI5OTVlMDFkYTVlZjBjY2Q5ODAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQzNjQ3Mzg1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDM3MTc2MDB9.Ra7Bclq3ysxWNmi7oJol_1mcgz1sCK7WWgFG-59ZFmM')

    # Other configurations
    EXPIRY_DATE = "2025-05-29"
    # Market hours configuration
    MARKET_OPEN = time(9, 12)  # 09:15 AM
    MARKET_CLOSE = time(15, 30)  # 03:30 PM
    
    # Database clearing time window
    DB_CLEARING_START = time(9, 0)  # 09:00 AM
    DB_CLEARING_END = time(9, 15)  # 09:15 AM

    TRADING_DAYS = {0, 1, 2, 3, 4}  # Monday to Friday

    SENSEX_EXPIRIES = [
        "2025-05-06",
        "2025-05-13",
        "2025-05-20",
        "2025-05-27"
    ]

    BANKEX_EXPIRIES = [
        "2025-05-27"
    ]

    NIFTY_EXPIRIES = [
        "2025-05-08",
        "2025-05-15",
        "2025-05-22",
        "2025-05-29"
    ]

    # Data collection times with buffer windows (1-3 minutes before and after target times)
    # This ensures cron jobs can trigger within a small window around the desired times
    COMBINED_DATA_COLLECTION_TIMES = [
        # 10:00 AM buffer window (9:57 AM to 10:03 AM)
        
        time(10, 00),
        
        # 12:00 PM buffer window (11:57 AM to 12:03 PM)
        
        time(11, 50),

        time(14, 25),
        # 3:00 PM buffer window (2:57 PM to 3:03 PM)
        
        time(15, 25)
        
    ]

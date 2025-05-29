import os
from datetime import time


class Config:
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://swingtrade_db_user:ZlewRq8aZKimqMwrP2LdRTuFsvhi9qDw@dpg-cvh8gfpu0jms73bj6gm0-a.oregon-postgres.render.com/swingtrade_db')

    WORKER_URL = os.getenv('WORKER_URL', '')
    
    # Upstox API configuration
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyWEJSUFMiLCJqdGkiOiI2N2VkZjI5OTVlMDFkYTVlZjBjY2Q5ODAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQzNjQ3Mzg1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDM3MTc2MDB9.Ra7Bclq3ysxWNmi7oJol_1mcgz1sCK7WWgFG-59ZFmM')
    ACCESS_TOKEN2 = os.getenv('ACCESS_TOKEN2', 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyWEJSUFMiLCJqdGkiOiI2N2VkZjI5OTVlMDFkYTVlZjBjY2Q5ODAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaWF0IjoxNzQzNjQ3Mzg1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NDM3MTc2MDB9.Ra7Bclq3ysxWNmi7oJol_1mcgz1sCK7WWgFG-59ZFmM')

    # Other configurations
    EXPIRY_DATE = "2025-06-26"
    # Market hours configuration
    MARKET_OPEN = time(9, 9)  # 09:15 AM
    MARKET_CLOSE = time(15, 33)  # 03:30 PM
    
    # Post-market window for financial data collection (3:35 PM - 3:39 PM)
    POST_MARKET_START = time(15, 35)
    POST_MARKET_END = time(15, 45)

    # Database clearing time window
    DB_CLEARING_START = time(9, 0)  # 09:00 AM
    DB_CLEARING_END = time(9, 10)  # 09:15 AM

    TRADING_DAYS = {0, 1, 2, 3, 4}  # Monday to Friday

    SENSEX_EXPIRIES = [
        "2025-06-03",
        "2025-06-10",
        "2025-06-17",
        "2025-06-24"
    ]

    BANKEX_EXPIRIES = [
        "2025-06-24"
    ]

    NIFTY_EXPIRIES = [
        "2025-06-05",
        "2025-06-12",
        "2025-06-19",
        "2025-06-26"
    ]

    # These should include all relevant expiry dates for different instruments
    INSTRUMENT_EXPIRIES = [
        "2025-06-26"
    ]

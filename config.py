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
    
    # Post-market window for financial data collection (3:35 PM - 3:39 PM)
    POST_MARKET_START = time(15, 35)
    POST_MARKET_END = time(15, 39)

    # Database clearing time window
    DB_CLEARING_START = time(9, 0)  # 09:00 AM
    DB_CLEARING_END = time(9, 15)  # 09:15 AM

    TRADING_DAYS = {0, 1, 2, 3, 4}  # Monday to Friday

    SENSEX_EXPIRIES = [
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

# Configuration file for Instagram Downloader

# Flask Configuration
DEBUG = True
HOST = '0.0.0.0'
PORT = 8000

# File Configuration
# NOTE: Files are stored in /tmp/igdl_downloads and auto-deleted after the
# browser downloads them — no permanent storage on disk.
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB

# Download Configuration
PREFERRED_QUALITY = 'best'  # Options: best, worst, or specific format code
TIMEOUT = 300  # Seconds

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = 'app.log'

# Features
ENABLE_DOWNLOAD_HISTORY = True
HISTORY_LIMIT = 5

# Rate limiting (optional)
ENABLE_RATE_LIMITING = False
MAX_REQUESTS_PER_MINUTE = 30


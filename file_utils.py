# file_utils.py
import json
from filelock import FileLock
import os

def atomic_json_write(filepath, data):
    """Thread-safe JSON file writing"""
    lock = FileLock(f"{filepath}.lock")
    temp_file = f"{filepath}.tmp"

    with lock:
        try:
            # Write to temporary file first
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=4)

            # Atomic rename operation
            os.replace(temp_file, filepath)
        except Exception as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e

def atomic_json_read(filepath):
    """Thread-safe JSON file reading"""
    lock = FileLock(f"{filepath}.lock")
    with lock:
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r') as f:
            return json.load(f)

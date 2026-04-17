import json
import os
import time
import threading
import subprocess
import tempfile
import io
import sys
from dotenv import load_dotenv, find_dotenv
from contextlib import contextmanager
from filelock import FileLock, Timeout
from datetime import datetime, date, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from Webhook import send_discord_urgent

from numpy import info

# Load environment variables
dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

# Establishing File Paths
BASE_DIR = os.environ.get("Model_Path")
SETTINGS_PATH = os.path.join(BASE_DIR, "Settings.txt")


# ** \\ JSON File Handling with Locking \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

LOCK_TIMEOUT_SEC = 5
READ_RETRIES = 5
WRITE_RETRIES = 5
RETRY_BACKOFF_SEC = 0.05

def _lock_path(file_path: str) -> str:
    """Generate a lock file path for a given file."""
    return f"{file_path}.lock"

@contextmanager
def _locked(file_path: str, timeout: int = LOCK_TIMEOUT_SEC):
    """Context manager to acquire a file lock."""
    lock = FileLock(_lock_path(file_path))
    lock.acquire(timeout=timeout)
    try:
        yield
    finally:
        lock.release()

def _atomic_replace(dst_path: str, temp_contents: str):
    # Write a temporary file in the same directory to ensure atomic rename on File System
    dir_ = os.path.dirname(os.path.abspath(dst_path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=dir_, text=True)
    try:
        with io.open(fd, "w", encoding="utf-8") as f:
            f.write(temp_contents)
            f.flush()
            os.fsync(f.fileno()) # durability on POSIX; no-op on some File Systems
        os.replace(tmp, dst_path)  # Atomic operation
    finally:
        """ If Something above threw, make sure we don't leak the temp file """
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass

def safe_read_json(file_path: str, default_value=None):
    """
    Cross-Process safe JSON Read.
     - Takes a shared/exclusive lock via filelock (exclusive but short duration)
     - Retries JSONDecodeError to survive a writer midswap
    """
    for attempt in range(1, READ_RETRIES + 1):
        try:
            with _locked(file_path):
                with io.open(file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            if attempt == READ_RETRIES - 1:
                if default_value is not None and isinstance(e, FileNotFoundError):
                    return default_value
                raise
            time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    raise RuntimeError("Unreachable code in safe_read_json")

def safe_write_json(file_path: str, data) -> bool:
    """
    Cross-process safe JSON write.
    - Uses an exclusive lock.
    - Serializes to text, fsyncs, then atomic os.replace.
    """
    for attempt in range(1, WRITE_RETRIES + 1):
        try:
            with _locked(file_path):
                payload = json.dumps(data, ensure_ascii=False, indent=4)
                _atomic_replace(file_path, payload)
            return True
        except (OSError, Timeout) as e:
            if attempt == WRITE_RETRIES:
                print(f"[safe_write_json] Failed after {WRITE_RETRIES} attempts: {e}")
                return False
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    return False


# ** \\ Settings Handling \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def load_settings():
    """
    Read key=value lines from Settings.txt (skip comments),
    return a dict of all settings.
    """
    settings = {}
    
    # Read existing settings
    with open(SETTINGS_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                settings[key.strip()] = val.strip() 

    return settings

def read_settings(key, default=None):
    """
    Read a specific setting from Settings.txt.
    Returns None if key not found or on error.
    """
    try:
        settings = {}
        
        # Read existing settings
        with open(SETTINGS_PATH) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    setting_key, val = line.split("=", 1)  # Fixed: renamed variable
                    settings[setting_key.strip()] = val.strip()
        
        # Check if the requested key exists and convert to float
        if key in settings:
            return float(settings[key])
        else:
            return default
            
    except (ValueError, FileNotFoundError, KeyError) as e:
        return default


# ** \\ Timezone Handling \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def to_et(dt):
    """Convert a naive or UTC datetime to Eastern Time (ET)."""
    ET_ZONE = ZoneInfo("America/New_York")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ET_ZONE)

def to_ct(dt):
    """Convert a naive or UTC datetime to Central Time (CT)."""
    CT_ZONE = ZoneInfo("America/Chicago")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(CT_ZONE)

def utc_now():
    """Return current time in UTC."""
    return datetime.now(ZoneInfo("UTC"))

def to_utc(dt, from_zone):
    """Convert a datetime from ET or CT to UTC."""
    if from_zone == "ET":
        from_tz = ZoneInfo("America/New_York")
    elif from_zone == "CT":
        from_tz = ZoneInfo("America/Chicago")
    else:
        raise ValueError(f"Invalid timezone: {from_zone}. Must be 'ET' or 'CT'")
    
    # If datetime is naive, localize it to the source timezone
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=from_tz)
    
    return dt.astimezone(ZoneInfo("UTC"))

def et_time(hour, minute):
    """Return a datetime in ET for today at given hour/minute."""
    ET_ZONE = ZoneInfo("America/New_York")
    now_utc = datetime.now(ZoneInfo("UTC"))
    today_et = now_utc.astimezone(ET_ZONE).date()
    dt_naive = datetime.combine(today_et, dt_time(hour, minute))
    return dt_naive.replace(tzinfo=ET_ZONE)
def ct_time(hour, minute):
    """Return a datetime in CT for today at given hour/minute."""
    CT_ZONE = ZoneInfo("America/Chicago")
    now_utc = datetime.now(ZoneInfo("UTC"))
    today_ct = now_utc.astimezone(CT_ZONE).date()
    dt_naive = datetime.combine(today_ct, dt_time(hour, minute))
    return dt_naive.replace(tzinfo=CT_ZONE)

def ct_now():
    """Return current time in Central Time."""
    CT_ZONE = ZoneInfo("America/Chicago")
    return datetime.now(ZoneInfo("UTC")).astimezone(CT_ZONE)

def et_now():
    """Return current time in Eastern Time."""
    ET_ZONE = ZoneInfo("America/New_York")
    return datetime.now(ZoneInfo("UTC")).astimezone(ET_ZONE)

def wait_until(target):
    """
    Pause in 30 sec increments until system clock ≥ target time.
    """
    ET_ZONE = ZoneInfo("America/New_York")
    while True:
        now_utc = datetime.now(ZoneInfo("UTC"))
        now_et = now_utc.astimezone(ET_ZONE)
        if now_et >= target:
            break
        time.sleep(30)

# ** \\ Log Archiving \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

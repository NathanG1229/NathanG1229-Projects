import sys
from time import perf_counter
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from numpy import info

# ** \\ Timezone Handling \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def to_et(dt):
    """Convert a naive or UTC datetime to Eastern Time (ET)."""
    ET_ZONE = ZoneInfo("America/New_York")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ET_ZONE)

def et_time(hour, minute):
    """Return a datetime in ET for today at given hour/minute."""
    ET_ZONE = ZoneInfo("America/New_York")
    now_utc = datetime.now(ZoneInfo("UTC"))
    today_et = now_utc.astimezone(ET_ZONE).date()
    dt_naive = datetime.combine(today_et, dt_time(hour, minute))
    return dt_naive.replace(tzinfo=ET_ZONE)

def et_now():
    """Return current time in Eastern Time."""
    ET_ZONE = ZoneInfo("America/New_York")
    return datetime.now(ZoneInfo("UTC")).astimezone(ET_ZONE)


# ** \\ Loading Bar \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

def print_progress(title, current, total, start_time, width=36):
    pct = (current / total) if total else 1
    filled = int(width * pct)
    bar = "█" * filled + "-" * (width - filled)

    elapsed = perf_counter() - start_time
    eta = (elapsed / current) * (total - current) if current else 0
    if eta > 86400:
        eta_str = f"{round(eta / 86400, 2)} days"
    elif eta > 3600:
        eta_str = f"{round(eta / 3600, 2)} hours"
    elif eta > 60:
        eta_str = f"{round(eta / 60, 2)} minutes"
    else:
        eta_str = f"{eta} seconds"

    sys.stdout.write(
        f"\r{title} |{bar}| {current}/{total} ({pct*100:5.1f}%) "
        f"Elapsed: {elapsed:6.1f}s ETA: {eta_str}"
    )
    sys.stdout.flush()
    if current == total:
        print()
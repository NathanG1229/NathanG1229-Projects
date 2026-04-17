import os
import time
from datetime import datetime, timedelta
from dateutil import tz
from Tools import safe_read_json, safe_write_json, et_now, et_time
from dotenv import load_dotenv, find_dotenv
from MonthlyReport import monthly_report
from Webhook import send_discord_urgent

dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

M_PATH = os.getenv("Model_Path")
portfolio_path = os.path.join(M_PATH, "Portfolio.json")
stats_path = os.path.join(M_PATH, "Status.json")

def check_calendar(today_date):
    """
    Check if today is an early close day based on Calendar.json.
    Returns True if it's an early close day, False otherwise.
    """
    # Find the index position of target date in the list
    portfolio_data = safe_read_json(portfolio_path, {})
    assets = portfolio_data.get("Assets", [])

    calendar_path = os.path.join(M_PATH, "Calendar.json")
    calendar_data = safe_read_json(calendar_path, {})
    all_dates = sorted(calendar_data.keys())
    target_idx = all_dates.index(today_date)
    open = int(calendar_data[all_dates[target_idx]][0].get("Closed"))
    early_close = int(calendar_data[all_dates[target_idx]][0].get("Early"))

    # Get the range of trading days
    start_idx = max(0, target_idx - 15)
    end_idx = min(len(all_dates), target_idx + 15)
    
    selected_date_keys = all_dates[start_idx:end_idx]

    today_date_dt = datetime.strptime(today_date, '%Y-%m-%d').date()

    trading_days = []
    for date_str in selected_date_keys:
        records = calendar_data.get(date_str, [])
    
        # Check if date has records and first record has Closed = 0
        if records and len(records) > 0 and records[0].get('Closed') == '0':
            try:
                trading_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                trading_days.append(trading_date)
            except ValueError:
                continue

    start_date = trading_days[0]

    for asset in assets:
        days_left = 0
        ticker = asset.get("Ticker")
        try:
            purchase_date_str = asset.get("Purchase Date", None)
            if not purchase_date_str:
                send_discord_urgent("Lifeline", "update_days_left", f"Missing Purchase Date for asset {ticker}")
                continue
            
            try:
                purchase_date = datetime.strptime(purchase_date_str, '%Y-%m-%d').date()
            except ValueError:
                send_discord_urgent("Lifeline", "update_days_left", f"Invalid Purchase Date format for asset {ticker}: {purchase_date_str}")
                continue

            days_between = 0
            if purchase_date > start_date:
                for trading_day in trading_days:

                    if purchase_date <= trading_day <= today_date_dt:
                        days_between += 1
                
                total_holding_days = 6
                days_left = total_holding_days - days_between
                if days_left < -1:
                    send_discord_urgent("Lifeline", "check_calendar", f"Asset {ticker} has expired without selling. Purchase Date: {purchase_date_str}")
            else:
                send_discord_urgent("Lifeline", "check_calendar", f"Asset {ticker} has expired without selling. Purchase Date: {purchase_date_str}")
                continue
        except Exception as e:
            send_discord_urgent("Lifeline", "check_calendar", f"Error processing asset {asset.get('Ticker', 'Unknown')}: {e}")
            continue

    if open == 0:
        if early_close == 1:
            return True, True
        else: 
            return True, False
    return False, False

def get_trading_times(early_close):
    """
    Get the appropriate trading times based on whether it's an early close day.
    """
    if early_close:
        return et_time(16, 0)      # Market end time (4:00 PM ET on early close)
    else:
        return et_time(20, 00)     # Market day end time (8:00 PM ET on regular days)
    
def lifecheck():

    portfolio_data = safe_read_json(portfolio_path, {})
    status_data = safe_read_json(stats_path, {})
    status = int(status_data.get("Status", 0))
    current_time = et_now()
    last_update_str = portfolio_data.get("Time")

    # Parse last_update and make it timezone-aware
    last_update = datetime.strptime(last_update_str, "%Y-%m-%dT%H:%M:%S")
    et_tz = tz.gettz('America/New_York')
    last_update = last_update.replace(tzinfo=et_tz)
    time_threshold = et_now() - timedelta(minutes=1)
    time_since_update = current_time - last_update
    if status == 0:
        if last_update is None or last_update < time_threshold:
            send_discord_urgent("Lifeline.py", "Lifeline Alert", f"Portfolio.json has fallen behind. Last update was {time_since_update.seconds//60} minutes ago.")
            status_data["Status"] = 1
        else:
            status_data["Status"] = 0

        safe_write_json(stats_path, status_data)
        return None

    
    elif status == 1:
        if last_update > time_threshold:
            send_discord_urgent("Lifeline.py", "Lifeline Info", f"Portfolio.json connection restored.")
            status_data["Status"] = 0
        else:
            status_data["Status"] = 1
        safe_write_json(stats_path, status_data)
        return None

    else:
        return None
    

def main():
    send_discord_urgent("Lifeline.py", "Lifeline Activated", "Lifeline.py script has started and is now monitoring Portfolio.json.")
    try:
        while True:
            today_date = et_now().strftime("%Y-%m-%d")
            if et_now().day == 26:
                monthly_report()
            print(f"Lifeline activated for {today_date}")
            next_activation_str = (et_now() + timedelta(days=1)).strftime("%Y-%m-%dT03:00:00")
            open, early_close = check_calendar(today_date)
            if open:
                market_end_time = get_trading_times(early_close)

                while True:

                    current_time = et_now()
                    current_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
                    if current_time > market_end_time:
                        break
                    loop_start = time.time()
                    lifecheck()

                    # Calculate elapsed time and wait remaining time to reach 60 seconds
                    elapsed = time.time() - loop_start
                    remaining = 60.0 - elapsed
                    
                    if remaining > 0:
                        # Wait for the remaining time, but check stop_event periodically
                        sleep_interval = min(1.0, remaining)
                        while remaining > 0:
                            time.sleep(sleep_interval)
                            remaining -= sleep_interval
                            sleep_interval = min(1.0, remaining)

        

            et_tz = tz.gettz('America/New_York')
            next_activation = datetime.strptime(next_activation_str, "%Y-%m-%dT%H:%M:%S")
            next_activation = next_activation.replace(tzinfo=et_tz)
            
            wait_seconds = (next_activation - et_now()).total_seconds()
            if wait_seconds > 0:
                time.sleep(wait_seconds + 60)  # Sleep until just after next activation
    except Exception as e:
        send_discord_urgent("Lifeline.py", "Lifeline Error", f"Lifeline.py encountered an error and has stopped. Error details: {e}")

if __name__ == "__main__":
    main()
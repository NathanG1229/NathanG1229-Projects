import os
import numpy as np
from datetime import datetime, timedelta
from dateutil import tz
from scipy import stats
import time
import traceback
import logging
import threading
from Tools import safe_read_json, safe_write_json, et_now, et_time, ct_now, load_settings
from dotenv import load_dotenv, find_dotenv
from Alpaca import snapshot_data, get_positions, get_all_orders, historic_bars, place_order, edit_order, close_position
from EODReset import reset_data
from MLCurrent import ml_current
from MLFilter import ml_filter
from Webhook import send_discord_urgent

# Establishing Environment Variables
dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

# Establishing File Paths
M_PATH = os.getenv("Model_Path")
portfolio_path = os.path.join(M_PATH, "Portfolio.json")
calendar_path = os.path.join(M_PATH, "Calendar.json")
reports_path = os.path.join(M_PATH, "Report.json")

# Establishing Logging variables
master_logger = logging.getLogger(__name__)
master_logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s:%(message)s")
file_handler = logging.FileHandler("Master_Output.log")
file_handler.setFormatter(formatter)
master_logger.addHandler(file_handler)

def log_action(asset, action_type, message_dict):
    """
    Log action in Master Logger
    Logs info in Report.json
    Action Types: 
    1. Purchase / Entry into Portfolio.json
        - Fill in Asset ID, Ticker, Purchase Date, Purchase Price
    2. Loss Probability Entry
        - Append Dictionary entry with Time, Original Price, Liklihood, and New Price
    3. Initiation Price Reach
        - Append Dictionary entry with Time and Price at time of initiation
    4. Sell Attempt
        - Append Dictionary entry with Time, Order Entry, Sell Reason, Reference High (If Available), Limit Price
    """
    current_time = ct_now()
    time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    tracker = asset.get("Tracker")
    if tracker == None:
        asset_id = asset.get("Asset ID")
        purchase_date = asset.get("Purchase Date")
        tracker = asset_id + "*" + purchase_date

    master_logger.info(message_dict.get("Message"))

    report_data = safe_read_json(reports_path, {})
    objects_data = report_data.get("Trade Objects", {})
    trade_object = objects_data.get(tracker, {})
    if trade_object == {}:
        new_dict = {
            "Asset ID": None,
            "Ticker": None,
            "Buy Date": None,
            "Buy Price": None,
            "Sell Date": None,
            "Sell Price": None,
            "Sell Reason": None,
            "Loss Probability Adjustments": [],
            "Initiations": [],
            "Reference Highs": [],
            "Sell Orders": {}
        }
        trade_object = new_dict

    if action_type == 1:
        trade_object["Asset ID"] = asset["Asset ID"]
        trade_object["Tracker"] = asset["Tracker"]
        trade_object["Ticker"] = asset["Ticker"]
        trade_object["Buy Date"] = asset["Purchase Date"]
        trade_object["Buy Price"] = asset["Purchase Price"]
    
    if action_type == 2:
        original_price = message_dict.get("Original Price")
        liklihood = message_dict.get("Liklihood")
        new_price = message_dict.get("New Price")
        probability_dict = {
            "Time": time,
            "Original Price": original_price,
            "Liklihood": liklihood,
            "New Price": new_price
        }
        trade_object["Loss Probability Adjustments"].append(probability_dict)
    if action_type == 3:
        initiation_price = message_dict.get("Initiation Price")
        initiation_dict = {
            "Time": time,
            "Price": initiation_price,
        }
        trade_object["Initiations"].append(initiation_dict)

    if action_type == 4:
        reference_high = message_dict.get("Reference High")
        stair_step = message_dict.get("Stair Step")
        stop_price = message_dict.get("Stop Price")
        method = message_dict.get("Method")
        reference_high_dict = {
            "Time": time,
            "Reference High": reference_high,
            "Stair Step": stair_step,
            "Stop Price": stop_price,
            "Method": method
        }
        trade_object["Reference Highs"].append(reference_high_dict)

    if action_type == 5:
        order_id = message_dict.get("Order ID")
        sell_reason = message_dict.get("Sell Reason")
        limit_price = message_dict.get("Sell Price")
        trade_object['Sell Orders'][order_id] = {
            "Time": time,
            "Sell Reason": sell_reason,
            "Limit Price": limit_price,
        }

    report_data["Trade Objects"][tracker] = trade_object
    safe_write_json(reports_path, report_data)

def get_trading_times():
    """
    Get the appropriate trading times based on whether it's an early close day.
    Returns tuple: (ml_time, market_end_time, sell_expired_time)
    """
    portfolio_data = safe_read_json(portfolio_path, {})
    if portfolio_data.get("EarlyClose", False):
        master_logger.debug("Portfolio EarlyClose is True")
        return (
            et_time(9, 30),    # Market open time (9:30 AM ET on early close)
            et_time(12, 50),   # ML analysis time (12:50 PM ET on early close)
            et_time(16, 0),    # Market end time (4:00 PM ET on early close)
            et_time(12, 30),    # Sell expired assets time (12:30 PM ET on early close)
            et_time(13, 00),   # Market day end time (1:00 PM ET on early close)
        )
    else:
        master_logger.debug("Portfolio EarlyClose is False")
        return (
            et_time(9, 30),    # Market open time (9:30 AM ET on regular days)
            et_time(15, 45),   # ML analysis time (3:45 PM ET on regular days)
            et_time(20, 00),    # Market end time (2:00 PM ET on regular days)
            et_time(15, 30),    # Sell expired assets time (3:30 PM ET on regular days)
            et_time(16, 00)     # Market day end time (4:00 PM ET on regular days)
        )
    

def day_startup():
    """Reset daily portfolio state and prepare assets for a new trading session."""
    
    portfolio_data = safe_read_json(portfolio_path, {})
    if portfolio_data["Startup"] == False:
        try:
            current_date_str = et_now().strftime("%Y-%m-%d")
            activation_date_str = f"{et_now().date() + timedelta(days=1)}T03:00:00"

            master_logger.info(f"Starting new trading day: {current_date_str}")

            
            portfolio_data["Today"] = current_date_str
            portfolio_data["Next Activation"] = activation_date_str
            portfolio_data["MLRan"] = False
            portfolio_data["SellExpiredRan"] = False
            portfolio_data["PastExpiredRan"] = False

            # Reset per-day workflow flags before the market loop begins.
            assets = portfolio_data.get("Assets", [])
            for asset in assets:
                master_logger.debug(f"Resetting {asset.get("Ticker")}: {asset}")

                if asset.get("Status", 0) == 2:
                    asset["Status"] = 0
                    asset["Order ID"] = None
                    asset["Reference High"] = None
                    asset["Loss Point Time"] = None
                    asset["Initiation Point Time"] = None
                
                elif asset.get("Status", 0) == 1:
                    asset["Status"] = 0
                    asset["Reference High"] = None
                    asset["Initiation Point Time"] = None
                    asset["Loss Point Time"] = None
                
                else:
                    asset["Loss Point Time"] = None

            
            portfolio_data["Startup"] = True

            portfolio_data["Assets"] = assets

            safe_write_json(portfolio_path, portfolio_data)
        except Exception as e:
            master_logger.critical(f"Error during day startup: {e}")
            send_discord_urgent("Master", "day_startup", str(e))
    else:
        return
    

def update_days_left(portfolio_data=None):
    """Recalculate remaining holding days for each asset from the trading calendar."""

    if portfolio_data is None:
        portfolio_data = safe_read_json(portfolio_path, {})
        assets = portfolio_data.get("Assets", [])
    else:
        assets = portfolio_data.get("Assets", [])
    calendar_data = safe_read_json(calendar_path, {})

    all_dates = sorted(calendar_data.keys())

    # Get target date (today)
    target_date = et_now().date()
    target_index = target_date.strftime("%Y-%m-%d")

    # Find the index position of target date in the list
    try:
        target_idx = all_dates.index(target_index)
        early_close = calendar_data[target_index][0].get("Early", False)
        if early_close == "1":
            portfolio_data["EarlyClose"] = True
        else:
            portfolio_data["EarlyClose"] = False
    except ValueError:
        send_discord_urgent("Master", "update_days_left", f"Target date {target_index} not found in calendar")
        master_logger.error(f"Target date {target_index} not found in calendar")
        return

    # Get the range of trading days
    start_idx = max(0, target_idx - 20)
    end_idx = min(len(all_dates), target_idx + 20)
    
    selected_date_keys = all_dates[start_idx:end_idx]

    # Only open-market dates count toward the holding window.
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
        tracker = asset.get("Tracker")
        if not tracker:
            asset["Tracker"] = asset.get("Asset ID") + "*" + asset.get("Purchase Date")
        days_left = -2
        ticker = asset.get("Ticker")
        try:
            purchase_date_str = asset.get("Purchase Date", None)
            if not purchase_date_str:
                asset["Days Left"] = days_left
                continue
            
            try:
                purchase_date = datetime.strptime(purchase_date_str, '%Y-%m-%d').date()
            except ValueError:
                master_logger.error(f"Error converting {asset.get("Ticker")} purchase date {asset.get("Purchase Date")} into date object")
                asset["Days Left"] = days_left
                continue

            days_between = 0
            if purchase_date >= start_date:
                for trading_day in trading_days:

                    if purchase_date <= trading_day <= target_date:
                        days_between += 1
                
                total_holding_days = 6
                days_left = total_holding_days - days_between
                asset["Days Left"] = days_left
                master_logger.info(f"Ticker {ticker}: Days Left {days_left}")
            else:
                asset["Days Left"] = days_left
                continue
        except Exception as e:
            send_discord_urgent("Master", "update_days_left", f"Error processing asset {asset.get('Ticker', 'Unknown')}: {e}")
            master_logger.error(f"Error processing asset {asset.get('Ticker', 'Unknown')}: {e}")
            asset["Days Left"] = days_left
            continue
        report_data = safe_read_json(reports_path, {})
        objects_data = report_data.get("Trade Objects", {})
        trade_object = objects_data.get(tracker, {})
        if trade_object == {}:
            message_dict = {
                "Message": f"{asset.get("Ticker")} is not in Report.json. Adding."
            }

            log_action(asset=asset, action_type=1, message_dict=message_dict)

    portfolio_data["Assets"] = assets
    safe_write_json(portfolio_path, portfolio_data)
    return portfolio_data


def get_snapshot(assets):
    """Fetch live Alpaca snapshot data for the active portfolio tickers."""
    try:
        if assets is None:
            error_msg = "get_snapshot: assets parameter is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "get_snapshot", error_msg)
            return {}
        
        if not isinstance(assets, list):
            error_msg = f"get_snapshot: assets is not a list, got {type(assets)}"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "get_snapshot", error_msg)
            return {}
        
        tickers = [asset.get("Ticker") for asset in assets if asset.get("Ticker")]
        
        if not tickers:
            master_logger.error("get_snapshot: No valid tickers found in assets")
            return {}
        
        tickers_string = ','.join(tickers)
        
        snapshots = snapshot_data(tickers_string)
        master_logger.debug(f"snapshot_data response: {snapshots}")
        
        if snapshots is None:
            error_msg = "get_snapshot: snapshot_data returned None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "get_snapshot", error_msg)
            return {}
        
        return snapshots

    except Exception as e:
        error_msg = f"Error in get_snapshot: {e}"
        send_discord_urgent("Master", "get_snapshot", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())
        return {}


def calculate_return(snapshots):
    """Calculate current unrealized return values for all portfolio assets."""
    try:
        if snapshots is None:
            error_msg = "calculate_return: snapshots parameter is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "calculate_return", error_msg)
            return {}
        
        portfolio_data = safe_read_json(portfolio_path, {})
        assets = portfolio_data.get("Assets", [])
        master_logger.debug(f"{len(assets)} asset(s) currently loaded")
        
        if assets is None:
            error_msg = "calculate_return: assets from portfolio is None"
            logging.warning(error_msg)
            send_discord_urgent("Master", "calculate_return", error_msg)
            return {}
        
        results = {}

        for asset in assets:
            ticker = asset.get("Ticker")
            purchase_price = asset.get("Purchase Price", 0)

            if ticker in snapshots:
                current_price = (snapshots[ticker]['minuteBar']['c'] + snapshots[ticker]['minuteBar']['vw']) / 2.0
                return_value = (current_price - purchase_price) / purchase_price
                master_logger.debug(f"Return Values for {ticker}: Purchase: ${purchase_price:.2f} Current: ${current_price:.2f} Return: {(return_value * 100):.2f}%")
                results[ticker] = {
                    "Current Price": current_price,
                    "Return Value": return_value
                }
            else:
                master_logger.error(f"{ticker} is not in snapshot")
                results[ticker] = {
                    "Current Price": None,
                    "Return Value": None
                }
        return results
    except Exception as e:
        error_msg = f"Error in calculate_return: {e}"
        send_discord_urgent("Master", "calculate_return", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())
        return {}
    
def run_ml_analysis():
    """Wrapper function to run ML analysis and update portfolio."""
    try:
        master_logger.debug("ML Current analysis starting")
        ml_current()
        master_logger.debug("ML Current analysis completed successfully")
    except Exception as e:
        error_msg = f"Error in ML analysis thread: {e}"
        send_discord_urgent("Master", "run_ml_analysis", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())

def sell_expired_assets(portfolio_data):
    """Submit or update sell orders for positions that have reached expiry rules."""

    master_logger.info("Beginning sell_expired_assets process")
    try:
        portfolio_data = safe_read_json(portfolio_path, {})
        portfolio_data["SellExpiredRan"] = True
        safe_write_json(portfolio_path, portfolio_data)
    except Exception as e:
        error_msg = f"Error in sell expired thread: {e}"
        send_discord_urgent("Master", "run_sell_expired", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())

    try:
        if portfolio_data is None:
            error_msg = "sell_expired_assets: portfolio_data is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_expired_assets", error_msg)
            return
        
        assets = portfolio_data.get("Assets", [])
        current_time = et_now().strftime("%Y-%m-%dT%H:%M:%S")

        snapshots = get_snapshot(assets)
        s = load_settings()
        goal = float(s.get('WinAmount', 0.025))
        
        if assets is None:
            error_msg = "sell_expired_assets: assets is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_expired_assets", error_msg)
            return

        for asset in assets:
            ticker = asset.get("Ticker")
            try:  # Add try-except around each asset
                days_left = asset.get("Days Left", 0)

                # Skips Assets with at least 2 or more days left
                if days_left > 1:
                    continue

                purchase_price = asset.get("Purchase Price")
                current_price = (snapshots[ticker]['minuteBar']['c'] + snapshots[ticker]['minuteBar']['vw']) / 2.0
                current_return = (current_price - purchase_price) / purchase_price

                # Includes Assets on their last day and assets above return goal on the last 2 days
                if days_left > 0 and current_return < goal:
                    continue

                limit_price = current_price * (1 - float(s.get('LimitOffset', 0.002)))
                if current_return >= goal:
                    limit_price = max(current_price * (1 - float(s.get('LimitOffset', 0.002))), purchase_price * (1 + goal))

                rounded_limit_price = np.round(limit_price)

                status = asset.get("Status", 0)
                if status == 2:
                    client_id = f"EXP 2 Limit-{ticker}-{rounded_limit_price}-{current_time}"
                    message = f"Pending order detected. Modifying sell order for expired {ticker} position at limit price ${rounded_limit_price}"
                    order_id = asset.get("Order ID")

                    response = edit_order(
                        order_id,
                        rounded_limit_price,
                        client_order_id=client_id
                    )
                    master_logger.debug(response)

                    message_dict = {
                        "Order ID": response.get("id"),
                        "Client Order ID": client_id,
                        "Sell Reason": "Expired",
                        "Sell Price": rounded_limit_price,
                        "Message": message
                    }

                    log_action(asset=asset, action_type=5, message_dict=message_dict)
                    continue

                else:

                    client_id = f"EXP 1 Limit-{ticker}-{rounded_limit_price}-{current_time}"
                    message = f"No pending order detected. Initiating sell for expired position {ticker} at limit price ${rounded_limit_price}"
                    
                    order_response = place_order(
                        symbol=ticker,
                        side="sell",
                        type="limit",
                        time_in_force="day",
                        limit_price=rounded_limit_price,
                        qty=asset.get("Shares", 0),
                        extended_hours=True
                    )

                    message_dict = {
                        "Order ID": order_response.get("id"),
                        "Client Order ID": client_id,
                        "Sell Reason": "Expired",
                        "Sell Price": rounded_limit_price,
                        "Message": message
                    }

                    log_action(asset=asset, action_type=5, message_dict=message_dict)
            
            except Exception as e:
                error_msg = f"Error processing asset {ticker} in sell_expired_assets: {e}"
                send_discord_urgent("Master", "sell_expired_assets", error_msg)
                master_logger.error(error_msg + " " + traceback.format_exc())
                continue  # Continue to next asset

        portfolio_data["Assets"] = assets
        safe_write_json(portfolio_path, portfolio_data)
        master_logger.debug("Sell expired assets completed successfully")
    
    except Exception as e:
        error_msg = f"Error in sell_expired_assets: {e}"
        send_discord_urgent("Master", "sell_expired_assets", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())
        portfolio_data["SellExpiredRan"] = False
        safe_write_json(portfolio_path, portfolio_data)

def remove_past_expired():
    """Force-close positions that are already beyond the allowed holding period."""

    portfolio_data = safe_read_json(portfolio_path, {})
    assets = portfolio_data.get("Assets", [])
    kept_assets = []

    # Anything beyond expiry is removed from the active portfolio set.
    for asset in assets:
        try:
            days_left = asset.get("Days Left", 0)
            if days_left < 0:
                ticker = asset.get("Ticker")
                tracker = asset.get("Tracker")
                message = f"{tracker} position is past expired, closing."
                response = close_position(ticker=ticker, percent=100)
                message_dict = {
                    "Order ID": response.get("id"),
                    "Sell Reason": "Expired",
                    "Sell Price": None,
                    "Message": message,
                }
                log_action(asset=asset, action_type=5, message_dict=message_dict)
            else:
                kept_assets.append(asset)

        except Exception as e:
            error_msg = f"Error in remove_past_expired: {e}"
            send_discord_urgent("Master", "remove_past_expired", error_msg)
            master_logger.error(error_msg + " " + traceback.format_exc())
            pass

    portfolio_data["Assets"] = kept_assets
    safe_write_json(portfolio_path, portfolio_data)
    


def positions_check(current_time):
    """Sync Portfolio.json with live brokerage positions and recent order history."""
    try:
        master_logger.debug("Beginning Positions Check Process")
        old_portfolio_data = safe_read_json(portfolio_path, {})
        assets = old_portfolio_data.get("Assets", [])
        
        try:
            positions = get_positions()
            master_logger.debug(f"Current Positions: {positions}")
            
            if positions is None:
                error_msg = "positions_check: get_positions returned None"
                master_logger.error(error_msg)
                send_discord_urgent("Master", "positions_check", error_msg)
                return old_portfolio_data
                
        except Exception as e:
            error_msg = f"Error fetching positions: {e}"
            send_discord_urgent("Master", "positions_check", error_msg)
            master_logger.error(error_msg + " " + traceback.format_exc())
            return old_portfolio_data

        
        # Build a quick lookup so local portfolio entries can be reconciled with Alpaca.
        positions_dict = {
            position.get("asset_id"): position
            for position in positions
            if isinstance(position, dict) and position.get("asset_id") is not None
        }

        assets_to_keep = []
        
        for asset in assets:
            asset_id = asset.get("Asset ID")
            ticker = asset.get("Ticker")

            if asset_id in positions_dict:
                position = positions_dict[asset_id]
                asset["Shares"] = float(position.get("qty", 0))
                prev_qty = asset.get("Shares", 0)
                if prev_qty != float(position.get("qty", 0)):
                    master_logger.info(f"Updated shares for {ticker}: {prev_qty} -> {position.get('qty', 0)}")
                    asset["Shares"] = float(position.get("qty", 0))
                if ticker != position.get("symbol"):
                    master_logger.info(f"Ticker mismatch for {asset_id}: {ticker} -> {position.get('symbol')}")
                    asset["Ticker"] = position.get("symbol")
                assets_to_keep.append(asset)
            else:
                master_logger.info(f"Removing {ticker} from Portfolio.json (not found in positions)")
        old_portfolio_data["Assets"] = assets_to_keep
                
        safe_write_json(portfolio_path, old_portfolio_data)

        portfolio_data = safe_read_json(portfolio_path, {})
        master_logger.debug(f"Updated Porfolio Data: {portfolio_data}")
        assets = portfolio_data.get("Assets", [])

        if len(positions_dict) == 0:
            master_logger.warning("Positions Dictionary is Empty")
            return old_portfolio_data

        assets_dict = {
            asset_id: asset
            for asset in assets
            if (asset_id := asset.get("Asset ID")) is not None
        }


        for position in positions_dict.values():
            ticker = position.get("symbol")
            position_asset_id = position.get("asset_id")


            tickers_updated = 0
            if position_asset_id not in assets_dict:

                purchase_date = current_time.strftime("%Y-%m-%d")

                try:

                    orders = get_all_orders(
                        status="closed",
                        limit=500,
                        after=(et_now() - timedelta(days=10)).strftime("%Y-%m-%d"),
                        until=purchase_date,
                    )
                    master_logger.debug(f"get_all_orders api output: {orders}")
                    
                    if orders is None:
                        error_msg = f"positions_check: get_orders returned None for {ticker}"
                        master_logger.error(error_msg)
                        send_discord_urgent("Master", "positions_check", error_msg)
                        orders = []
                        
                except Exception as e:
                    error_msg = f"Error fetching orders for {ticker}: {e}"
                    send_discord_urgent("Master", "positions_check", error_msg)
                    master_logger.error(error_msg + " " + traceback.format_exc())
                    orders = []
                    continue

                if orders != []:

                    for order in orders:
                        order_asset_id = order.get("asset_id")
                        side = order.get("side")
                        if side != "buy":
                            continue
                        order_status = order.get("order_status")
                        if order_status != "filled":
                            continue
                        if order_asset_id == position_asset_id:
                            purchase_date = order.get("filled_at").split("T")[0]
                            break
                        
                message = f"Adding new position {ticker} to Portfolio.json"
                new_asset = {
                    "Asset ID": position.get("asset_id"),
                    "Ticker": ticker,
                    "Tracker": position.get("asset_id") + "*" + purchase_date,
                    "Status": 0,
                    "Shares": float(position.get("qty", 0)),
                    "Purchase Price": float(position.get("avg_entry_price", 0)),
                    "Purchase Date": purchase_date,
                    "Days Left": 5,
                    "Initiation Point Price": float(position.get("avg_entry_price", 0)) * 1.025,
                    "Initiation Point Time": None,
                    "Reference High": None,
                    "Stair Step": None,
                    "Loss Point Time": None,
                    "Order ID": None
                }
                message_dict = {
                    "Message": message 
                }
                log_action(asset=new_asset, action_type=1, message_dict=message_dict)
                assets.append(new_asset)
                tickers_updated += 1
        asset_data = assets
        portfolio_data["Assets"] = asset_data
        master_logger.debug(f"Updating Portfolio Data: {asset_data}")
        safe_write_json(portfolio_path, portfolio_data)
        if tickers_updated > 0:
            portfolio_data = update_days_left(portfolio_data)
    
    except Exception as e:
        error_msg = f"Error in positions_check: {e}"
        send_discord_urgent("Master", "positions_check", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())
        return old_portfolio_data

def loss_probability(asset, snapshots, current_time, settings):
    """
    Calculate the probability of reaching a target return using Student's t-distribution.
    
    Args:
        historical_bars: List of daily bar data (from get_historical_loss_data)
        current_weighted_avg: Current weighted average price at loss point
        purchase_price: Original purchase price
        days_left: Number of days remaining in simulation after loss point
        target_return: Target return to reach (e.g., 0.025 for 2.5%)
    
    Returns:
        (probability, new_initiation_price): Probability as float [0,1] and new initiation price if < 60%
    """

    try:

        if asset == None:
            return None, None
        
        ticker = asset.get("Ticker")
        purchase_price = asset.get("Purchase Price", 0)
        days_left = asset.get("Days Left", 0)

        if ticker not in snapshots:
            logging.warning(f"{ticker} not in snapshot. Skipping Loss Probability")
            return None, None
            
        if current_time == None:
            logging.warning(f"{ticker} Current time is missing. Skipping Loss Probability")
            return None, None
        
        if days_left > 4:
            master_logger.debug(f"{ticker} has {days_left} days left. Skipping Loss Probability")
            return None, None


        loss_point_time = asset.get("Loss Point Time", None)
        Probability_Threshold = float(settings.get('Probability_Threshold'))
        initiation_price = asset.get("Initiation Point Price", None)
        New_Initiation_Probability = float(settings.get('New_Initiation_Probability'))
        current_price = (snapshots[ticker]['minuteBar']['c'] + snapshots[ticker]['minuteBar']['vw']) / 2.0

        current_return = (current_price - purchase_price) / purchase_price


        if isinstance(current_time, str):
            current_time = current_time
        else:
            current_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")

        if current_return > float(settings.get('LossThreshold', -0.025)):
            master_logger.debug(f"{ticker} has a return of {(current_return * 100):.4f}% > 2.5%. Skipping Loss Probability")
            return None, None
        if loss_point_time != None:
            last_update = datetime.strptime(loss_point_time, "%Y-%m-%dT%H:%M:%S")
            et_tz = tz.gettz('America/New_York')
            last_update = last_update.replace(tzinfo=et_tz)
            if loss_point_time > (et_now()-timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S"):
                return None, None
            
        try:
        
            historical_data = historic_bars(
                symbol=ticker,
                start=(et_now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                end=et_now().strftime("%Y-%m-%d"),
                timeframe="1Day"
            )
            master_logger.debug(f"{ticker} returned {len(historical_data)} days")

        except Exception as e:
            send_discord_urgent("Master", "loss_probability", f"Error fetching historical data for {ticker}: {e}")
            master_logger.error(f"Error fetching historical data for {ticker}: {e}")
            return None, None
        
        # Calculate the required return from current weighted avg to initiation point
        required_return = (initiation_price - current_price) / current_price
        day_0_return = (purchase_price / current_price) - 1

        if days_left == 0:
            required_return = min(day_0_return, required_return)  # Same-day, target is breakeven

        # Extract bars from the response
        if not historical_data or "bars" not in historical_data:
            logging.warning(f"No historical data returned for {ticker}")
            return None, None
        
        historical_bars = historical_data["bars"]
        
        if not historical_bars or days_left < 0:
            logging.warning(f"Insufficient historical data or invalid days left for {ticker}")
            return None, None
        
        # Calculate returns for each historical day based on future highs
        returns = []

        if days_left > 0:

        
            for idx in range(len(historical_bars) - days_left):
                current_bar = historical_bars[idx]
                current_avg = (current_bar["l"] + current_bar["c"]) / 2.0
                
                # Find maximum high in the next 1 to days_left days
                future_highs = []
                for offset in range(1, min(days_left + 1, len(historical_bars) - idx)):
                    future_bar = historical_bars[idx + offset]
                    future_highs.append(future_bar["h"])
                
                if future_highs:
                    max_future_high = max(future_highs)
                    day_return = (max_future_high - current_avg) / current_avg
                    returns.append(day_return)
        
        else:

            for idx in range(len(historical_bars)):
                current_bar = historical_bars[idx]
                current_avg = (current_bar["vw"] + current_bar["l"]) / 2
                current_high = current_bar["h"]
                day_return = (current_high - current_avg) / current_avg
                returns.append(day_return)
        
        if len(returns) < 2:
            logging.warning(f"Insufficient return samples: {len(returns)}")
            return None, None
        
        # Calculate mean and standard deviation
        returns_array = np.array(returns)
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array, ddof=1)  # Sample standard deviation

        
        if std_return == 0:
            # No variance - check if mean is above target
            if mean_return >= required_return:
                return None, None
            else:
                logging.warning(f"Standard deviation is zero and mean return {mean_return:.4f} is below target {required_return:.4f} for {ticker}")
                return None, None
        
        
        # Use Student's t-distribution
        df = len(returns) - 1  # Degrees of freedom
        t_stat = (required_return - mean_return) / (std_return / np.sqrt(len(returns)))
        
        # Calculate probability (1 - CDF because we want P(X >= required_return))
        probability = 1 - stats.t.cdf(t_stat, df)
        
        # If probability < threshold, find the return value at threshold percentile
        if probability < Probability_Threshold:
            # Calculate new initiation return
            percentile_return = stats.t.ppf(New_Initiation_Probability, df, loc=mean_return, scale=std_return / np.sqrt(len(returns)))
            
            # Convert to initiation price
            new_initiation_price = current_price * (1 + percentile_return)


            message = f"Ticker {ticker} has a probability of {probability:.4f} to reach the initiation price of ${initiation_price:.2f}, setting new initiation price to {new_initiation_price:.2f}"
            message_dict = {
                "Original Price": initiation_price,
                "Liklihood": probability,
                "New Price": new_initiation_price,
                "Message": message
            }

            log_action(asset=asset, action_type=2, message_dict=message_dict)

            return new_initiation_price, current_time
            
        return None, None
    
    except Exception as e:
        send_discord_urgent("Master", "loss_probability", f"Error in loss probability calculation for {asset.get('Ticker', 'Unknown')}: {e}")
        master_logger.error(f"Error in loss probability calculation for {asset.get('Ticker', 'Unknown')}: {e}")
        return None, None


def initiation_point_check(asset, snapshots, current_time):
    """Check whether a position has reached its initiation price trigger."""

    ticker = asset.get("Ticker")
    days_left = asset.get("Days Left", 0)
    initiation_price = asset.get("Initiation Point Price", None)

    if asset is None:
        logging.warning(f"Asset is None")
        return None
    
    if not initiation_price:
        logging.warning(f"Initiation Price for {ticker} is Missing.")
        return None
    
    if not snapshots.get(ticker):
        logging.warning(f"{ticker} not in snapshot.")
        return None
    
    if days_left > 4:
        master_logger.debug(f"Skipping {ticker}, days left > 4")
        return None

    current_high = snapshots[ticker]['minuteBar']['h']
    if current_high >= initiation_price:
        Initiation_Point = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        message = f"{ticker} has reached Initiation Point Price: {current_high} >= {initiation_price}"
        message_dict = {
            "Initiation Price": current_high,
            "Message": message
        }
        log_action(asset=asset, action_type=3, message_dict=message_dict)
        return Initiation_Point

    else:
        master_logger.debug(f"{ticker} has reached not Initiation Point Price: {current_high} < {initiation_price}")
        return None

   
def sell_check(asset, snapshots, current_time, settings):
    """Evaluate trailing-stop logic and place a sell order when exit rules trigger."""
    try:
        if asset is None:
            error_msg = "Asset is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_check", error_msg)
            return None, None, None
        
        ticker = asset.get("Ticker")
        days_left = asset.get("Days Left", 0)

        # Filter Out Irrelevant Assets

        if days_left > 4:
            master_logger.debug(f"{ticker} Days Left > 4, skipping sell check")
            return None, None, None

        if not ticker:
            error_msg = "sell_check: ticker is missing from asset"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_check", error_msg)
            return None, None, None

        if snapshots is None:
            error_msg = f"sell_check: snapshots is None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_check", error_msg)
            return None, None, None
        
        if ticker not in snapshots:
            error_msg = f"Ticker not in snapshots {ticker}"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "sell_check", error_msg)
            return None, None, None
        
        # Establish Variables
        
        # Setting Variables
        stop_offset = float(settings.get('StopOffset'))
        stop_min_offset = float(settings.get('StopMin'))
        limit_offset = float(settings.get('LimitOffset'))
        win_amount = float(settings.get('WinAmount'))

        # Asset Variables
        purchase_price = asset.get("Purchase Price")
        loss_point = asset.get("Loss Point Time")
    
        reference_high = asset.get("Reference High", None)

        if reference_high is None:
            reference_high = asset.get("Initiation Point Price")

        reference_high_return = (reference_high - purchase_price) / purchase_price
    
        reference_stairstep = asset.get("Stair Step", None)
        if not reference_stairstep:
            stairstep_return = np.floor(reference_high_return / 0.005) * 0.005
            reference_stairstep = purchase_price * (1 + stairstep_return)

    
        # Snapshot Variables
        current_price = (snapshots[ticker]['minuteBar']['c'] + snapshots[ticker]['minuteBar']['vw']) / 2.0
        current_high = snapshots[ticker]['minuteBar']['h']
        current_high_return = (current_high - purchase_price) / purchase_price
        stairstep_return = np.floor(current_high_return / 0.005) * 0.005
        stair_step = purchase_price * (1 + stairstep_return)
        stop_price = stair_step * (1 - stop_offset)

        # Normal Message
        message2 = f"Stop price = ${stop_price:.2f} Normal Stop Price Calculation."
        method = "Normal"

        # Determine if New High is Created
        new_high = None
        if current_high > reference_high:
            new_high = current_high
        
        # Adjust Stop Price and Message if Stair Step Price is 2.5% or lower        
        if stairstep_return <= 0.025 and loss_point == None:
            stop_price = reference_high * (1 - stop_min_offset) if not new_high else new_high * (1 - stop_min_offset)
            message2 = f" Stop price = ${stop_price:.2f} Minimum Stop Price Calculation."
            method = "Minimum"

        # Report New Stair Step if New Value is Greater than Previous Value
        new_stairstep = None
        if stair_step > reference_stairstep:
            message1 = f"{ticker} has reached a new reference high of {current_high}."
            new_stairstep = stair_step

            message_dict = {
                "Reference High": current_high,
                "Stair Step": stair_step,
                "Stop Price": stop_price,
                "Method": method,
                "Message": message1 + message2
            }
            log_action(asset=asset, action_type=4, message_dict=message_dict)

        # If price falls through the dynamic stop, transition from monitoring to exit.
        order_id = None
        if current_price <= stop_price:

            limit_price = stop_price * (1 - limit_offset)
            if not loss_point:
                limit_price = max(stop_price * (1 - limit_offset), purchase_price * (1 + win_amount))
            rounded_limit_price = round(limit_price, 2)

            client_id = f"NORM Limit-{ticker}-{rounded_limit_price}-{current_time}"
            message = f"Initiating sell for {ticker} at limit price {rounded_limit_price}"
            try:

                    api_response = place_order(
                        symbol=ticker,
                        qty=asset.get("Shares", 0),
                        side="sell",
                        type="limit",
                        time_in_force="day",
                        limit_price=rounded_limit_price,
                        extended_hours=True,
                        asset_id=client_id
                    )
                    order_id = api_response.get("id")

                    master_logger.debug(api_response)
                    message_dict = {
                        "Order ID": order_id,
                        "Client Order ID": client_id,
                        "Sell Reason": "Normal",
                        "Sell Price": rounded_limit_price,
                        "Message": message,
                    }
                    log_action(asset=asset, action_type=5, message_dict=message_dict)
        
            except Exception as e:
                error_msg = f"Error placing sell order for {ticker}: {e}"
                send_discord_urgent("Master", "sell_check", error_msg)
                master_logger.error(error_msg + " " + traceback.format_exc())
                return None, None, None

        return order_id, new_high, new_stairstep
    
    except Exception as e:
        error_msg = f"Error in sell_check for {ticker if 'ticker' in locals() else 'Unknown'}: {e}"
        send_discord_urgent("Master", "sell_check", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())


def asset_cycle(current_time, settings):
    """Run one full monitoring cycle across all tracked assets."""
    try:
        
        # positions_check will read, update, and write Portfolio.json
        positions_check(current_time)
        
        portfolio_data = safe_read_json(portfolio_path, {})
        assets = portfolio_data.get("Assets", [])

        if assets is None:
            error_msg = "asset_cycle: assets is None after positions_check"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "asset_cycle", error_msg)
            return
                
        if len(assets) == 0:
            return

        snapshots = get_snapshot(assets)
        master_logger.debug(snapshots)
        
        if snapshots is None:
            error_msg = "asset_cycle: get_snapshot returned None"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "asset_cycle", error_msg)
            return
        
        if not isinstance(snapshots, dict):
            error_msg = f"asset_cycle: snapshots is not a dict, got {type(snapshots)}"
            master_logger.error(error_msg)
            send_discord_urgent("Master", "asset_cycle", error_msg)
            return

        for asset in assets:
            if asset is None:
                error_msg = "asset_cycle: Found None asset in assets list"
                master_logger.error(error_msg)
                send_discord_urgent("Master", "asset_cycle", error_msg)
                continue

            time.sleep(0.1)  # Small delay to avoid rate limits
            
            ticker = asset.get("Ticker", "Unknown")
            status = asset.get("Status", 0)

            days_left = asset.get("Days Left", 0)
            if days_left < 0:
                continue
            
            try:
                if status == 1:
                    master_logger.debug(f"asset_cycle: Processing sell_check for {ticker}")
                    id, new_high, new_stairstep = sell_check(asset, snapshots, current_time, settings)
                    if new_high:
                        master_logger.debug(f"asset_cycle: Updating reference high for {ticker}: ${new_high:.2f}")
                        asset["Reference High"] = new_high
                        asset["Stair Step"] = new_stairstep
                    if id:
                        master_logger.info(f"Placed sell order for {ticker} with Order ID: {id}")
                        asset["Status"] = 2
                        asset["Order ID"] = id
                    else:
                        continue
                elif status == 0:
                    master_logger.debug(f"asset_cycle: Processing loss_probability for {ticker}")
                    new_initiation_price, loss_time = loss_probability(asset, snapshots, current_time, settings)
                    if new_initiation_price and loss_time:
                        master_logger.info(f"asset_cycle: Updating initiation price for {ticker}: ${new_initiation_price:.2f}")
                        asset["Initiation Point Price"] = new_initiation_price
                        asset["Loss Point Time"] = loss_time
                        continue
                    else:
                        master_logger.debug(f"asset_cycle: Checking initiation point for {ticker}")
                        initiation_time = initiation_point_check(asset, snapshots, current_time)
                        if initiation_time:
                            master_logger.info(f"asset_cycle: {ticker} reached initiation point")
                            asset["Status"] = 1
                            asset["Initiation Point Time"] = initiation_time
            except Exception as e:
                error_msg = f"Error processing asset {ticker}: {e}"
                send_discord_urgent("Master", "asset_cycle", error_msg)
                master_logger.error(error_msg + " " + traceback.format_exc())
    
        # Write back the updated assets
        portfolio_data["Assets"] = assets
        safe_write_json(portfolio_path, portfolio_data)
    
    except Exception as e:
        error_msg = f"Error in asset_cycle: {e}"
        send_discord_urgent("Master", "asset_cycle", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())


def day_run(settings):
    """Execute the intraday trading loop until the configured market end time."""
    try:
        master_logger.info("day_run: Starting day run")
        market_start, ml_time, market_end_time, sell_expired_time, market_day_end = get_trading_times()
        master_logger.debug(f"day_run: Trading times - ML: {ml_time}, Market End: {market_end_time}, Sell Expired: {sell_expired_time}, Day End: {market_day_end}")

        # Repeat the monitoring loop during market hours and trigger scheduled tasks.
        while True:
            try:
                current_time = et_now()
                current_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
                
                if current_time > market_end_time:
                    master_logger.info("Market end time reached. Exiting day run.")
                    break
                
                loop_start = time.time()
                asset_cycle(current_time, settings)

                portfolio_data = safe_read_json(portfolio_path, {})
                portfolio_data["Time"] = current_time_str

                if not portfolio_data.get("PastExpiredRan", True):
                    if current_time >= market_start and current_time < market_day_end:
                        master_logger.debug("day_run: Removing past expired assets")
                        portfolio_data["PastExpiredRan"] = True
                        safe_write_json(portfolio_path, portfolio_data)
                        past_expired_thread = threading.Thread(
                            target=remove_past_expired,
                            daemon=True
                        )
                        past_expired_thread.start()

                if not portfolio_data.get("SellExpiredRan", True):
                    if current_time >= sell_expired_time and current_time < market_day_end:
                        master_logger.debug("day_run: Selling expired assets")
                        portfolio_data["SellExpiredRan"] = True
                        sell_expired_thread = threading.Thread(
                            target=sell_expired_assets, 
                            args=(portfolio_data,), 
                            daemon=True
                        )
                        sell_expired_thread.start()

                if not portfolio_data.get("MLRan", True):
                    if current_time >= ml_time and current_time < market_day_end:
                            master_logger.debug("ML Running ML Analysis")
                            portfolio_data = safe_read_json(portfolio_path, {})
                            portfolio_data["MLRan"] = True
                            safe_write_json(portfolio_path, portfolio_data)
                            ml_thread_ = threading.Thread(
                                target=run_ml_analysis,
                                daemon=True
                            )
                            ml_thread_.start()
                
                safe_write_json(portfolio_path, portfolio_data)
                
                    
                # Calculate elapsed time and wait remaining time to reach 15 seconds
                elapsed = time.time() - loop_start
                remaining = 15.0 - elapsed
                
                if remaining > 0:
                    # Wait for the remaining time, but check stop_event periodically
                    sleep_interval = min(1.0, remaining)
                    while remaining > 0:
                        time.sleep(sleep_interval)
                        remaining -= sleep_interval
                        sleep_interval = min(1.0, remaining)
            
            except Exception as e:
                error_msg = f"Error in day_run loop iteration: {e}"
                send_discord_urgent("Master", "day_run", error_msg)
                master_logger.error(error_msg + " " + traceback.format_exc())
                time.sleep(15)  # Wait before next iteration
    
    except Exception as e:
        error_msg = f"Error in day_run: {e}"
        send_discord_urgent("Master", "day_run", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())


def end_of_day():
    """Mark the trading day as complete so the next session can reinitialize cleanly."""
    portfolio_data = safe_read_json(portfolio_path, {})
    portfolio_data["Startup"] = False
    safe_write_json(portfolio_path, portfolio_data)


def master():
    """Main scheduler that decides whether to run the market loop or wait for reactivation."""
    calendar_path = os.path.join(M_PATH, "Calendar.json")
    master_logger.info("Starting new iteration")
    try:
        while True:
            try:
                s = load_settings()
                day_startup()
                today = et_now().strftime("%Y-%m-%d")
                master_logger.info(f"Starting new day run for date: {today[0]}")
                calendar_data = safe_read_json(calendar_path, {})
                
                if calendar_data is None:
                    error_msg = "calendar_data is missing"
                    master_logger.error(error_msg)
                    send_discord_urgent("Master", "master", error_msg)
                    time.sleep(3600)  # Wait 1 hour before retry
                    continue
                
                today_str = et_now().strftime("%Y-%m-%d")
                today_info = calendar_data.get(today_str, [])
                
                if today_info is None:
                    error_msg = f"today_info is missing for {today_str}"
                    master_logger.error(error_msg)
                    send_discord_urgent("Master", "master", error_msg)
                    today_info = []
                closed = today_info[0].get("Closed", "1") if today_info else "1"
                special = today_info[0].get("Special", "0") if today_info else "0"
                master_logger.info(f"master: Market status - Closed: {closed}, Special: {special}")
                
                # Branch between active trading days and closed-market waiting periods.
                if closed == "0":
                    master_logger.debug("Market is open today. Starting day run.")
                    update_days_left()
                    day_run(settings=s)
                    reset_data()
                    ml_filter()
                    end_of_day()
                else:
                    end_of_day()
                    master_logger.debug("Market is closed today. Waiting for next activation.")


                portfolio_data = safe_read_json(portfolio_path, {})
                next_activation_str = portfolio_data.get("Next Activation", None)
                if next_activation_str:
                    master_logger.debug("Next Activation time:", next_activation_str)
                    next_activation = datetime.fromisoformat(next_activation_str)
                    # Make next_activation timezone-aware (ET timezone)
                    if next_activation.tzinfo is None:
                        et_tz = tz.gettz('America/New_York')
                        next_activation = next_activation.replace(tzinfo=et_tz)
                    
                    wait_seconds = (next_activation - et_now()).total_seconds()
                    if wait_seconds > 0:
                        master_logger.debug(f"Waiting for next activation at {next_activation_str} ET ({wait_seconds/3600:.2f} hours)")
                        time.sleep(wait_seconds + 10)  # Sleep until just after next activation
            
            except Exception as e:
                error_msg = f"Error in master loop iteration: {e}"
                send_discord_urgent("Master", "master", error_msg)
                master_logger.error(error_msg + " " + traceback.format_exc())
                time.sleep(3600)  # Wait 1 hour before retry
                
    except Exception as e:
        error_msg = f"Unexpected error in master loop: {e}"
        send_discord_urgent("Master", "master", error_msg)
        master_logger.error(error_msg + " " + traceback.format_exc())


master()


        


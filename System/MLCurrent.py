import os
import json
import pandas as pd
import numpy as np
import time
import logging
import requests
from sklearn.linear_model import LinearRegression
from dateutil.relativedelta import relativedelta
from datetime import date, timedelta
from dotenv import load_dotenv, find_dotenv
from Tools import safe_read_json, et_now
from Alpaca import get_all_orders, snapshot_data, get_account, place_order, get_positions, historic_bars
from Webhook import send_discord_urgent

dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

# ── File paths ───────────────────────────────────────────────────────────────
BASE_DIR = os.getenv("Model_Path")

# Establishing Logging variables
mlc_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("MLCurrent_Output.log")
mlc_logger.setLevel(logging.INFO)
mlc_logger.addHandler(file_handler)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s:%(message)s")

# ---- Helpers ----

def load_settings(filepath):
    """Parse Settings.txt into a dict of floats."""
    settings = {}
    with open(filepath, 'r') as f:
        for line in f:
            if '=' in line:
                key, val = line.strip().split('=', 1)
                settings[key] = float(val)
    return settings

def compute_dataframe(symbol):
    """
    Return a cleaned DataFrame or None if the file/data is invalid.
    Modified to use weighted volume calculation like MLHist-FunctionRun.py
    """
    from Alpaca import historic_bars
    from Tools import et_now

    start_date = (et_now() - timedelta(days=365)).strftime("%Y-%m-%d") 
    end_date = et_now().strftime("%Y-%m-%d")

    try:

        data = historic_bars(
            symbol=symbol,
            start=start_date,
            end=end_date,
            timeframe="1Day",
            )
        price_data = data.get("bars", [])
        mlc_logger.debug(f"{symbol}: returned {len(price_data)} day(s)")
        
        if price_data is None or len(price_data) == 0:
            logging.warning(f"No data returned for {symbol}")
            return None

        if price_data is None or len(price_data) == 0:
            logging.warning(f"\nNo bars data returned for {symbol}")
            mlc_logger.error(f"Data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            return None
        

    except Exception as e:
        mlc_logger.error(f"Error fetching data for {symbol}: {e}")
        return None

    # 3) Build DataFrame
    df = pd.DataFrame(price_data)
    try:
        df['t'] = pd.to_datetime(df['t'])
        df.set_index('t', inplace=True)
        df.sort_index(inplace=True)
    except Exception:
        return None

    # 5) Normalize columns
    df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
    required = ['o','h','l','c', 'v']

    # 6) Convert & compute features
    try:
        for col in required:
            df[col] = df[col].astype(float)
        # Calculate avg (same as original)
        df['avg'] = (df['h'] + df['l']) / 2
        
        # Calculate percentage change (same as original)
        df['pct_change'] = df['avg'] / df['avg'].shift(1) - 1

        # Get Close for today
        today_data = df.iloc[-1].to_dict()
        today_close = today_data['c']

        
        # Calculate y for training (next day's average)
        df['y'] = df['avg'].shift(-1)
        
        # Replace infinite values with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
    except Exception:
        return None

    return df, today_close

def get_current_forecast(df_full):
    """
    Get today's forecast using the same logic as MLHist-FunctionRun.py.
    Uses 1 year of training data before the most recent date to predict today.
    """
    if df_full is None or len(df_full) == 0:
        return None
    
    # Get the most recent date (today)
    current_date = df_full.index.max()
    
    # Prepare training window (1 year before current_date)
    train_start = current_date - relativedelta(years=1)
    train_df = df_full.loc[train_start : current_date - pd.Timedelta(days=1)].dropna(
        subset=['avg','rel_vol','pct_change','y'])
    
    # Calculate forecast if sufficient training data
    if len(train_df) >= 2:
        try:
            X = train_df[['avg','pct_change']].values
            y = train_df['y'].values
            model = LinearRegression().fit(X, y)
            
            # Get current day features
            current_row = df_full.loc[current_date]
            x_today = current_row[['avg','rel_vol','pct_change']].values.reshape(1,-1)
            
            # Make prediction
            y_hat = model.predict(x_today)[0]
            
            # Safety check: skip if avg is zero or near-zero to avoid divide by zero
            if current_row['avg'] > 0.001:
                forecast = (y_hat / current_row['avg']) - 1
                
                if not pd.isna(forecast):
                    return float(forecast)
        except:
            logging.warning("Current Prediction Failed")
            pass  # Skip forecast if calculation fails
    
    return None

def run_multilinear_analysis(settings):
    """Run multilinear analysis on tickers from FilteredTickers.json."""
    multilinear_tickers = []
    
    # Load FilteredTickers and run analysis
    f_tickers_path = os.path.join(BASE_DIR,"FilteredTickers.json")
    if not os.path.isfile(f_tickers_path):
        mlc_logger.error(f"FilteredTickers.json not found at {f_tickers_path}")
        return multilinear_tickers
    
    try:
        with open(f_tickers_path, 'r') as f:
            f_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        mlc_logger.error("Error loading FilteredTickers.json")
        return multilinear_tickers
    
    # FilteredTickers.json is now a simple list
    if not isinstance(f_data, list):
        mlc_logger.error("FilteredTickers.json is not in expected list format")
        return multilinear_tickers
    
    
    # Extract all ticker info objects from the nested structure
    all_tickers = f_data
    
    mlc_logger.debug(f"Processing {len(all_tickers)} tickers from FilteredTickers.json for multilinear analysis...")
    
    # Process each ticker in FilteredTickers with multilinear analysis
    ticker_error = 0
    for ticker_info in all_tickers:
        if not isinstance(ticker_info, dict):
            ticker_error += 1
            continue
            
        ticker = ticker_info.get('Ticker', '')
        if not ticker:
            ticker_error += 1
            continue
            
        mlc_logger.debug(f"  → Processing {ticker}...", end='', flush=True)
                
        # Step 2: Compute dataframe using MLHist-FunctionRun logic
        df_full, today_close = compute_dataframe(ticker)
        
        if df_full is None:
            mlc_logger.debug(" Data processing failed - SKIP")
            ticker_error += 1
            continue

        if len(df_full) < float(settings.get('MLCurrentDayMin', 240)):
            mlc_logger.debug(f"{ticker}: Not enough data ({len(df_full)} rows) - SKIP")
            continue

        # Step 3: Get today's forecast
        forecast = get_current_forecast(df_full)
        
        if forecast is None:
            mlc_logger.debug(" Forecast calculation failed - SKIP")
            ticker_error += 1
            continue
        
        # Step 4: Compare against ticker's specific PercentileBoundary_Threshold
        ticker_threshold = ticker_info.get('PercentileBoundary')
        if ticker_threshold is None:
            mlc_logger.debug(" No PercentileBoundary found - SKIP")
            ticker_error += 1
            continue
            
        if forecast > ticker_threshold:
            # Include ticker info with forecast and current price
            ticker_with_forecast = ticker_info.copy()
            ticker_with_forecast['forecast'] = forecast
            ticker_with_forecast['CurrentPrice'] = today_close  # Add current price from API data
            ticker_with_forecast['Score'] = ticker_info.get('Score')
            
            multilinear_tickers.append({
                'ticker': ticker,
                'forecast': forecast,
                'info': ticker_with_forecast,
            })
            mlc_logger.debug(f" forecast={forecast:.4f} > {ticker_threshold:.4f} PASS (Price: ${today_close:.2f})")
        
        # Small delay to respect API rate limits
        time.sleep(0.1)
    error_ticker_rate = (ticker_error / len(all_tickers)) * 100 if len(all_tickers) > 0 else 0

    if error_ticker_rate > float(settings.get('ErrorRatio', 10.0)):
        logging.warning(f"Warning: High ticker error rate detected: {error_ticker_rate:.2f}%")
        return []
    
    mlc_logger.info(f"Found {len(multilinear_tickers)} tickers passing multilinear test")
    mlc_logger.info(f"Tickers that passed: {[t['ticker'] for t in multilinear_tickers]}")
    return multilinear_tickers

# ---- Trading Regulation Compliance ----

def get_todays_spent_tickers():
    """
    Fetch today's closed orders from Alpaca API and return list of tickers that were sold today.
    This prevents repurchasing stocks that were already sold today (trading regulation compliance).
    """
    try:
        # Get today's date in YYYY-MM-DD format
        today_date = date.today().strftime("%Y-%m-%d")
        
        # Alpaca API credentials
        api_headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": os.getenv("ALPACA_PUBLIC_KEY"),
            "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY"),
        }
        
        # API URL to get closed orders from today
        try:
            orders = get_all_orders(
                status="closed",
                after=today_date
            )
            positions = get_positions()
        except Exception as e:
            send_discord_urgent("MLCurrent", "get_todays_spent_tickers", f"Error fetching orders: {e}")
            mlc_logger.error(f"Error fetching orders: {e}")
            return [], True
                        
        # Extract tickers that were sold today
        sold_tickers = set()
        for order in orders:
            sold_tickers.add(order["symbol"])
        for position in positions:
            if position["symbol"] not in sold_tickers:
                sold_tickers.add(position["symbol"])
        
        sold_tickers_list = list(sold_tickers)
        mlc_logger.info(f"Found {len(sold_tickers_list)} unique tickers sold today: {sold_tickers_list}")
        
        return sold_tickers_list, False
        
    except requests.exceptions.RequestException as e:
        send_discord_urgent("ERROR", "MLCurrent", f"Error fetching orders from Alpaca API: {e}")
        mlc_logger.error(f"Error fetching orders from Alpaca API: {e}")
        return [], True
    except Exception as e:
        send_discord_urgent("ERROR", "MLCurrent", f"Unexpected error in get_todays_spent_tickers: {e}")
        mlc_logger.error(f"Unexpected error in get_todays_spent_tickers: {e}")
        return [], True

def filter_out_sold_tickers(filtered_tickers, sold_tickers):
    """
    Remove any tickers from filtered_tickers that were sold today.
    This ensures compliance with trading regulations.
    """
    if not sold_tickers:
        mlc_logger.info("No sold tickers to filter out.")
        return filtered_tickers
    
    mlc_logger.info(f"\nFiltering out {len(sold_tickers)} tickers sold today...")
    
    original_count = len(filtered_tickers)
    filtered_out = []
    
    # Filter out sold tickers
    remaining_tickers = []
    for ticker_data in filtered_tickers:
        ticker = ticker_data.get('ticker', '')
        if ticker in sold_tickers:
            filtered_out.append(ticker)
        else:
            remaining_tickers.append(ticker_data)
    
    mlc_logger.debug(f"Filtered out {len(filtered_out)} tickers that were sold today: {filtered_out}")
    mlc_logger.info(f"Remaining tickers: {len(remaining_tickers)} (was {original_count})")
    
    return remaining_tickers

def purchase_tickers(compliant_tickers, settings):
    """Purchase compliant tickers based on available cash and portfolio slots."""
    mlc_logger.debug("\n Beginning Purchase Process...")
    if not compliant_tickers:
        mlc_logger.debug("No compliant tickers to purchase.")
        return
    
    # Get account information from Alpaca API
    account_info = get_account()
    cash_value = float(account_info.get("cash", 0))
    cash_min_buffer = settings.get("CashBuffer", 15)
    cash_available = cash_value - cash_min_buffer
    
    if cash_available <= 0:
        mlc_logger.info("Insufficient cash available for purchases after buffer.")
        return
    
    
    portfolio_path = os.path.join(BASE_DIR, "Portfolio.json")
    portfolio_data = safe_read_json(portfolio_path, {})
    assets = portfolio_data.get("Assets", [])
    asset_count = len(assets)
    max_assets = settings.get("MaximumPortfolio", 50)
    max_slots = settings.get("MaxAvailableSlots", 10)

    # Ensure slot counts are ints before using them for slicing
    available_slots = int(min(max_assets - asset_count, max_slots, len(compliant_tickers)))

    mlc_logger.info(f"Cash Available for Purchases: ${cash_available:.2f}")

    # If Cash Available is less than $20, determine maximum purchasable slots to have more than $1.50 per asset
    if cash_available < 20:
        max_purchasable_slots = int(cash_available // 1.5)
        available_slots = min(available_slots, max_purchasable_slots)
    
    # If no slots are available, exit
    if available_slots <= 0:
        mlc_logger.info("Portfolio is full, cannot purchase more assets.")
        return
    
    mlc_logger.info(f"Available Slots for Purchases: {available_slots}")
    
    
    # Sort compliant tickers by Score descending
    sorted_tickers = sorted(compliant_tickers, key=lambda x: x.get("info", {}).get("Score", 0), reverse=True)

    # Collect Top N tickers based on available slots
    filtered_tickers = sorted_tickers[:available_slots]
    
    if len(sorted_tickers) == 0:
        mlc_logger.info("No tickers available after sorting.")
        return
    
    # Define Rank Based Bonuses
    rank_1_bonus = 2
    rank_2_bonus = 1
    total_bonus = rank_1_bonus + rank_2_bonus

    # Define Portion Calculation
    portion = cash_available / (available_slots + rank_1_bonus + total_bonus)
        
    # Get snapshot data
    ticker_symbols = ','.join([t['ticker'] for t in filtered_tickers])
    snapshot = snapshot_data(ticker_symbols)

    start_date = (et_now() - timedelta(days=30)).strftime("%Y-%m-%d") 
    end_date = et_now().strftime("%Y-%m-%d")

    trade_averages = {}
    for ticker_dict in filtered_tickers:
        ticker = ticker_dict['ticker']
        data = historic_bars(
            symbol=ticker,
            start=start_date,
            end=end_date,
            timeframe="1Day",
            )
        
        trades = 0
        price_data = data.get("bars", [])
        if not price_data:  # Avoid division by zero
            mlc_logger.debug(f"No price data available for {ticker}, skipping trade average calculation.")
            continue
            
        for day in price_data:
            trades += float(day["n"])

        average_trades = trades / len(price_data)
        trade_averages[ticker] = average_trades * settings.get("MaxShareFactor")


    
    if not snapshot:
        logging.warning("Failed to get snapshot data.")
        return
        
    for rank, tickers in enumerate(filtered_tickers, start=1):
        if rank == 1:
            alotted_cash = portion + (rank_1_bonus * portion)
        elif rank == 2:
            alotted_cash = portion + (rank_2_bonus * portion)
        else:
            alotted_cash = portion
        ticker = tickers["ticker"]
        
        # Handle snapshot as dictionary or list
        ticker_data = None
        if isinstance(snapshot, dict):
            ticker_data = snapshot.get(ticker, {})
        elif isinstance(snapshot, list):
            # Find ticker in list by matching symbol field
            ticker_data = next((item for item in snapshot if item.get("symbol") == ticker), None)
        
        if not ticker_data:
            logging.warning(f"Ticker {ticker} not found in snapshot data, skipping...")
            continue
        
        # Try different possible price field names
        current_price = None
        if isinstance(ticker_data, dict):
            latest_trade = ticker_data.get("latestTrade", {})
            current_price = latest_trade.get("p")
            
            # Fallback to other possible price field names
            if current_price is None:
                current_price = ticker_data.get("price") or ticker_data.get("lastPrice")
        
        if current_price is None:
            logging.warning(f"No price data available for {ticker}, skipping...")
            continue
        
        # Round down shares to the nearest 8th decimal place
        max_shares_by_cash = alotted_cash / current_price
        trade_limit = trade_averages.get(ticker, float('inf'))  # No limit if not found
        shares = min(max_shares_by_cash, trade_limit)
        rounded_shares = round(shares, 8)
        
        mlc_logger.info(f"Placing order: Buy {rounded_shares} shares of {ticker} at ${current_price}")
        try:
            response = place_order(
                symbol=ticker,
                qty=rounded_shares,
                side="buy",
                type="market",
                time_in_force="day"
            )
            mlc_logger.debug(response)
        except Exception as e:
            mlc_logger.debug(f"Error placing order for {ticker}: {e}")
            send_discord_urgent("MLCurrent", "purchase_tickers", f"Error placing order for {ticker}: {e}")
        
        time.sleep(0.2)  # Small delay to respect API rate limits

# ---- Main pipeline ----

def ml_current():
    """Main execution function."""
    try: 

        # Load settings for multilinear analysis
        settings_path = os.path.join(BASE_DIR, "Settings.txt")
        if not os.path.isfile(settings_path):
            mlc_logger.critical(f"Settings.txt not found at {settings_path}")
            return multilinear_tickers
        
        settings = load_settings(settings_path)
        multilinear_tickers = run_multilinear_analysis(settings)
        if not multilinear_tickers:
            mlc_logger.info("No tickers passed multilinear analysis. Exiting.")
            return
        
        mlc_logger.debug("Checking for tickers sold today to prevent same-day repurchase...")
        
        # Get list of tickers sold today
        sold_tickers, error_flag = get_todays_spent_tickers()
        if error_flag:
            return
        
        # Filter out any tickers that were sold today
        compliant_tickers = filter_out_sold_tickers(multilinear_tickers, sold_tickers)
        
        # Save results

        mlc_logger.info(f"ML Current Analysis Complete")
        mlc_logger.info(f"Final selection: {len(compliant_tickers)} tickers")
        mlc_logger.debug(f"Original count: {len(multilinear_tickers)} tickers")
        mlc_logger.debug(f"Filtered out: {len(multilinear_tickers) - len(compliant_tickers)} tickers (sold today)")

        # Purchase compliant tickers
        purchase_tickers(compliant_tickers, settings)

    except Exception as e:
        mlc_logger.error(f"Error in ml_current(): {e}")
        send_discord_urgent("ERROR", "MLCurrent", f"Error in main(): {e}")


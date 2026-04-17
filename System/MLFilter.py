import os
import json
import pandas as pd
import numpy as np
import gc
import logging
from sklearn.linear_model import LinearRegression
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, timedelta
from dotenv import load_dotenv, find_dotenv
from Tools import safe_read_json, load_settings
from Webhook import send_discord_urgent
from Alpaca import get_positions

dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

BASE_DIR = os.getenv("Model_Path")

# Establishing Logging variables
mlf_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("MLFilter_Output.log")
mlf_logger.setLevel(logging.INFO)
mlf_logger.addHandler(file_handler)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s:%(message)s")

def compute_dataframe(symbol):
    """
    Return a cleaned DataFrame or None if the file/data is invalid.
    Modified to use weighted volume calculation like MLHist-FunctionRun.py
    """
    from Alpaca import historic_bars
    from Tools import et_now

    start_date = (et_now() - timedelta(days=365 * 4)).strftime("%Y-%m-%d") 
    end_date = et_now().strftime("%Y-%m-%d")

    try:

        data = historic_bars(
            symbol=symbol,
            start=start_date,
            end=end_date,
            timeframe="1Day",
            )
        
        price_data = data.get("bars", [])
        
        if price_data is None or len(price_data) == 0:
            mlf_logger.debug(f"No data returned for {symbol}")
            return None

        if price_data is None or len(price_data) == 0:
            mlf_logger.debug(f"\nNo bars data returned for {symbol}")
            logging.warning(f"Data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            return None
        
        mlf_logger.debug(f"\nReceived {len(price_data)} bars for {symbol}")

    except Exception as e:
        mlf_logger.error(f"Error fetching data for {symbol}: {e}")
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
        
        # Calculate y for training (next day's average)
        df['y'] = df['avg'].shift(-1)

        # Calculate 5-day high return (next 5 days' max high vs current avg)
        df['high_return'] = (df['h'].shift(-1).rolling(5).max() - df['avg']) / df['avg']
        
        # Replace infinite values with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
    except Exception:
        return None

    return df

def percentile_rank(value, array):
    """Rank value within array, capped so <min→0.001, >max→1."""
    if len(array) == 0 or pd.isna(value):
        return np.nan
    
    # Convert to numpy array if it's a list
    if isinstance(array, list):
        array = np.array(array)

    # Ensure Value is Numeric
    try:
        value = float(value)
    except (ValueError, TypeError):
        return np.nan
    
    try:
        # Ensure array is numeric and handle non-numeric values
        array = pd.to_numeric(array, errors='coerce')
        clean_array = array[~np.isnan(array)]
    except (ValueError, TypeError):
        return np.nan
    
    if len(clean_array) == 0:
        return np.nan
    
    if value < clean_array.min():
        return 0.001
    if value > clean_array.max():
        return 1.0
    return np.sum(clean_array <= value) / len(clean_array)



def process_ticker_filter(ticker, settings):
    """
    Process a single ticker to generate filter test results similar to MLHist-TestRun.py
    but focused on the most recent date evaluation. Optimized for memory efficiency.
    """
    
    # Load full dataframe
    df_full = compute_dataframe(ticker)
    if df_full is None:
        return None, "data processing failed", 1
    
    # Get settings values
    percentile_boundary = float(settings.get('PercentileBoundary'))
    instance_condition = float(settings.get('InstanceNumberCondition'))
    high_sum_condition = float(settings.get('HighSumValueCondition'))

    # Get Weighted Point System Ranges
    level1_range = float(settings.get('Level1Range'))
    level2_range = float(settings.get('Level2Range'))
    level3_range = float(settings.get('Level3Range'))
    level4_range = float(settings.get('Level4Range'))

    # Get Weighted Point System Values
    level1_points = float(settings.get('Level1Points'))
    level2_points = float(settings.get('Level2Points'))
    level3_points = float(settings.get('Level3Points'))
    level4_points = float(settings.get('Level4Points'))

    # Get Return Minimum and Ratio
    return_minimum = float(settings.get('ReturnMinimum'))
    return_ratio_condition = float(settings.get('ReturnRatioCondition'))

    # Determine test date (most recent date)
    most_recent_date = df_full.index.max()
    
    # Determine calculation start date (2 years after earliest to ensure training data)
    earliest_date = df_full.index.min()
    calc_start_date = earliest_date + relativedelta(years=2)
    
    # Check if most recent date is at least 1 year after calc start (for 1-year test window)
    test_start_date = most_recent_date - relativedelta(years=1)
    
    if test_start_date < calc_start_date:
        return None, "insufficient data for 1-year test window", 0
    
    # Get forecast dates and calculate forecasts efficiently
    forecast_dates = []
    forecasts = {}
    
    calc_dates = df_full.loc[calc_start_date:most_recent_date].index
    
    for current_date in calc_dates:
        # Prepare training window (1 year before current_date)
        train_start = current_date - relativedelta(years=1)
        train_df = df_full.loc[train_start : current_date - pd.Timedelta(days=1)]

        # Filter for required columns and drop NaN
        train_df = train_df.dropna(subset=['avg','pct_change','y'])
        
        # Calculate forecast if sufficient training data
        if len(train_df) >= 2:
            try:
                X = train_df[['avg','pct_change']].values
                y = train_df['y'].values
                model = LinearRegression().fit(X, y)
                
                # Get current day features
                current_row = df_full.loc[current_date]
                
                # Check for valid data before prediction
                if (not pd.isna(current_row['avg']) and 
                    not pd.isna(current_row['rel_vol']) and 
                    not pd.isna(current_row['pct_change']) and
                    current_row['avg'] > 0.001):
                    
                    x_today = current_row[['avg','rel_vol','pct_change']].values.reshape(1,-1)
                    y_hat = model.predict(x_today)[0]
                    forecast = (y_hat / current_row['avg']) - 1
                    
                    if not pd.isna(forecast):
                        forecast_dates.append(current_date)
                        forecasts[current_date] = forecast
            except:
                pass  # Skip forecast for this date if calculation fails
    
    if len(forecast_dates) == 0:
        return None, "no forecasts generated", 0
    
    # Filter forecast dates to test window (1 year before most recent date)
    # Ensure both dates are datetime objects for comparison
    test_dates = []
    for d in forecast_dates:
        if pd.to_datetime(d) >= pd.to_datetime(test_start_date):
            test_dates.append(d)

    if len(test_dates) == 0:
        return None, "no test dates in window", 0
        
    # Process test window to calculate metrics
    purchase_dates = []
    most_recent_percentile_boundary = np.nan  # Track the most recent percentile boundary value
    
    for test_date in test_dates:
        # Get 1-year window of forecasts (including current date)
        window_start = test_date - relativedelta(years=1)
        
        # Use list comprehension for better performance
        window_forecast_values = [forecasts[d] for d in forecast_dates 
                                if window_start <= d <= test_date and not pd.isna(forecasts[d])]
        
        if len(window_forecast_values) < 2:
            continue
        
        # Calculate percentile rank of current forecast
        current_forecast = forecasts[test_date]
        forecast_percentile = percentile_rank(current_forecast, window_forecast_values) * 100
        
        # Update the most recent percentile boundary value (for the most recent test date)
        # Calculate the 90th percentile threshold from the previous year's forecasts
        if test_date == most_recent_date:
            # Get forecasts from previous year (excluding current date)
            threshold_window_start = most_recent_date - relativedelta(years=1)
            threshold_forecast_values = [forecasts[d] for d in forecast_dates 
                                       if threshold_window_start <= d < most_recent_date and not pd.isna(forecasts[d])]
            
            if len(threshold_forecast_values) >= 2:
                most_recent_percentile_boundary = np.percentile(threshold_forecast_values, 90.0)
            else:
                most_recent_percentile_boundary = np.nan
        
        # Check if forecast meets percentile boundary
        if forecast_percentile >= percentile_boundary:
            purchase_dates.append(test_date)
    
    # Calculate test metrics
    instance_count = len(purchase_dates)
    
    if instance_count == 0:
        return None, f"no purchases (0 >= {percentile_boundary}%)", 0
    
    # Calculate High Sum Value using Weighted Point System
    weighted_sum = 0.0
    total_points = 0.0
    return_met_count = 0
    return_ratio = 0.0
    
    for purchase_date in purchase_dates:
        if purchase_date not in df_full.index:
            continue

        high_return_value = df_full.at[purchase_date, 'high_return']
        if pd.isna(high_return_value):
            continue

        if high_return_value > return_minimum:
            return_met_count += 1

        # Get 1 Year Window for Percentile Calculation
        percentile_window_start = purchase_date - relativedelta(years=1)
        
        # Get high_return values from dataframe for the window
        window_df = df_full.loc[percentile_window_start:purchase_date]
        high_values = window_df['high_return'].dropna().values

        
        if len(high_values) >= 2:
            high_percentile = percentile_rank(high_return_value, high_values)
            if not pd.isna(high_percentile):
                most_recent_dt = pd.to_datetime(most_recent_date)
                purchase_dt = pd.to_datetime(purchase_date)
                # Determine which level the purchase_date falls into
                days_diff = (most_recent_dt - purchase_dt).days

                if days_diff <= level1_range:
                    points = level1_points
                elif days_diff <= level2_range:
                    points = level2_points
                elif days_diff <= level3_range:
                    points = level3_points
                elif days_diff <= level4_range:
                    points = level4_points
                else:
                    points = 0.0
                
                if points > 0.0:
                    weighted_sum += high_percentile * 100 * points
                    total_points += points

    if total_points > 0.0:
        high_sum_value = weighted_sum / total_points
    else:
        return None, "no valid percentiles for high sum value", 0

    if return_met_count == 0:
        return None, "no purchases met 2.5% return", 0
    else:
        return_ratio = (return_met_count / instance_count) * 100

    mlf_logger.debug(f"{ticker}: high_sum={high_sum_value:.2f}%, return_ratio={return_ratio:.2f}%, instances={instance_count}")
        
    # Check if conditions pass
    passes_return = return_ratio >= return_ratio_condition
    passes_instance = instance_count >= instance_condition
    passes_high = high_sum_value >= high_sum_condition
    
    if passes_instance and passes_high and passes_return:
        stock_score = (high_sum_value + return_ratio) / 2
        result = {
            'Ticker': ticker,
            'Score': stock_score,
            'PercentileBoundary': most_recent_percentile_boundary,
        }
        
        # Clean up memory before returning
        del df_full, forecasts
        gc.collect()
        
        return result, None, 0
    
    # Build failure message
    failures = []
    if not passes_instance:
        failures.append(f"instances {instance_count} < {instance_condition}")
    if not passes_high:
        failures.append(f"high_sum_value {high_sum_value:.2f}% < {high_sum_condition}%")
    if not passes_return:
        failures.append(f"return_ratio {return_ratio:.2f}% < 80%")

    # Clean up memory before returning
    del df_full, forecasts
    gc.collect()
    
    return None, "; ".join(failures), 0


# ---- Main pipeline ----

def load_tickers_from_file(filepath):

    tickers = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
        return tickers
    except Exception as FileNotFoundError:
        mlf_logger.error(f"Error: Could not read tickers from {filepath}: {FileNotFoundError}")
        return []
    except Exception as e:
        mlf_logger.error(f"Error reading Tickers.txt: {e}")

def save_ticker_to_p1(filepath, ticker_data):
    """Add a single ticker to P1-Tickers.json file."""
    # Load existing data
    filtered = safe_read_json(filepath, [])
    
    # Ensue that it's a list
    if not isinstance(filtered, list):
        filtered = []
    
    # Add Ticker
    filtered.append(ticker_data)
    
    # Save immediately
    with open(filepath, 'w') as f:
        json.dump(filtered, f, indent=4)

def filter_owned_tickers(tickers):
    """Filter out tickers that are already owned in Portfolio.json."""
    positions = get_positions()

    owned_tickers = set()
    for position in positions:
        symbol = position.get('symbol')
        if symbol:
            owned_tickers.add(symbol)


    filtered_tickers = [t for t in tickers if t not in owned_tickers]
    mlf_logger.debug(f"Filtered out {len(tickers) - len(filtered_tickers)} owned tickers from Portfolio.json.")
    return filtered_tickers


def limit_p1_tickers_by_high_sum(filepath, max_tickers):
    """
    Limit P1-Tickers.json to keep only the top max_tickers based on Score.
    If there are more tickers than max_tickers, keep only the ones with highest Score.
    """
    # Load existing data
    filtered = safe_read_json(filepath, [])
    
    # Ensure it's a list
    if not isinstance(filtered, list):
        logging.eror("P1-Tickers.json is not in list format. No filtering applied.")
        return
    
    # Check if we need to limit
    if len(filtered) <= max_tickers:
        mlf_logger.debug(f"Total tickers ({len(filtered)}) is within limit ({max_tickers}). No filtering needed.")
        return
    
    # Sort by Score in descending order (highest first)
    filtered.sort(key=lambda x: x.get('Score', 0), reverse=True)
    
    # Keep only top max_tickers
    top_tickers = filtered[:max_tickers]
    
    # Save the limited data
    with open(filepath, 'w') as f:
        json.dump(top_tickers, f, indent=4)
    
    mlf_logger.debug(f"Limited P1-Tickers.json from {len(filtered)} to {len(top_tickers)} tickers based on Score.")
    
    # Show the range of Scores kept
    if top_tickers:
        highest_value = top_tickers[0].get('Score', 0)
        lowest_value = top_tickers[-1].get('Score', 0)
        mlf_logger.debug(f"Score range kept: {lowest_value:.2f} to {highest_value:.2f}")

def ml_filter():
    try:
        mlf_logger.debug("Starting MLFilter...")
        
        # 1) Load settings
        settings = load_settings()
        max_ml = int(settings.get('MaximumML', 500))

        # 2) Setup output directory and clear existing FilteredTickers.json
        f_filepath = os.path.join(BASE_DIR, "FilteredTickers.json")
        
        # Clear existing FilteredTickers.json at start
        with open(f_filepath, 'w') as f:
            json.dump([], f, indent=4)

        # 3) Load tickers from Tickers.txt
        try:
            tickers_filepath = os.path.join(BASE_DIR, "Tickers.txt")
            tickers = load_tickers_from_file(tickers_filepath)
        except Exception as e:
            send_discord_urgent("ERROR", "MLFilter", f"Error loading Tickers.txt: {e}")
            mlf_logger.error(f"Error loading Tickers.txt: {e}")
            return

        # Filter out tickers already owned in Portfolio.json
        tickers = filter_owned_tickers(tickers)

        if not tickers:
            logging.warning("No tickers found in Tickers.txt. Exiting.")
            return
        
        mlf_logger.debug(f"Loaded {len(tickers)} tickers from Tickers.txt.")

        total_tickers = 0
        passed_tickers = 0
        error_count = 0

        for ticker in tickers:
            total_tickers += 1
            mlf_logger.debug(f"\nProcessing {ticker}…", end='', flush=True)

            # Process ticker using new method
            result, error_msg, error_flag = process_ticker_filter(ticker, s)
            if error_flag:
                error_count += 1
                mlf_logger.debug(f"{ticker} Error: {error_msg}")
            
            if result is not None:
                # Add calculated metrics to the ticker info

                save_ticker_to_p1(f_filepath, result)
                passed_tickers += 1
                score = result.get('Score', 0.0)
                mlf_logger.debug(f" passed all filters. Score: {score:.2f}%")
            else:
                mlf_logger.debug(f" failed: {error_msg}")
            
            # Clear variables for memory efficiency
            del result, error_msg
            gc.collect()
        
        error_rate = (error_count / total_tickers) * 100 if total_tickers > 0 else 0
        if error_rate > float(s.get('ErrorRatio', 10.0)):
            send_discord_urgent("WARNING", "MLFilter", f"High error rate detected: {error_rate:.2f}% ({error_count} errors out of {total_tickers} tickers)")
            logging.warning(f"High error rate detected: {error_rate:.2f}% ({error_count} errors out of {total_tickers} tickers)")

        # Apply MaximumML limit - keep only top tickers by High_Sum_Value
        limit_p1_tickers_by_high_sum(f_filepath, max_ml)

        # Summary
        mlf_logger.debug("SUMMARY:")
        mlf_logger.debug(f"Total tickers processed: {total_tickers}")
        mlf_logger.debug(f"Tickers passed filters: {passed_tickers}")
        mlf_logger.debug(f"Tickers filtered out: {total_tickers - passed_tickers}")
    
    except Exception as e:
        send_discord_urgent("ERROR", "MLFilter", f"Error in main(): {e}")
        mlf_logger.error(f"Error in MLFilter: {e}")


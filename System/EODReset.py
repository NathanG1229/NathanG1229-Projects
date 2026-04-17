import os
import requests
import logging
import math
from datetime import datetime, date, timedelta
from dotenv import load_dotenv, find_dotenv
from Alpaca import historic_data
from Tools import load_settings

# Establishing Logging variables
eod_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("EOD_Output.log")
eod_logger.setLevel(logging.INFO)
eod_logger.addHandler(file_handler)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s:%(name)s:%(lineno)d:%(message)s")

dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

DIR_PATH = os.getenv("File_Path")

s = load_settings()

# Date limits
MIN_PRICE_DAYS = s.get("MinPriceDays")  # 3 years
MIN_AVERAGE_TRADES = s.get("MinTrades")  # Minimum average daily trades
BATCH_SIZE = s.get("BatchSize")  # Number of tickers to process in each batch


def get_price_data_from_alpaca(tickers_list):
    """Get price data for a ticker from Alpaca API and check if it has 3+ years and sufficient trades"""
    # Calculate date 4 years ago for maximum data retrieval
    start_date = (datetime.now() - timedelta(days=365 * 4)).strftime("%Y-%m-%d") 
    end_date = datetime.now().strftime("%Y-%m-%d")

    symbols_parameter = ",".join(tickers_list) if isinstance(tickers_list, list) else tickers_list
    try:
        bar_data = historic_data(
            symbols=symbols_parameter,
            start=start_date,
            end=end_date,
            timeframe="1Day",
            limit=10000
        )

        results = {}
        for ticker in (tickers_list if isinstance(tickers_list, list) else [tickers_list]):
            bars = bar_data.get("bars", {}).get(ticker, [])

            if not bars:
                results[ticker] = (None, False)
                continue
            
            # Convert to our format and check date range and trades
            price_data = []
            trades_counts = []
            
            for bar in bars:
                price_data.append({
                    "date": bar["t"][:10],  # Extract date from timestamp
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume_weighted": bar["vw"],
                    "trade_count": bar["n"],
                    "volume": bar["v"]
                })
                # Collect trades count (n value) for averaging
                if "n" in bar:
                    trades_counts.append(bar["n"])
            
            # Check if earliest day is more than 1095 days ago and if last close price is greater than $1
            if price_data:
                earliest_date_str = price_data[0]["date"]
                earliest_date = datetime.strptime(earliest_date_str, "%Y-%m-%d").date()
                today = date.today()
                days_of_data = (today - earliest_date).days
                
                last_close_price = price_data[-1]["close"]
                has_sufficient_data = days_of_data >= MIN_PRICE_DAYS and (last_close_price > 1)
                
                # Check average trades count
                has_sufficient_trades = True
                if trades_counts:
                    average_trades = sum(trades_counts) / len(trades_counts)
                    has_sufficient_trades = average_trades >= MIN_AVERAGE_TRADES
                    
                    # Log the trades information for debugging
                    eod_logger.debug(f"  {ticker}: {len(trades_counts)} days, avg trades: {average_trades:.0f}")
                else:
                    # If no trades data available, fail the filter
                    has_sufficient_trades = False
                    eod_logger.debug(f"  {ticker}: No trades data available")
                
                # Both conditions must be met
                passes_all_filters = has_sufficient_data and has_sufficient_trades
                results[ticker] = (price_data, passes_all_filters)
            else:
                results[ticker] = (None, False)
            
        return results
            
    except requests.RequestException as e:
        eod_logger.error(f"Warning: Alpaca price request failed for {ticker}: {e}")
        return {ticker: (None, False) for ticker in tickers_list}
    except Exception as e:
        eod_logger.error(f"Warning: Unexpected error getting price data for {ticker}: {e}")
        return {ticker: (None, False) for ticker in tickers_list}
    
    
def reset_data():
    """Save tickers data to Tickers.txt"""

    from Alpaca import download_raw_ciks, raw_asset_list

    M_PATH = os.getenv("Model_Path")
    
    TICKERS_PATH = os.path.join(
        M_PATH,
        "Tickers.txt"
        )
    
    asset_data = raw_asset_list()
    ticker_data = download_raw_ciks()

    try:
        os.makedirs(os.path.dirname(TICKERS_PATH), exist_ok=True)

        tradable_tickers = []

        with open(TICKERS_PATH, 'w') as f:

            for ticker_info in ticker_data:
                symbol = ticker_info.get("Symbol")
                tradable = None
                for asset in asset_data:
                    symbol_upper = symbol.upper()
                    if asset.get('symbol', ' ').upper() == symbol_upper:
                        if asset.get('tradable') != None and asset.get('status') == 'active':
                            tradable = asset.get('tradable')
                            fractionable = asset.get('fractionable', False)
                if tradable == False or tradable == None:
                    continue
                elif fractionable == None or fractionable == False:
                    continue
                else:
                    tradable_tickers.append({
                        "ticker": symbol
                    })
            eod_logger.debug(f"Found {len(tradable_tickers)} tradable tickers.")
            eod_logger.debug(f"Processing in batches of {BATCH_SIZE}...")

            total_batches = math.ceil(len(tradable_tickers) / BATCH_SIZE)
            processed_count = 0

            for batch_index in range(0, len(tradable_tickers), BATCH_SIZE):
                batch = tradable_tickers[batch_index:batch_index + BATCH_SIZE]
                batch_number = (batch_index // BATCH_SIZE) + 1


                batch_symbols = [t["ticker"] for t in batch]
                price_results = get_price_data_from_alpaca(batch_symbols)

                for symbol_info in batch:
                    symbol = symbol_info["ticker"]
                    price_data, passes_filters = price_results.get(symbol, (None, False))

                    if not passes_filters or price_data is None:
                        continue

                    # Write passing symbols to file
                    f.write(f"{symbol}\n")
                    processed_count += 1

        
        eod_logger.debug(f"Saved {len(ticker_data)} tickers to Tickers.txt")
        
    except Exception as e:
        eod_logger.critical(f"ERROR: Failed to save Tickers.txt: {e}")

def main():
    reset_data()

if __name__ == "__main__":
    main()

        
#!/usr/bin/env python3
"""
alpaca_order.py - Alpaca trading order placement
Cross-platform compatible (Windows/Linux)
"""

import requests
import sys
import argparse
import os
import json
from datetime import datetime

from dotenv import load_dotenv, find_dotenv

# Cross-platform environment loading with fallback
dotenv_path = find_dotenv()
if not dotenv_path:
    # Fallback for Linux - look in common locations
    possible_paths = [
        os.path.expanduser("~/.env"),
        os.path.join(os.path.expanduser("~/stock_data"), ".env"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    ]
    for path in possible_paths:
        if os.path.exists(path):
            dotenv_path = path
            break

if dotenv_path:
    load_dotenv(dotenv_path, override=True)
    print(f"Loaded environment from: {dotenv_path}")
else:
    print("Warning: No .env file found. Make sure environment variables are set.")

ALPACA_KEY = os.getenv("ALPACA_PUBLIC_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY")
ALPACA_PAPER_KEY = os.getenv("ALPACA_PAPER_PUBLIC_KEY")
ALPACA_PAPER_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")

# Validate required environment variables
if not ALPACA_KEY or not ALPACA_SECRET:
    print("Error: ALPACA_PUBLIC_KEY and ALPACA_SECRET_KEY must be set in environment variables", file=sys.stderr)
    print("Please check your .env file or environment configuration", file=sys.stderr)
    sys.exit(1)


ASSETS_URL_BASE = "https://api.alpaca.markets/v2/assets"
ASSETS_URL_PAPER = "https://paper-api.alpaca.markets/v2/assets"
ORDER_URL_BASE = "https://api.alpaca.markets/v2/orders"
ORDER_URL_PAPER = "https://paper-api.alpaca.markets/v2/orders"
ACCOUNT_URL_BASE = "https://api.alpaca.markets/v2/account"
ACCOUNT_URL_PAPER = "https://paper-api.alpaca.markets/v2/account"
POSITIONS_URL_BASE = "https://api.alpaca.markets/v2/positions"
POSITIONS_URL_PAPER = "https://paper-api.alpaca.markets/v2/positions"

HEADERS = {
    "accept":        "application/json",
    "content-type":  "application/json",
    "APCA-API-KEY-ID":     ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

PAPER_HEADERS = {
    "accept":        "application/json",
    "content-type":  "application/json",
    "APCA-API-KEY-ID":     ALPACA_PAPER_KEY,
    "APCA-API-SECRET-KEY": ALPACA_PAPER_SECRET
}

# ─── UTILITY FUNCTIONS ──────────────────────────────────────────────────────────

def validate_environment():
    """
    Validate that the required environment variables are properly configured.
    Returns True if valid, False otherwise.
    """
    issues = []
    
    if not ALPACA_KEY:
        issues.append("ALPACA_PUBLIC_KEY is not set")
    elif len(ALPACA_KEY) < 10:
        issues.append("ALPACA_PUBLIC_KEY appears to be invalid (too short)")
    
    if not ALPACA_SECRET:
        issues.append("ALPACA_SECRET_KEY is not set")
    elif len(ALPACA_SECRET) < 10:
        issues.append("ALPACA_SECRET_KEY appears to be invalid (too short)")
    
    if issues:
        print("Environment validation failed:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        return False
    
    return True

def test_connection():
    """
    Test connection to Alpaca API by making a simple request.
    Returns True if successful, False otherwise.
    """
    try:
        # Test with a simple account info request
        test_url = "https://api.alpaca.markets/v2/account"
        response = requests.get(test_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}", file=sys.stderr)
        return False
    

# ─── ASSET LIST FUNCTIONS ──────────────────────────────────────────────────────────
def raw_asset_list(
        status: str = "active",
        asset_class: str = None,
        exchange: str = None,
        output_filename: str = "RawAssets.json"
        ) -> dict:
    """
    Fetch the list of assets from Alpaca.
    
    Optional:
      status       – Asset status filter: "active", "inactive", "delisted" (default: "active")
      asset_class - us_equity, crypto, forex
      exchange - Optional AMEX, ARCA, BATS, NYSE, NASDAQ, NYSEARCA or OTC
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    try:
        params = {"status": status,
                  "asset_class": asset_class,
                  "exchange": exchange
                }
        response = requests.get(ASSETS_URL_BASE, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        # Determine the output path (use Model_Path if available)
        base_dir = os.getenv("Model_Path")
        if not base_dir:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        
        output_path = os.path.join(base_dir, output_filename)
        
        # Save to file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, indent=2)
        
        print(f"✓ Successfully saved asset list to {output_path}")
        print(f"  Total assets: {len(response.json())}")
        
        return response.json()
    
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching asset list: {e}")
    
def download_raw_ciks(output_filename: str = "RawCIKs.json"):
    """
    Download SEC company tickers exchange data and save as JSON file.
    
    Args:
        output_filename: Name of the output JSON file (default: "RawCIKs.json")
    
    Returns:
        dict: The downloaded JSON data
    
    Raises:
        ConnectionError: If unable to connect to SEC website
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If SEC returns an error
        Exception: For any other unexpected errors
    """
    # SEC requires a User-Agent header
    headers = {
        "User-Agent": "Nathan Goff jollymountainman0403@outlook.com"  # Replace with your info
    }
    
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    
    try:
        print(f"Downloading SEC CIK data from {url}...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse the JSON data
        data = response.json()
        ticker_data = data.get('data', [])

        transformed_data = []
        for row in ticker_data:
            if len(row) >= 4:
                if row[3] == "OTC":
                    continue
                else:
                    record = {
                        "Symbol": row[2],
                        "Name": row[1],
                        "Exchange": row[3],
                        "CIK": row[0]
                    }
                    transformed_data.append(record)
        
        # Determine the output path (use Model_Path if available)
        base_dir = os.getenv("Model_Path")
        if not base_dir:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        
        output_path = os.path.join(base_dir, output_filename)
        
        # Save to file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2)
        
        print(f"✓ Successfully saved CIK data to {output_path}")
        print(f"  Total records: {len(data.get('data', []))}")

        return transformed_data
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to SEC website. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to SEC website timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = f" Status: {response.status_code}"
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text[:200]}"
        raise requests.exceptions.HTTPError(f"SEC website returned error {response.status_code}.{error_detail}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON response from SEC: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error downloading SEC CIK data: {e}")    

# ─── STOCK DATA FUNCTIONS ──────────────────────────────────────────────────────────

def historic_data(
    symbols: str,
    start: str = None,
    end: str = None,
    timeframe: str = None,
    limit: int = None,
    page_token: str = None,
    sort: str = "asc",
    feed: str = "sip"
) -> dict:
    """
    Fetch historical bar data from Alpaca.
    
    Required:
      symbols      – Comma-separated list of symbols (e.g., "AAPL,MSFT")
    
    Optional:
      start        – Start time (RFC-3339 format, e.g., "2022-01-01T00:00:00Z")
      end          – End time (RFC-3339 format, e.g., "2022-12-31T23:59:59Z")
      timeframe    – Bar timeframe: "1Min", "5Min", "15Min", "30Min", "1Hour", "1Day", "1Week", "1Month"
      limit        – Number of bars to return (max 10000)
      page_token   – Token for pagination
      sort         – Sort order: "asc" or "desc"
      feed         – Data feed: "iex" or "sip"
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # Build the data API URL
    data_url = "https://data.alpaca.markets/v2/stocks/bars"
    
    # Build headers for data API
    data_headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET
    }
    
    # Build query parameters, dropping None values
    params = {
        "symbols": symbols,
        "start": start,
        "end": end,
        "timeframe": timeframe,
        "limit": str(limit) if limit is not None else None,
        "adjustment": "all",
        "page_token": page_token,
        "sort": sort,
        "feed": feed
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(data_url, params=params, headers=data_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca Data API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca Data API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca Data API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching historical data: {e}")
    
def historic_bars(
    symbol: str,
    start: str = None,
    end: str = None,
    timeframe: str = None,
    limit: int = None,
    page_token: str = None,
    sort: str = "asc",
    feed: str = "sip"
) -> dict:
    """
    Fetch historical bar data from Alpaca.
    
    Required:
      symbols      – Comma-separated list of symbols (e.g., "AAPL,MSFT")
    
    Optional:
      start        – Start time (RFC-3339 format, e.g., "2022-01-01T00:00:00Z")
      end          – End time (RFC-3339 format, e.g., "2022-12-31T23:59:59Z")
      timeframe    – Bar timeframe: "1Min", "5Min", "15Min", "30Min", "1Hour", "1Day", "1Week", "1Month"
      limit        – Number of bars to return (max 10000)
      page_token   – Token for pagination
      sort         – Sort order: "asc" or "desc"
      feed         – Data feed: "iex" or "sip"
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """

    # Load environment variables
    from dotenv import load_dotenv, find_dotenv
    dotenv = find_dotenv()
    load_dotenv(dotenv, override=True)

    # Build the data API URL
    data_url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    
    # Build headers for data API
    data_headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": os.getenv("ALPACA_PUBLIC_KEY"),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY")
    }
    
    # Build query parameters, dropping None values
    params = {
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "limit": limit if limit is not None else None,
        "adjustment": "all",
        "feed": feed,
        "page_token": page_token if page_token is not None else None,
        "sort": sort
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(data_url, params=params, headers=data_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca Data API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca Data API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca Data API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching historical data: {e}")
    
def historic_trades(
    symbol: str,
    start: str = None,
    end: str = None,
    limit: int = 10000,
    feed: str = "sip",
    page_token: str = None,
    sort: str = "asc"
) -> dict:
    """
    Fetch historical bar data from Alpaca.
    
    Required:
      symbols      – Comma-separated list of symbols (e.g., "AAPL,MSFT")
    
    Optional:
      start        – Start time (RFC-3339 format, e.g., "2022-01-01T00:00:00Z")
      end          – End time (RFC-3339 format, e.g., "2022-12-31T23:59:59Z")
      timeframe    – Bar timeframe: "1Min", "5Min", "15Min", "30Min", "1Hour", "1Day", "1Week", "1Month"
      limit        – Number of bars to return (max 10000)
      page_token   – Token for pagination
      sort         – Sort order: "asc" or "desc"
      feed         – Data feed: "iex" or "sip"
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # Load environment variables
    from dotenv import load_dotenv, find_dotenv
    dotenv = find_dotenv()
    load_dotenv(dotenv, override=True)

    # Build the data API URL
    data_url = f"https://data.alpaca.markets/v2/stocks/{symbol}/trades"
    
    # Build headers for data API
    data_headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": os.getenv("ALPACA_PUBLIC_KEY"),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY")
    }
    
    # Build query parameters, dropping None values
    params = {
        "start": start,
        "end": end,
        "limit": str(limit),
        "feed": feed,
        "page_token": page_token if page_token is not None else None,
        "sort": sort
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(data_url, params=params, headers=data_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca Data API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca Data API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca Data API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching historical data: {e}")

def historic_quotes(
    symbol: str,
    start: str = None,
    end: str = None,
    limit: int = 10000,
    feed: str = "sip",
    page_token: str = None,
    sort: str = "asc"
) -> dict:
    """
    Fetch historical bar data from Alpaca.
    
    Required:
      symbols      – Comma-separated list of symbols (e.g., "AAPL,MSFT")
    
    Optional:
      start        – Start time (RFC-3339 format, e.g., "2022-01-01T00:00:00Z")
      end          – End time (RFC-3339 format, e.g., "2022-12-31T23:59:59Z")
      timeframe    – Bar timeframe: "1Min", "5Min", "15Min", "30Min", "1Hour", "1Day", "1Week", "1Month"
      limit        – Number of bars to return (max 10000)
      page_token   – Token for pagination
      sort         – Sort order: "asc" or "desc"
      feed         – Data feed: "iex" or "sip"
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # Load environment variables
    from dotenv import load_dotenv, find_dotenv
    dotenv = find_dotenv()
    load_dotenv(dotenv, override=True)

    # Build the data API URL
    data_url = f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes"
    
    # Build headers for data API
    data_headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": os.getenv("ALPACA_PUBLIC_KEY"),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY")
    }
    
    # Build query parameters, dropping None values
    params = {
        "start": start,
        "end": end,
        "limit": str(limit),
        "feed": feed,
        "page_token": page_token if page_token is not None else None,
        "sort": sort
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(data_url, params=params, headers=data_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca Data API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca Data API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca Data API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching historical data: {e}")

def snapshot_data(
    symbols: str,
    feed: str = "sip"
) -> dict:
    """
    Fetch current snapshot data (latest trade, quote, and daily bar) from Alpaca.
    
    Required:
      symbols      – Comma-separated list of symbols (e.g., "AAPL,MSFT")
    
    Optional:
      feed         – Data feed: "iex" or "sip" (default: "sip")
    
    Returns the JSON response from Alpaca containing:
    - Latest trade information
    - Latest quote (bid/ask) information  
    - Daily bar data (open, high, low, close, volume)
    - Previous day's close
    
    Raises HTTPError on non-2xx response.
    """
    # Build the data API URL
    data_url = "https://data.alpaca.markets/v2/stocks/snapshots"
    
    # Build headers for data API
    data_headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET
    }
    
    # Build query parameters
    params = {
        "symbols": symbols,
        "feed": feed
    }
    
    try:
        response = requests.get(data_url, params=params, headers=data_headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca Data API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca Data API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca Data API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching snapshot data: {e}")



# ─── CORE FUNCTION ──────────────────────────────────────────────────
def place_order(
    symbol: str,
    side:   str,
    type:        str,
    time_in_force: str,
    qty:         float = None,
    notional:    float = None,
    limit_price: float = None,
    stop_price:  float = None,
    trail_percent: float = None,
    trail_price:   float = None,
    extended_hours: bool = False,
    asset_id: str = None,
    record_to_pending: bool = True,
) -> dict:
    """
    Send an order to Alpaca.

    Required:
      symbol       – e.g. "TSLA"
      side         – "buy" or "sell"
      type         – "market", "limit", "stop", "stop_limit", "trailing_stop"
      time_in_force– "day", "gtc", "opg", "cls", "ioc", "fok"

    One of:
      qty          – number of shares (float or int)
      notional     – dollar amount (float)

    Optional:
      limit_price  – for limit / stop_limit orders (required for limit orders)
      stop_price   – for stop / stop_limit / trailing_stop
      trail_percent– for trailing_stop
      trail_price  – for trailing_stop
      extended_hours – allow trading during extended hours (default: False)
      record_to_today – record order to Today.json (default: True)
      record_to_pending – add order to Positions.json Pending array (default: True)
      asset_id     – asset ID for sell orders (for Today.json recording)

    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # 1) Validate qty vs notional
    if (qty is None) == (notional is None):
        raise ValueError("Must specify exactly one of 'qty' or 'notional'")
    
    # 2) Validate limit order requirements
    if type == "limit" and limit_price is None:
        raise ValueError("limit_price is required for limit orders")

    # 3) Generate client_order_id if needed (for buy orders without asset_id)
    if side.upper() == "BUY" and asset_id is None:
        import random
        import string
        # Generate a unique client_order_id for buy orders
        asset_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

    # 4) Build payload, dropping None values
    payload = {
        "symbol":         symbol,
        "side":           side,
        "type":           type,
        "time_in_force":  time_in_force,
        "qty":            str(qty)      if qty      is not None else None,
        "notional":       str(notional) if notional is not None else None,
        "limit_price":    str(limit_price)    if limit_price    is not None else None,
        "stop_price":     str(stop_price)     if stop_price     is not None else None,
        "trail_percent":  str(trail_percent)  if trail_percent  is not None else None,
        "trail_price":    str(trail_price)    if trail_price    is not None else None,
        "extended_hours": extended_hours,
        "client_order_id": asset_id if asset_id is not None else None,
    }
    # remove None entries
    payload = {k: v for k, v in payload.items() if v is not None}

    # 4) POST to Alpaca with enhanced error handling
    try:
        response = requests.post(ORDER_URL_BASE, json=payload, headers=HEADERS, timeout=30)
        response.raise_for_status()
        order_response = response.json()       
        return order_response
    
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error placing order: {e}")
    
def paper_order(
    symbol: str,
    side:   str,
    type:        str,
    time_in_force: str,
    qty:         float = None,
    notional:    float = None,
    limit_price: float = None,
    stop_price:  float = None,
    trail_percent: float = None,
    trail_price:   float = None,
    extended_hours: bool = False,
    asset_id: str = None,
) -> dict:
    """
    Send a paper trading order to Alpaca.

    Required:
      symbol       – e.g. "TSLA"
      side         – "buy" or "sell"
      type         – "market", "limit", "stop", "stop_limit", "trailing_stop"
      time_in_force– "day", "gtc", "opg", "cls", "ioc", "fok"

    One of:
      qty          – number of shares (float or int)
      notional     – dollar amount (float)

    Optional:
      limit_price  – for limit / stop_limit orders (required for limit orders)
      stop_price   – for stop / stop_limit / trailing_stop
      trail_percent– for trailing_stop
      trail_price  – for trailing_stop
      extended_hours – allow trading during extended hours (default: False)
      record_to_today – record order to Today.json (default: True)
      record_to_pending – add order to Positions.json Pending array (default: True)
      asset_id     – asset ID for sell orders (for Today.json recording)

    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # 1) Validate qty vs notional
    if (qty is None) == (notional is None):
        raise ValueError("Must specify exactly one of 'qty' or 'notional'")
    
    # 2) Validate limit order requirements
    if type == "limit" and limit_price is None:
        raise ValueError("limit_price is required for limit orders")

    # 3) Generate internal asset_id for tracking if not provided
    if asset_id is None:
        import random
        import string
        # Generate a unique internal asset_id for tracking purposes
        asset_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

    # 3.5) Round price fields to nearest penny as required by Alpaca API
    if limit_price is not None:
        limit_price = round(float(limit_price), 2)
    if stop_price is not None:
        stop_price = round(float(stop_price), 2)
    if trail_price is not None:
        trail_price = round(float(trail_price), 2)

    # 4) Build payload, dropping None values (excluding client_order_id)
    payload = {
        "symbol":         symbol,
        "side":           side,
        "type":           type,
        "time_in_force":  time_in_force,
        "qty":            str(qty)      if qty      is not None else None,
        "notional":       str(notional) if notional is not None else None,
        "limit_price":    str(limit_price)    if limit_price    is not None else None,
        "stop_price":     str(stop_price)     if stop_price     is not None else None,
        "trail_percent":  str(trail_percent)  if trail_percent  is not None else None,
        "trail_price":    str(trail_price)    if trail_price    is not None else None,
        "extended_hours": extended_hours,
        "client_order_id": asset_id if asset_id is not None else None,
    }
    # remove None entries
    payload = {k: v for k, v in payload.items() if v is not None}

    # 5) POST to Alpaca with enhanced error handling
    try:
        # Debug: Print the exact payload being sent
        print(f"DEBUG: Sending payload to {ORDER_URL_PAPER}")
        print(f"DEBUG: Payload: {payload}")
        print(f"DEBUG: Headers: {PAPER_HEADERS}")
        
        response = requests.post(ORDER_URL_PAPER, json=payload, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        order_response = response.json()
        return order_response
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error placing order: {e}")

# ─── ORDER DELETION FUNCTIONS ────────────────────────────────────────────────
def delete_order(order_id: str) -> dict:
    """
    Delete an order by order ID using live trading API.
    
    Required:
      order_id     – The order ID to delete (UUID format)
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # Build the delete URL
    delete_url = f"{ORDER_URL_BASE}/{order_id}"
    
    try:
        response = requests.delete(delete_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Successful deletion typically returns 204 No Content
        if response.status_code == 204:
            return {"status": "success", "message": f"Order {order_id} deleted successfully"}
        else:
            return response.json()
            
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            if response.content:
                error_response = response.json()
                error_detail = f" Details: {error_response}"
            else:
                error_detail = f" Status: {response.status_code}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error deleting order: {e}")

def paper_delete_order(order_id: str) -> dict:
    """
    Delete an order by order ID using paper trading API.
    
    Required:
      order_id     – The order ID to delete (UUID format)
    
    Returns the JSON response from Alpaca.
    Raises HTTPError on non-2xx response.
    """
    # Validate order_id
    if not order_id or not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("order_id must be a non-empty string")
    
    order_id = order_id.strip()
    
    # Build the delete URL
    delete_url = f"{ORDER_URL_PAPER}/{order_id}"
    
    try:
        response = requests.delete(delete_url, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        
        # Successful deletion typically returns 204 No Content
        if response.status_code == 204:
            return {"status": "success", "message": f"Paper order {order_id} deleted successfully"}
        else:
            return response.json()
            
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            if response.content:
                error_response = response.json()
                error_detail = f" Details: {error_response}"
            else:
                error_detail = f" Status: {response.status_code}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error deleting paper order: {e}")

# ─── ORDER RETRIEVAL FUNCTIONS ──────────────────────────────────────────────
def get_all_orders(
    status: str = None,
    limit: int = None,
    after: str = None,
    until: str = None,
    direction: str = "desc",
    nested: bool = None,
    symbols: str = None
) -> dict:
    """
    Get all orders using live trading API.
    
    Optional parameters:
      status       – Order status: "open", "closed", "all" (default: "open")
      limit        – Number of orders to return (max 500, default 50)
      after        – Start time for filtering (RFC-3339 or market date)
      until        – End time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
      nested       – If true, include nested multi-leg orders
      symbols      – Comma-separated list of symbols to filter by
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    # Build query parameters, dropping None values
    params = {
        "status": status,
        "limit": str(limit) if limit is not None else None,
        "after": after,
        "until": until,
        "direction": direction,
        "nested": str(nested).lower() if nested is not None else None,
        "symbols": symbols
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ORDER_URL_BASE, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")

def paper_get_all_orders(
    status: str = None,
    limit: int = None,
    after: str = None,
    until: str = None,
    direction: str = "desc",
    nested: bool = None,
    symbols: str = None
) -> dict:
    """
    Get all orders using paper trading API.
    
    Optional parameters:
      status       – Order status: "open", "closed", "all" (default: "open")
      limit        – Number of orders to return (max 500, default 50)
      after        – Start time for filtering (RFC-3339 or market date)
      until        – End time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
      nested       – If true, include nested multi-leg orders
      symbols      – Comma-separated list of symbols to filter by
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    # Build query parameters, dropping None values
    params = {
        "status": status,
        "limit": str(limit) if limit is not None else None,
        "after": after,
        "until": until,
        "direction": direction,
        "nested": str(nested).lower() if nested is not None else None,
        "symbols": symbols
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ORDER_URL_PAPER, params=params, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper orders: {e}")

def get_clientID_order(client_order_id: str) -> dict:
    """
    Get a single order by client order ID using live trading API.
    
    Required:
      client_order_id  – The client-provided order ID to retrieve
    
    Returns the JSON response from Alpaca containing the order details.
    Raises HTTPError on non-2xx response.
    """
    # Build the URL for retrieving order by client order ID
    api_url = "https://api.alpaca.markets/v2/orders:by_client_order_id"
    
    # Build query parameters
    params = {
        "client_order_id": client_order_id
    }
    
    try:
        response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting order by client ID: {e}")

def paper_get_clientID_order(client_order_id: str) -> dict:
    """
    Get a single order by client order ID using paper trading API.
    
    Required:
      client_order_id  – The client-provided order ID to retrieve
    
    Returns the JSON response from Alpaca containing the order details.
    Raises HTTPError on non-2xx response.
    """
    # Build the URL for retrieving order by client order ID
    api_url = "https://paper-api.alpaca.markets/v2/orders:by_client_order_id"
    
    # Build query parameters
    params = {
        "client_order_id": client_order_id
    }
    
    try:
        response = requests.get(api_url, params=params, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper order by client ID: {e}")
    
def get_order(order_id: str) -> dict:
    """
    Get a single order by order ID using paper trading API.

    Required:
      order_id  – The order ID to retrieve

    Returns the JSON response from Alpaca containing the order details.
    Raises HTTPError on non-2xx response.
    """
    # Build the URL for retrieving order by order ID
    api_url = f"https://api.alpaca.markets/v2/orders/{order_id}"
    
    try:
        response = requests.get(api_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper order by client ID: {e}")
    
def paper_get_order(order_id: str) -> dict:
    """
    Get a single order by order ID using paper trading API.

    Required:
      order_id  – The order ID to retrieve

    Returns the JSON response from Alpaca containing the order details.
    Raises HTTPError on non-2xx response.
    """
    # Build the URL for retrieving order by order ID
    api_url = f"https://paper-api.alpaca.markets/v2/orders/{order_id}"
        
    try:
        response = requests.get(api_url, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper order by client ID: {e}")


def edit_order(
    order_id: str,
    new_limit_price: float,
    client_order_id: str = None     
    ):
    """
    Edit an existing order's limit price using live trading API.
    """
    # Build the URL for retrieving order by order ID
    api_url = f"https://api.alpaca.markets/v2/orders/{order_id}"

    parameters = {
        "limit_price": str(new_limit_price),
        "client_order_id": client_order_id if client_order_id is not None else None
    }
    
    try:
        response = requests.patch(api_url, headers=HEADERS, json=parameters, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper order by client ID: {e}")

def paper_edit_order(
    order_id: str,
    new_limit_price: float,
    client_order_id: str = None     
    ):
    """
    Edit an existing order's limit price using live trading API.
    """
    # Build the URL for retrieving order by order ID
    api_url = f"https://paper-api.alpaca.markets/v2/orders/{order_id}"

    parameters = {
        "limit_price": str(new_limit_price),
        "client_order_id": client_order_id if client_order_id is not None else None
    }
    
    try:
        response = requests.patch(api_url, headers=PAPER_HEADERS, json=parameters, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper order by client ID: {e}")



def get_account():
    """
    Get account information from live trading API.
    
    Returns:
        dict: Account information including buying power, cash, portfolio value, etc.
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = ACCOUNT_URL_BASE
    
    try:
        response = requests.get(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting account information: {e}")


def paper_get_account():
    """
    Get account information from paper trading API.
    
    Returns:
        dict: Account information including buying power, cash, portfolio value, etc.
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = ACCOUNT_URL_PAPER
    
    try:
        response = requests.get(API_URL, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper account information: {e}")


def get_positions():
    """
    Get all open positions from live trading API.
    
    Returns:
        list: List of position objects with information about current holdings
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = POSITIONS_URL_BASE
    
    try:
        response = requests.get(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting positions: {e}")


def paper_get_positions():
    """
    Get all open positions from paper trading API.
    
    Returns:
        list: List of position objects with information about current holdings
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = POSITIONS_URL_PAPER
    
    try:
        response = requests.get(API_URL, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting paper positions: {e}")
    
def paper_close_position(
    ticker: str,
    qty: float = None,
    percent: float = None,
) -> dict:
    """
    Get all activity using paper trading API.
    
    Optional parameters:
      ticker     – symbol to close position for
      qty        – number of shares to close
      percent    – percentage of position to close
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    # Build query parameters, dropping None values
    if qty is not None:
        params = {
            "ticker": ticker,
            "qty": qty,
            "percent": None,
        }
    
    else: # percent is not None
        params = {
            "ticker": ticker,
            "qty": None,
            "percent": percent,
        }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}

    close_positions_url = f"{POSITIONS_URL_PAPER}/{ticker}"
    
    try:
        response = requests.delete(close_positions_url, params=params, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
def close_position(
    ticker: str,
    qty: float = None,
    percent: float = None,
) -> dict:
    """
    Get all activity using paper trading API.
    
    Optional parameters:
      ticker     – symbol to close position for
      qty        – number of shares to close
      percent    – percentage of position to close
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    # Build query parameters, dropping None values
    if qty is not None:
        params = {
            "qty": qty,
            "percent": None,
        }
    
    else: # percent is not None
        params = {
            "qty": None,
            "percent": percent,
        }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}

    close_positions_url = f"{POSITIONS_URL_BASE}/{ticker}"
    
    try:
        response = requests.delete(close_positions_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
    
def get_acc_history(
        end_date: str = None,
    ):
    """
    Get all activity using paper trading API.
    
    Optional parameters:
      category     – "trade_activity", "non_trade_activity"
      date         – Single Day (RFC-3339 or market date) to filter by
      until        – Multi Day End time for filtering (RFC-3339 or market date)
      after        – Multi Day Start time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    ACTIVITY_URL = f"{ACCOUNT_URL_BASE}/portfolio/history"
    # Build query parameters, dropping None values
    params = {
        "period": "1M",
        "timeframe": "1D",
        "intraday_reporting": "continuous",
        "pnl_reset": "no_reset",
        "end_date": end_date if end_date is not None else datetime.now().strftime("%Y-%m-%d"),
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ACTIVITY_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
def paper_get_acc_history(
        end_date: str = None,
    ):
    """
    Get all activity using paper trading API.
    
    Optional parameters:
      category     – "trade_activity", "non_trade_activity"
      date         – Single Day (RFC-3339 or market date) to filter by
      until        – Multi Day End time for filtering (RFC-3339 or market date)
      after        – Multi Day Start time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    ACTIVITY_URL = f"{ACCOUNT_URL_PAPER}/portfolio/history"
    # Build query parameters, dropping None values
    params = {
        "period": "29D",
        "timeframe": "15Min",
        "intraday_reporting": "continuous",
        "pnl_reset": "no_reset",
        "end_date": end_date if end_date is not None else datetime.now().strftime("%Y-%m-%d"),
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ACTIVITY_URL, params=params, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
def get_activity(
    category: str = "trade_activity",
    date: str = None,
    until: str = None,
    after: str = None,
    direction: str = "desc",
    page_size: str = "100"
) -> dict:
    """
    Get all activity using live trading API.
    
    Optional parameters:
      category     – "trade_activity", "non_trade_activity"
      date         – Single Day (RFC-3339 or market date) to filter by
      until        – Multi Day End time for filtering (RFC-3339 or market date)
      after        – Multi Day Start time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    ACTIVITY_URL = f"{ACCOUNT_URL_BASE}/activities"
    # Build query parameters, dropping None values
    params = {
        "category": category,
        "date": date if date is not None else None,
        "after": after if after is not None else None,
        "until": until if until is not None else None,
        "direction": direction,
        "page_size": page_size if page_size is not None else None,
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ACTIVITY_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
def paper_get_activity(
    category: str = "trade_activity",
    date: str = None,
    until: str = None,
    after: str = None,
    direction: str = "desc",
    page_size: str = "100"
) -> dict:
    """
    Get all activity using paper trading API.
    
    Optional parameters:
      category     – "trade_activity", "non_trade_activity"
      date         – Single Day (RFC-3339 or market date) to filter by
      until        – Multi Day End time for filtering (RFC-3339 or market date)
      after        – Multi Day Start time for filtering (RFC-3339 or market date)
      direction    – Sort direction: "asc" or "desc" (default: "desc")
    
    Returns the JSON response from Alpaca containing array of orders.
    Raises HTTPError on non-2xx response.
    """
    ACTIVITY_URL = f"{ACCOUNT_URL_PAPER}/activities"
    # Build query parameters, dropping None values
    params = {
        "category": category,
        "date": date if date is not None else None,
        "after": after if after is not None else None,
        "until": until if until is not None else None,
        "direction": direction,
        "page_size": page_size if page_size is not None else None,
    }
    # Remove None entries
    params = {k: v for k, v in params.items() if v is not None}
    
    try:
        response = requests.get(ACTIVITY_URL, params=params, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error getting orders: {e}")
    
def liquidate():
    """
    Liquidate (close) all open positions using live trading API.
    This will create market sell orders for all long positions and market buy orders for all short positions.
    
    Returns:
        list: List of order responses for each position that was liquidated
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = POSITIONS_URL_BASE
    
    try:
        response = requests.delete(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error liquidating positions: {e}")


def paper_liquidate():
    """
    Liquidate (close) all open positions using paper trading API.
    This will create market sell orders for all long positions and market buy orders for all short positions.
    
    Returns:
        list: List of order responses for each position that was liquidated
    
    Raises:
        ConnectionError: If unable to connect to Alpaca API
        TimeoutError: If request times out
        requests.exceptions.HTTPError: If API returns an error
        Exception: For any other unexpected errors
    """
    API_URL = POSITIONS_URL_PAPER
    
    try:
        response = requests.delete(API_URL, headers=PAPER_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Failed to connect to Alpaca API. Check your internet connection. Error: {e}")
    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Request to Alpaca API timed out. Error: {e}")
    except requests.exceptions.HTTPError as e:
        error_detail = ""
        try:
            error_response = response.json()
            error_detail = f" Details: {error_response}"
        except:
            error_detail = f" Response: {response.text}"
        raise requests.exceptions.HTTPError(f"Alpaca API returned error {response.status_code}.{error_detail}")
    except Exception as e:
        raise Exception(f"Unexpected error liquidating paper positions: {e}")


# ─── COMMAND-LINE INTERFACE WITH CROSS-PLATFORM SUPPORT ────────────────
def _cli():
    """Command-line interface for Alpaca order operations."""
    parser = argparse.ArgumentParser(
        description=f"Alpaca order operations CLI (Running on {sys.platform})"
    )
    
    # Add subcommands for different operations
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Place order subcommand
    place_parser = subparsers.add_parser('place', help='Place a new order')
    place_parser.add_argument("symbol",  help="Ticker, e.g. TSLA")
    place_parser.add_argument("side",    choices=["buy", "sell"])
    group = place_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--qty",      type=float, help="Count of shares")
    group.add_argument("--notional", type=float, help="Dollar amount")

    place_parser.add_argument("--type",           default="market",
                        choices=["market","limit","stop","stop_limit","trailing_stop"])
    place_parser.add_argument("--time-in-force", default="day",
                        choices=["day","gtc","opg","cls","ioc","fok"])
    place_parser.add_argument("--limit-price",  type=float,
                        help="Limit price (for limit or stop_limit)")
    place_parser.add_argument("--stop-price",   type=float,
                        help="Stop price (for stop or trailing_stop)")
    place_parser.add_argument("--trail-percent",type=float,
                        help="Trail percent drop (for trailing_stop)")
    place_parser.add_argument("--trail-price",  type=float,
                        help="Trail dollar drop (for trailing_stop)")
    place_parser.add_argument("--extended-hours", action="store_true",
                        help="Allow trading during extended hours")
    place_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # Delete order subcommand
    delete_parser = subparsers.add_parser('delete', help='Delete an existing order')
    delete_parser.add_argument("order_id", help="Order ID to delete (UUID format)")
    delete_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # List orders subcommand
    list_parser = subparsers.add_parser('list', help='Get all orders')
    list_parser.add_argument("--status", choices=["open", "closed", "all"],
                        help="Order status filter (default: open)")
    list_parser.add_argument("--limit", type=int,
                        help="Number of orders to return (max 500, default 50)")
    list_parser.add_argument("--after", 
                        help="Start time for filtering (RFC-3339 or market date)")
    list_parser.add_argument("--until",
                        help="End time for filtering (RFC-3339 or market date)")
    list_parser.add_argument("--direction", choices=["asc", "desc"], default="desc",
                        help="Sort direction (default: desc)")
    list_parser.add_argument("--nested", action="store_true",
                        help="Include nested multi-leg orders")
    list_parser.add_argument("--symbols",
                        help="Comma-separated list of symbols to filter by")
    list_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # Account info subcommand
    account_parser = subparsers.add_parser('account', help='Get account information')
    account_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # Positions subcommand
    positions_parser = subparsers.add_parser('positions', help='Get all open positions')
    positions_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # Get order by client ID subcommand
    client_order_parser = subparsers.add_parser('get-by-client-id', help='Get order by client order ID')
    client_order_parser.add_argument("client_order_id", help="Client order ID to retrieve")
    client_order_parser.add_argument("--paper", action="store_true",
                        help="Use paper trading API")
    
    # Add verbose flag for debugging
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose output for debugging")

    args = parser.parse_args()
    
    # Check if command was provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Validate environment before proceeding
    if not validate_environment():
        print("Please fix the environment configuration and try again.", file=sys.stderr)
        sys.exit(1)
    
    if args.verbose:
        print(f"Running on platform: {sys.platform}")
        print(f"Python version: {sys.version}")
        print(f"Environment file: {dotenv_path if dotenv_path else 'Not found'}")
        
        print("Testing API connection...")
        if test_connection():
            print("✓ API connection successful")
        else:
            print("✗ API connection failed")
            sys.exit(1)

    try:
        if args.command == 'place':
            if args.verbose:
                api_url = ORDER_URL_PAPER if args.paper else ORDER_URL_BASE
                print(f"API URL: {api_url}")
                print(f"Order payload: symbol={args.symbol}, side={args.side}, type={args.type}")
            
            # Choose function based on paper flag
            order_func = paper_order if args.paper else place_order
            
            resp = order_func(
                symbol=args.symbol,
                side=args.side,
                type=args.type,
                time_in_force=args.time_in_force,
                qty=args.qty,
                notional=args.notional,
                limit_price=args.limit_price,
                stop_price=args.stop_price,
                trail_percent=args.trail_percent,
                trail_price=args.trail_price,
                extended_hours=args.extended_hours,
            )
            print("Order successful:", resp)
            
        elif args.command == 'delete':
            if args.verbose:
                api_url = ORDER_URL_PAPER if args.paper else ORDER_URL_BASE
                print(f"API URL: {api_url}")
                print(f"Deleting order ID: {args.order_id}")
            
            # Choose function based on paper flag
            delete_func = paper_delete_order if args.paper else delete_order
            
            resp = delete_func(args.order_id)
            print("Delete successful:", resp)
            
        elif args.command == 'list':
            if args.verbose:
                api_url = ORDER_URL_PAPER if args.paper else ORDER_URL_BASE
                print(f"API URL: {api_url}")
                print(f"Listing orders with filters: status={args.status}, limit={args.limit}")
            
            # Choose function based on paper flag
            list_func = paper_get_all_orders if args.paper else get_all_orders
            
            # Convert symbols string to list if provided
            symbols_list = args.symbols.split(",") if args.symbols else None
            
            resp = list_func(
                status=args.status,
                limit=args.limit,
                after=args.after,
                until=args.until,
                direction=args.direction,
                nested=args.nested,
                symbols=symbols_list
            )
            
            # Pretty print the orders
            if isinstance(resp, list):
                print(f"Found {len(resp)} orders:")
                for i, order in enumerate(resp, 1):
                    print(f"\nOrder {i}:")
                    print(f"  ID: {order.get('id', 'N/A')}")
                    print(f"  Symbol: {order.get('symbol', 'N/A')}")
                    print(f"  Side: {order.get('side', 'N/A')}")
                    print(f"  Type: {order.get('order_type', 'N/A')}")
                    print(f"  Status: {order.get('status', 'N/A')}")
                    print(f"  Qty: {order.get('qty', 'N/A')}")
                    if order.get('limit_price'):
                        print(f"  Limit Price: ${order.get('limit_price')}")
                    print(f"  Created: {order.get('created_at', 'N/A')}")
            else:
                print("Orders retrieved:", resp)
                
        elif args.command == 'account':
            if args.verbose:
                api_url = ORDER_URL_PAPER if args.paper else ORDER_URL_BASE
                print(f"API URL: {api_url}")
                print("Getting account information...")
            
            # Choose function based on paper flag
            account_func = paper_get_account if args.paper else get_account
            
            resp = account_func()
            
            # Pretty print account information
            print("Account Information:")
            print(f"  Account ID: {resp.get('id', 'N/A')}")
            print(f"  Status: {resp.get('status', 'N/A')}")
            print(f"  Currency: {resp.get('currency', 'N/A')}")
            print(f"  Pattern Day Trader: {resp.get('pattern_day_trader', 'N/A')}")
            print(f"  Trading Blocked: {resp.get('trading_blocked', 'N/A')}")
            print(f"  Transfers Blocked: {resp.get('transfers_blocked', 'N/A')}")
            print(f"  Account Blocked: {resp.get('account_blocked', 'N/A')}")
            print(f"  Buying Power: ${resp.get('buying_power', 'N/A')}")
            print(f"  Cash: ${resp.get('cash', 'N/A')}")
            print(f"  Portfolio Value: ${resp.get('portfolio_value', 'N/A')}")
            print(f"  Equity: ${resp.get('equity', 'N/A')}")
            print(f"  Long Market Value: ${resp.get('long_market_value', 'N/A')}")
            print(f"  Short Market Value: ${resp.get('short_market_value', 'N/A')}")
            print(f"  Day Trade Count: {resp.get('daytrade_count', 'N/A')}")
            print(f"  Last Equity: ${resp.get('last_equity', 'N/A')}")
            
        elif args.command == 'positions':
            if args.verbose:
                api_url = ORDER_URL_PAPER if args.paper else ORDER_URL_BASE
                print(f"API URL: {api_url}")
                print("Getting positions...")
            
            # Choose function based on paper flag
            positions_func = paper_get_positions if args.paper else get_positions
            
            resp = positions_func()
            
            # Pretty print positions
            if isinstance(resp, list):
                if len(resp) == 0:
                    print("No open positions found.")
                else:
                    print(f"Found {len(resp)} open position(s):")
                    for i, position in enumerate(resp, 1):
                        print(f"\nPosition {i}:")
                        print(f"  Symbol: {position.get('symbol', 'N/A')}")
                        print(f"  Quantity: {position.get('qty', 'N/A')}")
                        print(f"  Side: {position.get('side', 'N/A')}")
                        print(f"  Market Value: ${position.get('market_value', 'N/A')}")
                        print(f"  Cost Basis: ${position.get('cost_basis', 'N/A')}")
                        print(f"  Unrealized P&L: ${position.get('unrealized_pl', 'N/A')}")
                        print(f"  Unrealized P&L %: {position.get('unrealized_plpc', 'N/A')}")
                        print(f"  Current Price: ${position.get('current_price', 'N/A')}")
                        print(f"  Last Day Price: ${position.get('lastday_price', 'N/A')}")
                        print(f"  Change Today: ${position.get('change_today', 'N/A')}")
            else:
                print("Positions retrieved:", resp)
                
        elif args.command == 'get-by-client-id':
            if args.verbose:
                api_url = "https://paper-api.alpaca.markets/v2/orders:by_client_order_id" if args.paper else "https://api.alpaca.markets/v2/orders:by_client_order_id"
                print(f"API URL: {api_url}")
                print(f"Getting order by client ID: {args.client_order_id}")
            
            # Choose function based on paper flag
            client_order_func = paper_get_clientID_order if args.paper else get_clientID_order
            
            resp = client_order_func(args.client_order_id)
            
            # Pretty print the order
            print("Order Information:")
            print(f"  Order ID: {resp.get('id', 'N/A')}")
            print(f"  Client Order ID: {resp.get('client_order_id', 'N/A')}")
            print(f"  Symbol: {resp.get('symbol', 'N/A')}")
            print(f"  Side: {resp.get('side', 'N/A')}")
            print(f"  Type: {resp.get('order_type', 'N/A')}")
            print(f"  Status: {resp.get('status', 'N/A')}")
            print(f"  Quantity: {resp.get('qty', 'N/A')}")
            print(f"  Filled Quantity: {resp.get('filled_qty', 'N/A')}")
            if resp.get('limit_price'):
                print(f"  Limit Price: ${resp.get('limit_price')}")
            if resp.get('stop_price'):
                print(f"  Stop Price: ${resp.get('stop_price')}")
            print(f"  Time in Force: {resp.get('time_in_force', 'N/A')}")
            print(f"  Created: {resp.get('created_at', 'N/A')}")
            print(f"  Updated: {resp.get('updated_at', 'N/A')}")
            if resp.get('filled_at'):
                print(f"  Filled: {resp.get('filled_at')}")
            if resp.get('canceled_at'):
                print(f"  Canceled: {resp.get('canceled_at')}")
            if resp.get('expired_at'):
                print(f"  Expired: {resp.get('expired_at')}")
                
    except ValueError as e:
        print(f"Input validation error: {e}", file=sys.stderr)
        sys.exit(1)
    except ConnectionError as e:
        print(f"Connection error: {e}", file=sys.stderr)
        sys.exit(1)
    except TimeoutError as e:
        print(f"Timeout error: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"API error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        _cli()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
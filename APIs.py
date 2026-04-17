
import requests
import os
import json
from dotenv import load_dotenv, find_dotenv

dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

ALPACA_KEY = os.getenv("ALPACA_PUBLIC_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY")

HEADERS = {
    "accept":        "application/json",
    "content-type":  "application/json",
    "APCA-API-KEY-ID":     ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

base_path = r"C:\Portfolio Projects\Stock Analysis Tool"

# ─── ASSET LIST FUNCTIONS ──────────────────────────────────────────────────────────
    
def download_raw_ciks():
    """
    Download SEC company tickers exchange data.
    
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
        "User-Agent": "Nathan Goff jollymountainman0403@outlook.com"
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
    
def company_facts(cik):
    """
    Run Company Facts api to collect financial data based on company cik. 
    """    
    cik = str(cik)
    new_cik = ('0' * (10 - len(cik))) + cik
    api_link = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{new_cik}.json'
    filing_link = f'https://data.sec.gov/submissions/CIK{new_cik}.json'

    headers = {"User-Agent": "Nathaniel Goff jollymountainman0403@outlook.com"}

    try:

        resp = requests.get(filing_link, headers=headers, timeout=30)
        resp.raise_for_status()
        filing_data = resp.json()
    
    except Exception as E:
        return {}, {}

    comp_data = {}

    comp_data["Company"] = filing_data["name"]
    comp_data["Ticker"] = filing_data["tickers"][0]
    comp_data["Sector"] = filing_data["ownerOrg"]
    comp_data["Industry"] = filing_data["sicDescription"]
    comp_data["SIC Code"] = filing_data["sic"]

    street = f"{filing_data["addresses"]["business"]['street1']} {filing_data["addresses"]["business"]['street2']}" if filing_data["addresses"]["business"]['street2'] != None else filing_data["addresses"]["business"]['street1']
    city = filing_data["addresses"]["business"]['city']
    state = filing_data["addresses"]["business"]['stateOrCountry']
    zip = filing_data["addresses"]["business"]['zipCode']

    comp_data["Address"] = f"{street} {city}, {state} {zip}"

    filing_file = os.path.join(base_path, "filings.json")

    # safe_write_json(filing_file, filing_data)

    try:
        resp = requests.get(api_link, headers=headers, timeout=30)
        resp.raise_for_status()
        fact_data = resp.json()
        label_data = fact_data['facts']['us-gaap']
    except Exception as E:
        return {}, {}


    return comp_data, label_data


# ─── Share Price Functions ──────────────────────────────────────────────────────────

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
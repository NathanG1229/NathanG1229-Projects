# NathanG1229-Projects
A public access into the projects that I have created.

You can find them in the branches 2 branches of this repository.

## GG M-11

GG M-11 is the 11th iteration of my automated stock-trading system built around the Alpaca API. The scripts in the `System` folder work together to maintain a tradeable ticker universe, evaluate machine-learning candidates, monitor open positions, place buy and sell orders, and send Discord alerts.

---

### System Overview

The `System` folder is the operational core of the project. It handles:

- market-day scheduling
- ticker screening and filtering
- current-day ML-based purchase decisions
- position monitoring and sell logic
- portfolio/report maintenance
- Discord notifications and health checks

---

### Main Scripts

#### `System/Master.py`
The main orchestrator for the trading engine.

Responsibilities:
- starts the daily trading cycle
- checks whether the market is open or closed
- updates holding periods in `Portfolio.json`
- monitors active positions throughout the day
- triggers ML analysis and sell logic at scheduled times
- runs end-of-day reset and filtering tasks

#### `System/MLFilter.py`
Builds the filtered candidate list used for trading.

Responsibilities:
- loads tickers from `Tickers.txt`
- fetches historical price data
- runs a regression-based historical filter
- scores candidates using hit rate, instance count, and weighted return behavior
- saves approved tickers to `FilteredTickers.json`

#### `System/MLCurrent.py`
Runs same-day candidate selection and purchase execution.

Responsibilities:
- loads `FilteredTickers.json`
- computes current forecasts for approved symbols
- removes symbols already sold or currently held
- ranks final candidates
- calculates position sizing from cash and portfolio limits
- places market buy orders through Alpaca

#### `System/EODReset.py`
Refreshes the base ticker universe.

Responsibilities:
- downloads raw SEC ticker/CIK data
- cross-checks Alpaca asset availability
- filters out inactive, non-tradable, or non-fractionable assets
- checks minimum price-history and trade-activity thresholds
- rewrites `Tickers.txt`

#### `System/Lifeline.py`
Acts as a health-monitoring watchdog.

Responsibilities:
- watches `Portfolio.json` for stale updates
- sends urgent Discord alerts if the main trading loop falls behind
- checks calendar timing and expired holdings
- triggers monthly reporting on schedule

#### `System/MonthlyReport.py`
Creates a monthly portfolio performance report.

Responsibilities:
- downloads account equity history from Alpaca
- calculates high, low, and return metrics
- builds a performance chart image
- sends the report to Discord

#### `System/Alpaca.py`
API wrapper for brokerage and market-data actions.

Used for:
- account details
- positions and orders
- historical bars and snapshots
- placing, editing, and closing orders
- downloading tradable asset lists

#### `System/Tools.py`
Shared utility functions.

Includes:
- locked JSON reads and writes
- timezone helpers for ET and CT
- settings loading from `Settings.txt`
- timing utilities used across the system

#### `System/Webhook.py`
Handles Discord webhook notifications for status updates and urgent alerts.

---

### Helper Folder

The `Helper` folder contains supporting tools for remote control, quick account access, and shared helper logic.

#### `Helper/Ida2.py`
A Discord-based control bot for interacting with the trading system.

Capabilities:
- respond to Discord commands like portfolio, start, end, liquidate, and sleep
- display current holdings and account balance
- start selected Python scripts from the model directory
- attempt to stop running scripts by name
- provide a lightweight remote control interface for portfolio operations

#### `Helper/Alpaca.py`
A helper-facing Alpaca API wrapper used by the Discord assistant and utility scripts.

Used for:
- account lookups
- position retrieval
- market data snapshots and historical bars
- order placement and liquidation support

#### `Helper/Tools.py`
Shared helper utilities mirroring the core project helpers.

Includes:
- safe JSON read/write helpers
- settings and environment loading
- timezone conversions and scheduling helpers
- reusable support functions for helper-side scripts

---

### Daily Workflow

1. `Master.py` starts and checks the trading calendar.
2. If the market is open, it initializes the day and updates portfolio timing fields.
3. During market hours, it repeatedly:
   - syncs open positions
   - checks loss probability and initiation points
   - updates trailing sell logic
4. Later in the day, `MLCurrent.py` runs to evaluate and buy strong candidates.
5. At end of day, `EODReset.py` rebuilds the ticker universe.
6. `MLFilter.py` refreshes the filtered watchlist for the next session.
7. `Lifeline.py` monitors the system and alerts if updates stop.

---

### Key Data Files

The system depends on several JSON and text files stored in the project path:

- `Portfolio.json` — active holdings and trade state
- `Report.json` — detailed trade-object log/history
- `FilteredTickers.json` — symbols that passed the ML filter
- `Tickers.txt` — current ticker universe
- `Settings.txt` — thresholds and runtime configuration
- `Calendar.json` — market open/close schedule
- `Status.json` — watchdog status for Lifeline

---

### Configuration

Environment variables are loaded from a `.env` file.

Expected values include:
- `Model_Path`
- `File_Path`
- `ALPACA_PUBLIC_KEY`
- `ALPACA_SECRET_KEY`
- `ALPACA_PAPER_PUBLIC_KEY`
- `ALPACA_PAPER_SECRET_KEY`

The trading behavior is primarily controlled through `System/Settings.txt`, including:

- portfolio size limits
- batch size and liquidity filters
- ML thresholds
- stop-loss and sell offsets
- error-rate handling

---

### Installation

1. Create and activate a Python environment.
2. Install the dependencies from `requirements.txt`.
3. Configure the `.env` file with your Alpaca and path settings.
4. Make sure the required JSON support files exist in your model directory.

---

### Running the System

Typical entry points:

- Run `System/Master.py` to start the trading engine
- Run `System/Lifeline.py` to monitor system health

---

### Logs and Notifications

The system writes output logs such as:

- `Master_Output.log`
- `MLCurrent_Output.log`
- `MLFilter_Output.log`
- `EOD_Output.log`

It also sends normal and urgent notifications to Discord webhooks.

---

### Notes

- This project is designed around U.S. market hours and Eastern Time scheduling.
- It assumes Alpaca connectivity and valid account permissions.
- The scripts are tightly linked through shared files and environment settings.
- Review all trading logic carefully before using with live capital.




## Stock Analysis Tool
### Overview
This project is a hybrid Excel and Python workflow for in-depth stock analysis.

It is designed to help you:

review a company's current and historical stock-price performance,
review current and historical financial performance,
compare a company against similar businesses in the same industry,
group peers using industry classification and approximate market-cap similarity.
The workflow pulls market data from Alpaca, financial filing data from the SEC, stores processed results in SQLite, and supports analysis through the included Excel workbook.

### Main Components
Company Analysis.xlsm — the primary Microsoft Excel workbook for analysis and presentation.
DB_Builder.py — builds and updates the SQLite database with company facts and price history.
APIs.py — handles SEC and Alpaca API requests.
Table_Tools.py — combines and manages SQLite tables.
Tools.py — shared utility helpers.

### Requirements
To use this project as intended, you will need the following:

1) Microsoft 365 account
This project uses a macro-enabled Excel workbook. A desktop version of Microsoft Excel included with Microsoft 365 is recommended.

2) Alpaca Market Data API keys
You must create an Alpaca account and generate:

a public API key
a secret API key
These keys are required for downloading historical share-price data.

3) SQLite 3
SQLite is used as the local database engine that stores the processed company and sector data.

4) ODBC driver
An ODBC driver is required if Excel or related tools are connecting to the SQLite database through ODBC.

## Additional Software Needed
In practice, you should also have the following installed:

Python 3.10+
pip for Python package installation
Install Python dependencies with:

pip install -r requirements.txt
Environment Setup
Create a local .env file in the project folder and define your credentials and local path settings.

Example:

ALPACA_PUBLIC_KEY=your_public_key_here
ALPACA_SECRET_KEY=your_secret_key_here
file_path=C:\Path\To\Stock Analysis Tool
Important
Do not share or commit your real API keys.
If keys were ever exposed publicly, rotate them immediately in your Alpaca account.
Update any hard-coded local paths in the project if your folder location is different.

### How the Project Works
The workbook provides the user-facing analysis experience.
The Python scripts download:
SEC company facts,
SEC company reference data,
historical stock-price data from Alpaca.
The data is cleaned, transformed, and written into a SQLite database.
Sector tables are combined into a single table for broader analysis.
Excel can then be used to explore and compare companies.

### Typical Setup Steps
Clone or download this project.
Install Python and the required packages.
Install SQLite 3.
Install an SQLite-compatible ODBC driver.
Add your Alpaca API keys to the local .env file.
Confirm that the project folder path matches your local machine.
Run the database build process:
python DB_Builder.py
Open Company Analysis.xlsm in Excel and enable macros if prompted.

### Data Sources
This project relies on third-party data sources, including:

SEC EDGAR / Company Facts API for company filing and financial statement data
Alpaca Market Data API for historical stock-price data
Availability, accuracy, and rate limits depend on those external services.

### Disclosure and Use Notice
This project is intended for research, educational, and analytical use.

It should not be treated as:

financial advice,
investment advice,
a guarantee of accuracy or completeness.
**All market and company data should be independently verified before making financial decisions.**

### Known Setup Notes
Some paths in the Python scripts may be configured for a local Windows environment and may need to be updated on another machine.
The workbook is designed around Microsoft Excel and may not function correctly in non-Microsoft spreadsheet tools.
Internet access is required for API calls.
Large database builds may take time depending on the number of companies and API response speed.
Troubleshooting
If the project does not run correctly, check the following:

your Alpaca keys are valid,
the .env file is present,
required Python packages are installed,
SQLite and the ODBC driver are installed correctly,
the workbook path and project path match your machine,
Excel macros are enabled.
Security Reminder
Before sharing this repository publicly:

remove all real credentials,
keep the .env file private,
consider adding .env, database files, and virtual-environment folders to .gitignore.
Disclaimer
The author and contributors are not responsible for trading losses, data errors, or decisions made using this tool.

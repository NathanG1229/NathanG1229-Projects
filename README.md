# GG M-11

GG M-11 is the 11th iteration of my automated stock-trading system built around the Alpaca API. The scripts in the `System` folder work together to maintain a tradeable ticker universe, evaluate machine-learning candidates, monitor open positions, place buy and sell orders, and send Discord alerts.

---

## System Overview

The `System` folder is the operational core of the project. It handles:

- market-day scheduling
- ticker screening and filtering
- current-day ML-based purchase decisions
- position monitoring and sell logic
- portfolio/report maintenance
- Discord notifications and health checks

---

## Main Scripts

### `System/Master.py`
The main orchestrator for the trading engine.

Responsibilities:
- starts the daily trading cycle
- checks whether the market is open or closed
- updates holding periods in `Portfolio.json`
- monitors active positions throughout the day
- triggers ML analysis and sell logic at scheduled times
- runs end-of-day reset and filtering tasks

### `System/MLFilter.py`
Builds the filtered candidate list used for trading.

Responsibilities:
- loads tickers from `Tickers.txt`
- fetches historical price data
- runs a regression-based historical filter
- scores candidates using hit rate, instance count, and weighted return behavior
- saves approved tickers to `FilteredTickers.json`

### `System/MLCurrent.py`
Runs same-day candidate selection and purchase execution.

Responsibilities:
- loads `FilteredTickers.json`
- computes current forecasts for approved symbols
- removes symbols already sold or currently held
- ranks final candidates
- calculates position sizing from cash and portfolio limits
- places market buy orders through Alpaca

### `System/EODReset.py`
Refreshes the base ticker universe.

Responsibilities:
- downloads raw SEC ticker/CIK data
- cross-checks Alpaca asset availability
- filters out inactive, non-tradable, or non-fractionable assets
- checks minimum price-history and trade-activity thresholds
- rewrites `Tickers.txt`

### `System/Lifeline.py`
Acts as a health-monitoring watchdog.

Responsibilities:
- watches `Portfolio.json` for stale updates
- sends urgent Discord alerts if the main trading loop falls behind
- checks calendar timing and expired holdings
- triggers monthly reporting on schedule

### `System/MonthlyReport.py`
Creates a monthly portfolio performance report.

Responsibilities:
- downloads account equity history from Alpaca
- calculates high, low, and return metrics
- builds a performance chart image
- sends the report to Discord

### `System/Alpaca.py`
API wrapper for brokerage and market-data actions.

Used for:
- account details
- positions and orders
- historical bars and snapshots
- placing, editing, and closing orders
- downloading tradable asset lists

### `System/Tools.py`
Shared utility functions.

Includes:
- locked JSON reads and writes
- timezone helpers for ET and CT
- settings loading from `Settings.txt`
- timing utilities used across the system

### `System/Webhook.py`
Handles Discord webhook notifications for status updates and urgent alerts.

---

## Helper Folder

The `Helper` folder contains supporting tools for remote control, quick account access, and shared helper logic.

### `Helper/Ida2.py`
A Discord-based control bot for interacting with the trading system.

Capabilities:
- respond to Discord commands like portfolio, start, end, liquidate, and sleep
- display current holdings and account balance
- start selected Python scripts from the model directory
- attempt to stop running scripts by name
- provide a lightweight remote control interface for portfolio operations

### `Helper/Alpaca.py`
A helper-facing Alpaca API wrapper used by the Discord assistant and utility scripts.

Used for:
- account lookups
- position retrieval
- market data snapshots and historical bars
- order placement and liquidation support

### `Helper/Tools.py`
Shared helper utilities mirroring the core project helpers.

Includes:
- safe JSON read/write helpers
- settings and environment loading
- timezone conversions and scheduling helpers
- reusable support functions for helper-side scripts

---

## Daily Workflow

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

## Key Data Files

The system depends on several JSON and text files stored in the project path:

- `Portfolio.json` — active holdings and trade state
- `Report.json` — detailed trade-object log/history
- `FilteredTickers.json` — symbols that passed the ML filter
- `Tickers.txt` — current ticker universe
- `Settings.txt` — thresholds and runtime configuration
- `Calendar.json` — market open/close schedule
- `Status.json` — watchdog status for Lifeline

---

## Configuration

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

## Installation

1. Create and activate a Python environment.
2. Install the dependencies from `requirements.txt`.
3. Configure the `.env` file with your Alpaca and path settings.
4. Make sure the required JSON support files exist in your model directory.

---

## Running the System

Typical entry points:

- Run `System/Master.py` to start the trading engine
- Run `System/Lifeline.py` to monitor system health

---

## Logs and Notifications

The system writes output logs such as:

- `Master_Output.log`
- `MLCurrent_Output.log`
- `MLFilter_Output.log`
- `EOD_Output.log`

It also sends normal and urgent notifications to Discord webhooks.

---

## Notes

- This project is designed around U.S. market hours and Eastern Time scheduling.
- It assumes Alpaca connectivity and valid account permissions.
- The scripts are tightly linked through shared files and environment settings.
- Review all trading logic carefully before using with live capital.

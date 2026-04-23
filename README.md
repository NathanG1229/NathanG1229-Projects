# NathanG1229-Projects
A public access into the projects that I have created.

You can find them in the branches 2 branches of this repository.

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
How the Project Works
The workbook provides the user-facing analysis experience.
The Python scripts download:
SEC company facts,
SEC company reference data,
historical stock-price data from Alpaca.
The data is cleaned, transformed, and written into a SQLite database.
Sector tables are combined into a single table for broader analysis.
Excel can then be used to explore and compare companies.
Typical Setup Steps
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

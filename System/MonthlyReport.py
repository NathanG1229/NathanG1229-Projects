#!/usr/bin/env python3
"""
AlpacaPortfolioReport.py - Generate and send monthly portfolio performance report using Alpaca API
"""

import os
import json
import requests
from datetime import datetime, timedelta, date
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from Alpaca import get_acc_history
from Webhook import send_discord_message

# Load environment variables
dotenv = find_dotenv()
load_dotenv(dotenv, override=True)

M_PATH = os.getenv("Model_Path")

master_log = os.path.join(M_PATH, 'Master_Output.log')

def return_log_info(start_date, end_date, max_lines=None):
    """
    Read a log file and return its lines.
    If max_lines is set, returns only the last N lines.
    """
    try:
        with open(master_log, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return lines[-max_lines:] if max_lines else lines
    except FileNotFoundError:
        return [f"Log file not found: {master_log}"]
    except Exception as e:
        return [f"Error reading log file: {e}"]

def process_portfolio_data():
    """
    Process API data into a format suitable for analysis.
    
    Returns:
        list: List of tuples (datetime_obj, equity_value) sorted chronologically
    """
    api_data = get_acc_history()
    if not api_data or not api_data["timestamp"] or not api_data["equity"]:
        return []
    
    portfolio_data = []
    
    for timestamp, equity in zip(api_data["timestamp"], api_data["equity"]):
        try:
            # Convert Unix timestamp to datetime object
            dt_obj = datetime.fromtimestamp(timestamp)
            portfolio_data.append((dt_obj, float(equity)))
        except (ValueError, TypeError) as e:
            print(f"Error processing data point: timestamp={timestamp}, equity={equity}, error={e}")
            continue
    
    # Sort by datetime
    portfolio_data.sort(key=lambda x: x[0])
    
    # Get date range
    if portfolio_data:
        start_date = portfolio_data[0][0].date()
        end_date = portfolio_data[-1][0].date()
        print(f"Portfolio data range: {start_date} to {end_date}")
    
    return portfolio_data




def calculate_performance_metrics(portfolio_data):
    """
    Calculate performance metrics from portfolio data.
    
    Args:
        portfolio_data (list): List of tuples (datetime_obj, equity_value)
    
    Returns:
        dict: Dictionary containing performance metrics
    """
    if not portfolio_data:
        return None
    
    equity_values = [entry[1] for entry in portfolio_data]
    
    # Calculate metrics
    high_value = max(equity_values)
    low_value = min(equity_values)
    first_value = equity_values[0]
    last_value = equity_values[-1]
    
    # Calculate return percentage
    return_percent = ((last_value - first_value) / first_value) * 100
    
    # Find datetime for high and low
    high_entry = next(entry for entry in portfolio_data if entry[1] == high_value)
    low_entry = next(entry for entry in portfolio_data if entry[1] == low_value)
    
    metrics = {
        'high_value': high_value,
        'high_datetime': high_entry[0],
        'low_value': low_value,
        'low_datetime': low_entry[0],
        'first_value': first_value,
        'first_datetime': portfolio_data[0][0],
        'last_value': last_value,
        'last_datetime': portfolio_data[-1][0],
        'return_percent': return_percent,
        'total_data_points': len(portfolio_data)
    }
    
    return metrics


def create_portfolio_chart(portfolio_data, start_date, end_date):
    """
    Create a line chart of portfolio performance and save it as Graph.png.
    
    Args:
        portfolio_data (list): List of tuples (datetime_obj, equity_value)
        start_date (date): Start date for the chart title
        end_date (date): End date for the chart title
    
    Returns:
        str: Path to the saved PNG file, or None if failed
    """
    try:
        if not portfolio_data:
            return None
        
        # Prepare data for plotting
        dates = [entry[0] for entry in portfolio_data]
        values = [entry[1] for entry in portfolio_data]
        
        if not dates:
            return None
        
        # Create the plot
        plt.figure(figsize=(12, 8))
        plt.plot(dates, values, linewidth=2, color='#2E86AB', alpha=0.8)
        
        # Customize the plot
        plt.title(f'Portfolio Performance: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Date', fontsize=12, fontweight='bold')
        plt.ylabel('Portfolio Value ($)', fontsize=12, fontweight='bold')
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(set(d.date() for d in dates)) // 10)))
        plt.xticks(rotation=45)
        
        # Add grid
        plt.grid(True, alpha=0.3)
        
        # Format y-axis to show dollar signs
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        # Save the chart as Graph.png in the same directory as the script
        chart_path = os.path.join(M_PATH, "Graph.png")
        plt.savefig(chart_path, format='png', dpi=150, bbox_inches='tight')
        
        # Close the plot to free memory
        plt.close()
        
        return chart_path
        
    except Exception as e:
        print(f"Error creating chart: {e}")
        return None


def send_discord_message_with_chart(title, origin, body, chart_path):
    """
    Send a formatted message to Discord webhook with chart attachment.
    
    Args:
        title (str): The title text that will be displayed in bold
        origin (str): The origin/source text that will be displayed in italic
        body (str): The main message body in plain text
        chart_path (str): Path to the chart PNG file
    
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    webhook_url = "https://discordapp.com/api/webhooks/1455968794366185495/rxKXtXZM8JGpaqPH613QpOxN-SZ3eksITaDywYPHDL4pvslObk_xn7KFVLTkbMkhptiv"
    
    try:
        # Format the message with Discord markdown
        formatted_message = f"**{title}**\n*{origin}*\n\n{body}"
        
        # Prepare the files for upload
        files = {
            # JSON payload for content and embed
            'payload_json': (None, json.dumps({
                "content": formatted_message,
                "embeds": [
                    {
                        "image": {"url": "attachment://Graph.png"}
                    }
                ]
            })),
            # Attach the chart file
            'Graph.png': open(chart_path, 'rb')
        }
        
        response = requests.post(webhook_url, files=files)
        
        # Close the file
        files['Graph.png'].close()
        
        if response.status_code == 200:
            print("Discord message with chart sent successfully!")
            return True
        else:
            print(f"Failed to send Discord message. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error sending Discord message with chart: {e}")
        return False


def format_performance_message(metrics, start_date, end_date):
    """
    Format the performance data into a Discord message.
    
    Args:
        metrics (dict): Performance metrics dictionary
        start_date (date): Start date of the analysis
        end_date (date): End date of the analysis
    
    Returns:
        str: Formatted message for Discord
    """
    if not metrics:
        return "No portfolio data found for the specified period."
    
    # Format the return percentage with appropriate emoji
    return_emoji = "📈" if metrics['return_percent'] >= 0 else "📉"
    return_sign = "+" if metrics['return_percent'] >= 0 else ""
    
    message = f"""📊 **MONTHLY PORTFOLIO PERFORMANCE REPORT (ALPACA API)**
📅 Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}

💰 **PERFORMANCE METRICS:**
🏆 High: ${metrics['high_value']:,.2f} (on {metrics['high_datetime'].strftime('%m/%d')} at {metrics['high_datetime'].strftime('%H:%M:%S')})
📉 Low: ${metrics['low_value']:,.2f} (on {metrics['low_datetime'].strftime('%m/%d')} at {metrics['low_datetime'].strftime('%H:%M:%S')})
🎯 Return: {return_emoji} {return_sign}{metrics['return_percent']:.2f}%

📊 **PERIOD COMPARISON:**
🟢 Start: ${metrics['first_value']:,.2f} ({metrics['first_datetime'].strftime('%m/%d')} at {metrics['first_datetime'].strftime('%H:%M:%S')})
🔴 End: ${metrics['last_value']:,.2f} ({metrics['last_datetime'].strftime('%m/%d')} at {metrics['last_datetime'].strftime('%H:%M:%S')})
💵 Net Change: ${metrics['last_value'] - metrics['first_value']:,.2f}

📈 **DATA SUMMARY:**
🔢 Total Data Points: {metrics['total_data_points']:,}
📊 Range: ${metrics['high_value'] - metrics['low_value']:,.2f}
📋 Average: ${sum([metrics['high_value'], metrics['low_value']]) / 2:,.2f}"""

    return message


def monthly_report():
    """
    Main function to generate and send monthly portfolio performance report using Alpaca API.
    """
    try:
        print("Starting Alpaca Portfolio Performance Report...")
        
        
        # Process the API data
        portfolio_data = process_portfolio_data()
        
        if not portfolio_data:
            error_message = "No valid portfolio data found in API response"
            print(error_message)
            send_discord_message(
                "Alpaca Portfolio Report Error",
                "AlpacaPortfolioReport.py",
                error_message
            )
            return
        
        # Get date range from the data
        start_date = portfolio_data[0][0].date()
        end_date = portfolio_data[-1][0].date()
        
        print(f"Analyzing Alpaca portfolio performance from {start_date} to {end_date}")
        
        # Calculate performance metrics
        metrics = calculate_performance_metrics(portfolio_data)
        
        if not metrics:
            error_message = "Failed to calculate performance metrics"
            print(error_message)
            send_discord_message(
                "Alpaca Portfolio Report Error",
                "AlpacaPortfolioReport.py",
                error_message
            )
            return
        
        # Create performance chart
        print("Generating portfolio performance chart...")
        chart_path = create_portfolio_chart(portfolio_data, start_date, end_date)
        
        if chart_path and os.path.exists(chart_path):
            print(f"Chart generated successfully: {chart_path}")
        else:
            print("Warning: Failed to generate chart")
        
        # Format the message
        message_body = format_performance_message(metrics, start_date, end_date)
        
        print("Sending Discord notification...")
        
        # Send Discord message with chart attachment if chart was created
        if chart_path and os.path.exists(chart_path):
            success = send_discord_message_with_chart(
                "Alpaca Portfolio Performance Report",
                "AlpacaPortfolioReport.py",
                message_body,
                chart_path
            )
        else:
            # Fallback to text-only message if chart failed
            success = send_discord_message(
                "Alpaca Portfolio Performance Report",
                "AlpacaPortfolioReport.py",
                message_body
            )
        
        if success:
            print("Alpaca portfolio performance report sent successfully!")
        else:
            print("Failed to send portfolio performance report")
        
        # Print summary to console
        print("\n" + "="*60)
        print("ALPACA PORTFOLIO PERFORMANCE SUMMARY")
        print("="*60)
        print(f"Period: {start_date} to {end_date}")
        print(f"High: ${metrics['high_value']:,.2f}")
        print(f"Low: ${metrics['low_value']:,.2f}")
        print(f"Return: {metrics['return_percent']:+.2f}%")
        print(f"Data Points: {metrics['total_data_points']:,}")
        print("="*60)
        
    except Exception as e:
        error_message = f"Unexpected error in Alpaca portfolio report generation: {e}"
        print(error_message)
        try:
            send_discord_message(
                "Alpaca Portfolio Report Critical Error",
                "AlpacaPortfolioReport.py",
                error_message
            )
        except:
            pass  # Don't fail if Discord notification also fails


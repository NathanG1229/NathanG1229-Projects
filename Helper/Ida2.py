import subprocess
import sys
import os
import discord
import json
from datetime import datetime
from dotenv import load_dotenv
from Alpaca import get_account, get_positions, liquidate
from Tools import safe_read_json


load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Set up Discord client with intents
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Dictionary to track users waiting for script name input
waiting_for_script_name = {}


def kill_process(script_name):
    """Find and kill a process by script name."""
    try:
        # Use ps and grep to find processes matching the script name
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            return 0, f"Error getting process list."
        
        lines = result.stdout.split('\n')
        if len(lines) <= 1:
            return 0, f"No processes found."
        
        killed_count = 0
        found_processes = []
        
        for line in lines:
            # Skip if the line contains this script itself (Ida2.py) or grep
            if 'Ida2.py' in line or 'grep' in line:
                continue
                
            # Check if our target script is in this line
            if script_name in line and 'python' in line.lower():
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    found_processes.append(pid)
                    
                    # Kill the process
                    try:
                        subprocess.run(['kill', '-9', pid], check=True, capture_output=True)
                        print(f"✓ Killed process {pid}")
                        killed_count += 1
                    except subprocess.CalledProcessError as e:
                        print(f"✗ Could not kill process {pid}: {e}")
        
        if killed_count == 0:
            if found_processes:
                return 0, f"Found process(es) matching '{script_name}' but failed to kill them."
            else:
                return 0, f"No processes found matching '{script_name}'."
        else:
            return 1, f"Successfully killed {killed_count} process(es) matching '{script_name}'."
            
    except subprocess.CalledProcessError as e:
        return 0, f"Error finding processes: {e}"
    except Exception as e:
        return 0, f"Unexpected error: {e}"
    
async def handle_sleep_command(message):
    """Handle the !sleep command - ask user which script to sleep"""
    try:
        # Mark this user as waiting for script name input (with 'sleep' context)
        waiting_for_script_name[message.author.id] = 'sleep'

        await message.channel.send(f"Sounds good. Goodnight!")
        
        sys.exit(0)
        
    except Exception as e:
        await message.channel.send(f"❌ **YOU CAN'T KILL ME**")

async def handle_end_command(message):
    """Handle the !end command - ask user which script to end"""
    try:
        # Mark this user as waiting for script name input (with 'end' context)
        waiting_for_script_name[message.author.id] = 'end'
        
        # Ask which script to end
        await message.channel.send("Which script do you want to end?")
        
    except Exception as e:
        await message.channel.send(f"❌ **Error: {str(e)}**")

async def handle_script_name_response(message, script_name):
    """Handle the user's response with script name to kill"""
    try:
        # Kill the process
        success, result_message = kill_process(script_name)
        
        if success == 0:
            await message.channel.send(f"❌ {result_message}")
        else:
            await message.channel.send(f"✅ {result_message}")
        
        # Remove user from waiting list
        if message.author.id in waiting_for_script_name:
            del waiting_for_script_name[message.author.id]
        
    except Exception as e:
        await message.channel.send(f"❌ **Error when ending script: {str(e)}**")
        # Clean up waiting state
        if message.author.id in waiting_for_script_name:
            del waiting_for_script_name[message.author.id]

async def handle_start_command(message):
    """Handle the !start command - ask user which script to start"""
    try:
        # Mark this user as waiting for script name input (with 'start' context)
        waiting_for_script_name[message.author.id] = 'start'
        
        # Ask which script to start
        await message.channel.send("Which script do you want to start?")
        
    except Exception as e:
        await message.channel.send(f"❌ **Error: {str(e)}**")

async def handle_start_script_response(message, script_name):
    """Handle the user's response with script name to start"""
    try:
        # Get the Model_Path from environment
        model_path = os.getenv("Model_Path")
        if not model_path:
            await message.channel.send("❌ **Error**: Model_Path environment variable not set")
            return
        
        # Check if Model_Path directory exists
        if not os.path.exists(model_path):
            await message.channel.send(f"❌ **Error**: Model_Path directory does not exist: {model_path}")
            return
        
        # Add .py extension if not provided
        if not script_name.endswith('.py'):
            script_name = f"{script_name}.py"
        
        # Check if script exists in the Model_Path directory
        script_path = os.path.join(model_path, script_name)
        if not os.path.exists(script_path):
            await message.channel.send(f"❌ **Error**: {script_name} not found in {model_path}")
            return
        
        # Look for the venv Python interpreter in the Model-11 directory
        venv_python = os.path.join(model_path, "venv", "bin", "python")
        if not os.path.exists(venv_python):
            await message.channel.send(f"❌ **Error**: venv Python interpreter not found at {venv_python}")
            return
        
        # Send initial message
        await message.channel.send(f"🚀 **Starting {script_name} with nohup...** This may take a moment.")
        
        try:
            # Construct the nohup command using the Model-11 venv Python
            log_file = os.path.join(model_path, f'{script_name.replace(".py", "")}.log')
            nohup_command = f"nohup {venv_python} {script_path} > {log_file} 2>&1 &"
            
            # Execute the command using subprocess
            result = subprocess.run(
                nohup_command,
                shell=True,
                cwd=model_path,
                capture_output=True,
                text=True
            )
            
            # Send success message
            success_message = f"""
✅ **{script_name} Started Successfully!**

📊 **Details**:
• **Script**: {script_name}
• **Directory**: {model_path}
• **Python**: {venv_python}
• **Log file**: {script_name.replace(".py", "")}.log

Hello Nathan! I have successfully started the {script_name} script in nohup mode. The script is now running in the background with its own venv and will continue even if you close the terminal.
            """.strip()
            
            await message.channel.send(success_message)
            
        except subprocess.CalledProcessError as e:
            await message.channel.send(f"❌ **Error starting {script_name}**: {str(e)}")
        except Exception as e:
            await message.channel.send(f"❌ **Error executing nohup command**: {str(e)}")
        
        # Remove user from waiting list
        if message.author.id in waiting_for_script_name:
            del waiting_for_script_name[message.author.id]
            
    except Exception as e:
        await message.channel.send(f"❌ **Unexpected error**: {str(e)}")
        # Clean up waiting state
        if message.author.id in waiting_for_script_name:
            del waiting_for_script_name[message.author.id]

async def handle_liquidate_command(message):
    """Handle the !liquidate command"""
    try:
        # Send initial confirmation message
        await message.channel.send("🔄 **Liquidating all positions...** This may take a moment.")
        
        # Get current positions before liquidation
        try:
            positions_before = get_positions()
            position_count = len(positions_before)
        except Exception as e:
            await message.channel.send(f"❌ **Error getting current positions**: {str(e)}")
            return
        
        if position_count == 0:
            await message.channel.send("ℹ️ **No open positions found to liquidate.**")
            return
        
        # Perform liquidation
        try:
            liquidation_result = liquidate()
            
            # Check if liquidation was successful
            if isinstance(liquidation_result, list):
                successful_orders = len([order for order in liquidation_result if order.get('status') not in ['rejected', 'canceled']])
                
                # Send detailed success message
                success_message = f"""
✅ **Liquidation Complete!**

📊 **Summary**:
• **Positions liquidated**: {position_count}
• **Orders created**: {len(liquidation_result)}
• **Successful orders**: {successful_orders}

Hello Nathan! I have successfully liquidated all positions in your portfolio.
                """.strip()
                
                await message.channel.send(success_message)
                
            else:
                # Handle case where API returns different format
                await message.channel.send(f"✅ **Liquidation request sent successfully!** Result: {liquidation_result}")
                
        except Exception as e:
            await message.channel.send(f"❌ **Error during liquidation**: {str(e)}")
            
    except Exception as e:
        await message.channel.send(f"❌ **Unexpected error**: {str(e)}")

def format_money(value):
    """Format money value with commas and dollar sign"""
    try:
        value = float(value)
        return f"${value:,.2f}"
    except (ValueError, TypeError):
        return "N/A"

def format_return_percentage(current_price, purchase_price):
    """Format return as percentage with 2 decimal places and color coding"""
    try:
        current_price = float(current_price)
        purchase_price = float(purchase_price)
        return_value = (current_price - purchase_price) / purchase_price
        percentage = return_value * 100
        
        # Add color coding with Discord markdown
        if percentage > 0:
            return f"▲  **+{percentage:.2f}%**"
        elif percentage < 0:
            return f"▼ **{percentage:.2f}%**"
        else:
            return f"⚪ **{percentage:.2f}%**"
    except (ValueError, ZeroDivisionError):
        return "❓ **N/A**"

def format_shares(shares):
    """Format shares with commas if it's a number"""
    try:
        shares_num = float(shares)
        if shares_num == int(shares_num):
            return f"{int(shares_num):,}"
        else:
            return f"{shares_num:,.2f}"
    except (ValueError, TypeError):
        return str(shares)

async def handle_portfolio_command(message):
    """Handle the !portfolio command"""
    try:
        # Load positions data
        portfolio_path = os.path.join(os.getenv("Model_Path"), "Portfolio.json")
        portfolio_data = safe_read_json(portfolio_path)
        if "error" in portfolio_data:
            await message.channel.send(f"❌ **Error**: {portfolio_data['error']}")
            return
        
        # Get account data
        try:
            account_data = get_account()
            portfolio_value = account_data.get('portfolio_value', 'N/A')
        except Exception as e:
            await message.channel.send(f"❌ **Error getting account data**: {str(e)}")
            return
        
        # Get current positions from Alpaca
        try:
            alpaca_positions = get_positions()
            # Create a dictionary for quick lookup by symbol
            alpaca_prices = {}
            for pos in alpaca_positions:
                symbol = pos.get('symbol')
                current_price = pos.get('current_price')
                if symbol and current_price:
                    alpaca_prices[symbol] = current_price
        except Exception as e:
            await message.channel.send(f"❌ **Error getting positions data**: {str(e)}")
            return
        
        # Build portfolio response
        response = "Hello Nathan! Here is your current portfolio balance:\n\n"
        response += f"💰 **Current Balance**: {format_money(portfolio_value)}\n\n"
        response += "📈 **Assets**:\n"
        
        # Process each asset from Positions.json
        assets = portfolio_data.get('Assets', [])
        if not assets:
            response += "No assets found in portfolio.\n"
        else:
            for i, asset in enumerate(assets, 1):
                ticker = asset.get('Ticker', 'N/A')
                shares = asset.get('Shares', 'N/A')
                days_left = asset.get('Days Left', 'N/A')
                purchase_price = asset.get('Purchase Price', 'N/A')
                
                # Get current price from Alpaca positions
                current_price = alpaca_prices.get(ticker, 'N/A')
                
                # Calculate return percentage
                return_pct = format_return_percentage(current_price, purchase_price)
                
                response += f"\n**Asset {i}**:\n"
                response += f"• **Ticker**: {ticker}\n"
                response += f"• **Shares**: {format_shares(shares)}\n"
                response += f"• **Days Left**: {days_left}\n"
                response += f"• **Purchase Price**: {format_money(purchase_price)}\n"
                response += f"• **Current Price**: {format_money(current_price)}\n"
                response += f"• **Return**: {return_pct}\n"
        
        # Split response if it's too long for Discord (2000 character limit)
        MAX_LENGTH = 1900
        if len(response) > MAX_LENGTH:
            # Send in chunks
            for i in range(0, len(response), MAX_LENGTH):
                await message.channel.send(response[i:i+MAX_LENGTH])
        else:
            await message.channel.send(response)
            
    except Exception as e:
        await message.channel.send(f"❌ **Unexpected error**: {str(e)}")

@client.event
async def on_ready():
    print(f'Portfolio Bot logged in as {client.user}')

@client.event
async def on_message(message):
    # Don't respond to the bot's own messages
    if message.author == client.user:
        return
    
    content = message.content.strip()
    
    # Check if user is waiting to provide a script name
    if message.author.id in waiting_for_script_name:
        # User is responding with a script name
        context = waiting_for_script_name[message.author.id]
        if context == 'start':
            await handle_start_script_response(message, content)
        else:
            # Default to 'end' context
            await handle_script_name_response(message, content)
        return
    
    # Check if bot should respond (DM, mention, or starts with bot name)
    should_respond = (
        isinstance(message.channel, discord.DMChannel) or 
        client.user in message.mentions or 
        content.startswith("!")
    )
    
    if not should_respond:
        return
    
    # Clean up the message content
    if client.user in message.mentions:
        content = content.replace(f"<@{client.user.id}>", "").replace(f"<@!{client.user.id}>", "").strip()
    if content.startswith("!"):
        content = content[1:].strip()
    
    if not content:
        return
    
    # Handle commands
    if content.lower() == "portfolio":
        await handle_portfolio_command(message)
        return
    elif content.lower() == "end":
        await handle_end_command(message)
        return
    elif content.lower() == "sleep":
        await handle_sleep_command(message)
        return
    elif content.lower() == "liquidate":
        await handle_liquidate_command(message)
        return
    elif content.lower() == "start":
        await handle_start_command(message)
        return
    
    # If no recognized command, send a help message
    help_message = """
🤖 **Ida Commands**

• `!portfolio` - Get a snapshot of your current portfolio balance and assets
• `!start` - Start DayRun2.py script with nohup in the background
• `!end` - End the Model-11 script by setting END_STATUS=1
• `!liquidate` - Liquidate (close) all open positions in your portfolio
• `!sleep` - Put the bot to sleep (temporarily stop responding)

For help with other commands, please contact support.
    """
    await message.channel.send(help_message.strip())

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in environment variables")
        exit(1)
    
    print("Starting Portfolio Bot...")
    client.run(DISCORD_TOKEN)
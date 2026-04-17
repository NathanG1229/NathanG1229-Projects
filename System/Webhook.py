import requests
    
def send_discord_message(title, origin, body):
    """
    Send a formatted message to Discord webhook.
    
    Args:
        title (str): The title text that will be displayed in bold
        origin (str): The origin/source text that will be displayed in italic
        body (str): The main message body in plain text
    
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    webhook_url = "place_webhook_url_here"
    
    # Format the message with Discord markdown
    formatted_message = f"**{title}**\n*{origin}*\n\n{body}"
    
    data = {
        "content": formatted_message,
        "username": "The Goose"  # constant username
    }

    try:
        response = requests.post(webhook_url, json=data)
        
        if response.status_code == 204:
            print("Message sent successfully!")
            return True
        else:
            print(f"Failed to send message: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        print(f"Error sending webhook: {e}")
        return False
    
def send_discord_urgent(title, origin, body):
    """
    Send a formatted message to Discord webhook.
    
    Args:
        title (str): The title text that will be displayed in bold
        origin (str): The origin/source text that will be displayed in italic
        body (str): The main message body in plain text
    
    Returns:
        bool: True if message sent successfully, False otherwise
    """
    webhook_url = "place_webhook_url_here"
    
    # Format the message with Discord markdown
    formatted_message = f"**{title}**\n*{origin}*\n\n{body}"
    
    data = {
        "content": formatted_message,
        "username": "Error Message"  # constant username
    }

    try:
        response = requests.post(webhook_url, json=data)
        
        if response.status_code == 204:
            print("Message sent successfully!")
            return True
        else:
            print(f"Failed to send message: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        print(f"Error sending webhook: {e}")
        return False
    





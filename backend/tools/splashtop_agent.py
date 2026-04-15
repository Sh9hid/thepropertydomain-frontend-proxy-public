import asyncio
from browser_use import Agent, Browser, ChatGoogle
from dotenv import load_dotenv
from pathlib import Path
import os

# Load API keys
load_dotenv(Path("backend/.env").resolve())

async def main():
    # We use a separate profile directory so we don't crash if your main Brave is already open
    user_data_dir = str(Path("tmp/splashtop_agent_profile").resolve())
    
    print("Launching Brave...")
    browser = Browser(
        headless=False, # Must be False so you can see it and log in if needed
        executable_path="D:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe",
        user_data_dir=user_data_dir,
        args=[
            "--disable-blink-features=AutomationControlled",
        ]
    )
    
    # The instructions for the AI
    task = """
    Go to https://my.splashtop.com/w/web_client/77155128
    
    CRITICAL INSTRUCTION: If you are presented with a login screen, STOP and WAIT for the human user to log in. Do not try to guess credentials. Just wait until you see the systems dashboard.
    
    Once you are on the dashboard with the systems:
    1. Look for the system that ends with the number '2' (there are others like abid and mansi).
    2. Click on that system to initiate the remote connection.
    3. Once the remote desktop stream is fully loaded and you can see the remote computer's screen, interact with the remote desktop to open a new browser tab.
    """
    
    llm = ChatGoogle(model="gemini-2.0-flash-exp")

    agent = Agent(
        task=task,
        llm=llm,
        browser=browser,
        wait_between_actions=2.0 # Standard human delay
    )
    
    print("Agent started. Waiting for completion...")
    await agent.run()
    
    print("Task completed. Keeping browser open for 60 seconds...")
    await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())

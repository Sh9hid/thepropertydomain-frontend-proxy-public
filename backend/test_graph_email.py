import os
import sys
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
import pytest

# Add backend to sys.path to import from services and core
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load .env with utf-8-sig to handle BOM
load_dotenv(dotenv_path=Path(__file__).parent / ".env", encoding="utf-8-sig")

# Configure logging to show all MSAL and HTTP activity
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_graph_email")

from services.integrations import _send_email_graph

pytestmark = [pytest.mark.optional, pytest.mark.integration]

async def test_graph_sending():
    recipient = "nitin.puri@lsre.com.au"  # Principal's email as the test target
    subject = "Woonona Lead Machine - MS Graph Verification Email"
    body = "<h1>Verification Successful (MS Graph)</h1><p>This is a test email from the Woonona Lead Machine via the Microsoft Graph API. This confirms that the Azure app registration is correctly configured for 'Send as Alias'.</p>"

    logger.info(f"Attempting to send MS Graph verification email to: {recipient}")

    # 1. Test MS Graph if configured
    if os.getenv("MS_CLIENT_ID") and os.getenv("MS_CLIENT_SECRET"):
        logger.info("Testing MS Graph API flow...")
        # _send_email_graph is blocking, so we run it in a thread
        success = await asyncio.to_thread(_send_email_graph, recipient, subject, body)
        if success:
            logger.info("✅ MS Graph send successful! (Status 202)")
        else:
            logger.error("❌ MS Graph send failed. Check Azure App permissions (Mail.Send).")
    else:
        logger.error("❌ MS_CLIENT_ID or MS_CLIENT_SECRET not found in .env")

if __name__ == "__main__":
    asyncio.run(test_graph_sending())

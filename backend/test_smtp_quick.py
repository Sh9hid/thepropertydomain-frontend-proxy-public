import os
import smtplib
from email.message import EmailMessage

import pytest

pytestmark = [pytest.mark.optional, pytest.mark.integration]


def test_smtp_login_with_env_password() -> None:
    password = (os.getenv("SMTP_TEST_PASSWORD") or "").strip()
    if not password:
        pytest.skip("Set SMTP_TEST_PASSWORD to run SMTP integration validation.")

    host = "smtp.office365.com"
    port = 587
    user = "nitin.puri@lsre.com.au"
    from_email = "oakville@lsre.com.au"

    server = smtplib.SMTP(host, port, timeout=10)
    try:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(user, password)

        msg = EmailMessage()
        msg["Subject"] = "Woonona Lead Machine - SMTP Test"
        msg["From"] = from_email
        msg["To"] = user
        msg.set_content("This is a test confirming standard SMTP login works.")
        server.send_message(msg)
    finally:
        server.quit()

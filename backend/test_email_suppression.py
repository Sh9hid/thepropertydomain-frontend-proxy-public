from models.schemas import SendEmailRequest
from services.integrations import send_email_service


def test_send_email_service_suppresses_operator_example(monkeypatch):
    monkeypatch.delenv("EMAIL_SUPPRESSION_RECIPIENTS", raising=False)

    payload = SendEmailRequest(
        account_id="waitlist-auto",
        recipient="operator@example.com",
        subject="Test",
        body="<p>Hi</p>",
        plain_text=False,
    )

    result = send_email_service(None, payload)
    assert result["ok"] is False
    assert result["provider"] == "suppressed"
    assert result["suppressed"] is True


def test_send_email_service_suppresses_env_recipients(monkeypatch):
    monkeypatch.setenv("EMAIL_SUPPRESSION_RECIPIENTS", "blocked@domain.com,second@domain.com")

    payload = SendEmailRequest(
        account_id="waitlist-auto",
        recipient="blocked@domain.com",
        subject="Test",
        body="<p>Hi</p>",
        plain_text=False,
    )

    result = send_email_service(None, payload)
    assert result["provider"] == "suppressed"
    assert result["suppressed"] is True

from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


def send_failure_email(*, subject: str, body: str) -> bool:
    recipients_raw = os.getenv("PREFECT_FAILURE_EMAIL_TO", "")
    recipients = [value.strip() for value in recipients_raw.split(",") if value.strip()]
    if not recipients:
        return False

    smtp_host = os.getenv("PREFECT_FAILURE_SMTP_HOST")
    smtp_port = int(os.getenv("PREFECT_FAILURE_SMTP_PORT", "587"))
    smtp_username = os.getenv("PREFECT_FAILURE_SMTP_USERNAME")
    smtp_password = os.getenv("PREFECT_FAILURE_SMTP_PASSWORD")
    sender = os.getenv("PREFECT_FAILURE_EMAIL_FROM") or smtp_username
    use_tls = os.getenv("PREFECT_FAILURE_SMTP_USE_TLS", "true").strip().lower() not in {"0", "false", "no"}

    if not smtp_host or not sender:
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
        if use_tls:
            smtp.starttls()
        if smtp_username:
            smtp.login(smtp_username, smtp_password or "")
        smtp.send_message(message)

    return True
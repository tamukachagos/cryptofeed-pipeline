"""Transactional email utilities (SMTP).

Configuration (set in .env):
    SMTP_HOST      — mail server hostname (default: localhost)
    SMTP_PORT      — port (default: 587)
    SMTP_USER      — SMTP username / login
    SMTP_PASSWORD  — SMTP password
    SMTP_FROM      — sender address (default: noreply@cryptodataapi.dev)
    SMTP_TLS       — "starttls" (default) | "ssl" | "none"

Usage:
    from api.email_utils import send_otp
    send_otp("user@example.com", "482917", plan="pro")
"""
from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage

_HOST     = os.getenv("SMTP_HOST", "localhost")
_PORT     = int(os.getenv("SMTP_PORT", "587"))
_USER     = os.getenv("SMTP_USER", "")
_PASSWORD = os.getenv("SMTP_PASSWORD", "")
_FROM     = os.getenv("SMTP_FROM", "noreply@cryptodataapi.dev")
_TLS      = os.getenv("SMTP_TLS", "starttls").lower()   # starttls | ssl | none


def send_otp(to: str, otp: str, plan: str = "pro") -> None:
    """Send a 6-digit OTP email for checkout email verification.

    Raises smtplib.SMTPException on delivery failure.
    """
    msg = EmailMessage()
    msg["Subject"] = "Your CryptoDataAPI verification code"
    msg["From"]    = _FROM
    msg["To"]      = to

    msg.set_content(
        f"Your CryptoDataAPI verification code ({plan} plan):\n\n"
        f"    {otp}\n\n"
        f"This code expires in 10 minutes. Do not share it.\n\n"
        f"If you did not request a CryptoDataAPI subscription, ignore this email."
    )
    msg.add_alternative(
        f"""<html><body style="font-family:Arial,sans-serif;padding:24px">
        <p>Your CryptoDataAPI verification code for the
           <strong>{plan.capitalize()}</strong> plan:</p>
        <div style="font-size:32px;font-weight:bold;letter-spacing:6px;
                    background:#f4f4f4;padding:16px 24px;display:inline-block;
                    border-radius:6px;margin:16px 0">{otp}</div>
        <p>This code expires in <strong>10 minutes</strong>. Do not share it.</p>
        <p style="color:#888;font-size:12px">
            If you did not request a CryptoDataAPI subscription, ignore this email.
        </p>
        </body></html>""",
        subtype="html",
    )
    _deliver(msg)


def _deliver(msg: EmailMessage) -> None:
    ctx = ssl.create_default_context()
    if _TLS == "ssl":
        with smtplib.SMTP_SSL(_HOST, _PORT, context=ctx) as smtp:
            if _USER:
                smtp.login(_USER, _PASSWORD)
            smtp.send_message(msg)
    elif _TLS == "none":
        with smtplib.SMTP(_HOST, _PORT) as smtp:
            if _USER:
                smtp.login(_USER, _PASSWORD)
            smtp.send_message(msg)
    else:  # starttls (default)
        with smtplib.SMTP(_HOST, _PORT) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ctx)
            smtp.ehlo()
            if _USER:
                smtp.login(_USER, _PASSWORD)
            smtp.send_message(msg)

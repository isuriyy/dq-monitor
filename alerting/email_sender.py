"""
Email alert sender using Gmail SMTP.
Uses smtplib — built into Python, no extra install needed.

Setup:
  1. Go to myaccount.google.com → Security → App Passwords
  2. Create a new App Password for "Mail"
  3. Put the 16-character password in EMAIL_PASSWORD in your .env
"""
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(subject: str, html_body: str,
               sender: str = None, password: str = None,
               receiver: str = None, smtp_host: str = None,
               smtp_port: int = None) -> bool:
    """
    Sends an HTML email via Gmail SMTP.
    Falls back to environment variables if args not provided.
    Returns True if sent successfully, False otherwise.
    """
    sender    = sender    or os.getenv("EMAIL_SENDER", "")
    password  = password  or os.getenv("EMAIL_PASSWORD", "")
    receiver  = receiver  or os.getenv("EMAIL_RECEIVER", "")
    smtp_host = smtp_host or os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
    smtp_port = smtp_port or int(os.getenv("EMAIL_SMTP_PORT", "587"))

    if not sender or not password or not receiver:
        print("  [Email] EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER not configured — skipping")
        return False

    if "your_email" in sender or "your_16" in password:
        print("  [Email] Placeholder credentials detected — skipping")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = sender
        msg["To"]      = receiver
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())

        print(f"  [Email] Alert sent to {receiver}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("  [Email] Authentication failed — check EMAIL_PASSWORD (use App Password, not Gmail password)")
        return False
    except Exception as e:
        print(f"  [Email] Error: {e}")
        return False

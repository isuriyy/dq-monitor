"""
Slack alert sender.
Uses Incoming Webhooks — no OAuth token needed.
Set SLACK_WEBHOOK_URL in your .env file.
"""
import requests
import json
import os


def send_slack(payload: dict, webhook_url: str = None) -> bool:
    """
    Sends a Slack Block Kit payload to the webhook URL.
    Returns True if sent successfully, False otherwise.
    """
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")

    if not url or url.startswith("https://hooks.slack.com/services/YOUR"):
        print("  [Slack] SLACK_WEBHOOK_URL not configured — skipping")
        return False

    try:
        resp = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("  [Slack] Alert sent successfully")
            return True
        else:
            print(f"  [Slack] Failed: HTTP {resp.status_code} — {resp.text}")
            return False
    except Exception as e:
        print(f"  [Slack] Error: {e}")
        return False

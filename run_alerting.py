"""
Phase 5 — Alerting Pipeline
=============================
Run from inside dq_monitor folder:

    python run_alerting.py

What it does:
  1. Reads anomaly_report.json  (from Phase 4)
  2. Reads gx_report.json       (from Phase 2)
  3. Decides which alerts to fire based on severity thresholds
  4. Deduplicates — skips alerts sent in the last 60 minutes
  5. Sends Slack alert  (if SLACK_WEBHOOK_URL is configured)
  6. Sends email alert  (if EMAIL_* vars are configured)
  7. Prints a full dry-run preview even if channels are not configured
  8. Saves alert history to metadata.db

Even without Slack/email configured, run this to see exactly
what alerts WOULD be sent — useful for testing and demos.
"""

import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from alerting.formatter import (
    format_slack_anomaly, format_slack_gx,
    format_email_anomaly, format_email_gx,
)
from alerting.slack_sender import send_slack
from alerting.email_sender import send_email
from alerting.dedup_store import DeduplicationStore

load_dotenv()
console = Console(width=120)

SEVERITY_COLOR = {
    "CRITICAL": "bold red",
    "HIGH":     "red",
    "MEDIUM":   "yellow",
}


def load_report(path: str) -> dict | None:
    if not os.path.exists(path):
        console.print(f"  [yellow]Warning: {path} not found — run previous phases first[/yellow]")
        return None
    with open(path) as f:
        return json.load(f)


def should_alert(severity: str) -> bool:
    thresholds = {
        "CRITICAL": os.getenv("ALERT_ON_CRITICAL", "true").lower() == "true",
        "HIGH":     os.getenv("ALERT_ON_HIGH",     "true").lower() == "true",
        "MEDIUM":   os.getenv("ALERT_ON_MEDIUM",   "false").lower() == "true",
    }
    return thresholds.get(severity, False)


def main():
    console.print(Panel(
        "[bold cyan]Phase 5 — Alerting Pipeline[/bold cyan]\n"
        "[dim]Slack  |  Email  |  Deduplication  |  Severity Routing[/dim]",
        expand=False
    ))

    dedup = DeduplicationStore(
        window_minutes=int(os.getenv("DEDUP_WINDOW_MINUTES", "60"))
    )

    # ── Load reports ──────────────────────────────────────────
    console.print("\n[bold]Step 1 — Loading reports[/bold]")
    anomaly_report = load_report("anomaly_report.json")
    gx_report      = load_report("gx_report.json")

    alerts_to_send = []

    # ── Evaluate anomaly report ───────────────────────────────
    if anomaly_report:
        status = anomaly_report["overall_status"]
        console.print(f"  anomaly_report.json → status: [{SEVERITY_COLOR.get(status,'white')}]{status}[/]  "
                      f"| {anomaly_report['total_anomalies']} anomalies")

        if status != "CLEAN" and should_alert(status):
            alerts_to_send.append({
                "type":     "anomaly",
                "severity": status,
                "report":   anomaly_report,
                "summary":  f"Anomaly alert: {anomaly_report['total_anomalies']} anomalies — status {status}",
            })

    # ── Evaluate GX report ────────────────────────────────────
    if gx_report:
        failed = gx_report.get("total_failed", 0)
        status = "CLEAN" if failed == 0 else "HIGH"
        console.print(f"  gx_report.json       → status: [{SEVERITY_COLOR.get(status,'green')}]{status}[/]  "
                      f"| {failed} failed checks")

        if failed > 0 and should_alert(status):
            alerts_to_send.append({
                "type":     "gx",
                "severity": status,
                "report":   gx_report,
                "summary":  f"GX suite: {failed} check(s) failed",
            })

    # ── Preview all alerts ────────────────────────────────────
    console.print(f"\n[bold]Step 2 — Alert preview ({len(alerts_to_send)} to send)[/bold]")

    if not alerts_to_send:
        console.print("  [green]No alerts to send — all systems clean.[/green]")
    else:
        tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        tbl.add_column("Type",     min_width=10)
        tbl.add_column("Severity", min_width=10)
        tbl.add_column("Summary",  min_width=50)
        tbl.add_column("Action",   min_width=15)

        for a in alerts_to_send:
            sev_color = SEVERITY_COLOR.get(a["severity"], "white")
            already   = dedup.already_sent("all", a["type"], a["severity"], "slack")
            action    = "[dim]DEDUPED — skip[/dim]" if already else "[green]SEND[/green]"
            tbl.add_row(
                a["type"],
                f"[{sev_color}]{a['severity']}[/{sev_color}]",
                a["summary"],
                action,
            )
        console.print(tbl)

    # ── Send alerts ───────────────────────────────────────────
    console.print("\n[bold]Step 3 — Sending alerts[/bold]")
    sent_count = 0

    for alert in alerts_to_send:
        atype    = alert["type"]
        severity = alert["severity"]
        report   = alert["report"]
        summary  = alert["summary"]

        # Deduplication check
        if dedup.already_sent("all", atype, severity, "slack"):
            console.print(f"  [dim]Skipping {atype} alert — already sent within dedup window[/dim]")
            continue

        console.print(f"\n  Sending [{SEVERITY_COLOR.get(severity,'white')}]{severity}[/] "
                      f"{atype} alert...")

        # ── Slack ─────────────────────────────────────────────
        if atype == "anomaly":
            slack_payload = format_slack_anomaly(report)
            email_subject, email_html = format_email_anomaly(report)
        else:
            slack_payload = format_slack_gx(report)
            email_subject, email_html = format_email_gx(report)

        slack_ok = send_slack(slack_payload)
        email_ok = send_email(email_subject, email_html)

        if slack_ok or email_ok:
            dedup.record("all", atype, severity, "slack", summary)
            sent_count += 1

        # Always record even in dry-run so demo shows dedup working
        if not slack_ok and not email_ok:
            console.print(
                f"  [dim]Dry run — no channels configured. "
                f"In production this would send:\n"
                f"    Slack: {len(str(slack_payload))} chars payload\n"
                f"    Email: Subject → {email_subject[:60]}...[/dim]"
            )
            dedup.record("all", atype, severity, "slack", summary)
            sent_count += 1

    # ── Alert history ─────────────────────────────────────────
    console.print("\n[bold]Step 4 — Recent alert history[/bold]")
    recent = dedup.get_recent_alerts(limit=10)

    if not recent:
        console.print("  [dim]No alert history yet.[/dim]")
    else:
        htbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        htbl.add_column("Sent at",  min_width=22)
        htbl.add_column("Channel", min_width=8)
        htbl.add_column("Severity", min_width=10)
        htbl.add_column("Summary",  min_width=50)

        for r in recent:
            sev_color = SEVERITY_COLOR.get(r["severity"], "white")
            htbl.add_row(
                r["sent_at"][:19],
                r["channel"],
                f"[{sev_color}]{r['severity']}[/{sev_color}]",
                r["summary"],
            )
        console.print(htbl)

    # ── Summary ───────────────────────────────────────────────
    console.print(f"\n[bold]Done.[/bold] {sent_count} alert(s) processed.\n")


if __name__ == "__main__":
    main()

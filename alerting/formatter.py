"""
Formats alert messages for Slack and email.
Builds structured, human-readable messages from the JSON reports
produced by Phase 2 (GX) and Phase 4 (Anomaly Detection).
"""
from datetime import datetime

SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH":     "🟠",
    "MEDIUM":   "🟡",
    "CLEAN":    "🟢",
}

STATUS_EMOJI = {
    "CRITICAL": "🚨",
    "HIGH":     "⚠️",
    "MEDIUM":   "💛",
    "CLEAN":    "✅",
}


# ──────────────────────────────────────────────────────────────
#  SLACK MESSAGES
# ──────────────────────────────────────────────────────────────

def format_slack_anomaly(report: dict) -> dict:
    """Formats anomaly_report.json into a Slack Block Kit payload."""
    status  = report["overall_status"]
    emoji   = STATUS_EMOJI.get(status, "❓")
    ts      = report.get("run_at", datetime.now().isoformat())[:19]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} DQ Anomaly Alert — {status}",
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Run at:*\n{ts}"},
                {"type": "mrkdwn", "text": f"*Anomalies:*\n{report['total_anomalies']} found"},
                {"type": "mrkdwn", "text": f"*Critical:*\n{report['critical']}"},
                {"type": "mrkdwn", "text": f"*High:*\n{report['high']}"},
            ]
        },
        {"type": "divider"},
    ]

    for table, result in report.get("tables", {}).items():
        if not result["anomalies"]:
            continue

        lines = [f"*Table: `{table}`*"]
        for a in result["anomalies"][:5]:   # cap at 5 per table
            sev    = a["severity"]
            emoji2 = SEVERITY_EMOJI.get(sev, "❓")
            metric = a["metric"]
            today  = a.get("today", "—")
            exp    = a.get("expected", a.get("lower_fence", "—"))
            score  = a.get("z_score", a.get("iqr_distance", a.get("anomaly_score", "—")))
            lines.append(
                f"{emoji2} `{metric}` — today: *{today}*, expected: ~{exp} (score: {score})"
            )

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": "DQ Monitor — Phase 5 Alerting | anomaly_report.json"}]
    })

    return {"blocks": blocks}


def format_slack_gx(report: dict) -> dict:
    """Formats gx_report.json into a Slack Block Kit payload."""
    failed  = report.get("total_failed", 0)
    passed  = report.get("total_passed", 0)
    status  = "CLEAN" if failed == 0 else "FAILED"
    emoji   = STATUS_EMOJI.get("CLEAN" if failed == 0 else "HIGH", "❓")
    ts      = report.get("run_at", datetime.now().isoformat())[:19]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"{emoji} GX Check Suite — {status}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Run at:*\n{ts}"},
                {"type": "mrkdwn", "text": f"*Total checks:*\n{passed + failed}"},
                {"type": "mrkdwn", "text": f"*Passed:*\n✅ {passed}"},
                {"type": "mrkdwn", "text": f"*Failed:*\n❌ {failed}"},
            ]
        },
    ]

    if failed > 0:
        blocks.append({"type": "divider"})
        lines = ["*Failed checks:*"]
        for t in report.get("tables", []):
            if not t.get("success"):
                lines.append(
                    f"🔴 `{t['table']}` — {t['failed']} failure(s)"
                )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)}
        })

    return {"blocks": blocks}


# ──────────────────────────────────────────────────────────────
#  EMAIL MESSAGES
# ──────────────────────────────────────────────────────────────

def format_email_anomaly(report: dict) -> tuple[str, str]:
    """Returns (subject, html_body) for anomaly report email."""
    status  = report["overall_status"]
    emoji   = STATUS_EMOJI.get(status, "❓")
    ts      = report.get("run_at", "")[:19]
    total   = report["total_anomalies"]

    subject = f"{emoji} DQ Anomaly Alert [{status}] — {total} anomalies — {ts}"

    rows = ""
    for table, result in report.get("tables", {}).items():
        if not result["anomalies"]:
            rows += f"<tr><td><b>{table}</b></td><td colspan='5' style='color:green'>✅ Clean</td></tr>"
            continue
        for a in result["anomalies"]:
            sev    = a["severity"]
            color  = {"CRITICAL":"#c0392b","HIGH":"#e67e22","MEDIUM":"#f1c40f"}.get(sev,"#888")
            rows += f"""
            <tr>
              <td><b>{table}</b></td>
              <td style='color:{color};font-weight:bold'>{sev}</td>
              <td><code>{a['metric']}</code></td>
              <td>{a['detector']}</td>
              <td>{a.get('today','—')}</td>
              <td>{a.get('z_score', a.get('iqr_distance', a.get('anomaly_score','—')))}</td>
            </tr>"""

    html = f"""
    <html><body style='font-family:Arial,sans-serif;max-width:800px'>
      <h2 style='color:#2c3e50'>{emoji} Data Quality Anomaly Alert</h2>
      <p><b>Status:</b> <span style='color:{"#c0392b" if status=="CRITICAL" else "#e67e22"}'>{status}</span></p>
      <p><b>Run at:</b> {ts} &nbsp;|&nbsp;
         <b>Total anomalies:</b> {total} &nbsp;|&nbsp;
         <b>Critical:</b> {report['critical']} &nbsp;|&nbsp;
         <b>High:</b> {report['high']}</p>
      <table border='1' cellpadding='8' cellspacing='0'
             style='border-collapse:collapse;width:100%'>
        <tr style='background:#2c3e50;color:white'>
          <th>Table</th><th>Severity</th><th>Metric</th>
          <th>Detector</th><th>Today</th><th>Score</th>
        </tr>
        {rows}
      </table>
      <p style='color:#888;font-size:12px'>
        Generated by DQ Monitor Phase 5 Alerting
      </p>
    </body></html>
    """
    return subject, html


def format_email_gx(report: dict) -> tuple[str, str]:
    """Returns (subject, html_body) for GX check suite email."""
    failed  = report.get("total_failed", 0)
    passed  = report.get("total_passed", 0)
    status  = "CLEAN" if failed == 0 else "FAILED"
    emoji   = "✅" if failed == 0 else "❌"
    ts      = report.get("run_at", "")[:19]

    subject = f"{emoji} GX Check Suite [{status}] — {failed} failures — {ts}"

    rows = ""
    for t in report.get("tables", []):
        color = "#27ae60" if t.get("success") else "#c0392b"
        label = "PASSED" if t.get("success") else "FAILED"
        rows += f"""
        <tr>
          <td><b>{t['table']}</b></td>
          <td style='color:{color};font-weight:bold'>{label}</td>
          <td style='color:#27ae60'>{t['passed']}</td>
          <td style='color:#c0392b'>{t['failed']}</td>
        </tr>"""

    html = f"""
    <html><body style='font-family:Arial,sans-serif;max-width:600px'>
      <h2 style='color:#2c3e50'>{emoji} Great Expectations Check Suite</h2>
      <p><b>Status:</b> {status} &nbsp;|&nbsp;
         <b>Run at:</b> {ts}</p>
      <p>✅ Passed: <b>{passed}</b> &nbsp;|&nbsp; ❌ Failed: <b>{failed}</b></p>
      <table border='1' cellpadding='8' cellspacing='0'
             style='border-collapse:collapse;width:100%'>
        <tr style='background:#2c3e50;color:white'>
          <th>Table</th><th>Status</th><th>Passed</th><th>Failed</th>
        </tr>
        {rows}
      </table>
      <p style='color:#888;font-size:12px'>Generated by DQ Monitor Phase 5</p>
    </body></html>
    """
    return subject, html

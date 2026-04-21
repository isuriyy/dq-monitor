"""
anomaly/llm_assistant.py
========================
Two LLM-powered features:

1. Root cause analysis — structured investigation for each anomaly
   Produces: what happened, why (3 ranked causes), what to check, how to fix

2. AI chat assistant — answers questions about dashboard data
   Context: current scores, anomalies, alerts, sources
   Used by: the chat panel in the web dashboard via api_server.py
"""

import os
import json


def _load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def _get_client():
    """Returns (client, provider) — Gemini preferred, Anthropic fallback."""
    _load_env()
    gemini_key    = os.getenv("GEMINI_API_KEY", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

    if gemini_key and not gemini_key.startswith("AIzaSyYOUR"):
        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
            return client, "gemini"
        except ImportError:
            pass

    if anthropic_key and not anthropic_key.startswith("sk-ant-YOUR"):
        try:
            import anthropic
            return anthropic.Anthropic(api_key=anthropic_key), "anthropic"
        except ImportError:
            pass

    return None, "none"


def _call_llm(client, provider: str, prompt: str, max_tokens: int = 400) -> str:
    """Call whichever LLM is available."""
    try:
        if provider == "gemini":
            MODELS = ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.0-pro"]
            import time
            for model in MODELS:
                try:
                    r = client.models.generate_content(model=model, contents=prompt)
                    return r.text.strip()
                except Exception as e:
                    if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                        time.sleep(2)
                        continue
                    raise e
            return ""

        elif provider == "anthropic":
            r = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            return r.content[0].text.strip()

    except Exception as e:
        print(f"  [LLM error: {e}]")
        return ""


# ════════════════════════════════════════════════════════════════
#  FEATURE 1 — ROOT CAUSE ANALYSIS
# ════════════════════════════════════════════════════════════════

def _rule_based_root_cause(anomaly: dict, table: str) -> dict:
    """Rule-based root cause analysis when no API key is available."""
    metric    = anomaly.get("metric", "")
    today     = anomaly.get("today", "?")
    expected  = anomaly.get("expected", "?")
    direction = anomaly.get("direction", "HIGH")

    if metric == "row_count":
        if direction == "LOW":
            return {
                "what_happened": f"The {table} table row count dropped to {today} — significantly below the expected ~{expected}.",
                "causes": [
                    "Pipeline failure — the ETL job crashed or was interrupted mid-load",
                    "Source system issue — the upstream database or API returned no data",
                    "Accidental deletion — a DELETE statement ran without a WHERE clause",
                ],
                "what_to_check": [
                    f"Check ETL job logs for errors around the last run time",
                    f"Query the source system directly: SELECT COUNT(*) FROM {table.split('.')[-1]}",
                    "Check git history for recent DELETE migrations or data cleanup scripts",
                ],
                "how_to_fix": "Identify the root cause from logs, restore from backup if data was lost, re-run the pipeline after fixing the upstream issue.",
            }
        else:
            return {
                "what_happened": f"The {table} table row count spiked to {today} — significantly above the expected ~{expected}.",
                "causes": [
                    "Duplicate load — the pipeline ran twice and inserted data twice",
                    "Missing deduplication — a DISTINCT or dedup step was removed from the pipeline",
                    "Fanout join — a JOIN in the pipeline created duplicate rows",
                ],
                "what_to_check": [
                    "Check pipeline scheduler logs for duplicate runs",
                    f"Run: SELECT COUNT(*), COUNT(DISTINCT id) FROM {table.split('.')[-1]} to check for duplicates",
                    "Review recent pipeline code changes for removed deduplication logic",
                ],
                "how_to_fix": "Deduplicate the table using the primary key, fix the pipeline to prevent future duplicates, add a dedup step before loading.",
            }

    if metric.startswith("null_pct."):
        col = metric.replace("null_pct.", "")
        if direction == "HIGH":
            return {
                "what_happened": f"The null rate for {table}.{col} jumped to {today}% — far above the expected ~{expected}%.",
                "causes": [
                    f"Upstream source stopped sending {col} — API or schema change",
                    f"JOIN failure — the table providing {col} values wasn't joining correctly",
                    f"Pipeline bug — a transformation step that populates {col} was skipped",
                ],
                "what_to_check": [
                    f"Check the upstream source for {col} — is it still being provided?",
                    f"Run: SELECT COUNT(*) FROM {table.split('.')[-1]} WHERE {col} IS NULL",
                    "Review recent pipeline changes for modifications to the transformation that produces this column",
                ],
                "how_to_fix": "Fix the upstream source or JOIN condition, backfill the NULL values if possible, add a NOT NULL check to the pipeline.",
            }

    if metric.startswith("std."):
        col = metric.replace("std.", "")
        return {
            "what_happened": f"The spread of {table}.{col} values spiked to {today} — much wider than the expected range ~{expected}.",
            "causes": [
                f"Outlier rows loaded — a small number of extreme {col} values are skewing the distribution",
                f"Unit change — {col} values switched units (e.g. dollars to cents) for some rows",
                "Data corruption — invalid values slipped through validation",
            ],
            "what_to_check": [
                f"Run: SELECT MIN({col}), MAX({col}), AVG({col}) FROM {table.split('.')[-1]}",
                f"Run: SELECT * FROM {table.split('.')[-1]} WHERE {col} > (SELECT AVG({col})*10 FROM {table.split('.')[-1]})",
                "Check if any source system changes happened that could affect value ranges",
            ],
            "how_to_fix": "Identify and remove or correct the outlier rows, add range validation to the pipeline, investigate the source of the extreme values.",
        }

    # Generic fallback
    return {
        "what_happened": f"Anomaly detected in {table}.{metric}: today's value ({today}) is significantly {'above' if direction == 'HIGH' else 'below'} expected ({expected}).",
        "causes": [
            "Pipeline bug — a recent code change affected how this metric is calculated",
            "Source data change — the upstream system changed its output format",
            "Data quality issue — invalid or unexpected values passed validation",
        ],
        "what_to_check": [
            "Review recent pipeline deployments and code changes",
            "Check the upstream data source for changes",
            f"Query the {table} table directly to inspect recent rows",
        ],
        "how_to_fix": "Identify the source of the change, fix the pipeline or source, re-run validation after the fix.",
    }


def analyze_root_cause(anomaly: dict, table: str) -> dict:
    """
    Generates structured root cause analysis for one anomaly.
    Returns dict with: what_happened, causes (list), what_to_check (list), how_to_fix
    """
    client, provider = _get_client()

    if not client:
        return _rule_based_root_cause(anomaly, table)

    metric    = anomaly.get("metric", "")
    today     = anomaly.get("today", "")
    expected  = anomaly.get("expected", "")
    severity  = anomaly.get("severity", "")
    direction = anomaly.get("direction", "")
    detector  = anomaly.get("detector", "")

    prompt = f"""You are a senior data engineer investigating a data quality anomaly. Provide a structured root cause analysis.

Anomaly details:
- Table: {table}
- Metric: {metric}
- Today's value: {today}
- Expected value/range: {expected}
- Direction: {direction} (higher or lower than normal)
- Severity: {severity}
- Detector: {detector}

Respond in this EXACT JSON format (no markdown, no backticks, just raw JSON):
{{
  "what_happened": "One sentence describing what the data shows",
  "causes": [
    "Most likely cause — be specific",
    "Second most likely cause — be specific",
    "Third possible cause — be specific"
  ],
  "what_to_check": [
    "Specific SQL query or log to check first",
    "Second thing to investigate",
    "Third diagnostic step"
  ],
  "how_to_fix": "Concrete action to resolve this issue"
}}"""

    response = _call_llm(client, provider, prompt, max_tokens=500)

    if response:
        try:
            # Clean up response — remove any markdown
            clean = response.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            pass

    return _rule_based_root_cause(anomaly, table)


def analyze_all_anomalies(report: dict) -> dict:
    """
    Adds root cause analysis to every anomaly in the report.
    Modifies report in-place, adds 'root_cause' field to each anomaly.
    """
    client, provider = _get_client()
    mode = {"gemini": "Google Gemini", "anthropic": "Claude", "none": "rule-based"}.get(provider, "rule-based")

    total = sum(len(r.get("anomalies", [])) for r in report.get("tables", {}).values())
    if total == 0:
        return report

    print(f"  Generating {total} root cause analysis(es) [{mode}]...")

    for table, result in report.get("tables", {}).items():
        for anomaly in result.get("anomalies", []):
            if not anomaly.get("root_cause"):
                anomaly["root_cause"] = analyze_root_cause(anomaly, table)

    return report


# ════════════════════════════════════════════════════════════════
#  FEATURE 2 — AI CHAT ASSISTANT
# ════════════════════════════════════════════════════════════════

def build_dashboard_context() -> str:
    """Reads current dashboard data and builds a context string for the LLM."""
    context_parts = []

    # Load summary
    summary_path = "./web_dashboard/data/summary.json"
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            s = json.load(f)
        context_parts.append(f"""SYSTEM STATUS:
- Overall status: {s.get('overall_status','UNKNOWN')}
- Average DQ Score: {s.get('avg_dq_score',0)}/100
- Total anomalies: {s.get('total_anomalies',0)} ({s.get('critical',0)} critical, {s.get('high',0)} high, {s.get('medium',0)} medium)
- Sources monitored: {s.get('source_count',1)} ({', '.join(s.get('sources',[]))})
- Total snapshots: {s.get('total_snapshots',0)}
- Last pipeline run: {s.get('last_run','unknown')}""")

    # Load scores
    scores_path = "./web_dashboard/data/dq_scores.json"
    if os.path.exists(scores_path):
        with open(scores_path) as f:
            scores = json.load(f)
        score_lines = [f"  - {s['table']}: {s['score']}/100 ({s['status']}){' — ' + '; '.join(s['issues']) if s.get('issues') else ''}"
                       for s in scores]
        context_parts.append("TABLE HEALTH SCORES:\n" + "\n".join(score_lines))

    # Load anomalies
    anomalies_path = "./web_dashboard/data/anomalies.json"
    if os.path.exists(anomalies_path):
        with open(anomalies_path) as f:
            anomalies = json.load(f)
        if anomalies:
            anom_lines = [
                f"  - [{a['severity']}] {a['table']}.{a['metric']}: {a['today']} (expected ~{a['expected']}) — {a.get('explanation','')}"
                for a in anomalies[:10]
            ]
            context_parts.append("ACTIVE ANOMALIES:\n" + "\n".join(anom_lines))
        else:
            context_parts.append("ACTIVE ANOMALIES: None — all metrics within normal range")

    # Load alert history
    alert_path = "./web_dashboard/data/alert_log.json"
    if os.path.exists(alert_path):
        with open(alert_path) as f:
            alerts = json.load(f)
        if alerts:
            alert_lines = [f"  - [{a['severity']}] {a['sent_at'][:16]} via {a['channel']}: {a['summary']}"
                           for a in alerts[:5]]
            context_parts.append("RECENT ALERTS:\n" + "\n".join(alert_lines))

    return "\n\n".join(context_parts)


def chat(user_message: str, conversation_history: list = None) -> str:
    """
    Main chat function — answers questions about dashboard data.

    Args:
        user_message: The user's question
        conversation_history: List of {"role": "user"|"assistant", "content": "..."} dicts

    Returns:
        AI response string
    """
    client, provider = _get_client()

    context = build_dashboard_context()
    history = conversation_history or []

    system_prompt = f"""You are DQ Monitor AI — a data quality assistant embedded in a data monitoring dashboard.
You help data engineers and analysts understand their data quality issues.

You have access to the current state of the monitored databases:

{context}

Answer questions clearly and concisely. Reference specific tables, scores, and anomalies from the data above.
If asked what to do, give actionable advice. If asked to explain something technical, keep it accessible.
Keep responses under 150 words unless the question requires more detail.
Do not make up data — only reference what is shown above."""

    if not client:
        return _rule_based_chat(user_message, context)

    try:
        if provider == "gemini":
            # Build conversation for Gemini
            conversation = f"{system_prompt}\n\n"
            for msg in history[-6:]:  # Last 6 messages for context
                role = "User" if msg["role"] == "user" else "Assistant"
                conversation += f"{role}: {msg['content']}\n"
            conversation += f"User: {user_message}\nAssistant:"

            response = _call_llm(client, provider, conversation, max_tokens=300)
            return response or _rule_based_chat(user_message, context)

        elif provider == "anthropic":
            messages = []
            for msg in history[-6:]:
                messages.append({"role": msg["role"], "content": msg["content"]})
            messages.append({"role": "user", "content": user_message})

            r = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                system=system_prompt,
                messages=messages
            )
            return r.content[0].text.strip()

    except Exception as e:
        return f"I encountered an error: {str(e)[:100]}. Please try again."


def _rule_based_chat(message: str, context: str) -> str:
    """Simple rule-based chat when no API key is available."""
    msg = message.lower()

    if any(w in msg for w in ["status", "overall", "health", "summary"]):
        lines = [l for l in context.split('\n') if 'status' in l.lower() or 'score' in l.lower() or 'anomal' in l.lower()]
        return "Based on current data:\n" + "\n".join(lines[:5])

    if any(w in msg for w in ["anomal", "wrong", "issue", "problem", "critical"]):
        lines = [l for l in context.split('\n') if 'ANOMAL' in l or 'CRITICAL' in l or 'WARNING' in l]
        if lines:
            return "Current anomalies detected:\n" + "\n".join(lines[:5])
        return "No anomalies detected — all metrics are within normal ranges."

    if any(w in msg for w in ["alert", "notif", "slack", "email"]):
        lines = [l for l in context.split('\n') if 'ALERT' in l or 'slack' in l.lower() or 'email' in l.lower()]
        return "Recent alerts:\n" + "\n".join(lines[:5]) if lines else "No recent alerts."

    if any(w in msg for w in ["worst", "bad", "lowest", "investigate first"]):
        lines = [l for l in context.split('\n') if 'CRITICAL' in l or 'WARNING' in l]
        return "Tables needing attention:\n" + "\n".join(lines[:3]) if lines else "All tables are healthy."

    return ("I can answer questions about your data quality status, anomalies, alerts, and table health scores. "
            "Try asking: 'What's wrong with my data?' or 'Which table should I investigate first?' "
            "Note: Add GEMINI_API_KEY to .env for full AI responses.")

"""
anomaly/llm_explainer.py
========================
Generates plain-English explanations for anomalies.

Priority order:
  1. Google Gemini  — if GEMINI_API_KEY is set (free tier: 1500 req/day)
  2. Anthropic      — if ANTHROPIC_API_KEY is set
  3. Rule-based     — always works, no API key needed

Set in .env:
  GEMINI_API_KEY=AIzaSy...
  or
  ANTHROPIC_API_KEY=sk-ant-...
"""
import os


# ── Prompt template (shared across all LLMs) ─────────────────
def _build_prompt(anomaly: dict, table: str) -> str:
    contrib_text = ""
    if anomaly.get("top_contributors"):
        contribs = [f"{c['metric']} (Z={c['z_score']})"
                    for c in anomaly["top_contributors"][:3]]
        contrib_text = f"Top contributors: {', '.join(contribs)}."

    return f"""You are a data quality expert. Write exactly one plain-English sentence explaining this data anomaly to a business analyst. Be specific about what likely went wrong. Do not use technical jargon like Z-score or IQR. Do not start with "I". Be direct and actionable. End with a period.

Anomaly:
- Table: {table}
- Metric: {anomaly.get('metric', '')}
- Today's value: {anomaly.get('today', '')}
- Expected value/range: {anomaly.get('expected', '')}
- Direction: {anomaly.get('direction', '')} (higher or lower than normal)
- Severity: {anomaly.get('severity', '')}
- Detector: {anomaly.get('detector', '')}
{contrib_text}

One sentence only. No bullet points. No preamble."""


# ── Rule-based fallback ───────────────────────────────────────
def _rule_based(anomaly: dict, table: str) -> str:
    metric    = anomaly.get("metric", "")
    today     = anomaly.get("today", "?")
    expected  = anomaly.get("expected", "?")
    direction = anomaly.get("direction", "HIGH")
    detector  = anomaly.get("detector", "")
    severity  = anomaly.get("severity", "")

    if metric == "row_count":
        if direction == "LOW":
            try:
                pct = round((1 - float(today) / float(expected)) * 100, 1)
                return (f"The {table} table row count dropped {pct}% from its historical average "
                        f"({today} vs expected ~{expected}) — this likely indicates a pipeline "
                        f"failure, a truncated load, or an unexpected deletion.")
            except Exception:
                return (f"The {table} table has significantly fewer rows than expected "
                        f"— possible pipeline failure or data deletion.")
        else:
            try:
                pct = round((float(today) / float(expected) - 1) * 100, 1)
                return (f"The {table} table row count spiked {pct}% above its historical average "
                        f"({today} vs expected ~{expected}) — this may indicate duplicate data "
                        f"was loaded or a pipeline ran twice.")
            except Exception:
                return (f"The {table} table has significantly more rows than expected "
                        f"— possible duplicate data load.")

    if metric.startswith("null_pct."):
        col = metric.replace("null_pct.", "")
        if direction == "HIGH":
            return (f"The null rate for {table}.{col} jumped to {today}% today "
                    f"(expected ~{expected}%) — upstream data may be arriving incomplete, "
                    f"a JOIN may be failing, or a data source connection was lost.")
        return (f"The null rate for {table}.{col} dropped to {today}% "
                f"(expected ~{expected}%) — data that was previously missing is now being populated.")

    if metric.startswith("mean."):
        col = metric.replace("mean.", "")
        return (f"The average value of {table}.{col} is {today}, significantly "
                f"{'above' if direction == 'HIGH' else 'below'} "
                f"the expected range (~{expected}) — check for incorrect values, "
                f"a currency or unit change, or a data entry error.")

    if metric.startswith("std."):
        col = metric.replace("std.", "")
        return (f"The spread of values in {table}.{col} is unusually "
                f"{'high' if direction == 'HIGH' else 'low'} today (std={today}, "
                f"expected ~{expected}) — outlier values may have been loaded or "
                f"the data distribution has changed unexpectedly.")

    if metric.startswith("distinct_count."):
        col = metric.replace("distinct_count.", "")
        return (f"The number of unique values in {table}.{col} changed unexpectedly "
                f"({today} vs expected ~{expected}) — new categories may have appeared "
                f"or existing ones were merged or deleted.")

    if detector == "isolation_forest":
        contributors = anomaly.get("top_contributors", [])
        top = contributors[0].get("metric", "multiple metrics") if contributors else "multiple metrics"
        return (f"The overall data profile of {table} is statistically anomalous today — "
                f"no single metric triggered the alert alone, but the combination of values "
                f"(driven by {top}) is unusual compared to the past 30 days.")

    return (f"{severity} anomaly detected in {table}.{metric}: today's value ({today}) is "
            f"significantly {'above' if direction == 'HIGH' else 'below'} "
            f"the expected range (~{expected}) — investigate recent pipeline runs.")


# ── Gemini explainer ──────────────────────────────────────────
# Try these models in order — first one with free quota wins
GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.0-pro",
]

def _gemini_explanation(anomaly: dict, table: str, api_key: str) -> str:
    try:
        import time
        from google import genai
        client = genai.Client(api_key=api_key)
        prompt = _build_prompt(anomaly, table)

        for model in GEMINI_MODELS:
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=prompt,
                )
                text = response.text.strip()
                if text and not text.endswith((".", "!", "?")):
                    text += "."
                return text
            except Exception as model_err:
                err_str = str(model_err)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    # Rate limited on this model — try next
                    time.sleep(1)
                    continue
                else:
                    raise model_err

        # All models rate limited — fall back
        print("    [Gemini: all models rate limited — using rule-based]")
        return _rule_based(anomaly, table)

    except ImportError:
        print("    [Gemini error: run: pip install google-genai]")
        return _rule_based(anomaly, table)
    except Exception as e:
        print(f"    [Gemini error: {type(e).__name__}: {str(e)[:80]}]")
        return _rule_based(anomaly, table)


# ── Anthropic explainer ───────────────────────────────────────
def _anthropic_explanation(anomaly: dict, table: str, api_key: str) -> str:
    try:
        import anthropic
        client   = anthropic.Anthropic(api_key=api_key)
        prompt   = _build_prompt(anomaly, table)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        if text and not text.endswith((".", "!", "?")):
            text += "."
        return text
    except Exception as e:
        print(f"    [Anthropic error: {e} — falling back to rule-based]")
        return _rule_based(anomaly, table)


# ── Public API ────────────────────────────────────────────────
def explain_anomaly(anomaly: dict, table: str) -> str:
    """
    Returns a plain-English explanation for one anomaly.
    Uses Gemini → Anthropic → rule-based, in that priority order.
    """
    gemini_key    = os.getenv("GEMINI_API_KEY", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

    if gemini_key and not gemini_key.startswith("AIzaSyYOUR"):
        return _gemini_explanation(anomaly, table, gemini_key)
    if anthropic_key and not anthropic_key.startswith("sk-ant-YOUR"):
        return _anthropic_explanation(anomaly, table, anthropic_key)
    return _rule_based(anomaly, table)


def explain_anomalies(report: dict) -> dict:
    """
    Enriches anomaly report with plain-English explanations.
    Adds 'explanation' field to each anomaly in-place.
    """
    gemini_key    = os.getenv("GEMINI_API_KEY", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")

    if gemini_key and not gemini_key.startswith("AIzaSyYOUR"):
        mode = "Google Gemini (free)"
    elif anthropic_key and not anthropic_key.startswith("sk-ant-YOUR"):
        mode = "Anthropic Claude"
    else:
        mode = "rule-based (no API key)"

    total = sum(
        len(r.get("anomalies", []))
        for r in report.get("tables", {}).values()
    )

    if total == 0:
        return report

    print(f"  Generating {total} explanation(s) [{mode}]...")

    for table, result in report.get("tables", {}).items():
        for anomaly in result.get("anomalies", []):
            if not anomaly.get("explanation"):
                anomaly["explanation"] = explain_anomaly(anomaly, table)

    return report

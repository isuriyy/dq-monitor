"""
anomaly/ai_assistant.py
=======================
AI assistant that answers questions about your data quality.

Two modes:
  1. General Q&A  — answers questions about scores, tables, pipeline runs
  2. Root cause   — deep analysis when asking about specific anomalies

Uses Gemini (free) → Anthropic → rule-based fallback.
"""
import os
import json


def _load_env():
    if os.path.exists(".env"):
        for line in open(".env"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def _build_context() -> dict:
    """Load all dashboard data to give the AI full context."""
    base = "./web_dashboard/data"
    context = {}
    for fname in ["summary", "dq_scores", "anomalies", "alert_history",
                  "history", "sources", "charts"]:
        path = f"{base}/{fname}.json"
        if os.path.exists(path):
            with open(path) as f:
                context[fname] = json.load(f)
    return context


def _build_system_prompt(context: dict) -> str:
    """Build a system prompt with full dashboard context."""
    summary    = context.get("summary", {})
    scores     = context.get("dq_scores", [])
    anomalies  = context.get("anomalies", [])
    sources    = context.get("sources", {})
    history    = context.get("history", [])
    alert_hist = context.get("alert_history", [])

    # Summarise scores
    score_lines = "\n".join(
        f"  - {s['table']}: score={s['score']}/100, status={s['status']}"
        + (f", issues: {'; '.join(s['issues'])}" if s.get('issues') else "")
        for s in scores
    )

    # Summarise anomalies
    anom_lines = "\n".join(
        f"  - [{a['severity']}] {a['table']}.{a['metric']}: "
        f"today={a['today']}, expected={a['expected']}, detector={a['detector']}"
        + (f"\n    explanation: {a['explanation']}" if a.get('explanation') else "")
        for a in anomalies[:10]
    ) or "  None detected"

    # Summarise sources
    src_lines = "\n".join(
        f"  - {name}: {data.get('dialect','').upper()}, "
        f"{data.get('table_count',0)} tables"
        for name, data in sources.items()
    ) if isinstance(sources, dict) else "  Not available"

    # Last pipeline run
    last_run = history[0] if history else {}
    last_run_str = (
        f"{last_run.get('run_at','unknown')} — "
        f"{last_run.get('table_count',0)} tables profiled across "
        f"{', '.join(last_run.get('sources',[]))}"
    ) if last_run else "No runs recorded"

    # Recent alerts
    alert_lines = "\n".join(
        f"  - {a.get('sent_at','')[:19]} [{a.get('severity','')}] "
        f"via {a.get('channel','')}: {a.get('summary','')}"
        for a in alert_hist[:5]
    ) or "  No alerts sent"

    return f"""You are an expert data quality analyst assistant embedded in DQ Monitor — a data quality monitoring platform.

You have access to LIVE data from the user's databases. Answer questions directly and specifically using this data. Be concise but thorough. Use plain English — no jargon unless explaining technical terms.

═══ CURRENT SYSTEM STATUS ═══
Overall status: {summary.get('overall_status', 'UNKNOWN')}
Average DQ score: {summary.get('avg_dq_score', 0)}/100
Total anomalies: {summary.get('total_anomalies', 0)} ({summary.get('critical', 0)} critical, {summary.get('high', 0)} high, {summary.get('medium', 0)} medium)
Last pipeline run: {last_run_str}
Sources monitored: {summary.get('source_count', 0)}

═══ TABLE HEALTH SCORES ═══
{score_lines or "  No data"}

═══ ACTIVE ANOMALIES ═══
{anom_lines}

═══ CONNECTED SOURCES ═══
{src_lines}

═══ RECENT ALERTS ═══
{alert_lines}

═══ YOUR ROLE ═══
- For general questions: give a clear, direct answer using the data above
- For anomaly questions: provide structured root cause analysis with ranked causes
- For "what should I do" questions: give specific, actionable next steps
- For "explain X" questions: explain in plain English what the metric means and why it matters
- Always reference actual table names, scores, and numbers from the data above
- If you don't have enough data to answer, say so clearly
- Keep responses under 300 words unless a deep analysis is requested"""


def _is_root_cause_question(question: str) -> bool:
    """Detect if the question needs root cause analysis mode."""
    keywords = [
        "why", "cause", "reason", "what happened", "root cause",
        "investigate", "diagnose", "fix", "how to", "what should",
        "explain anomaly", "explain the anomaly", "what caused",
        "critical", "flagged", "detected", "alert"
    ]
    q_lower = question.lower()
    return any(kw in q_lower for kw in keywords)


def _root_cause_prompt(question: str, context: dict) -> str:
    """Build a structured root cause analysis prompt."""
    return f"""The user is asking about a potential data quality issue. Provide a structured root cause analysis.

User question: {question}

Format your response EXACTLY like this:

**What happened:**
[One clear sentence describing the anomaly]

**Most likely causes (ranked):**
1. [Most likely cause — 70%+ probability]
2. [Second possibility]
3. [Third possibility]

**What to check first:**
- [Specific thing to investigate]
- [Second check]

**How to fix it:**
[Concrete steps to resolve]

**Prevention:**
[One sentence on how to prevent this in future]

Keep each section brief and actionable. Use the live data provided in your context."""


def ask(question: str) -> dict:
    """
    Ask the AI assistant a question.
    Returns dict with: answer, mode, model_used
    """
    _load_env()
    context = _build_context()
    system  = _build_system_prompt(context)
    mode    = "root_cause" if _is_root_cause_question(question) else "general"

    if mode == "root_cause":
        user_message = _root_cause_prompt(question, context)
    else:
        user_message = question

    # Try Gemini first
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if gemini_key and not gemini_key.startswith("AIzaSyYOUR"):
        result = _ask_gemini(system, user_message, gemini_key)
        if result:
            return {"answer": result, "mode": mode, "model": "Gemini"}

    # Try Anthropic
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    if anthropic_key and not anthropic_key.startswith("sk-ant-YOUR"):
        result = _ask_anthropic(system, user_message, anthropic_key)
        if result:
            return {"answer": result, "mode": mode, "model": "Claude"}

    # Rule-based fallback
    return {
        "answer": _rule_based_answer(question, context),
        "mode": mode,
        "model": "rule-based"
    }


def _ask_gemini(system: str, question: str, api_key: str) -> str:
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)
        models = ["gemini-2.0-flash-lite", "gemini-2.0-flash",
                  "gemini-2.5-flash-preview-04-17"]
        for model in models:
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=question,
                    config=types.GenerateContentConfig(
                        system_instruction=system,
                        max_output_tokens=600,
                        temperature=0.3,
                    )
                )
                return response.text.strip()
            except Exception as e:
                if "429" in str(e) or "404" in str(e):
                    continue
                raise e
        return None
    except Exception:
        return None


def _ask_anthropic(system: str, question: str, api_key: str) -> str:
    try:
        import anthropic
        client   = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            system=system,
            messages=[{"role": "user", "content": question}]
        )
        return response.content[0].text.strip()
    except Exception:
        return None


def _rule_based_answer(question: str, context: dict) -> str:
    """Fallback answers using live data — no API needed."""
    summary   = context.get("summary", {})
    scores    = context.get("dq_scores", [])
    anomalies = context.get("anomalies", [])
    q         = question.lower()

    if any(w in q for w in ["worst", "critical", "bad", "problem", "issue"]):
        worst = [s for s in scores if s["score"] < 90]
        if worst:
            w = worst[0]
            return (f"The worst table right now is **{w['table']}** with a DQ score of "
                    f"{w['score']}/100 ({w['status']}). "
                    + (f"Issues: {'; '.join(w['issues'])}." if w.get('issues') else "")
                    + f" There are {summary.get('total_anomalies',0)} total anomalies detected "
                    f"across all sources.")
        return "All tables are currently healthy — no critical issues detected."

    if any(w in q for w in ["score", "health", "status", "overview"]):
        status = summary.get('overall_status','UNKNOWN')
        avg    = summary.get('avg_dq_score', 0)
        return (f"Overall system status is **{status}**. Average DQ score: {avg}/100. "
                f"{summary.get('total_anomalies',0)} anomaly(s) detected — "
                f"{summary.get('critical',0)} critical, {summary.get('high',0)} high. "
                f"Monitoring {len(scores)} tables across {summary.get('source_count',1)} source(s).")

    if any(w in q for w in ["anomaly", "anomalies", "flag", "detected"]):
        if not anomalies:
            return "No anomalies currently detected — all metrics are within normal ranges."
        lines = [f"- [{a['severity']}] {a['table']}.{a['metric']}: {a.get('explanation','')}"
                 for a in anomalies[:5]]
        return f"**{len(anomalies)} anomaly(s) detected:**\n" + "\n".join(lines)

    if any(w in q for w in ["last run", "pipeline", "when", "latest"]):
        history = context.get("history", [])
        if history:
            r = history[0]
            return (f"Last pipeline run: **{r.get('run_at','unknown')}**. "
                    f"Profiled {r.get('table_count',0)} tables across "
                    f"{', '.join(r.get('sources',[]))}.")
        return "No pipeline run history found. Run python main.py to start profiling."

    if any(w in q for w in ["source", "database", "connection"]):
        sources = context.get("sources", {})
        if isinstance(sources, dict) and sources:
            lines = [f"- **{n}**: {d.get('dialect','').upper()}, {d.get('table_count',0)} tables"
                     for n, d in sources.items()]
            return "**Connected sources:**\n" + "\n".join(lines)
        return "No sources configured. Add databases to config/sources.yaml."

    # Generic fallback
    status = summary.get('overall_status', 'UNKNOWN')
    avg    = summary.get('avg_dq_score', 0)
    return (f"Current system status: **{status}**. Average DQ score: {avg}/100. "
            f"{summary.get('total_anomalies', 0)} anomaly(s) detected. "
            f"Ask me about specific tables, anomalies, pipeline runs, or data quality scores.")

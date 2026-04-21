"""
Dashboard data loader.
Reads everything the dashboard needs from:
  - metadata.db  (snapshot history, alert log)
  - anomaly_report.json
  - gx_report.json / dbt_report.json
"""
import sqlite3
import json
import os
import pandas as pd
from datetime import datetime


DB_PATH = "./metadata.db"


# ── Snapshot history ──────────────────────────────────────────
def load_snapshot_history(source="ecommerce_db") -> dict:
    """Returns {table_name: DataFrame} with all historical snapshots."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT table_name, profiled_at, row_count, profile_json
        FROM profile_snapshots
        WHERE source = ?
        ORDER BY table_name, profiled_at
    """, (source,)).fetchall()
    conn.close()

    tables = {}
    for table_name, profiled_at, row_count, profile_json in rows:
        profile = json.loads(profile_json)
        record = {
            "profiled_at": profiled_at,
            "row_count":   row_count,
        }
        for col_name, col_data in profile.get("columns", {}).items():
            if isinstance(col_data, dict):
                for metric in ["null_pct", "mean", "std", "distinct_count"]:
                    if metric in col_data and col_data[metric] is not None:
                        record[f"{metric}.{col_name}"] = float(col_data[metric])

        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append(record)

    result = {}
    for table_name, records in tables.items():
        df = pd.DataFrame(records)
        df["profiled_at"] = pd.to_datetime(df["profiled_at"], format="ISO8601")
        df = df.sort_values("profiled_at").reset_index(drop=True)
        result[table_name] = df

    return result


# ── DQ Score ──────────────────────────────────────────────────
def compute_dq_scores(snapshot_history: dict, anomaly_report: dict, gx_report: dict) -> list:
    """
    Computes a DQ score 0-100 for each table.
    Based on:
      - Anomaly findings (each CRITICAL = -15, HIGH = -8, MEDIUM = -3)
      - GX failures (each failure = -10)
      - Null % in latest snapshot (high nulls = penalty)
    """
    scores = []
    tables = list(snapshot_history.keys())

    for table in tables:
        score = 100
        issues = []

        # Anomaly penalty
        if anomaly_report:
            table_anomalies = anomaly_report.get("tables", {}).get(table, {})
            for a in table_anomalies.get("anomalies", []):
                sev = a.get("severity", "")
                if sev == "CRITICAL":
                    score -= 15
                    issues.append(f"CRITICAL anomaly: {a['metric']}")
                elif sev == "HIGH":
                    score -= 8
                    issues.append(f"HIGH anomaly: {a['metric']}")
                elif sev == "MEDIUM":
                    score -= 3

        # GX penalty
        if gx_report:
            for t in gx_report.get("tables", []):
                if t.get("table") == table:
                    score -= t.get("failed", 0) * 10

        # Null % penalty from latest snapshot
        df = snapshot_history.get(table)
        if df is not None and len(df) > 0:
            latest = df.iloc[-1]
            null_cols = [c for c in df.columns if c.startswith("null_pct.")]
            for col in null_cols:
                val = latest.get(col, 0)
                if pd.notna(val):
                    if val > 20:
                        score -= 5
                        issues.append(f"High nulls in {col}: {val:.1f}%")
                    elif val > 10:
                        score -= 2

        score = max(0, min(100, score))

        if score >= 90:
            status = "HEALTHY"
            color  = "#27ae60"
        elif score >= 70:
            status = "WARNING"
            color  = "#f39c12"
        else:
            status = "CRITICAL"
            color  = "#c0392b"

        scores.append({
            "table":  table,
            "score":  score,
            "status": status,
            "color":  color,
            "issues": issues,
        })

    return sorted(scores, key=lambda x: x["score"])


# ── Null % heatmap data ───────────────────────────────────────
def build_null_heatmap(snapshot_history: dict) -> pd.DataFrame:
    """
    Returns a DataFrame where:
      rows    = metric columns (null_pct.*)
      columns = dates
      values  = null percentage
    """
    frames = []
    for table, df in snapshot_history.items():
        null_cols = [c for c in df.columns if c.startswith("null_pct.")]
        for col in null_cols:
            sub = df[["profiled_at", col]].copy()
            sub = sub.rename(columns={col: "null_pct"})
            sub["metric"] = f"{table}.{col.replace('null_pct.', '')}"
            frames.append(sub)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = combined["profiled_at"].dt.date
    pivot = combined.groupby(["metric", "date"])["null_pct"].mean().reset_index()
    heatmap = pivot.pivot(index="metric", columns="date", values="null_pct")
    return heatmap


# ── Anomaly history ───────────────────────────────────────────
def load_anomaly_history() -> list:
    """Reads anomaly_report.json and returns flat list of anomaly dicts."""
    if not os.path.exists("anomaly_report.json"):
        return []
    with open("anomaly_report.json") as f:
        report = json.load(f)

    anomalies = []
    run_at = report.get("run_at", "")[:19]
    for table, result in report.get("tables", {}).items():
        for a in result.get("anomalies", []):
            anomalies.append({
                "detected_at": run_at,
                "table":       table,
                "severity":    a.get("severity", ""),
                "metric":      a.get("metric", ""),
                "detector":    a.get("detector", ""),
                "today":       a.get("today", "—"),
                "expected":    a.get("expected", "—"),
                "score":       a.get("z_score", a.get("iqr_distance", a.get("anomaly_score", "—"))),
            })
    return anomalies


# ── Alert log ─────────────────────────────────────────────────
def load_alert_log(limit=50) -> pd.DataFrame:
    """Returns recent alert history from metadata.db."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("""
            SELECT sent_at, channel, severity, summary
            FROM alert_log
            ORDER BY sent_at DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=["Sent at", "Channel", "Severity", "Summary"])
    except Exception:
        conn.close()
        return pd.DataFrame()


# ── Report loaders ────────────────────────────────────────────
def load_json_report(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)

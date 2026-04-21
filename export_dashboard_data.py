"""
Exports dashboard data for ALL sources in sources.yaml.
Run after any pipeline run to update the web dashboard.

    python export_dashboard_data.py
"""
import json, sqlite3, os, yaml
from datetime import datetime

OUT = "./web_dashboard/data"
os.makedirs(OUT, exist_ok=True)
DB = "./metadata.db"

def load_sources():
    with open("config/sources.yaml") as f:
        return yaml.safe_load(f).get("sources", [])

def load_snapshots_for_source(source_name):
    conn = sqlite3.connect(DB)
    rows = conn.execute("""
        SELECT table_name, profiled_at, row_count, profile_json
        FROM profile_snapshots
        WHERE source=?
        ORDER BY table_name, profiled_at
    """, (source_name,)).fetchall()
    conn.close()
    tables = {}
    for table, ts, rc, pj in rows:
        profile = json.loads(pj)
        rec = {"profiled_at": ts, "row_count": rc}
        for col, data in profile.get("columns", {}).items():
            if isinstance(data, dict):
                for m in ["null_pct","mean","std","distinct_count"]:
                    if m in data and data[m] is not None:
                        rec[f"{m}.{col}"] = float(data[m])
        tables.setdefault(table, []).append(rec)
    return tables

def load_alert_log():
    conn = sqlite3.connect(DB)
    try:
        rows = conn.execute(
            "SELECT sent_at, channel, severity, summary FROM alert_log ORDER BY sent_at DESC LIMIT 50"
        ).fetchall()
        conn.close()
        return [{"sent_at":r[0],"channel":r[1],"severity":r[2],"summary":r[3]} for r in rows]
    except:
        conn.close()
        return []

def load_report(path):
    if not os.path.exists(path): return {}
    with open(path) as f: return json.load(f)

def compute_scores(snapshots, anomaly_report, source_name=""):
    """
    Match anomalies to tables correctly.
    Anomaly report uses 'source.table' keys (e.g. 'ecommerce_db.orders').
    Snapshots use plain table names (e.g. 'orders').
    We try both formats.
    """
    scores = []
    anomaly_tables = anomaly_report.get("tables", {})

    for table, records in snapshots.items():
        score = 100; issues = []

        # Try both plain name and namespaced name
        candidates = [
            table,                           # plain: 'orders'
            f"{source_name}.{table}",        # namespaced: 'ecommerce_db.orders'
        ]
        anomalies = []
        for candidate in candidates:
            if candidate in anomaly_tables:
                anomalies = anomaly_tables[candidate].get("anomalies", [])
                break

        for a in anomalies:
            sev = a.get("severity","")
            if sev=="CRITICAL": score-=15; issues.append(f"CRITICAL: {a['metric']}")
            elif sev=="HIGH":   score-=8
            elif sev=="MEDIUM": score-=3

        if records:
            last = records[-1]
            for k,v in last.items():
                if k.startswith("null_pct.") and isinstance(v,(int,float)):
                    if v>20: score-=5; issues.append(f"High nulls: {k}={v:.1f}%")
                    elif v>10: score-=2

        score = max(0, min(100, score))
        status = "HEALTHY" if score>=90 else "WARNING" if score>=70 else "CRITICAL"
        scores.append({"table":table,"score":score,"status":status,"issues":issues})
    return sorted(scores, key=lambda x: x["score"])

def build_charts(snapshots):
    charts = {}
    for table, records in snapshots.items():
        charts[table] = {
            "dates":      [r["profiled_at"][:10] for r in records],
            "row_counts": [r["row_count"] for r in records],
        }
        for k in (records[0] if records else {}):
            if k.startswith("null_pct."):
                charts[table][k] = [r.get(k,0) for r in records]
    return charts

def build_null_heatmap(snapshots):
    heatmap = {}
    for table, records in snapshots.items():
        for rec in records:
            date = rec["profiled_at"][:10]
            for k, v in rec.items():
                if k.startswith("null_pct."):
                    col = f"{table}.{k.replace('null_pct.','')}"
                    heatmap.setdefault(col, {})[date] = round(v, 2)
    return heatmap

# ── Main export ────────────────────────────────────────────────
print("Exporting dashboard data...")

sources        = load_sources()
anomaly_report = load_report("anomaly_report.json")
gx_report      = load_report("gx_report.json")
alert_log      = load_alert_log()

# Per-source data
all_sources_data = {}
all_snapshots    = {}
all_scores       = []

for src in sources:
    sname     = src["name"]
    snapshots = load_snapshots_for_source(sname)
    scores    = compute_scores(snapshots, anomaly_report, source_name=sname)
    charts    = build_charts(snapshots)
    heatmap   = build_null_heatmap(snapshots)

    all_sources_data[sname] = {
        "name":        sname,
        "description": src.get("description", ""),
        "dialect":     src.get("dialect", ""),
        "tables":      list(snapshots.keys()),
        "table_count": len(snapshots),
        "scores":      scores,
        "charts":      charts,
        "null_heatmap":heatmap,
    }
    all_snapshots.update(snapshots)
    all_scores.extend(scores)

# Combined anomaly list
anomalies = []
run_at = anomaly_report.get("run_at","")[:19]
for table, result in anomaly_report.get("tables",{}).items():
    for a in result.get("anomalies",[]):
        anomalies.append({
            "detected_at":  run_at,
            "table":        table,
            "severity":     a.get("severity",""),
            "metric":       a.get("metric",""),
            "detector":     a.get("detector",""),
            "today":        str(a.get("today","—")),
            "expected":     str(a.get("expected","—")),
            "score":        str(a.get("z_score", a.get("iqr_distance", a.get("anomaly_score","—")))),
            "explanation":  a.get("explanation",""),
            "root_cause":   a.get("root_cause", {}),
        })

# Overall summary across ALL sources
total_snapshots = sum(
    sum(len(v) for v in all_sources_data[s]["charts"].values())
    for s in all_sources_data
)
avg_score = round(sum(s["score"] for s in all_scores)/len(all_scores),1) if all_scores else 0
overall_status = anomaly_report.get("overall_status","CLEAN")

summary = {
    "exported_at":    datetime.now().isoformat(),
    "overall_status": overall_status,
    "avg_dq_score":   avg_score,
    "total_anomalies":anomaly_report.get("total_anomalies",0),
    "critical":       anomaly_report.get("critical",0),
    "high":           anomaly_report.get("high",0),
    "medium":         anomaly_report.get("medium",0),
    "gx_passed":      gx_report.get("total_passed",0),
    "gx_failed":      gx_report.get("total_failed",0),
    "total_snapshots":total_snapshots,
    "source_count":   len(sources),
    "sources":        [s["name"] for s in sources],
    "last_run":       anomaly_report.get("run_at","—")[:19],
}

# Combined charts and heatmap (all sources merged)
combined_charts   = {}
combined_heatmap  = {}
for sdata in all_sources_data.values():
    combined_charts.update(sdata["charts"])
    combined_heatmap.update(sdata["null_heatmap"])

# History / audit trail
def build_history(db_path="./metadata.db"):
    conn = sqlite3.connect(db_path)
    # Pipeline runs — grouped by profiled_at date+hour
    rows = conn.execute("""
        SELECT source, table_name, profiled_at, row_count
        FROM profile_snapshots
        ORDER BY profiled_at DESC
        LIMIT 500
    """).fetchall()

    # Group into runs by rounded timestamp
    runs = {}
    for source, table, ts, rc in rows:
        run_key = ts[:16]  # YYYY-MM-DDTHH:MM
        runs.setdefault(run_key, {"run_at": run_key, "tables": [], "sources": set()})
        runs[run_key]["tables"].append({"source": source, "table": table, "row_count": rc})
        runs[run_key]["sources"].add(source)

    history = []
    for run_key, run in sorted(runs.items(), reverse=True)[:50]:
        history.append({
            "run_at":      run["run_at"],
            "sources":     list(run["sources"]),
            "table_count": len(run["tables"]),
            "tables":      run["tables"][:20],
        })

    # Alert log
    try:
        alerts = conn.execute(
            "SELECT sent_at, channel, severity, summary FROM alert_log ORDER BY sent_at DESC LIMIT 100"
        ).fetchall()
        alert_history = [{"sent_at":r[0],"channel":r[1],"severity":r[2],"summary":r[3]} for r in alerts]
    except:
        alert_history = []

    conn.close()
    return history, alert_history

pipeline_history, alert_history = build_history()

# Write JSON files
files = {
    "summary.json":     summary,
    "sources.json":     all_sources_data,
    "dq_scores.json":   all_scores,
    "charts.json":      combined_charts,
    "null_heatmap.json":combined_heatmap,
    "anomalies.json":   anomalies,
    "alert_log.json":   alert_log,
    "history.json":     pipeline_history,
    "alert_history.json": alert_history,
}

for fname, data in files.items():
    with open(f"{OUT}/{fname}", "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  checkmark {OUT}/{fname}")

print(f"\nDone — {len(sources)} source(s) exported.")
print(f"  Sources: {', '.join(s['name'] for s in sources)}")
print(f"  Total tables: {sum(len(all_sources_data[s]['tables']) for s in all_sources_data)}")

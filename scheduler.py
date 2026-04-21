"""
DQ Monitor — Scheduled Pipeline Runner
=======================================
Runs the full pipeline automatically on a schedule.
No manual commands needed — just start this once and leave it running.

Pipeline order (every run):
    1. Profile all sources        (main.py logic)
    2. Cross-DB consistency checks
    3. Anomaly detection          (Z-Score, IQR, Isolation Forest)
    4. Send alerts                (Slack + email if anomalies found)
    5. Export dashboard data      (updates web dashboard instantly)

Configuration:
    Edit the SCHEDULE section below, or pass command-line args.

Usage:
    python scheduler.py                      # runs every hour (default)
    python scheduler.py --interval 30        # every 30 minutes
    python scheduler.py --interval 360       # every 6 hours
    python scheduler.py --cron "0 8 * * *"  # every day at 8am
    python scheduler.py --once               # run once immediately then exit
    python scheduler.py --now               # run once immediately, then schedule

Run in background on Windows:
    start /B python scheduler.py > scheduler.log 2>&1
"""

import sys
import os
import json
import yaml
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ── Scheduler ─────────────────────────────────────────────────
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

# ── Internal modules ───────────────────────────────────────────
from profiler.connector import DBConnector
from profiler.profiler import TableProfiler
from profiler.schema_fingerprint import SchemaFingerprinter
from store.metadata_db import MetadataStore
from anomaly.loader import load_snapshot_history
from anomaly.zscore_detector import zscore_detect
from anomaly.iqr_detector import iqr_detect
from anomaly.isolation_forest_detector import isolation_forest_detect
from anomaly.reporter import build_report, save_report

console = Console(width=120)
db_lock = threading.Lock()

# ── Pipeline run history ────────────────────────────────────────
RUN_HISTORY_PATH = "./scheduler_history.json"


# ════════════════════════════════════════════════════════════════
#  SCHEDULE CONFIGURATION
#  Change these to control when the pipeline runs.
# ════════════════════════════════════════════════════════════════
DEFAULT_INTERVAL_MINUTES = 60   # Run every 60 minutes by default


def load_sources():
    with open("config/sources.yaml") as f:
        return yaml.safe_load(f).get("sources", [])


def load_alerting_config():
    """Load .env for alert settings."""
    config = {
        "slack_webhook": os.getenv("SLACK_WEBHOOK_URL", ""),
        "email_sender":  os.getenv("EMAIL_SENDER", ""),
        "email_password":os.getenv("EMAIL_PASSWORD", ""),
        "email_receiver":os.getenv("EMAIL_RECEIVER", ""),
        "alert_on_critical": os.getenv("ALERT_ON_CRITICAL", "true").lower() == "true",
        "alert_on_high":     os.getenv("ALERT_ON_HIGH", "true").lower() == "true",
    }
    return config


def log_run(run_record: dict):
    """Append run result to scheduler_history.json."""
    history = []
    if os.path.exists(RUN_HISTORY_PATH):
        try:
            with open(RUN_HISTORY_PATH) as f:
                history = json.load(f)
        except Exception:
            history = []
    history.append(run_record)
    history = history[-100:]  # Keep last 100 runs
    with open(RUN_HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2, default=str)


# ════════════════════════════════════════════════════════════════
#  STEP 1 — Profile all sources (parallel)
# ════════════════════════════════════════════════════════════════
def profile_source(source: dict, connector: DBConnector) -> dict:
    source_name = source["name"]
    result = {"source": source_name, "ok": False, "snapshots": [], "error": None}
    try:
        engine        = connector.get_engine(source_name)
        profiler      = TableProfiler(engine)
        fingerprinter = SchemaFingerprinter(engine)
        tables        = connector.get_tables(engine)
        for table in tables:
            try:
                profile = profiler.profile_table(table)
                new_fp  = fingerprinter.fingerprint(table)
                result["snapshots"].append((source_name, profile, json.dumps(new_fp["columns"])))
            except Exception as e:
                console.print(f"  [yellow]  ⚠ {source_name}.{table}: {e}[/yellow]")
        result["ok"] = True
    except Exception as e:
        result["error"] = str(e)
    return result


def run_profiler(sources, store, connector) -> dict:
    console.print("  [bold]Step 1 — Profiling all sources (parallel)[/bold]")
    results = {}
    with ThreadPoolExecutor(max_workers=min(len(sources), 8)) as executor:
        futures = {executor.submit(profile_source, src, connector): src["name"] for src in sources}
        for future in as_completed(futures):
            r = future.result()
            name = r["source"]
            if r["ok"]:
                tables = len(r["snapshots"])
                console.print(f"    [green]✓[/green] {name}: {tables} table(s) profiled")
                with db_lock:
                    for src_name, profile, fp_json in r["snapshots"]:
                        store.save_snapshot(src_name, profile, fp_json)
            else:
                console.print(f"    [red]✗[/red] {name}: {r['error']}")
            results[name] = r["ok"]
    return results


# ════════════════════════════════════════════════════════════════
#  STEP 2 — Cross-DB checks
# ════════════════════════════════════════════════════════════════
def run_cross_db_checks() -> dict:
    console.print("  [bold]Step 2 — Cross-database checks[/bold]")
    try:
        conn = sqlite3.connect("./metadata.db")
        rows = conn.execute("""
            SELECT source, table_name, row_count, profile_json, profiled_at
            FROM profile_snapshots
            WHERE (source, table_name, profiled_at) IN (
                SELECT source, table_name, MAX(profiled_at)
                FROM profile_snapshots GROUP BY source, table_name
            )
        """).fetchall()
        conn.close()

        snapshots = {}
        for src, table, rc, pj, ts in rows:
            snapshots.setdefault(src, {})[table] = {"row_count": rc, "profile": json.loads(pj)}

        issues = []
        if len(snapshots) >= 2:
            # Missing table check
            all_tables = {s: set(t.lower() for t in snapshots[s]) for s in snapshots}
            all_names  = set().union(*all_tables.values())
            for table in all_names:
                present  = [s for s in snapshots if table in all_tables[s]]
                missing  = [s for s in snapshots if table not in all_tables[s]]
                if missing and len(present) >= 1:
                    issues.append({"check": "missing_table", "severity": "MEDIUM",
                                   "table": table, "present_in": present, "missing_from": missing})

        report = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "sources": list(snapshots.keys()),
            "total_issues": len(issues),
            "issues": issues,
            "overall_status": "CLEAN" if not issues else
                              ("HIGH" if any(i["severity"]=="HIGH" for i in issues) else "MEDIUM"),
        }
        with open("cross_db_report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)

        status = f"[green]✓ Clean[/green]" if not issues else f"[yellow]{len(issues)} issue(s)[/yellow]"
        console.print(f"    {status}")
        return report
    except Exception as e:
        console.print(f"    [yellow]⚠ Cross-DB check error: {e}[/yellow]")
        return {"total_issues": 0, "issues": []}


# ════════════════════════════════════════════════════════════════
#  STEP 3 — Anomaly detection
# ════════════════════════════════════════════════════════════════
def run_anomaly_detection() -> dict:
    console.print("  [bold]Step 3 — Anomaly detection[/bold]")
    try:
        history = load_snapshot_history()
        if not history:
            console.print("    [yellow]No history yet — skipping[/yellow]")
            return {}

        table_results = {}
        for key, df in history.items():
            table_results[key] = {
                "zscore":           zscore_detect(df),
                "iqr":              iqr_detect(df),
                "isolation_forest": isolation_forest_detect(df),
            }

        report = build_report(table_results)
        # Add plain-English explanations
        from anomaly.llm_explainer import explain_anomalies
        report = explain_anomalies(report)
        save_report(report)

        total     = report.get("total_anomalies", 0)
        critical  = report.get("critical", 0)
        status_c  = "bold red" if critical > 0 else "green"
        console.print(
            f"    [{status_c}]{'✗' if critical else '✓'}[/{status_c}] "
            f"{total} anomaly(s) — {critical} critical"
        )
        return report
    except Exception as e:
        console.print(f"    [yellow]⚠ Anomaly detection error: {e}[/yellow]")
        return {}


# ════════════════════════════════════════════════════════════════
#  STEP 4 — Alerts
# ════════════════════════════════════════════════════════════════
def run_alerts(anomaly_report: dict):
    console.print("  [bold]Step 4 — Alerts[/bold]")
    if not anomaly_report:
        console.print("    [dim]No report to alert on[/dim]")
        return

    total    = anomaly_report.get("total_anomalies", 0)
    critical = anomaly_report.get("critical", 0)
    status   = anomaly_report.get("overall_status", "CLEAN")

    if status == "CLEAN" or total == 0:
        console.print("    [green]✓ No anomalies — no alerts sent[/green]")
        return

    try:
        from alerting.slack_sender import send_slack
        from alerting.email_sender import send_email
        from alerting.formatter import format_slack_anomaly, format_email_anomaly
        from alerting.dedup_store import DeduplicationStore

        dedup  = DeduplicationStore()
        config = load_alerting_config()
        sent   = 0

        for table, result in anomaly_report.get("tables", {}).items():
            for anomaly in result.get("anomalies", []):
                sev    = anomaly.get("severity","")
                metric = anomaly.get("metric","")
                if sev not in ("CRITICAL","HIGH"):
                    continue
                if dedup.already_sent(table, metric, sev, "scheduler"):
                    continue
                # Slack
                if config["slack_webhook"]:
                    payload = format_slack_anomaly(anomaly_report)
                    send_slack(payload, webhook_url=config["slack_webhook"])
                    sent += 1
                # Email
                if config["email_sender"] and config["email_receiver"]:
                    subject, html = format_email_anomaly(anomaly_report)
                    send_email(subject=subject, html_body=html,
                               sender=config["email_sender"],
                               password=config["email_password"],
                               receiver=config["email_receiver"])
                    sent += 1
                dedup.record(table, metric, sev, "scheduler", f"Auto: {metric} in {table}")
                break  # One alert per run to avoid spam

        console.print(f"    [green]✓[/green] {sent} alert(s) sent")
    except Exception as e:
        console.print(f"    [yellow]⚠ Alert error: {e}[/yellow]")


# ════════════════════════════════════════════════════════════════
#  STEP 5 — Export dashboard data
# ════════════════════════════════════════════════════════════════
def run_export(sources):
    console.print("  [bold]Step 5 — Exporting dashboard data[/bold]")
    try:
        import sqlite3 as _sq
        OUT = "./web_dashboard/data"
        os.makedirs(OUT, exist_ok=True)
        DB  = "./metadata.db"

        def _snapshots(source_name):
            conn = _sq.connect(DB)
            rows = conn.execute("""
                SELECT table_name, profiled_at, row_count, profile_json
                FROM profile_snapshots WHERE source=?
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

        anomaly_report = {}
        gx_report      = {}
        if os.path.exists("anomaly_report.json"):
            with open("anomaly_report.json") as f: anomaly_report = json.load(f)
        if os.path.exists("gx_report.json"):
            with open("gx_report.json") as f: gx_report = json.load(f)

        conn = _sq.connect(DB)
        try:
            alert_rows = conn.execute(
                "SELECT sent_at,channel,severity,summary FROM alert_log ORDER BY sent_at DESC LIMIT 50"
            ).fetchall()
            alert_log = [{"sent_at":r[0],"channel":r[1],"severity":r[2],"summary":r[3]} for r in alert_rows]
        except:
            alert_log = []
        conn.close()

        all_sources_data = {}
        all_scores       = []

        for src in sources:
            sname     = src["name"]
            snapshots = _snapshots(sname)

            scores = []
            for table, records in snapshots.items():
                score = 100; issues = []
                for a in anomaly_report.get("tables",{}).get(table,{}).get("anomalies",[]):
                    sev = a.get("severity","")
                    if sev=="CRITICAL": score-=15; issues.append(f"CRITICAL: {a['metric']}")
                    elif sev=="HIGH":   score-=8
                    elif sev=="MEDIUM": score-=3
                score = max(0,min(100,score))
                status = "HEALTHY" if score>=90 else "WARNING" if score>=70 else "CRITICAL"
                scores.append({"table":table,"score":score,"status":status,"issues":issues})

            charts = {}
            heatmap = {}
            for table, records in snapshots.items():
                charts[table] = {
                    "dates":      [r["profiled_at"][:10] for r in records],
                    "row_counts": [r["row_count"] for r in records],
                }
                for k in (records[0] if records else {}):
                    if k.startswith("null_pct."):
                        charts[table][k] = [r.get(k,0) for r in records]
                for rec in records:
                    date = rec["profiled_at"][:10]
                    for k,v in rec.items():
                        if k.startswith("null_pct."):
                            col = f"{table}.{k.replace('null_pct.','')}"
                            heatmap.setdefault(col,{})[date] = round(v,2)

            all_sources_data[sname] = {
                "name": sname, "description": src.get("description",""),
                "dialect": src.get("dialect",""), "tables": list(snapshots.keys()),
                "table_count": len(snapshots), "scores": scores,
                "charts": charts, "null_heatmap": heatmap,
            }
            all_scores.extend(scores)

        combined_charts  = {}
        combined_heatmap = {}
        for sd in all_sources_data.values():
            combined_charts.update(sd["charts"])
            combined_heatmap.update(sd["null_heatmap"])

        total_snaps = sum(
            sum(len(sd["charts"].get(t,{}).get("dates",[])) for t in sd["tables"])
            for sd in all_sources_data.values()
        )
        avg_score   = round(sum(s["score"] for s in all_scores)/len(all_scores),1) if all_scores else 0
        anomalies   = []
        run_at      = anomaly_report.get("run_at","")[:19]
        for table, result in anomaly_report.get("tables",{}).items():
            for a in result.get("anomalies",[]):
                anomalies.append({
                    "detected_at": run_at, "table": table,
                    "severity": a.get("severity",""), "metric": a.get("metric",""),
                    "detector": a.get("detector",""), "today": str(a.get("today","—")),
                    "expected": str(a.get("expected","—")),
                    "score": str(a.get("z_score", a.get("iqr_distance", a.get("anomaly_score","—")))),
                })

        summary = {
            "exported_at":    datetime.now(timezone.utc).isoformat(),
            "overall_status": anomaly_report.get("overall_status","CLEAN"),
            "avg_dq_score":   avg_score,
            "total_anomalies":anomaly_report.get("total_anomalies",0),
            "critical":       anomaly_report.get("critical",0),
            "high":           anomaly_report.get("high",0),
            "medium":         anomaly_report.get("medium",0),
            "gx_passed":      gx_report.get("total_passed",0),
            "gx_failed":      gx_report.get("total_failed",0),
            "total_snapshots":total_snaps,
            "source_count":   len(sources),
            "sources":        [s["name"] for s in sources],
            "last_run":       datetime.now(timezone.utc).isoformat()[:19],
        }

        files = {
            "summary.json":     summary,
            "sources.json":     all_sources_data,
            "dq_scores.json":   all_scores,
            "charts.json":      combined_charts,
            "null_heatmap.json":combined_heatmap,
            "anomalies.json":   anomalies,
            "alert_log.json":   alert_log,
        }
        for fname, data in files.items():
            with open(f"{OUT}/{fname}", "w") as f:
                json.dump(data, f, indent=2, default=str)

        console.print(f"    [green]✓[/green] Dashboard updated — {len(all_scores)} tables, {total_snaps} snapshots")
    except Exception as e:
        console.print(f"    [yellow]⚠ Export error: {e}[/yellow]")
        traceback.print_exc()


# ════════════════════════════════════════════════════════════════
#  FULL PIPELINE RUN
# ════════════════════════════════════════════════════════════════
def run_pipeline():
    start    = time.time()
    run_at   = datetime.now(timezone.utc).isoformat()
    sources  = load_sources()
    store    = MetadataStore()
    connector= DBConnector("config/sources.yaml")

    console.print(Panel(
        f"[bold cyan]Pipeline Run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/bold cyan]\n"
        f"[dim]Sources: {', '.join(s['name'] for s in sources)}[/dim]",
        expand=False
    ))

    errors = []
    anomaly_report = {}

    try:
        # Step 1 — Profile
        profiler_results = run_profiler(sources, store, connector)

        # Step 2 — Cross-DB checks
        run_cross_db_checks()

        # Step 3 — Anomaly detection
        anomaly_report = run_anomaly_detection()

        # Step 4 — Alerts
        run_alerts(anomaly_report)

        # Step 5 — Export
        run_export(sources)

    except Exception as e:
        errors.append(str(e))
        console.print(f"\n[red]Pipeline error: {e}[/red]")
        traceback.print_exc()

    elapsed = round(time.time() - start, 1)
    status  = anomaly_report.get("overall_status", "UNKNOWN") if anomaly_report else "ERROR"
    total_a = anomaly_report.get("total_anomalies", 0) if anomaly_report else 0

    console.print(
        f"\n  [bold]Run complete in {elapsed}s[/bold] — "
        f"Status: [{'red' if status=='CRITICAL' else 'green'}]{status}[/{'red' if status=='CRITICAL' else 'green'}]  "
        f"Anomalies: {total_a}"
    )
    console.print(f"  [dim]Dashboard auto-updated. Refresh browser to see latest data.[/dim]\n")

    # Log this run
    log_run({
        "run_at":      run_at,
        "elapsed_s":   elapsed,
        "status":      status,
        "anomalies":   total_a,
        "sources":     [s["name"] for s in sources],
        "errors":      errors,
    })

    return status


# ════════════════════════════════════════════════════════════════
#  SCHEDULER ENTRY POINT
# ════════════════════════════════════════════════════════════════
def print_run_history():
    if not os.path.exists(RUN_HISTORY_PATH):
        return
    try:
        with open(RUN_HISTORY_PATH) as f:
            history = json.load(f)
        if not history:
            return
        console.print("\n[bold]Last 5 pipeline runs:[/bold]")
        tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        tbl.add_column("Run at",    min_width=20)
        tbl.add_column("Status",    min_width=10)
        tbl.add_column("Anomalies", min_width=10, justify="right")
        tbl.add_column("Duration",  min_width=10, justify="right")
        for r in history[-5:][::-1]:
            sc = "red" if r["status"]=="CRITICAL" else "green"
            tbl.add_row(
                r["run_at"][:19],
                f"[{sc}]{r['status']}[/{sc}]",
                str(r.get("anomalies",0)),
                f"{r.get('elapsed_s','?')}s",
            )
        console.print(tbl)
    except Exception:
        pass


def main():
    # Parse arguments
    interval_minutes = DEFAULT_INTERVAL_MINUTES
    cron_expr        = None
    run_once         = "--once" in sys.argv
    run_now_flag     = "--now" in sys.argv

    if "--interval" in sys.argv:
        idx = sys.argv.index("--interval")
        if idx + 1 < len(sys.argv):
            interval_minutes = int(sys.argv[idx + 1])

    if "--cron" in sys.argv:
        idx = sys.argv.index("--cron")
        if idx + 1 < len(sys.argv):
            cron_expr = sys.argv[idx + 1]

    # Load .env if present
    env_path = ".env"
    if os.path.exists(env_path):
        for line in open(env_path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    sources = load_sources()
    console.print(Panel(
        f"[bold cyan]DQ Monitor — Scheduled Pipeline Runner[/bold cyan]\n\n"
        f"  Sources:  [yellow]{', '.join(s['name'] for s in sources)}[/yellow]\n"
        f"  Schedule: [yellow]{'Once' if run_once else f'Every {interval_minutes} min' if not cron_expr else f'Cron: {cron_expr}'}[/yellow]\n"
        f"  Log file: [yellow]scheduler_history.json[/yellow]\n\n"
        f"  [dim]Press Ctrl+C to stop[/dim]",
        expand=False
    ))

    print_run_history()

    # Run once and exit
    if run_once:
        run_pipeline()
        return

    # Run immediately if --now flag
    if run_now_flag:
        console.print("[dim]Running pipeline immediately before starting schedule...[/dim]\n")
        run_pipeline()

    # Set up scheduler
    scheduler = BlockingScheduler(timezone="UTC")

    if cron_expr:
        parts = cron_expr.split()
        if len(parts) == 5:
            trigger = CronTrigger(
                minute=parts[0], hour=parts[1],
                day=parts[2], month=parts[3], day_of_week=parts[4]
            )
        else:
            console.print(f"[red]Invalid cron expression: {cron_expr}[/red]")
            sys.exit(1)
    else:
        trigger = IntervalTrigger(minutes=interval_minutes)

    scheduler.add_job(
        run_pipeline,
        trigger=trigger,
        id="dq_pipeline",
        name="DQ Monitor Pipeline",
        max_instances=1,          # Never run two pipelines simultaneously
        misfire_grace_time=300,   # OK to run up to 5 min late
    )

    try:
        job      = scheduler.get_jobs()[0]
        next_run = getattr(job, 'next_run_time', None) or getattr(job, 'next_fire_time', '(scheduled)')
    except Exception:
        next_run = '(scheduled)'
    console.print(f"[green]Scheduler started.[/green] Next run: [cyan]{next_run}[/cyan]\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        console.print("\n[yellow]Scheduler stopped.[/yellow]")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()

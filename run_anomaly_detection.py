import os

# Load .env file before anything else
def _load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
_load_env()

"""
Phase 4 — Anomaly Detection Engine
===================================
Run from inside dq_monitor folder:

    python run_anomaly_detection.py

What it does:
  1. Loads 30-day snapshot history from metadata.db
  2. Runs Z-Score detector   (statistical — per metric)
  3. Runs IQR detector       (robust stats — per metric)
  4. Runs Isolation Forest   (ML — all metrics combined)
  5. Deduplicates + ranks findings by severity
  6. Prints colour-coded report
  7. Saves anomaly_report.json (used by Phase 5 alerting)
"""

import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from anomaly.loader import load_snapshot_history
from anomaly.zscore_detector import zscore_detect
from anomaly.iqr_detector import iqr_detect
from anomaly.isolation_forest_detector import isolation_forest_detect
from anomaly.reporter import build_report, save_report
from anomaly.llm_explainer import explain_anomalies
from anomaly.llm_assistant import analyze_all_anomalies

console = Console(width=120)

SEVERITY_COLOR = {
    "CRITICAL": "bold red",
    "HIGH":     "red",
    "MEDIUM":   "yellow",
}


def main():
    console.print(Panel(
        "[bold cyan]Phase 4 — Anomaly Detection Engine[/bold cyan]\n"
        "[dim]Z-Score  |  IQR  |  Isolation Forest[/dim]",
        expand=False
    ))

    # ── 1. Load history (ALL sources) ────────────────────────
    console.print("\n[bold]Step 1 — Loading snapshot history (all sources)[/bold]")
    history = load_snapshot_history()  # now loads all sources
    if not history:
        console.print("  [yellow]No snapshots found. Run python main.py first.[/yellow]")
        return
    for key, df in history.items():
        src_label = f"[dim]({df['source'].iloc[0]})[/dim]" if 'source' in df.columns else ""
        console.print(f"  [green]✓[/green] {key}: {len(df)} snapshots {src_label} "
                      f"({df['profiled_at'].min().date()} → {df['profiled_at'].max().date()})")
    console.print(f"  [bold]Total: {len(history)} table(s) across all sources[/bold]")

    # ── 2. Run all detectors per table ────────────────────────
    console.print("\n[bold]Step 2 — Running detectors[/bold]")
    table_results = {}

    for table, df in history.items():
        console.print(f"\n  [cyan]Table: {table}[/cyan]")

        zs  = zscore_detect(df)
        iqr = iqr_detect(df)
        iso = isolation_forest_detect(df)

        console.print(f"    Z-Score findings:          {len(zs)}")
        console.print(f"    IQR findings:              {len(iqr)}")
        console.print(f"    Isolation Forest findings: {len(iso)}")

        table_results[table] = {
            "zscore":           zs,
            "iqr":              iqr,
            "isolation_forest": iso,
        }

    # ── 3. Build + save report ────────────────────────────────
    report = build_report(table_results)
    # ── LLM / rule-based explanations ────────────────────────
    console.print("\n[bold]Step 3b — Generating plain-English explanations[/bold]")
    gemini_key    = os.getenv("GEMINI_API_KEY", "")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    if gemini_key:
        console.print("  [green]Gemini API key found — using Google Gemini (free)[/green]")
    elif anthropic_key:
        console.print("  [green]Anthropic API key found — using Claude[/green]")
    else:
        console.print("  [dim]No API key found — using rule-based explanations[/dim]")
        console.print("  [dim]Add GEMINI_API_KEY=AIzaSy... to .env to enable Gemini (free)[/dim]")
    report = explain_anomalies(report)

    # ── Root cause analysis ───────────────────────────────────
    console.print("\n[bold]Step 3c — Root cause analysis[/bold]")
    report = analyze_all_anomalies(report)
    save_report(report)

    # ── 4. Print results ──────────────────────────────────────
    console.print("\n[bold]Step 3 — Anomaly Report[/bold]")
    console.print(f"[dim]Run at: {report['run_at']}[/dim]\n")

    for table, result in report["tables"].items():
        console.print(f"[bold white]━━━ {table.upper()} ━━━[/bold white]")

        if not result["anomalies"]:
            console.print("  [green]✓ All metrics within normal range[/green]\n")
            continue

        tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        tbl.add_column("Severity",  min_width=10)
        tbl.add_column("Metric",    min_width=28)
        tbl.add_column("Detector",  min_width=18)
        tbl.add_column("Today",     justify="right", min_width=12)
        tbl.add_column("Expected",  justify="right", min_width=12)
        tbl.add_column("Score",     justify="right", min_width=10)
        tbl.add_column("Direction", min_width=12)

        for a in result["anomalies"]:
            sev_color = SEVERITY_COLOR.get(a["severity"], "white")
            severity  = f"[{sev_color}]{a['severity']}[/{sev_color}]"

            # Format detector-specific fields
            if a["detector"] == "zscore":
                score    = f"Z={a['z_score']}"
                expected = str(a.get("expected", "—"))
            elif a["detector"] == "iqr":
                score    = f"dist={a['iqr_distance']}"
                expected = f"{a.get('lower_fence','?')}–{a.get('upper_fence','?')}"
            else:
                score    = f"iso={a['anomaly_score']}"
                expected = "multivariate"

            tbl.add_row(
                severity,
                a["metric"],
                a["detector"],
                str(a.get("today", "—")),
                expected,
                score,
                a.get("direction", "—"),
            )

            # Show top contributors for Isolation Forest
            if a["detector"] == "isolation_forest" and "top_contributors" in a:
                for c in a["top_contributors"]:
                    tbl.add_row(
                        "", f"  ↳ {c['metric']}", "", "", "",
                        f"Z={c['z_score']}", ""
                    )

        console.print(tbl)

    # ── 5. Summary ────────────────────────────────────────────
    console.print("[bold white]━━━ SUMMARY ━━━[/bold white]")
    status_color = {
        "CLEAN":    "green",
        "MEDIUM":   "yellow",
        "HIGH":     "red",
        "CRITICAL": "bold red",
    }.get(report["overall_status"], "white")

    console.print(
        f"  Total anomalies: [bold]{report['total_anomalies']}[/bold]  |  "
        f"[bold red]Critical: {report['critical']}[/bold red]  |  "
        f"[red]High: {report['high']}[/red]  |  "
        f"[yellow]Medium: {report['medium']}[/yellow]"
    )
    console.print(
        f"  Overall status: [{status_color}]{report['overall_status']}[/{status_color}]"
    )
    console.print(f"\n  [dim]Full report saved to: anomaly_report.json[/dim]\n")

    sys.exit(0 if report["overall_status"] == "CLEAN" else 1)


if __name__ == "__main__":
    main()

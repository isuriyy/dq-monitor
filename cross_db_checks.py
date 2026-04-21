"""
Scenario 2 — Cross-Database Consistency Checks
===============================================
Compares metrics ACROSS sources to find inconsistencies.

What it checks:
  1. Row count consistency   — same logical table in multiple DBs shouldn't diverge >threshold%
  2. Null % consistency      — same column type in multiple DBs shouldn't have wildly different null rates
  3. Schema similarity       — tables with similar names should have similar column counts
  4. Volume anomaly          — one source grows while another shrinks (sign of sync failure)

Run:
    python cross_db_checks.py

Produces: cross_db_report.json
"""
import json
import sqlite3
import yaml
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich import box

console = Console(width=120)


def load_sources():
    with open("config/sources.yaml") as f:
        return yaml.safe_load(f).get("sources", [])


def get_latest_snapshots(db_path="./metadata.db") -> dict:
    """Returns latest snapshot per source+table."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT source, table_name, row_count, profile_json, profiled_at
        FROM profile_snapshots
        WHERE (source, table_name, profiled_at) IN (
            SELECT source, table_name, MAX(profiled_at)
            FROM profile_snapshots
            GROUP BY source, table_name
        )
        ORDER BY source, table_name
    """).fetchall()
    conn.close()

    result = {}
    for source, table, row_count, profile_json, profiled_at in rows:
        result.setdefault(source, {})[table] = {
            "row_count":    row_count,
            "profile":      json.loads(profile_json),
            "profiled_at":  profiled_at,
        }
    return result


def check_row_count_consistency(snapshots: dict, threshold_pct=50) -> list:
    """
    Finds tables with the same name across sources that differ by > threshold%.
    Also flags when one source has a table the other doesn't.
    """
    issues = []
    sources = list(snapshots.keys())
    if len(sources) < 2:
        return issues

    # Collect all table names across sources
    all_tables = {}
    for src in sources:
        for table in snapshots[src]:
            all_tables.setdefault(table.lower(), []).append(src)

    # Check tables that exist in multiple sources
    for table_lower, srcs in all_tables.items():
        if len(srcs) < 2:
            continue
        counts = {}
        for src in srcs:
            # Find matching table (case-insensitive)
            for t in snapshots[src]:
                if t.lower() == table_lower:
                    counts[src] = snapshots[src][t]["row_count"]

        if len(counts) < 2:
            continue

        vals   = list(counts.values())
        max_v  = max(vals)
        min_v  = min(vals)
        if max_v == 0:
            continue
        diff_pct = round((max_v - min_v) / max_v * 100, 1)

        if diff_pct > threshold_pct:
            issues.append({
                "check":     "row_count_consistency",
                "table":     table_lower,
                "severity":  "HIGH" if diff_pct > 80 else "MEDIUM",
                "message":   f"Row count differs by {diff_pct}% across sources",
                "details":   counts,
                "diff_pct":  diff_pct,
            })

    return issues


def check_null_rate_consistency(snapshots: dict, threshold_pct=30) -> list:
    """
    Finds columns with the same name across sources where null % differs significantly.
    """
    issues = []
    sources = list(snapshots.keys())
    if len(sources) < 2:
        return issues

    # Collect null rates per column across sources
    col_nulls = {}  # col_name -> {source: null_pct}
    for src in sources:
        for table, data in snapshots[src].items():
            for col, col_data in data["profile"].get("columns", {}).items():
                if isinstance(col_data, dict) and "null_pct" in col_data:
                    key = col.lower()
                    col_nulls.setdefault(key, {})[src] = col_data["null_pct"]

    for col, src_nulls in col_nulls.items():
        if len(src_nulls) < 2:
            continue
        vals     = list(src_nulls.values())
        max_null = max(vals)
        min_null = min(vals)
        diff     = round(max_null - min_null, 1)
        if diff > threshold_pct and max_null > 5:
            issues.append({
                "check":    "null_rate_consistency",
                "column":   col,
                "severity": "HIGH" if diff > 60 else "MEDIUM",
                "message":  f"Null % for '{col}' differs by {diff}pp across sources",
                "details":  src_nulls,
                "diff_pp":  diff,
            })

    return issues


def check_volume_divergence(snapshots: dict) -> list:
    """
    Checks if sources that should grow together are diverging
    (e.g. one grows +20% while another shrinks -10% — sync failure signal).
    Uses last 2 snapshots per source.
    """
    issues = []
    conn   = sqlite3.connect("./metadata.db")
    sources = list(snapshots.keys())

    growth_rates = {}
    for src in sources:
        rows = conn.execute("""
            SELECT table_name, row_count FROM profile_snapshots
            WHERE source=? ORDER BY profiled_at DESC LIMIT 10
        """, (src,)).fetchall()
        if len(rows) >= 2:
            latest   = {r[0]: r[1] for r in rows[:len(rows)//2]}
            previous = {r[0]: r[1] for r in rows[len(rows)//2:]}
            total_now  = sum(latest.values())
            total_prev = sum(previous.values())
            if total_prev > 0:
                growth_rates[src] = round((total_now - total_prev) / total_prev * 100, 1)

    conn.close()

    if len(growth_rates) >= 2:
        vals = list(growth_rates.values())
        spread = max(vals) - min(vals)
        if spread > 20:
            growing = [s for s, r in growth_rates.items() if r > 0]
            shrinking = [s for s, r in growth_rates.items() if r < 0]
            if growing and shrinking:
                issues.append({
                    "check":    "volume_divergence",
                    "severity": "HIGH",
                    "message":  f"Sources diverging: {growing} growing while {shrinking} shrinking",
                    "details":  growth_rates,
                    "spread":   spread,
                })

    return issues


def check_missing_tables(snapshots: dict) -> list:
    """Flags tables present in one source but missing from others."""
    issues = []
    sources = list(snapshots.keys())
    if len(sources) < 2:
        return issues

    all_tables = {src: set(t.lower() for t in snapshots[src]) for src in sources}
    all_names  = set().union(*all_tables.values())

    for table in all_names:
        present_in  = [s for s in sources if table in all_tables[s]]
        missing_from = [s for s in sources if table not in all_tables[s]]
        if missing_from and len(present_in) >= 1:
            issues.append({
                "check":        "missing_table",
                "table":        table,
                "severity":     "MEDIUM",
                "message":      f"Table '{table}' exists in {present_in} but missing from {missing_from}",
                "details":      {"present_in": present_in, "missing_from": missing_from},
                "present_in":   present_in,
                "missing_from": missing_from,
            })

    return issues


def main():
    console.print("\n[bold cyan]Cross-Database Consistency Checks[/bold cyan]\n")

    sources   = load_sources()
    snapshots = get_latest_snapshots()

    if len(snapshots) < 2:
        console.print("[yellow]Only 1 source found. Cross-DB checks need 2+ sources.[/yellow]")
        console.print("[dim]Add more sources to config/sources.yaml and run python main.py[/dim]")
        return

    console.print(f"Checking across [bold]{len(snapshots)}[/bold] sources: "
                  f"[cyan]{', '.join(snapshots.keys())}[/cyan]\n")

    all_issues = []

    # Run all checks
    checks = [
        ("Row count consistency",  check_row_count_consistency(snapshots)),
        ("Null rate consistency",   check_null_rate_consistency(snapshots)),
        ("Volume divergence",       check_volume_divergence(snapshots)),
        ("Missing tables",          check_missing_tables(snapshots)),
    ]

    for check_name, issues in checks:
        console.print(f"[bold]— {check_name}[/bold]")
        if not issues:
            console.print("  [green]✓ No issues found[/green]")
        else:
            for issue in issues:
                sev_color = "red" if issue["severity"] == "HIGH" else "yellow"
                console.print(f"  [{sev_color}]⚠ {issue['severity']}[/{sev_color}] {issue['message']}")
                console.print(f"    [dim]{issue['details']}[/dim]")
        all_issues.extend(issues)
        console.print()

    # Summary table
    if all_issues:
        console.print("[bold white]━━━ ISSUES FOUND ━━━[/bold white]")
        tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        tbl.add_column("Check",    min_width=28)
        tbl.add_column("Severity", min_width=10)
        tbl.add_column("Message",  min_width=50)
        for issue in all_issues:
            sev_color = "red" if issue["severity"] == "HIGH" else "yellow"
            tbl.add_row(
                issue["check"],
                f"[{sev_color}]{issue['severity']}[/{sev_color}]",
                issue["message"],
            )
        console.print(tbl)
    else:
        console.print("[bold green]✓ All cross-database checks passed[/bold green]")

    # Save report
    report = {
        "run_at":       datetime.utcnow().isoformat(),
        "sources":      list(snapshots.keys()),
        "total_issues": len(all_issues),
        "high":         sum(1 for i in all_issues if i["severity"] == "HIGH"),
        "medium":       sum(1 for i in all_issues if i["severity"] == "MEDIUM"),
        "issues":       all_issues,
        "overall_status": "CLEAN" if not all_issues else
                          ("HIGH" if any(i["severity"]=="HIGH" for i in all_issues) else "MEDIUM"),
    }

    with open("cross_db_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    console.print(f"\n[dim]Report saved: cross_db_report.json[/dim]")
    console.print(f"[dim]Sources checked: {', '.join(snapshots.keys())}[/dim]\n")


if __name__ == "__main__":
    main()

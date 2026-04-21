"""
Phase 2 entry point — runs all GX check suites across all tables
and produces a combined DQ report saved to gx_report.json
"""
import json
import sys
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich import box

from gx_checks.suite_orders import run_orders_suite
from gx_checks.suite_products import run_products_suite
from gx_checks.suite_users import run_users_suite

console = Console(width=120)

def main():
    console.print("\n[bold cyan]Phase 2 — Great Expectations DQ Suite[/bold cyan]")
    console.print(f"[dim]Run started: {datetime.utcnow().isoformat()}[/dim]\n")

    results = []
    results.append(run_orders_suite())
    results.append(run_products_suite())
    results.append(run_users_suite())

    # ── Summary table ─────────────────────────────────────────
    console.print("\n\n" + "="*55)
    console.print("  OVERALL SUMMARY")
    console.print("="*55)

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold")
    tbl.add_column("Table",   style="cyan",  min_width=12)
    tbl.add_column("Passed",  justify="right", min_width=8)
    tbl.add_column("Failed",  justify="right", min_width=8)
    tbl.add_column("Status",  min_width=16)

    total_passed = total_failed = 0
    for r in results:
        status = "[green]ALL PASSED[/green]" if r["success"] else "[red]FAILED[/red]"
        tbl.add_row(
            r["table"],
            f"[green]{r['passed']}[/green]",
            f"[red]{r['failed']}[/red]" if r["failed"] else "0",
            status
        )
        total_passed += r["passed"]
        total_failed += r["failed"]

    console.print(tbl)
    console.print(f"  Total checks: {total_passed + total_failed}  |  "
                  f"[green]Passed: {total_passed}[/green]  |  "
                  f"[red]Failed: {total_failed}[/red]")

    # ── Save JSON report ──────────────────────────────────────
    report = {
        "run_at": datetime.utcnow().isoformat(),
        "total_checks": total_passed + total_failed,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "overall_success": total_failed == 0,
        "tables": results
    }
    with open("gx_report.json", "w") as f:
        json.dump(report, f, indent=2)

    console.print(f"\n  [dim]Full report saved to: gx_report.json[/dim]")

    if total_failed > 0:
        console.print("\n  [bold red]DQ PIPELINE: FAILED — fix issues before proceeding.[/bold red]\n")
        sys.exit(1)
    else:
        console.print("\n  [bold green]DQ PIPELINE: PASSED — data is clean.[/bold green]\n")
        sys.exit(0)

if __name__ == "__main__":
    main()

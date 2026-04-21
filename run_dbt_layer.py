"""
Phase 3A — dbt-style layer using pure Python + DuckDB.
Run this from inside the dq_monitor folder:

    python run_dbt_layer.py

What it does (same as dbt run + dbt test):
  1. Loads raw tables from SQLite → DuckDB
  2. Builds 3 staging views   (stg_orders, stg_users, stg_products)
  3. Builds 2 mart tables     (mart_orders_summary, mart_user_activity)
  4. Runs 20+ schema tests    (unique, not_null, accepted_values)
  5. Runs 4  custom SQL tests (business rules)
  6. Prints full report + saves dbt_report.json
"""

import sys
import duckdb
from rich.console import Console
from rich.panel import Panel

# Add dbt_layer to path so engine.py can be imported
sys.path.insert(0, ".")

from dbt_layer.engine import (
    load_raw_tables,
    build_staging_models,
    build_mart_models,
    run_schema_tests,
    run_custom_tests,
    print_report,
    DB_PATH,
)

console = Console(width=120)


def main():
    console.print(Panel(
        "[bold cyan]Phase 3A — dbt-style Transform + Test Layer[/bold cyan]\n"
        "[dim]Pure Python + DuckDB — no dbt command required[/dim]",
        expand=False
    ))

    # Connect to DuckDB (creates file if it doesn't exist)
    conn = duckdb.connect(DB_PATH)

    # Run all phases
    load_raw_tables(conn)
    build_staging_models(conn)
    build_mart_models(conn)

    schema_results = run_schema_tests(conn)
    custom_results = run_custom_tests(conn)

    success = print_report(schema_results, custom_results)

    conn.close()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

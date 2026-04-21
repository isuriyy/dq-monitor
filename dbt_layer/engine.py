"""
dbt-style engine — pure Python + DuckDB.
Does exactly what dbt does:
  1. Loads raw tables from SQLite into DuckDB
  2. Builds staging models (views) from SQL files
  3. Builds mart models (tables) from SQL files
  4. Runs schema tests (unique, not_null, accepted_values)
  5. Runs custom SQL tests
  6. Prints a full test report
No dbt command needed. Works on any Python version.
"""

import duckdb
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich import box

console = Console(width=120)

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
TESTS_DIR  = os.path.join(os.path.dirname(__file__), "tests")
DB_PATH    = "./data/ecommerce.duckdb"


# ─────────────────────────────────────────────
#  STEP 1: Load raw SQLite data into DuckDB
# ─────────────────────────────────────────────
def load_raw_tables(conn):
    console.print("\n[bold cyan]Step 1 — Loading raw tables into DuckDB[/bold cyan]")
    sqlite_conn = sqlite3.connect("./data/ecommerce.db")
    for table in ["users", "products", "orders"]:
        df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        console.print(f"  [green]✓[/green] {table}: {len(df):,} rows loaded")
    sqlite_conn.close()


# ─────────────────────────────────────────────
#  STEP 2: Build staging models (views)
# ─────────────────────────────────────────────
def build_staging_models(conn):
    console.print("\n[bold cyan]Step 2 — Building staging models (views)[/bold cyan]")
    staging_models = ["stg_orders", "stg_users", "stg_products"]
    for model in staging_models:
        sql_path = os.path.join(MODELS_DIR, f"{model}.sql")
        with open(sql_path) as f:
            sql = f.read()
        conn.execute(f"DROP VIEW IF EXISTS {model}")
        conn.execute(f"CREATE VIEW {model} AS {sql}")
        count = conn.execute(f"SELECT COUNT(*) FROM {model}").fetchone()[0]
        console.print(f"  [green]✓[/green] {model}: view created ({count:,} rows)")


# ─────────────────────────────────────────────
#  STEP 3: Build mart models (tables)
# ─────────────────────────────────────────────
def build_mart_models(conn):
    console.print("\n[bold cyan]Step 3 — Building mart models (tables)[/bold cyan]")
    mart_models = ["mart_orders_summary", "mart_user_activity"]
    for model in mart_models:
        sql_path = os.path.join(MODELS_DIR, f"{model}.sql")
        with open(sql_path) as f:
            sql = f.read()
        conn.execute(f"DROP TABLE IF EXISTS {model}")
        conn.execute(f"CREATE TABLE {model} AS {sql}")
        count = conn.execute(f"SELECT COUNT(*) FROM {model}").fetchone()[0]
        console.print(f"  [green]✓[/green] {model}: table created ({count:,} rows)")


# ─────────────────────────────────────────────
#  STEP 4: Schema tests (like dbt built-ins)
# ─────────────────────────────────────────────
SCHEMA_TESTS = {
    "stg_orders": {
        "unique":          ["order_id"],
        "not_null":        ["order_id", "user_id", "product_id", "order_status", "order_date"],
        "not_null_mostly": [("order_total", 0.90)],
        "accepted_values": [
            ("order_status", ["paid", "pending", "cancelled", "refunded"])
        ],
    },
    "stg_users": {
        "unique":          ["user_id", "email"],
        "not_null":        ["user_id", "email", "joined_date"],
        "not_null_mostly": [("age", 0.90)],
        "accepted_values": [
            ("country_code", ["LK", "US", "UK", "IN", "AU", "DE"])
        ],
    },
    "stg_products": {
        "unique":          ["product_id"],
        "not_null":        ["product_id", "product_name", "price", "category"],
        "not_null_mostly": [("stock_qty", 0.80)],
        "accepted_values": [
            ("category", ["Electronics", "Clothing", "Books", "Home", "Sports"])
        ],
    },
    "mart_orders_summary": {
        "not_null": ["order_date", "order_status", "total_orders"],
    },
    "mart_user_activity": {
        "unique":   ["user_id"],
        "not_null": ["user_id", "email"],
    },
}


def run_schema_tests(conn):
    console.print("\n[bold cyan]Step 4 — Running schema tests[/bold cyan]")
    results = []

    for model, tests in SCHEMA_TESTS.items():

        # unique
        for col in tests.get("unique", []):
            dupes = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {col} FROM {model}
                    WHERE {col} IS NOT NULL
                    GROUP BY {col} HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            results.append(_result(model, col, "unique", dupes == 0,
                                   f"{dupes} duplicate(s)" if dupes else None))

        # not_null (strict)
        for col in tests.get("not_null", []):
            nulls = conn.execute(f"""
                SELECT COUNT(*) FROM {model} WHERE {col} IS NULL
            """).fetchone()[0]
            results.append(_result(model, col, "not_null", nulls == 0,
                                   f"{nulls} null(s)" if nulls else None))

        # not_null with mostly threshold
        for col, threshold in tests.get("not_null_mostly", []):
            total = conn.execute(f"SELECT COUNT(*) FROM {model}").fetchone()[0]
            nulls = conn.execute(f"SELECT COUNT(*) FROM {model} WHERE {col} IS NULL").fetchone()[0]
            pct_present = (total - nulls) / total if total else 0
            passed = pct_present >= threshold
            results.append(_result(model, col, f"not_null (≥{int(threshold*100)}%)",
                                   passed,
                                   f"{nulls} nulls ({round((1-pct_present)*100,1)}%)" if not passed else None))

        # accepted_values
        for col, allowed in tests.get("accepted_values", []):
            quoted = ", ".join(f"'{v}'" for v in allowed)
            bad = conn.execute(f"""
                SELECT COUNT(*) FROM {model}
                WHERE {col} IS NOT NULL
                  AND {col} NOT IN ({quoted})
            """).fetchone()[0]
            results.append(_result(model, col, "accepted_values", bad == 0,
                                   f"{bad} invalid value(s)" if bad else None))

    return results


def _result(model, col, test, passed, detail=None):
    return {
        "model":   model,
        "column":  col,
        "test":    test,
        "passed":  passed,
        "detail":  detail,
    }


# ─────────────────────────────────────────────
#  STEP 5: Custom SQL tests
# ─────────────────────────────────────────────
CUSTOM_TESTS = [
    {
        "name":        "no negative order totals",
        "description": "order_total must never be < 0",
        "sql": """
            SELECT order_id, order_total
            FROM stg_orders
            WHERE order_total < 0
        """,
    },
    {
        "name":        "daily revenue always positive",
        "description": "every day must have total_revenue > 0",
        "sql": """
            SELECT order_date, SUM(total_revenue) AS day_revenue
            FROM mart_orders_summary
            GROUP BY order_date
            HAVING SUM(total_revenue) <= 0
        """,
    },
    {
        "name":        "no suspicious order volumes",
        "description": "no user should have more than 500 orders",
        "sql": """
            SELECT user_id, total_orders
            FROM mart_user_activity
            WHERE total_orders > 500
        """,
    },
    {
        "name":        "price always positive",
        "description": "product price must be > 0",
        "sql": """
            SELECT product_id, price
            FROM stg_products
            WHERE price IS NOT NULL AND price <= 0
        """,
    },
]


def run_custom_tests(conn):
    console.print("\n[bold cyan]Step 5 — Running custom SQL tests[/bold cyan]")
    results = []
    for t in CUSTOM_TESTS:
        rows = conn.execute(t["sql"]).fetchall()
        passed = len(rows) == 0
        results.append({
            "model":   "custom",
            "column":  "—",
            "test":    t["name"],
            "passed":  passed,
            "detail":  f"{len(rows)} failing row(s): {t['description']}" if not passed else None,
        })
    return results


# ─────────────────────────────────────────────
#  STEP 6: Print report + save JSON
# ─────────────────────────────────────────────
def print_report(schema_results, custom_results):
    all_results = schema_results + custom_results

    console.print("\n[bold white]" + "═" * 60 + "[/bold white]")
    console.print("  [bold]TEST REPORT[/bold]")
    console.print("[bold white]" + "═" * 60 + "[/bold white]")

    tbl = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
    tbl.add_column("Model",   style="cyan",  min_width=22)
    tbl.add_column("Column",  min_width=14)
    tbl.add_column("Test",    min_width=22)
    tbl.add_column("Status",  min_width=8)
    tbl.add_column("Detail",  style="dim", min_width=20)

    passed = failed = 0
    for r in all_results:
        if r["passed"]:
            passed += 1
            status = "[green]PASS[/green]"
        else:
            failed += 1
            status = "[red]FAIL[/red]"
        tbl.add_row(
            r["model"], r["column"], r["test"],
            status, r["detail"] or ""
        )

    console.print(tbl)
    console.print(f"\n  Total: {passed + failed}  |  "
                  f"[green]Passed: {passed}[/green]  |  "
                  f"[red]Failed: {failed}[/red]")

    # Save JSON report
    report = {
        "run_at":       datetime.now().isoformat(),
        "total":        passed + failed,
        "passed":       passed,
        "failed":       failed,
        "success":      failed == 0,
        "results":      all_results,
    }
    with open("dbt_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    if failed == 0:
        console.print("\n  [bold green]ALL TESTS PASSED — data is clean.[/bold green]\n")
    else:
        console.print("\n  [bold red]TESTS FAILED — review failures above.[/bold red]\n")

    return failed == 0

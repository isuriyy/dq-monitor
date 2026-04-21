"""
Great Expectations check suite for the ORDERS table.
Defines rules (expectations) that the data must pass.
"""
import great_expectations as gx
import pandas as pd
import sqlite3
import json
from datetime import datetime

def run_orders_suite(db_path="./data/ecommerce.db"):
    print("\n" + "="*55)
    print("  ORDERS TABLE — DQ Check Suite")
    print("="*55)

    # ── 1. Load data ──────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM orders", conn)
    conn.close()

    # ── 2. Create GX context + data source ───────────────────
    context = gx.get_context(mode="ephemeral")

    ds = context.data_sources.add_pandas("orders_source")
    da = ds.add_dataframe_asset("orders_asset")
    batch_def = da.add_batch_definition_whole_dataframe("orders_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # ── 3. Define Expectation Suite ───────────────────────────
    suite = context.suites.add(gx.ExpectationSuite(name="orders_suite"))

    # Row count must be between 100 and 1,000,000
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=100, max_value=1_000_000
    ))

    # id must be unique and never null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
        column="id"
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="id"
    ))

    # user_id must never be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="user_id"
    ))

    # total must be > 0 (allow up to 5% nulls for missing data)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="total", min_value=0, max_value=100_000,
        mostly=0.95   # 95% of rows must pass (allows ~5% nulls/outliers)
    ))

    # status must only contain known values
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["paid", "pending", "cancelled", "refunded"]
    ))

    # total null % must stay below 10%
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="total", mostly=0.90
    ))

    # product_id must not be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="product_id"
    ))

    # ── 4. Run validation ─────────────────────────────────────
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="orders_validation",
            data=batch_def,
            suite=suite
        )
    )

    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name="orders_checkpoint",
        validation_definitions=[vd]
    ))

    result = checkpoint.run(batch_parameters={"dataframe": df})

    # ── 5. Print results ──────────────────────────────────────
    return parse_and_print(result, "orders")


def parse_and_print(result, table_name):
    passed = 0
    failed = 0
    details = []

    for vr in result.run_results.values():
        for res in vr["results"]:
            expectation_type = res["expectation_config"]["type"]
            success = res["success"]
            kwargs = res["expectation_config"].get("kwargs", {})

            # Build a human-readable label
            col = kwargs.get("column", "TABLE")
            label = f"[{col}] {expectation_type.replace('expect_', '').replace('_', ' ')}"

            if success:
                passed += 1
                status = "PASS"
            else:
                failed += 1
                status = "FAIL"
                observed = res.get("result", {}).get("observed_value", "—")
                details.append({
                    "check": label,
                    "observed": observed
                })

            print(f"  {'✓' if success else '✗'} {status}  {label}")

    print(f"\n  Result: {passed} passed, {failed} failed")
    if details:
        print("\n  FAILURES:")
        for d in details:
            print(f"    → {d['check']}")
            print(f"      Observed: {d['observed']}")

    overall = failed == 0
    print(f"\n  Overall: {'ALL CHECKS PASSED' if overall else 'CHECKS FAILED — review above'}")
    return {"table": table_name, "passed": passed, "failed": failed, "success": overall}


if __name__ == "__main__":
    run_orders_suite()

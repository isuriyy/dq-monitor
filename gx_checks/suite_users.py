"""
Great Expectations check suite for the USERS table.
"""
import great_expectations as gx
import pandas as pd
import sqlite3


def run_users_suite(db_path="./data/ecommerce.db"):
    print("\n" + "="*55)
    print("  USERS TABLE — DQ Check Suite")
    print("="*55)

    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM users", conn)
    conn.close()

    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("users_source")
    da = ds.add_dataframe_asset("users_asset")
    batch_def = da.add_batch_definition_whole_dataframe("users_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name="users_suite"))

    # Row count
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1, max_value=50_000_000
    ))

    # id: unique and not null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))

    # email: not null and must look like an email
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="email"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email",
        regex=r"^[^@]+@[^@]+\.[^@]+$",
        mostly=0.99
    ))

    # email must be unique (no duplicate accounts)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="email"))

    # country: must be from known set
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="country",
        value_set=["LK", "US", "UK", "IN", "AU", "DE"]
    ))

    # age: realistic range, allow some nulls
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=13, max_value=120, mostly=0.95
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="age", mostly=0.90
    ))

    # joined: must not be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="joined"))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="users_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name="users_checkpoint", validation_definitions=[vd]
    ))
    result = checkpoint.run(batch_parameters={"dataframe": df})

    from gx_checks.suite_orders import parse_and_print
    return parse_and_print(result, "users")


if __name__ == "__main__":
    run_users_suite()

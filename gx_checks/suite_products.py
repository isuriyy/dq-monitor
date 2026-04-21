"""
Great Expectations check suite for the PRODUCTS table.
"""
import great_expectations as gx
import pandas as pd
import sqlite3


def run_products_suite(db_path="./data/ecommerce.db"):
    print("\n" + "="*55)
    print("  PRODUCTS TABLE — DQ Check Suite")
    print("="*55)

    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()

    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("products_source")
    da = ds.add_dataframe_asset("products_asset")
    batch_def = da.add_batch_definition_whole_dataframe("products_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name="products_suite"))

    # Must have at least 1 product
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1, max_value=10_000_000
    ))

    # id must be unique and not null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))

    # name must never be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))

    # price must be positive
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="price", min_value=0.01, max_value=99_999
    ))

    # price must never be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="price"))

    # category must be from known set
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="category",
        value_set=["Electronics", "Clothing", "Books", "Home", "Sports"]
    ))

    # stock nulls must stay below 20% (some products may be unlisted)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="stock", mostly=0.80
    ))

    # stock values must be >= 0 when present
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="stock", min_value=0, max_value=100_000, mostly=0.95
    ))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="products_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name="products_checkpoint", validation_definitions=[vd]
    ))
    result = checkpoint.run(batch_parameters={"dataframe": df})

    from gx_checks.suite_orders import parse_and_print
    return parse_and_print(result, "products")


if __name__ == "__main__":
    run_products_suite()

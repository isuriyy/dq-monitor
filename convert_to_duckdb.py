"""
Reads data from ecommerce.db (SQLite) and loads it into
ecommerce.duckdb (DuckDB) so dbt can work with it.
Run this once whenever you regenerate sample data.
"""
import sqlite3
import duckdb
import pandas as pd

print("Converting SQLite → DuckDB...")

sqlite_conn = sqlite3.connect("./data/ecommerce.db")
duck_conn   = duckdb.connect("./data/ecommerce.duckdb")

for table in ["users", "products", "orders"]:
    df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
    duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
    duck_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
    print(f"  {table}: {len(df):,} rows loaded")

sqlite_conn.close()
duck_conn.close()
print("\nDone. ecommerce.duckdb is ready for dbt.")

"""
Injects 30 days of clean history + 1 dramatic anomaly day
directly into metadata.db so the detectors have enough to work with.
"""
import sqlite3, json, random
from datetime import datetime, timedelta

conn = sqlite3.connect("./metadata.db")

# Clear old snapshots
conn.execute("DELETE FROM profile_snapshots")

base = datetime(2026, 3, 18)

for day in range(30):
    ts = (base + timedelta(days=day)).isoformat()
    rc_orders   = int(random.gauss(2000, 60))
    rc_products = int(random.gauss(50,   2))
    rc_users    = int(random.gauss(500,  10))

    for table, rc, profile in [
        ("orders", rc_orders, {
            "table": "orders", "profiled_at": ts,
            "row_count": rc_orders, "column_count": 6,
            "columns": {
                "total":   {"null_pct": round(random.gauss(2.0, 0.3), 2), "distinct_count": rc_orders-10, "mean": round(random.gauss(1500, 50), 2), "std": round(random.gauss(400, 20), 2)},
                "status":  {"null_pct": 0.0, "distinct_count": 4},
                "user_id": {"null_pct": 0.0, "distinct_count": random.randint(480, 500)},
            }
        }),
        ("products", rc_products, {
            "table": "products", "profiled_at": ts,
            "row_count": rc_products, "column_count": 5,
            "columns": {
                "price": {"null_pct": 0.0, "distinct_count": rc_products, "mean": round(random.gauss(750, 30), 2), "std": round(random.gauss(200, 15), 2)},
                "stock": {"null_pct": round(random.gauss(10, 1), 2), "distinct_count": random.randint(38, 45)},
            }
        }),
        ("users", rc_users, {
            "table": "users", "profiled_at": ts,
            "row_count": rc_users, "column_count": 5,
            "columns": {
                "age":     {"null_pct": round(random.gauss(4.8, 0.5), 2), "distinct_count": random.randint(45, 50), "mean": round(random.gauss(38, 1.5), 2), "std": round(random.gauss(12, 1), 2)},
                "country": {"null_pct": 0.0, "distinct_count": 6},
                "email":   {"null_pct": 0.0, "distinct_count": rc_users},
            }
        }),
    ]:
        conn.execute(
            "INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json, schema_fingerprint) VALUES (?,?,?,?,?,?)",
            ("ecommerce_db", table, ts, rc, json.dumps(profile), "{}")
        )

# TODAY — inject 3 dramatic anomalies
today = datetime.now().isoformat()

# orders: row count crashes to 9, nulls spike to 45%
conn.execute("INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json, schema_fingerprint) VALUES (?,?,?,?,?,?)",
    ("ecommerce_db", "orders", today, 9, json.dumps({
        "table": "orders", "profiled_at": today, "row_count": 9, "column_count": 6,
        "columns": {
            "total":   {"null_pct": 45.2, "distinct_count": 5, "mean": 4850.0, "std": 980.0},
            "status":  {"null_pct": 0.0,  "distinct_count": 4},
            "user_id": {"null_pct": 0.0,  "distinct_count": 9},
        }
    }), "{}"))

# products: price mean spikes to 5800
conn.execute("INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json, schema_fingerprint) VALUES (?,?,?,?,?,?)",
    ("ecommerce_db", "products", today, 51, json.dumps({
        "table": "products", "profiled_at": today, "row_count": 51, "column_count": 5,
        "columns": {
            "price": {"null_pct": 0.0, "distinct_count": 51, "mean": 5800.0, "std": 3200.0},
            "stock": {"null_pct": 10.2, "distinct_count": 41},
        }
    }), "{}"))

# users: age nulls jump to 62%
conn.execute("INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json, schema_fingerprint) VALUES (?,?,?,?,?,?)",
    ("ecommerce_db", "users", today, 498, json.dumps({
        "table": "users", "profiled_at": today, "row_count": 498, "column_count": 5,
        "columns": {
            "age":     {"null_pct": 62.4, "distinct_count": 48, "mean": 38.5, "std": 11.8},
            "country": {"null_pct": 0.0,  "distinct_count": 6},
            "email":   {"null_pct": 0.0,  "distinct_count": 498},
        }
    }), "{}"))

conn.commit()
conn.close()
print("Done — 30 days clean history + 3 dramatic anomalies injected")
print("  orders:   row_count 2000 → 9,  null_pct 2% → 45%")
print("  products: mean price 750 → 5800")
print("  users:    age null% 4.8% → 62%")
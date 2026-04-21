import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('./data/ecommerce.db')

print("\nPhase 2 — Simple DQ Check Suite")
print("=" * 45)

checks = [
    ("orders",   "SELECT COUNT(*) FROM orders",                                                                 "row_count > 100",       lambda x: x > 100),
    ("orders",   "SELECT COUNT(*) FROM orders WHERE total IS NULL",                                             "total nulls < 10%",     lambda x: x / 2000 < 0.10),
    ("orders",   "SELECT COUNT(*) FROM orders WHERE status NOT IN ('paid','pending','cancelled','refunded')",   "valid status values",   lambda x: x == 0),
    ("orders",   "SELECT COUNT(*) FROM orders WHERE id IS NULL",                                                "order id not null",     lambda x: x == 0),
    ("products", "SELECT COUNT(*) FROM products WHERE price <= 0",                                              "price always positive", lambda x: x == 0),
    ("products", "SELECT COUNT(*) FROM products WHERE name IS NULL",                                            "name not null",         lambda x: x == 0),
    ("products", "SELECT COUNT(*) FROM products WHERE price IS NULL",                                           "price not null",        lambda x: x == 0),
    ("users",    "SELECT COUNT(*) FROM users WHERE email IS NULL",                                              "email not null",        lambda x: x == 0),
    ("users",    "SELECT COUNT(*) FROM users WHERE id IS NULL",                                                 "user id not null",      lambda x: x == 0),
    ("users",    "SELECT COUNT(*) FROM users WHERE country NOT IN ('LK','US','UK','IN','AU','DE')",             "valid country code",    lambda x: x == 0),
]

passed = 0
failed = 0
table_stats = {}

for table, sql, name, check_fn in checks:
    result = conn.execute(sql).fetchone()[0]
    ok = check_fn(result)

    if table not in table_stats:
        table_stats[table] = {"passed": 0, "failed": 0}

    if ok:
        passed += 1
        table_stats[table]["passed"] += 1
        print(f"  checkmark PASS  [{table}] {name}")
    else:
        failed += 1
        table_stats[table]["failed"] += 1
        print(f"  X        FAIL  [{table}] {name}  (got: {result})")

conn.close()

print(f"\n  Total: {passed + failed}  |  Passed: {passed}  |  Failed: {failed}")

tables_list = []
for t, stats in table_stats.items():
    tables_list.append({
        "table":   t,
        "passed":  stats["passed"],
        "failed":  stats["failed"],
        "success": stats["failed"] == 0,
    })

report = {
    "run_at":         datetime.now().isoformat(),
    "total_passed":   passed,
    "total_failed":   failed,
    "overall_success": failed == 0,
    "tables":         tables_list,
}

with open("gx_report.json", "w") as f:
    json.dump(report, f, indent=2)

print("  gx_report.json saved")

if failed == 0:
    print("\n  ALL CHECKS PASSED — data is clean.")
else:
    print("\n  CHECKS FAILED — review above.")
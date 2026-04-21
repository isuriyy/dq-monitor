"""
tests/test_data_quality.py
==========================
Data Quality tests — the most important type.

These tests prove the system's actual PURPOSE works correctly:
  "If something goes wrong with my data, does DQ Monitor catch it?"

Each test follows this pattern:
  1. Create a clean test database with known good data
  2. Build 30 days of normal snapshot history
  3. Inject a specific real-world data problem
  4. Run the anomaly detectors
  5. Assert the problem was caught with the right severity

These are called "oracle tests" in data engineering —
you know exactly what should be detected, so you can verify it is.

Run with:
    python -m pytest tests/test_data_quality.py -v
"""
import pytest
import sqlite3
import tempfile
import os
import sys
import shutil
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Fixtures ──────────────────────────────────────────────────────
@pytest.fixture
def clean_db():
    """
    Creates a fresh SQLite database with realistic clean data.
    Yields the db path, cleans up after the test.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE orders (
            id         INTEGER PRIMARY KEY,
            user_id    INTEGER,
            product_id INTEGER,
            total      REAL,
            status     TEXT,
            created_at TEXT
        );
        CREATE TABLE products (
            id       INTEGER PRIMARY KEY,
            name     TEXT,
            category TEXT,
            price    REAL,
            stock    INTEGER
        );
        CREATE TABLE users (
            id       INTEGER PRIMARY KEY,
            email    TEXT,
            country  TEXT,
            age      REAL,
            joined   TEXT
        );
    """)

    # Insert 2000 realistic orders
    np.random.seed(42)
    statuses  = ["paid", "shipped", "delivered", "refunded"]
    countries = ["US", "UK", "Canada", "Germany", "Australia"]

    for i in range(1, 2001):
        conn.execute("INSERT INTO orders VALUES (?,?,?,?,?,?)", (
            i, np.random.randint(1, 501), np.random.randint(1, 51),
            round(np.random.uniform(10, 2000), 2),
            np.random.choice(statuses),
            f"2026-01-{np.random.randint(1,28):02d}"
        ))

    for i in range(1, 51):
        conn.execute("INSERT INTO products VALUES (?,?,?,?,?)", (
            i, f"Product {i}", np.random.choice(["Electronics","Clothing","Food"]),
            round(np.random.uniform(5, 1500), 2),
            np.random.randint(0, 500)
        ))

    for i in range(1, 501):
        conn.execute("INSERT INTO users VALUES (?,?,?,?,?)", (
            i, f"user{i}@example.com",
            np.random.choice(countries),
            round(np.random.uniform(18, 65), 1),
            f"2025-{np.random.randint(1,13):02d}-01"
        ))

    conn.commit()
    conn.close()

    yield db_path
    os.unlink(db_path)


@pytest.fixture
def snapshot_store():
    """Creates a temporary metadata.db with 30 days of clean snapshots."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE profile_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT,
            table_name  TEXT,
            profiled_at TEXT,
            row_count   INTEGER,
            profile_json TEXT
        )
    """)

    import json
    np.random.seed(42)

    # 30 days of clean history for orders table
    for day in range(30):
        date  = f"2026-03-{day+1:02d}T10:00:00"
        rows  = 2000 + np.random.randint(-30, 30)
        total_mean   = round(1000 + np.random.uniform(-50, 50), 2)
        total_std    = round(400  + np.random.uniform(-20, 20), 2)
        null_pct     = round(2.0  + np.random.uniform(-0.5, 0.5), 2)

        profile = {
            "table": "orders", "row_count": rows,
            "profiled_at": date, "column_count": 6,
            "columns": {
                "total": {
                    "dtype": "float64",
                    "null_count": int(rows * null_pct / 100),
                    "null_pct": null_pct,
                    "distinct_count": rows - 5,
                    "mean": total_mean,
                    "std":  total_std,
                    "min":  10.0, "max": 2000.0,
                },
                "status": {
                    "dtype": "object",
                    "null_count": 0, "null_pct": 0.0,
                    "distinct_count": 4,
                }
            }
        }
        conn.execute(
            "INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json) VALUES (?,?,?,?,?)",
            ("test_source", "orders", date, rows, json.dumps(profile))
        )

    conn.commit()
    conn.close()
    yield db_path
    os.unlink(db_path)


def load_history_from_db(db_path: str, source: str, table: str) -> pd.DataFrame:
    """Load snapshot history from a specific test database."""
    import json
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT table_name, profiled_at, row_count, profile_json
        FROM profile_snapshots
        WHERE source=? AND table_name=?
        ORDER BY profiled_at
    """, (source, table)).fetchall()
    conn.close()

    records = []
    for _, ts, rc, pj in rows:
        profile = json.loads(pj)
        rec = {"profiled_at": ts, "row_count": rc}
        for col, data in profile.get("columns", {}).items():
            if isinstance(data, dict):
                for m in ["null_pct", "mean", "std", "distinct_count"]:
                    if m in data and data[m] is not None:
                        rec[f"{m}.{col}"] = float(data[m])
        records.append(rec)

    df = pd.DataFrame(records)
    df["profiled_at"] = pd.to_datetime(df["profiled_at"], format="ISO8601")
    return df.sort_values("profiled_at").reset_index(drop=True)


def add_snapshot(db_path: str, source: str, table: str,
                 row_count: int, overrides: dict = None):
    """Add one more snapshot to the history — simulates today's pipeline run."""
    import json
    overrides = overrides or {}
    profile = {
        "table": table, "row_count": row_count,
        "profiled_at": "2026-04-01T10:00:00", "column_count": 6,
        "columns": {
            "total": {
                "dtype": "float64",
                "null_count": overrides.get("null_count", 40),
                "null_pct":   overrides.get("null_pct", 2.0),
                "distinct_count": row_count - 5,
                "mean": overrides.get("mean", 1000.0),
                "std":  overrides.get("std", 400.0),
                "min": 10.0, "max": overrides.get("max", 2000.0),
            },
            "status": {
                "dtype": "object",
                "null_count": 0, "null_pct": 0.0,
                "distinct_count": overrides.get("status_distinct", 4),
            }
        }
    }
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO profile_snapshots (source, table_name, profiled_at, row_count, profile_json) VALUES (?,?,?,?,?)",
        (source, table, "2026-04-01T10:00:00", row_count, json.dumps(profile))
    )
    conn.commit()
    conn.close()


# ════════════════════════════════════════════════════════════════
#  DATA QUALITY TEST 1 — PIPELINE FAILURE (ROW COUNT CRASH)
# ════════════════════════════════════════════════════════════════

class TestPipelineFailureDetection:
    """
    Scenario: A pipeline bug loads only 9 rows instead of ~2000.
    Real-world cause: ETL job crashed mid-load, truncated INSERT, wrong filter.
    Expected: DQ Monitor flags this as CRITICAL immediately.
    """

    def test_detects_row_count_crash(self, snapshot_store):
        """
        GIVEN: 30 days of stable ~2000 row counts
        WHEN:  Today's load produces only 9 rows (99.5% drop)
        THEN:  System flags row_count as CRITICAL
        """
        from anomaly.zscore_detector import zscore_detect
        from anomaly.iqr_detector import iqr_detect

        # Inject the crash — simulates pipeline loading only 9 rows
        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=9)

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        assert len(df) == 31, "Should have 30 days history + today"

        z_findings  = zscore_detect(df)
        iqr_findings = iqr_detect(df)

        # Both detectors must catch it
        z_row   = [f for f in z_findings   if f["metric"] == "row_count"]
        iqr_row = [f for f in iqr_findings if f["metric"] == "row_count"]

        assert len(z_row)   > 0, "Z-Score must detect row count crash"
        assert len(iqr_row) > 0, "IQR must detect row count crash"

        # Must be CRITICAL — not just MEDIUM
        assert z_row[0]["severity"]   == "CRITICAL", f"Expected CRITICAL, got {z_row[0]['severity']}"
        assert iqr_row[0]["severity"] == "CRITICAL", f"Expected CRITICAL, got {iqr_row[0]['severity']}"

        # Must know it went DOWN
        assert z_row[0]["direction"]   == "LOW"
        assert iqr_row[0]["direction"] == "LOW"

        print(f"\n  ✓ Row count crash detected: {df.iloc[-1]['row_count']} rows "
              f"(Z={z_row[0]['z_score']})")

    def test_normal_fluctuation_not_flagged(self, snapshot_store):
        """
        GIVEN: 30 days of stable ~2000 row counts
        WHEN:  Today has a small normal fluctuation (-1.5%)
        THEN:  No anomaly is detected
        """
        from anomaly.zscore_detector import zscore_detect

        # Normal day — 30 rows less than average, totally normal
        add_snapshot(snapshot_store, "test_source", "orders", row_count=1970)

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        z_findings = zscore_detect(df)
        row_findings = [f for f in z_findings if f["metric"] == "row_count"]

        assert len(row_findings) == 0, \
            f"Normal fluctuation should NOT be flagged, got: {row_findings}"
        print("\n  ✓ Normal fluctuation correctly ignored")


# ════════════════════════════════════════════════════════════════
#  DATA QUALITY TEST 2 — NULL EXPLOSION
# ════════════════════════════════════════════════════════════════

class TestNullExplosionDetection:
    """
    Scenario: An upstream API change causes 45% of order totals to be NULL.
    Real-world cause: API dropped a field, JOIN failed, transformation bug.
    Expected: DQ Monitor catches the null spike immediately.
    """

    def test_detects_null_rate_spike(self, snapshot_store):
        """
        GIVEN: 30 days with ~2% null rate in orders.total
        WHEN:  Today's null rate jumps to 45.2%
        THEN:  System flags null_pct.total as CRITICAL
        """
        from anomaly.zscore_detector import zscore_detect
        from anomaly.iqr_detector import iqr_detect

        # Inject the null spike — upstream API stopped sending totals
        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=2000,
                     overrides={"null_pct": 45.2, "null_count": 904})

        df = load_history_from_db(snapshot_store, "test_source", "orders")

        z_findings   = zscore_detect(df)
        iqr_findings = iqr_detect(df)

        z_null   = [f for f in z_findings   if f["metric"] == "null_pct.total"]
        iqr_null = [f for f in iqr_findings if f["metric"] == "null_pct.total"]

        assert len(z_null) > 0,   "Z-Score must detect null spike"
        assert len(iqr_null) > 0, "IQR must detect null spike"

        assert z_null[0]["severity"] in ("CRITICAL", "HIGH"), \
            f"Null spike of 45% should be HIGH or CRITICAL, got {z_null[0]['severity']}"
        assert z_null[0]["direction"] == "HIGH"

        print(f"\n  ✓ Null spike detected: {z_null[0]['today']}% "
              f"(Z={z_null[0]['z_score']})")

    def test_small_null_increase_not_flagged(self, snapshot_store):
        """
        GIVEN: 30 days with ~2% null rate
        WHEN:  Today's null rate is 2.8% (small normal variation)
        THEN:  No anomaly detected
        """
        from anomaly.zscore_detector import zscore_detect

        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=2000,
                     overrides={"null_pct": 2.8, "null_count": 56})

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        z_findings = zscore_detect(df)
        null_findings = [f for f in z_findings if "null_pct" in f["metric"]]

        assert len(null_findings) == 0, \
            f"Small null increase should not be flagged, got: {null_findings}"
        print("\n  ✓ Small null variation correctly ignored")


# ════════════════════════════════════════════════════════════════
#  DATA QUALITY TEST 3 — PRICE / VALUE CORRUPTION
# ════════════════════════════════════════════════════════════════

class TestValueCorruptionDetection:
    """
    Scenario: A currency conversion bug multiplies all order totals by 100.
    Real-world cause: USD → cents conversion applied twice, wrong multiplier.
    Expected: DQ Monitor detects the mean and std spike.
    """

    def test_detects_mean_price_spike(self, snapshot_store):
        """
        GIVEN: 30 days with order total mean ~$1000
        WHEN:  Today's mean jumps to $100,000 (100x — currency bug)
        THEN:  System flags mean.total as CRITICAL
        """
        from anomaly.zscore_detector import zscore_detect

        # Inject the currency bug — totals multiplied by 100
        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=2000,
                     overrides={"mean": 100000.0, "std": 40000.0, "max": 200000.0})

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        z_findings = zscore_detect(df)

        mean_findings = [f for f in z_findings if f["metric"] == "mean.total"]
        assert len(mean_findings) > 0, "Currency bug (mean spike) must be detected"
        assert mean_findings[0]["severity"] in ("CRITICAL", "HIGH")
        assert mean_findings[0]["direction"] == "HIGH"

        print(f"\n  ✓ Price corruption detected: mean={mean_findings[0]['today']} "
              f"(expected ~{mean_findings[0]['expected']}, Z={mean_findings[0]['z_score']})")

    def test_detects_std_explosion(self, snapshot_store):
        """
        GIVEN: 30 days with consistent std ~400
        WHEN:  Today's std jumps to 4000 (outlier prices loaded)
        THEN:  System flags std.total as anomalous
        """
        from anomaly.iqr_detector import iqr_detect

        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=2000,
                     overrides={"std": 4000.0, "max": 200000.0})

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        iqr_findings = iqr_detect(df)

        std_findings = [f for f in iqr_findings if f["metric"] == "std.total"]
        assert len(std_findings) > 0, "Std explosion should be detected by IQR"
        assert std_findings[0]["severity"] in ("CRITICAL", "HIGH")

        print(f"\n  ✓ Std explosion detected: {std_findings[0]['today']} "
              f"(expected {std_findings[0]['lower_fence']}-{std_findings[0]['upper_fence']})")


# ════════════════════════════════════════════════════════════════
#  DATA QUALITY TEST 4 — SILENT DATA DUPLICATION
# ════════════════════════════════════════════════════════════════

class TestDuplicateLoadDetection:
    """
    Scenario: A pipeline runs twice — data is loaded twice, doubling row count.
    Real-world cause: Scheduler bug, manual re-run, idempotency failure.
    Expected: DQ Monitor detects the row count spike as anomalous.
    """

    def test_detects_duplicate_load(self, snapshot_store):
        """
        GIVEN: 30 days with ~2000 rows
        WHEN:  Today has 4000 rows (pipeline ran twice)
        THEN:  System flags row_count spike as HIGH or CRITICAL
        """
        from anomaly.zscore_detector import zscore_detect
        from anomaly.iqr_detector import iqr_detect

        # Pipeline ran twice — double the rows
        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=4000)

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        z_findings   = zscore_detect(df)
        iqr_findings = iqr_detect(df)

        z_row   = [f for f in z_findings   if f["metric"] == "row_count"]
        iqr_row = [f for f in iqr_findings if f["metric"] == "row_count"]

        assert len(z_row)   > 0, "Z-Score must detect duplicate load spike"
        assert len(iqr_row) > 0, "IQR must detect duplicate load spike"

        assert z_row[0]["direction"]   == "HIGH", "Duplicate load should be HIGH direction"
        assert iqr_row[0]["direction"] == "HIGH"
        assert z_row[0]["severity"] in ("CRITICAL", "HIGH")

        print(f"\n  ✓ Duplicate load detected: {df.iloc[-1]['row_count']} rows "
              f"(Z={z_row[0]['z_score']})")

    def test_clean_pipeline_all_green(self, snapshot_store):
        """
        GIVEN: 30 days of normal data
        WHEN:  Today's pipeline runs cleanly with normal data
        THEN:  Zero anomalies detected — system stays quiet
        
        This is the most important test — proves the system
        doesn't cry wolf on clean data.
        """
        from anomaly.zscore_detector import zscore_detect
        from anomaly.iqr_detector import iqr_detect

        # Perfect pipeline run — everything normal
        add_snapshot(snapshot_store, "test_source", "orders",
                     row_count=2005,
                     overrides={
                         "null_pct":  2.1,
                         "null_count": 42,
                         "mean":      1005.0,
                         "std":       398.0,
                         "max":       2000.0,
                     })

        df = load_history_from_db(snapshot_store, "test_source", "orders")
        z_findings   = zscore_detect(df)
        iqr_findings = iqr_detect(df)

        assert len(z_findings)   == 0, \
            f"Clean pipeline should produce 0 Z-Score findings, got: {z_findings}"
        assert len(iqr_findings) == 0, \
            f"Clean pipeline should produce 0 IQR findings, got: {iqr_findings}"

        print("\n  ✓ Clean pipeline run produces zero false positives")

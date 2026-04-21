"""
tests/test_integration.py
=========================
Integration tests using the real ecommerce.db and chinook.db databases.

Tests prove:
  1. The profiler connects to real databases and produces valid output
  2. The anomaly detector runs end-to-end on real snapshot history
  3. The export script produces valid JSON files
  4. The alert deduplication system works correctly

Run with:
    python -m pytest tests/test_integration.py -v
"""
import pytest
import json
import os
import sys
import sqlite3
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Skip all integration tests if databases don't exist
ECOMMERCE_DB = "./data/ecommerce.db"
CHINOOK_DB   = "./data/chinook.db"
METADATA_DB  = "./metadata.db"

pytestmark = pytest.mark.skipif(
    not os.path.exists(ECOMMERCE_DB),
    reason="ecommerce.db not found — run from dq_monitor directory"
)


# ════════════════════════════════════════════════════════════════
#  PROFILER INTEGRATION TESTS
# ════════════════════════════════════════════════════════════════

class TestProfilerIntegration:

    def test_connects_to_ecommerce_db(self):
        """Should successfully connect to ecommerce.db."""
        from profiler.connector import DBConnector
        connector = DBConnector("config/sources.yaml")
        engine = connector.get_engine("ecommerce_db")
        assert engine is not None

    def test_ecommerce_tables_found(self):
        """Should find orders, products, users tables."""
        from profiler.connector import DBConnector
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("ecommerce_db")
        tables    = connector.get_tables(engine)
        assert "orders"   in tables
        assert "products" in tables
        assert "users"    in tables

    def test_profile_orders_table(self):
        """Profiling orders table should return valid profile structure."""
        from profiler.connector import DBConnector
        from profiler.profiler import TableProfiler
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("ecommerce_db")
        profiler  = TableProfiler(engine)
        profile   = profiler.profile_table("orders")

        assert profile["table"]        == "orders"
        assert profile["row_count"]    >  0
        assert profile["column_count"] >  0
        assert "columns"               in profile
        assert "profiled_at"           in profile

    def test_profile_has_column_metrics(self):
        """Each column should have null_pct and distinct_count."""
        from profiler.connector import DBConnector
        from profiler.profiler import TableProfiler
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("ecommerce_db")
        profiler  = TableProfiler(engine)
        profile   = profiler.profile_table("orders")

        for col_name, col_data in profile["columns"].items():
            assert "null_pct"       in col_data, f"{col_name} missing null_pct"
            assert "distinct_count" in col_data, f"{col_name} missing distinct_count"
            assert col_data["null_pct"] >= 0
            assert col_data["null_pct"] <= 100

    def test_numeric_columns_have_stats(self):
        """Numeric columns should have min, max, mean, std."""
        from profiler.connector import DBConnector
        from profiler.profiler import TableProfiler
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("ecommerce_db")
        profiler  = TableProfiler(engine)
        profile   = profiler.profile_table("orders")

        # Find a numeric column (id should be numeric)
        numeric_cols = {
            name: data for name, data in profile["columns"].items()
            if "mean" in data
        }
        assert len(numeric_cols) > 0, "Should have at least one numeric column"
        for col, data in numeric_cols.items():
            assert "min" in data
            assert "max" in data
            assert data["min"] <= data["max"]

    def test_string_columns_dont_crash(self):
        """String columns (status, email) should not crash the profiler."""
        from profiler.connector import DBConnector
        from profiler.profiler import TableProfiler
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("ecommerce_db")
        profiler  = TableProfiler(engine)
        try:
            profile = profiler.profile_table("orders")
            # If status column exists, it's string — should not have crashed
            assert profile is not None
        except Exception as e:
            pytest.fail(f"Profiler crashed on string column: {e}")

    @pytest.mark.skipif(
        not os.path.exists(CHINOOK_DB),
        reason="chinook.db not found"
    )
    def test_profiles_chinook_db(self):
        """Should profile all chinook_db tables without errors."""
        from profiler.connector import DBConnector
        from profiler.profiler import TableProfiler
        connector = DBConnector("config/sources.yaml")
        engine    = connector.get_engine("chinook_db")
        tables    = connector.get_tables(engine)
        profiler  = TableProfiler(engine)

        assert len(tables) >= 3, "chinook_db should have at least 3 tables"
        for table in tables[:3]:  # Test first 3 to keep it fast
            profile = profiler.profile_table(table)
            assert profile["row_count"] > 0


# ════════════════════════════════════════════════════════════════
#  ANOMALY DETECTION INTEGRATION TESTS
# ════════════════════════════════════════════════════════════════

class TestAnomalyDetectionIntegration:

    @pytest.mark.skipif(
        not os.path.exists(METADATA_DB),
        reason="metadata.db not found — run python main.py first"
    )
    def test_loads_snapshot_history(self):
        """Should load snapshot history from metadata.db."""
        from anomaly.loader import load_snapshot_history
        history = load_snapshot_history()
        assert isinstance(history, dict)
        assert len(history) > 0, "Should have at least one table in history"

    @pytest.mark.skipif(
        not os.path.exists(METADATA_DB),
        reason="metadata.db not found"
    )
    def test_history_has_correct_columns(self):
        """Each history DataFrame should have required columns."""
        from anomaly.loader import load_snapshot_history
        history = load_snapshot_history()
        for key, df in history.items():
            assert "profiled_at" in df.columns, f"{key} missing profiled_at"
            assert "row_count"   in df.columns, f"{key} missing row_count"
            assert len(df) > 0,                f"{key} has no rows"

    @pytest.mark.skipif(
        not os.path.exists(METADATA_DB),
        reason="metadata.db not found"
    )
    def test_detectors_run_on_real_history(self):
        """All three detectors should run without crashing on real data."""
        from anomaly.loader import load_snapshot_history
        from anomaly.zscore_detector import zscore_detect
        from anomaly.iqr_detector import iqr_detect
        from anomaly.isolation_forest_detector import isolation_forest_detect

        history = load_snapshot_history()
        for key, df in history.items():
            try:
                z  = zscore_detect(df)
                iq = iqr_detect(df)
                iso = isolation_forest_detect(df)
                assert isinstance(z,  list)
                assert isinstance(iq, list)
                assert isinstance(iso, list)
            except Exception as e:
                pytest.fail(f"Detector crashed on {key}: {e}")


# ════════════════════════════════════════════════════════════════
#  EXPORT INTEGRATION TESTS
# ════════════════════════════════════════════════════════════════

class TestExportIntegration:

    @pytest.mark.skipif(
        not os.path.exists("web_dashboard/data/summary.json"),
        reason="summary.json not found — run python export_dashboard_data.py first"
    )
    def test_summary_json_valid(self):
        """summary.json should be valid JSON with required fields."""
        with open("web_dashboard/data/summary.json") as f:
            summary = json.load(f)
        assert "overall_status" in summary
        assert "avg_dq_score"   in summary
        assert "total_anomalies" in summary
        assert summary["overall_status"] in ("CLEAN", "MEDIUM", "HIGH", "CRITICAL")
        assert 0 <= summary["avg_dq_score"] <= 100

    @pytest.mark.skipif(
        not os.path.exists("web_dashboard/data/dq_scores.json"),
        reason="dq_scores.json not found"
    )
    def test_dq_scores_json_valid(self):
        """dq_scores.json should contain valid score objects."""
        with open("web_dashboard/data/dq_scores.json") as f:
            scores = json.load(f)
        assert isinstance(scores, list)
        assert len(scores) > 0
        for s in scores:
            assert "table"  in s
            assert "score"  in s
            assert "status" in s
            assert 0 <= s["score"] <= 100
            assert s["status"] in ("HEALTHY", "WARNING", "CRITICAL")

    @pytest.mark.skipif(
        not os.path.exists("web_dashboard/data/anomalies.json"),
        reason="anomalies.json not found"
    )
    def test_anomalies_json_valid(self):
        """anomalies.json should be a list of valid anomaly objects."""
        with open("web_dashboard/data/anomalies.json") as f:
            anomalies = json.load(f)
        assert isinstance(anomalies, list)
        for a in anomalies:
            assert "table"    in a
            assert "severity" in a
            assert "metric"   in a
            assert a["severity"] in ("CRITICAL", "HIGH", "MEDIUM", "LOW")


# ════════════════════════════════════════════════════════════════
#  ALERT DEDUPLICATION TESTS
# ════════════════════════════════════════════════════════════════

class TestAlertDeduplication:

    def test_first_alert_not_duplicate(self):
        """First time an alert key is seen should not be a duplicate."""
        from alerting.dedup_store import DeduplicationStore
        # Use a temp db to avoid polluting real data
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db = f.name
        try:
            store = DeduplicationStore(db_path=tmp_db, window_minutes=60)
            result = store.already_sent("orders", "row_count", "CRITICAL", "slack")
            assert result is False, "First alert should not be a duplicate"
        finally:
            os.unlink(tmp_db)

    def test_second_alert_is_duplicate(self):
        """Same alert sent twice within window should be deduplicated."""
        from alerting.dedup_store import DeduplicationStore
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db = f.name
        try:
            store = DeduplicationStore(db_path=tmp_db, window_minutes=60)
            store.record("orders", "row_count", "CRITICAL", "slack", "Test alert")
            result = store.already_sent("orders", "row_count", "CRITICAL", "slack")
            assert result is True, "Second alert within window should be duplicate"
        finally:
            os.unlink(tmp_db)

    def test_different_table_not_duplicate(self):
        """Alert for different table should not be deduplicated."""
        from alerting.dedup_store import DeduplicationStore
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db = f.name
        try:
            store = DeduplicationStore(db_path=tmp_db, window_minutes=60)
            store.record("orders", "row_count", "CRITICAL", "slack", "Test")
            result = store.already_sent("products", "row_count", "CRITICAL", "slack")
            assert result is False, "Different table should not be deduplicated"
        finally:
            os.unlink(tmp_db)

    def test_different_channel_not_duplicate(self):
        """Same alert on different channel should not be deduplicated."""
        from alerting.dedup_store import DeduplicationStore
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db = f.name
        try:
            store = DeduplicationStore(db_path=tmp_db, window_minutes=60)
            store.record("orders", "row_count", "CRITICAL", "slack", "Test")
            result = store.already_sent("orders", "row_count", "CRITICAL", "email")
            assert result is False, "Different channel should not be deduplicated"
        finally:
            os.unlink(tmp_db)

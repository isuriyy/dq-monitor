"""
tests/test_scoring.py
=====================
Unit tests for DQ score computation, connection URL building,
and LLM explanation generation.

Run with:
    python -m pytest tests/test_scoring.py -v
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ════════════════════════════════════════════════════════════════
#  DQ SCORE COMPUTATION TESTS
# ════════════════════════════════════════════════════════════════

class TestDQScores:
    """Tests for compute_scores() in export_dashboard_data.py"""

    def _compute(self, snapshots, anomaly_report, source_name=""):
        from export_dashboard_data import compute_scores
        return compute_scores(snapshots, anomaly_report, source_name)

    def test_clean_table_scores_100(self):
        """A table with no anomalies and no high nulls should score 100."""
        snapshots = {"orders": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
        report    = {"tables": {}}
        scores    = self._compute(snapshots, report)
        assert scores[0]["score"] == 100
        assert scores[0]["status"] == "HEALTHY"

    def test_critical_anomaly_reduces_score(self):
        """Each CRITICAL anomaly reduces score by 15."""
        snapshots = {"orders": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
        report = {"tables": {"orders": {"anomalies": [
            {"severity": "CRITICAL", "metric": "row_count"}
        ]}}}
        scores = self._compute(snapshots, report)
        assert scores[0]["score"] == 85
        assert scores[0]["status"] == "WARNING"

    def test_multiple_criticals_reduce_further(self):
        """Two CRITICAL anomalies reduce score by 30."""
        snapshots = {"orders": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
        report = {"tables": {"orders": {"anomalies": [
            {"severity": "CRITICAL", "metric": "row_count"},
            {"severity": "CRITICAL", "metric": "null_pct.total"},
        ]}}}
        scores = self._compute(snapshots, report)
        assert scores[0]["score"] == 70

    def test_score_never_goes_below_zero(self):
        """Score should be clamped at 0, never negative."""
        snapshots = {"orders": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
        report = {"tables": {"orders": {"anomalies": [
            {"severity": "CRITICAL", "metric": f"metric_{i}"}
            for i in range(10)
        ]}}}
        scores = self._compute(snapshots, report)
        assert scores[0]["score"] >= 0

    def test_high_null_reduces_score(self):
        """A column with >20% nulls reduces score by 5."""
        snapshots = {"orders": [
            {"profiled_at": "2026-01-01", "row_count": 1000, "null_pct.total": 25.0}
        ]}
        report = {"tables": {}}
        scores = self._compute(snapshots, report)
        assert scores[0]["score"] == 95

    def test_namespaced_table_lookup(self):
        """Anomaly report uses source.table keys — should still match plain table name."""
        snapshots = {"orders": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
        report = {"tables": {"ecommerce_db.orders": {"anomalies": [
            {"severity": "CRITICAL", "metric": "row_count"}
        ]}}}
        scores = self._compute(snapshots, report, source_name="ecommerce_db")
        assert scores[0]["score"] == 85, "Namespaced lookup should work"

    def test_status_labels_correct(self):
        """Status labels: >= 90 HEALTHY, >= 70 WARNING, < 70 CRITICAL."""
        for score, expected_status in [(100, "HEALTHY"), (90, "HEALTHY"),
                                        (89, "WARNING"), (70, "WARNING"),
                                        (69, "CRITICAL"), (0, "CRITICAL")]:
            snapshots = {"t": [{"profiled_at": "2026-01-01", "row_count": 1000}]}
            # Manufacture anomalies to hit the target score
            n_criticals = (100 - score) // 15
            report = {"tables": {"t": {"anomalies": [
                {"severity": "CRITICAL", "metric": f"m{i}"} for i in range(n_criticals)
            ]}}}
            scores = self._compute(snapshots, report)
            actual_score = scores[0]["score"]
            actual_status = scores[0]["status"]
            if actual_score >= 90:
                assert actual_status == "HEALTHY"
            elif actual_score >= 70:
                assert actual_status == "WARNING"
            else:
                assert actual_status == "CRITICAL"

    def test_sorted_by_score_ascending(self):
        """Scores should be sorted worst first."""
        snapshots = {
            "orders":   [{"profiled_at": "2026-01-01", "row_count": 1000}],
            "products": [{"profiled_at": "2026-01-01", "row_count": 500}],
            "users":    [{"profiled_at": "2026-01-01", "row_count": 200}],
        }
        report = {"tables": {
            "orders":   {"anomalies": [{"severity": "CRITICAL", "metric": "x"}]},
            "products": {"anomalies": []},
            "users":    {"anomalies": []},
        }}
        scores = self._compute(snapshots, report)
        score_values = [s["score"] for s in scores]
        assert score_values == sorted(score_values), "Should be sorted ascending"


# ════════════════════════════════════════════════════════════════
#  CONNECTION URL BUILDING TESTS
# ════════════════════════════════════════════════════════════════

class TestConnectionURLs:
    """Tests for build_url() in api_server.py"""

    def _build(self, src):
        from api_server import build_url
        return build_url(src)

    def test_sqlite_url(self):
        url = self._build({"dialect": "sqlite", "path": "./data/mydb.db"})
        assert url == "sqlite:///./data/mydb.db"

    def test_postgresql_url(self):
        url = self._build({
            "dialect": "postgresql",
            "user": "postgres", "password": "secret",
            "host": "localhost", "port": 5432,
            "database": "myapp"
        })
        assert "postgresql+psycopg2" in url
        assert "postgres:secret" in url
        assert "localhost:5432" in url
        assert "myapp" in url

    def test_mysql_url(self):
        url = self._build({
            "dialect": "mysql",
            "user": "root", "password": "pass",
            "host": "localhost", "port": 3306,
            "database": "shopdb"
        })
        assert "mysql+pymysql" in url
        assert "root:pass" in url
        assert "shopdb" in url

    def test_postgresql_default_port(self):
        """PostgreSQL should default to port 5432."""
        url = self._build({
            "dialect": "postgresql",
            "user": "u", "password": "p",
            "host": "h", "database": "db"
        })
        assert "5432" in url

    def test_mysql_default_port(self):
        """MySQL should default to port 3306."""
        url = self._build({
            "dialect": "mysql",
            "user": "u", "password": "p",
            "host": "h", "database": "db"
        })
        assert "3306" in url

    def test_unknown_dialect_returns_empty(self):
        """Unknown dialect should return empty string, not crash."""
        url = self._build({"dialect": "mongodb", "host": "localhost"})
        assert url == ""

    def test_cloud_uses_connection_string(self):
        """Cloud dialect should return the connection_string directly."""
        url = self._build({
            "dialect": "cloud",
            "connection_string": "postgresql://user:pass@host:5432/db"
        })
        assert url == "postgresql://user:pass@host:5432/db"


# ════════════════════════════════════════════════════════════════
#  LLM EXPLAINER — RULE-BASED TESTS
# ════════════════════════════════════════════════════════════════

class TestRuleBasedExplainer:
    """Tests for _rule_based() in llm_explainer.py — no API key needed."""

    def _explain(self, anomaly, table="orders"):
        from anomaly.llm_explainer import _rule_based
        return _rule_based(anomaly, table)

    def test_row_count_crash_explanation(self):
        result = self._explain({
            "metric": "row_count", "today": 9, "expected": 1991.3,
            "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"
        })
        assert "99.5%" in result or "row count" in result.lower()
        assert "pipeline" in result.lower() or "failure" in result.lower()
        assert result.endswith(".")

    def test_row_count_spike_explanation(self):
        result = self._explain({
            "metric": "row_count", "today": 9000, "expected": 1000,
            "direction": "HIGH", "severity": "HIGH", "detector": "zscore"
        })
        assert "spike" in result.lower() or "above" in result.lower() or "duplicate" in result.lower()

    def test_null_spike_explanation(self):
        result = self._explain({
            "metric": "null_pct.total", "today": 45.2, "expected": "1.99%",
            "direction": "HIGH", "severity": "CRITICAL", "detector": "zscore"
        })
        assert "null" in result.lower()
        assert "45.2" in result or "upstream" in result.lower()

    def test_std_spike_explanation(self):
        result = self._explain({
            "metric": "std.price", "today": 868.86, "expected": "344-471",
            "direction": "HIGH", "severity": "CRITICAL", "detector": "iqr"
        })
        assert "spread" in result.lower() or "std" in result.lower()
        assert "price" in result.lower()

    def test_explanation_always_ends_with_period(self):
        """Every explanation should end with a period."""
        anomalies = [
            {"metric": "row_count", "today": 5, "expected": 1000,
             "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"},
            {"metric": "null_pct.email", "today": 30, "expected": "2%",
             "direction": "HIGH", "severity": "HIGH", "detector": "iqr"},
            {"metric": "mean.price", "today": 9999, "expected": 500,
             "direction": "HIGH", "severity": "CRITICAL", "detector": "zscore"},
            {"metric": "distinct_count.status", "today": 10, "expected": 4,
             "direction": "HIGH", "severity": "MEDIUM", "detector": "iqr"},
        ]
        for a in anomalies:
            result = self._explain(a)
            assert result.endswith("."), f"Explanation should end with '.': {result}"

    def test_explanation_references_table_name(self):
        """Explanation should mention the table or column."""
        result = self._explain({
            "metric": "null_pct.email", "today": 40, "expected": "2%",
            "direction": "HIGH", "severity": "HIGH", "detector": "zscore"
        }, table="users")
        assert "users" in result.lower() or "email" in result.lower()

    def test_isolation_forest_explanation(self):
        result = self._explain({
            "metric": "all_metrics_combined",
            "today": None, "expected": "multivariate",
            "direction": "HIGH", "severity": "CRITICAL",
            "detector": "isolation_forest",
            "top_contributors": [{"metric": "row_count", "z_score": 3.5}]
        })
        assert "isolation" in result.lower() or "anomalous" in result.lower() or "multivariate" in result.lower()

    def test_explanation_not_empty(self):
        """Explanation should never be empty."""
        result = self._explain({
            "metric": "unknown_metric_xyz", "today": 999, "expected": 100,
            "direction": "HIGH", "severity": "HIGH", "detector": "iqr"
        })
        assert len(result) > 20, "Explanation should not be empty"


# ════════════════════════════════════════════════════════════════
#  ROOT CAUSE ANALYSIS — RULE-BASED TESTS
# ════════════════════════════════════════════════════════════════

class TestRuleBasedRootCause:
    """Tests for _rule_based_root_cause() in llm_assistant.py"""

    def _analyze(self, anomaly, table="orders"):
        from anomaly.llm_assistant import _rule_based_root_cause
        return _rule_based_root_cause(anomaly, table)

    def test_returns_required_fields(self):
        """Root cause must have all 4 required fields."""
        result = self._analyze({
            "metric": "row_count", "today": 9, "expected": 2000,
            "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"
        })
        assert "what_happened" in result
        assert "causes" in result
        assert "what_to_check" in result
        assert "how_to_fix" in result

    def test_causes_is_list_of_three(self):
        """Should return exactly 3 ranked causes."""
        result = self._analyze({
            "metric": "row_count", "today": 9, "expected": 2000,
            "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"
        })
        assert isinstance(result["causes"], list)
        assert len(result["causes"]) == 3

    def test_what_to_check_is_list(self):
        """what_to_check should be a list of actionable steps."""
        result = self._analyze({
            "metric": "null_pct.total", "today": 45, "expected": "2%",
            "direction": "HIGH", "severity": "CRITICAL", "detector": "zscore"
        })
        assert isinstance(result["what_to_check"], list)
        assert len(result["what_to_check"]) >= 2

    def test_row_count_crash_mentions_pipeline(self):
        """Row count crash root cause should mention pipeline failure."""
        result = self._analyze({
            "metric": "row_count", "today": 9, "expected": 2000,
            "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"
        })
        causes_text = " ".join(result["causes"]).lower()
        assert "pipeline" in causes_text or "etl" in causes_text or "truncat" in causes_text

    def test_null_spike_mentions_upstream(self):
        """Null spike root cause should mention upstream or JOIN."""
        result = self._analyze({
            "metric": "null_pct.email", "today": 50, "expected": "2%",
            "direction": "HIGH", "severity": "CRITICAL", "detector": "zscore"
        })
        causes_text = " ".join(result["causes"]).lower()
        assert "upstream" in causes_text or "join" in causes_text or "source" in causes_text

    def test_std_spike_mentions_outlier(self):
        """Std spike root cause should mention outlier values."""
        result = self._analyze({
            "metric": "std.price", "today": 868, "expected": "344-471",
            "direction": "HIGH", "severity": "CRITICAL", "detector": "iqr"
        })
        causes_text = " ".join(result["causes"]).lower()
        assert "outlier" in causes_text or "unit" in causes_text or "corrupt" in causes_text

    def test_how_to_fix_is_string(self):
        """how_to_fix should be a non-empty string."""
        result = self._analyze({
            "metric": "row_count", "today": 9, "expected": 2000,
            "direction": "LOW", "severity": "CRITICAL", "detector": "zscore"
        })
        assert isinstance(result["how_to_fix"], str)
        assert len(result["how_to_fix"]) > 10

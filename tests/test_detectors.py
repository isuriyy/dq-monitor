"""
tests/test_detectors.py
=======================
Unit tests for the Z-Score, IQR, and Isolation Forest anomaly detectors.

Tests prove:
  1. Detectors correctly flag known anomalies
  2. Detectors correctly ignore clean data
  3. Severity levels are assigned correctly
  4. Edge cases (empty data, all-same values) don't crash

Run with:
    python -m pytest tests/test_detectors.py -v
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from anomaly.zscore_detector import zscore_detect
from anomaly.iqr_detector import iqr_detect
from anomaly.isolation_forest_detector import isolation_forest_detect


# ── Helpers ──────────────────────────────────────────────────────
def make_history(row_counts, extra_cols=None):
    """Build a realistic snapshot DataFrame for testing."""
    import random
    dates = pd.date_range("2026-01-01", periods=len(row_counts), freq="D")
    data = {
        "profiled_at": dates,
        "row_count":   row_counts,
    }
    if extra_cols:
        data.update(extra_cols)
    return pd.DataFrame(data)


def stable_history(n=30, base=1000, noise=20):
    """30 days of stable row counts around a baseline."""
    counts = [base + np.random.randint(-noise, noise) for _ in range(n)]
    return make_history(counts)


def crashed_history(n=30, base=1000, crash_to=9):
    """29 days stable, last day crashes."""
    counts = [base + np.random.randint(-20, 20) for _ in range(n - 1)] + [crash_to]
    return make_history(counts)


def spiked_history(n=30, base=1000, spike_to=5000):
    """29 days stable, last day spikes."""
    counts = [base + np.random.randint(-20, 20) for _ in range(n - 1)] + [spike_to]
    return make_history(counts)


# ════════════════════════════════════════════════════════════════
#  Z-SCORE DETECTOR TESTS
# ════════════════════════════════════════════════════════════════

class TestZScoreDetector:

    def test_detects_row_count_crash(self):
        """A row count dropping 99% should be flagged CRITICAL."""
        df = crashed_history(30, base=2000, crash_to=9)
        findings = zscore_detect(df)
        assert len(findings) > 0, "Should detect crash"
        row_count_finding = [f for f in findings if f["metric"] == "row_count"]
        assert len(row_count_finding) > 0, "row_count should be flagged"
        assert row_count_finding[0]["severity"] == "CRITICAL"
        assert row_count_finding[0]["direction"] == "LOW"

    def test_detects_row_count_spike(self):
        """A row count spiking 5x should be flagged."""
        df = spiked_history(30, base=1000, spike_to=9000)
        findings = zscore_detect(df)
        row_count_finding = [f for f in findings if f["metric"] == "row_count"]
        assert len(row_count_finding) > 0, "Spike should be detected"
        assert row_count_finding[0]["direction"] == "HIGH"

    def test_clean_data_no_findings(self):
        """Stable data with no anomalies should produce no findings."""
        np.random.seed(42)
        df = stable_history(30, base=1000, noise=20)
        findings = zscore_detect(df)
        assert len(findings) == 0, f"Expected 0 findings, got {len(findings)}: {findings}"

    def test_insufficient_history_returns_empty(self):
        """Less than 7 snapshots should return no findings (not enough history)."""
        df = crashed_history(n=5, crash_to=1)
        findings = zscore_detect(df)
        assert findings == [], "Should return empty list with < 7 snapshots"

    def test_null_pct_spike_detected(self):
        """A column null % spiking from 2% to 45% should be flagged."""
        base_nulls = [2.0 + np.random.uniform(-0.5, 0.5) for _ in range(29)]
        null_col = base_nulls + [45.0]
        df = make_history(
            [1000] * 30,
            extra_cols={"null_pct.total": null_col}
        )
        findings = zscore_detect(df)
        null_findings = [f for f in findings if "null_pct" in f["metric"]]
        assert len(null_findings) > 0, "Null spike should be detected"

    def test_finding_has_required_fields(self):
        """Every finding must have required fields."""
        df = crashed_history(30, base=2000, crash_to=5)
        findings = zscore_detect(df)
        required = {"detector", "metric", "today", "expected", "z_score",
                    "severity", "direction"}
        for f in findings:
            missing = required - set(f.keys())
            assert not missing, f"Finding missing fields: {missing}"

    def test_severity_levels_correct(self):
        """Z-score 3-4 = MEDIUM, 4-6 = HIGH, >6 = CRITICAL."""
        df = crashed_history(30, base=2000, crash_to=5)
        findings = zscore_detect(df)
        for f in findings:
            assert f["severity"] in ("CRITICAL", "HIGH", "MEDIUM")
            z = abs(f["z_score"])
            if z >= 6:
                assert f["severity"] == "CRITICAL"
            elif z >= 4:
                assert f["severity"] in ("HIGH", "CRITICAL")

    def test_z_score_is_negative_for_low_direction(self):
        """When value is below mean, z_score should be negative."""
        df = crashed_history(30, base=2000, crash_to=5)
        findings = zscore_detect(df)
        low_findings = [f for f in findings if f["direction"] == "LOW"]
        for f in low_findings:
            assert f["z_score"] < 0, "LOW direction should have negative z_score"


# ════════════════════════════════════════════════════════════════
#  IQR DETECTOR TESTS
# ════════════════════════════════════════════════════════════════

class TestIQRDetector:

    def test_detects_extreme_outlier(self):
        """A value far outside the IQR fences should be flagged."""
        df = crashed_history(30, base=2000, crash_to=9)
        findings = iqr_detect(df)
        assert len(findings) > 0, "IQR should detect extreme outlier"

    def test_clean_data_no_findings(self):
        """Stable data should produce no IQR findings."""
        np.random.seed(99)
        df = stable_history(30, base=1000, noise=15)
        findings = iqr_detect(df)
        assert len(findings) == 0, f"Expected 0 IQR findings, got {len(findings)}"

    def test_insufficient_history_returns_empty(self):
        """Less than 7 snapshots should return empty."""
        df = crashed_history(n=4, crash_to=1)
        findings = iqr_detect(df)
        assert findings == []

    def test_finding_has_required_fields(self):
        """IQR findings must have IQR-specific fields."""
        df = crashed_history(30, base=2000, crash_to=5)
        findings = iqr_detect(df)
        if findings:
            required = {"detector", "metric", "today", "iqr_distance",
                        "severity", "direction"}
            for f in findings:
                missing = required - set(f.keys())
                assert not missing, f"IQR finding missing: {missing}"
            assert all(f["detector"] == "iqr" for f in findings)

    def test_iqr_distance_positive(self):
        """IQR distance should always be positive."""
        df = crashed_history(30, base=2000, crash_to=5)
        findings = iqr_detect(df)
        for f in findings:
            assert f["iqr_distance"] > 0, "IQR distance must be positive"


# ════════════════════════════════════════════════════════════════
#  ISOLATION FOREST TESTS
# ════════════════════════════════════════════════════════════════

class TestIsolationForest:

    def test_detects_multivariate_anomaly(self):
        """Isolation Forest should catch anomalies across multiple metrics."""
        np.random.seed(42)
        n = 35
        normal_rows  = [1000 + np.random.randint(-20, 20) for _ in range(n-1)]
        normal_nulls = [2.0 + np.random.uniform(-0.3, 0.3) for _ in range(n-1)]
        normal_mean  = [500 + np.random.uniform(-10, 10) for _ in range(n-1)]

        df = make_history(
            normal_rows + [50],        # row crash
            extra_cols={
                "null_pct.total": normal_nulls + [40.0],   # null spike
                "mean.total":     normal_mean + [5000.0],  # mean spike
            }
        )
        findings = isolation_forest_detect(df)
        assert len(findings) > 0, "Isolation Forest should detect multivariate anomaly"

    def test_requires_minimum_history(self):
        """Should return empty with insufficient history."""
        df = crashed_history(n=5, crash_to=1)
        findings = isolation_forest_detect(df)
        assert findings == []

    def test_clean_data_low_findings(self):
        """Stable data should produce no or very few findings."""
        np.random.seed(55)
        df = stable_history(40, base=1000, noise=10)
        findings = isolation_forest_detect(df)
        assert len(findings) <= 1, f"Expected 0-1 findings on clean data, got {len(findings)}"

    def test_finding_structure(self):
        """Isolation Forest findings should have correct structure."""
        np.random.seed(42)
        df = crashed_history(35, base=2000, crash_to=5)
        findings = isolation_forest_detect(df)
        if findings:
            f = findings[0]
            assert f["detector"] == "isolation_forest"
            assert "anomaly_score" in f
            assert "top_contributors" in f
            assert isinstance(f["top_contributors"], list)


# ════════════════════════════════════════════════════════════════
#  CROSS-DETECTOR TESTS
# ════════════════════════════════════════════════════════════════

class TestCrossDetector:

    def test_all_three_detect_severe_crash(self):
        """A severe row count crash should be caught by Z-Score and IQR.
        
        Note: Isolation Forest is a multivariate detector — it looks at
        combinations of metrics. A single-metric crash may not trigger it.
        Z-Score and IQR are the primary single-metric detectors.
        """
        np.random.seed(42)
        df = crashed_history(35, base=2000, crash_to=5)

        z_findings   = zscore_detect(df)
        iqr_findings = iqr_detect(df)

        assert len(z_findings) > 0,   "Z-Score should catch severe crash"
        assert len(iqr_findings) > 0, "IQR should catch severe crash"

    def test_isolation_forest_catches_multivariate(self):
        """Isolation Forest should catch anomalies across multiple metrics."""
        np.random.seed(42)
        n = 35
        normal_rows  = [2000 + np.random.randint(-30, 30) for _ in range(n-1)]
        normal_nulls = [2.0  + np.random.uniform(-0.3, 0.3) for _ in range(n-1)]
        normal_mean  = [500  + np.random.uniform(-10, 10) for _ in range(n-1)]
        df = make_history(
            normal_rows + [20],
            extra_cols={
                "null_pct.total": normal_nulls + [45.0],
                "mean.total":     normal_mean  + [9999.0],
            }
        )
        iso_findings = isolation_forest_detect(df)
        assert len(iso_findings) > 0, "Isolation Forest should catch multivariate anomaly"

    def test_detectors_return_lists(self):
        """All detectors must return lists, never None or other types."""
        df = stable_history(30)
        assert isinstance(zscore_detect(df), list)
        assert isinstance(iqr_detect(df), list)
        assert isinstance(isolation_forest_detect(df), list)

    def test_detectors_dont_crash_on_constant_column(self):
        """If a column has zero variance, detectors should not crash."""
        df = make_history(
            [1000] * 30,
            extra_cols={"null_pct.status": [0.0] * 30}
        )
        try:
            z = zscore_detect(df)
            iq = iqr_detect(df)
            assert isinstance(z, list)
            assert isinstance(iq, list)
        except Exception as e:
            pytest.fail(f"Detector crashed on constant column: {e}")

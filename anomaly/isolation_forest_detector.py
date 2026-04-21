"""
Technique 3 — Isolation Forest (ML-based detector).

Checks ALL metrics together as a multi-dimensional point.
Learns what "normal" looks like from history, then scores today
on how isolated (unusual) it is from the normal cluster.

Score < -0.1  →  anomaly (the more negative, the more anomalous)
Severity based on the anomaly score magnitude.

This catches multi-dimensional anomalies that single-metric
detectors miss — e.g. row_count looks OK, null_pct looks OK,
but their combination is unusual given the historical pattern.
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def isolation_forest_detect(
    df: pd.DataFrame,
    min_history: int = 10,
    contamination: float = 0.05,
) -> list[dict]:
    """
    df            : full history DataFrame (last row = today)
    contamination : expected fraction of anomalies in history (default 5%)
    returns       : list with one anomaly dict if today is anomalous, else []
    """
    if len(df) < min_history:
        return []

    # Only use numeric columns with no nulls
    numeric_cols = [
        c for c in df.columns
        if c not in ("profiled_at","source") and df[c].dtype in ("int64","float64","int32","float32") and df[c].notna().all()
    ]
    if len(numeric_cols) < 2:
        return []

    data    = df[numeric_cols].values
    history = data[:-1]
    today   = data[-1].reshape(1, -1)

    # Scale so all features contribute equally
    scaler       = StandardScaler()
    history_sc   = scaler.fit_transform(history)
    today_sc     = scaler.transform(today)

    # Fit Isolation Forest on history
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=42,
    )
    model.fit(history_sc)

    score      = float(model.score_samples(today_sc)[0])  # more negative = more anomalous
    prediction = model.predict(today_sc)[0]               # -1 = anomaly, 1 = normal

    if prediction == -1:
        # Find which metrics contributed most to the anomaly
        z_scores = {}
        history_df = pd.DataFrame(history, columns=numeric_cols)
        for col in numeric_cols:
            mean = history_df[col].mean()
            std  = history_df[col].std()
            if std > 0:
                col_idx = numeric_cols.index(col)
                z_scores[col] = abs((data[-1][col_idx] - mean) / std)

        top_contributors = sorted(z_scores.items(), key=lambda x: x[1], reverse=True)[:3]

        return [{
            "detector":          "isolation_forest",
            "metric":            "all_metrics_combined",
            "anomaly_score":     round(score, 4),
            "severity":          _severity(score),
            "top_contributors":  [
                {"metric": m, "z_score": round(z, 2)}
                for m, z in top_contributors
            ],
            "direction":         "MULTIVARIATE",
        }]

    return []


def _severity(score: float) -> str:
    # score is negative: more negative = worse
    if score < -0.3:
        return "CRITICAL"
    if score < -0.2:
        return "HIGH"
    return "MEDIUM"

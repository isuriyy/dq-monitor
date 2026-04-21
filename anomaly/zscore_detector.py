"""
Technique 1 — Z-Score detector.

For each metric, computes the historical mean and std deviation,
then calculates how many standard deviations today's value is from normal.

Z > 3 or Z < -3  →  anomaly flagged
Severity:
    |Z| >= 6  →  CRITICAL
    |Z| >= 4  →  HIGH
    |Z| >= 3  →  MEDIUM
"""
import pandas as pd
import numpy as np


def zscore_detect(df: pd.DataFrame, min_history: int = 7) -> list[dict]:
    """
    df     : full history DataFrame (all rows including today = last row)
    returns: list of anomaly dicts for today's values
    """
    if len(df) < min_history:
        return []

    history = df.iloc[:-1]   # all rows except today
    today   = df.iloc[-1]    # last row = today

    anomalies = []
    numeric_cols = [c for c in df.columns if c not in ("profiled_at","source") and df[c].dtype in ("int64","float64","int32","float32")]

    for col in numeric_cols:
        series = history[col].dropna()
        if len(series) < min_history or col not in today or pd.isna(today[col]):
            continue

        mean = series.mean()
        std  = series.std()
        if std == 0:
            continue

        z = (today[col] - mean) / std
        if abs(z) >= 3:
            anomalies.append({
                "detector":  "zscore",
                "metric":    col,
                "today":     round(float(today[col]), 4),
                "expected":  round(float(mean), 4),
                "z_score":   round(float(z), 2),
                "severity":  _severity(abs(z)),
                "direction": "LOW" if z < 0 else "HIGH",
            })

    return anomalies


def _severity(abs_z: float) -> str:
    if abs_z >= 6:
        return "CRITICAL"
    if abs_z >= 4:
        return "HIGH"
    return "MEDIUM"

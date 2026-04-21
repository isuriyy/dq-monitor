"""
Technique 2 — IQR (Interquartile Range) detector.

More robust than Z-Score for skewed distributions (e.g. revenue spikes).
Uses the middle 50% of historical values to define "normal range",
then flags anything outside 1.5× that range.

Lower fence = Q1 - 1.5 * IQR
Upper fence = Q3 + 1.5 * IQR

Anything outside fences → anomaly.
Severity based on how far beyond the fence the value falls.
"""
import pandas as pd
import numpy as np


def iqr_detect(df: pd.DataFrame, min_history: int = 7) -> list[dict]:
    """
    df     : full history DataFrame (all rows including today = last row)
    returns: list of anomaly dicts for today's values
    """
    if len(df) < min_history:
        return []

    history = df.iloc[:-1]
    today   = df.iloc[-1]

    anomalies = []
    numeric_cols = [c for c in df.columns if c not in ("profiled_at","source") and df[c].dtype in ("int64","float64","int32","float32")]

    for col in numeric_cols:
        series = history[col].dropna()
        if len(series) < min_history or col not in today or pd.isna(today[col]):
            continue

        q1  = series.quantile(0.25)
        q3  = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        val   = float(today[col])

        if val < lower or val > upper:
            # How many IQR widths outside the fence?
            if val < lower:
                distance = (lower - val) / iqr
                direction = "LOW"
            else:
                distance = (val - upper) / iqr
                direction = "HIGH"

            anomalies.append({
                "detector":      "iqr",
                "metric":        col,
                "today":         round(val, 4),
                "lower_fence":   round(lower, 4),
                "upper_fence":   round(upper, 4),
                "iqr_distance":  round(distance, 2),
                "severity":      _severity(distance),
                "direction":     direction,
            })

    return anomalies


def _severity(iqr_distance: float) -> str:
    if iqr_distance >= 4:
        return "CRITICAL"
    if iqr_distance >= 2:
        return "HIGH"
    return "MEDIUM"

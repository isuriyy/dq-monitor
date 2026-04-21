"""
Combines results from all three detectors,
deduplicates overlapping findings, ranks by severity,
and saves the final anomaly report to anomaly_report.json
"""
import json
from datetime import datetime


SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1}


def build_report(table_results: dict) -> dict:
    """
    table_results: {
        "orders": {
            "zscore":            [...anomaly dicts...],
            "iqr":               [...anomaly dicts...],
            "isolation_forest":  [...anomaly dicts...],
        },
        ...
    }
    Returns a structured report dict.
    """
    report = {
        "run_at":       datetime.now().isoformat(),
        "tables":       {},
        "total_anomalies": 0,
        "critical":     0,
        "high":         0,
        "medium":       0,
        "overall_status": "CLEAN",
    }

    for table, detectors in table_results.items():
        all_anomalies = []

        for detector_name, findings in detectors.items():
            for f in findings:
                f["detector"] = detector_name
                all_anomalies.append(f)

        # Deduplicate: if zscore and iqr both flag same metric, keep highest severity
        deduped = _deduplicate(all_anomalies)

        # Sort by severity
        deduped.sort(key=lambda x: SEVERITY_RANK.get(x["severity"], 0), reverse=True)

        report["tables"][table] = {
            "anomaly_count": len(deduped),
            "status":        "ANOMALOUS" if deduped else "CLEAN",
            "anomalies":     deduped,
        }

        for a in deduped:
            sev = a["severity"]
            report["total_anomalies"] += 1
            if sev == "CRITICAL":
                report["critical"] += 1
            elif sev == "HIGH":
                report["high"] += 1
            else:
                report["medium"] += 1

    if report["critical"] > 0:
        report["overall_status"] = "CRITICAL"
    elif report["high"] > 0:
        report["overall_status"] = "HIGH"
    elif report["medium"] > 0:
        report["overall_status"] = "MEDIUM"

    return report


def _deduplicate(anomalies: list) -> list:
    """Keep highest severity finding per metric."""
    best = {}
    for a in anomalies:
        key = a.get("metric", "unknown")
        if key not in best:
            best[key] = a
        else:
            if SEVERITY_RANK.get(a["severity"], 0) > SEVERITY_RANK.get(best[key]["severity"], 0):
                best[key] = a
    return list(best.values())


def save_report(report: dict, path: str = "anomaly_report.json"):
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)

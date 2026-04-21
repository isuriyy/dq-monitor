"""
Anomaly loader — supports multiple sources.
load_snapshot_history() now loads ALL sources by default.
"""
import sqlite3
import json
import yaml
import pandas as pd


def get_all_sources(config_path="./config/sources.yaml") -> list:
    try:
        with open(config_path) as f:
            return [s["name"] for s in yaml.safe_load(f).get("sources", [])]
    except:
        return ["ecommerce_db"]


def load_snapshot_history(
    db_path="./metadata.db",
    source=None,           # None = all sources
    config_path="./config/sources.yaml"
) -> dict:
    """
    Returns dict keyed by 'source.table' (multi-source) or 'table' (single source).
    Each value is a DataFrame of historical snapshots.
    """
    conn = sqlite3.connect(db_path)

    if source:
        sources = [source]
    else:
        sources = get_all_sources(config_path)

    rows = conn.execute(f"""
        SELECT source, table_name, profiled_at, row_count, profile_json
        FROM profile_snapshots
        WHERE source IN ({','.join('?'*len(sources))})
        ORDER BY source, table_name, profiled_at
    """, sources).fetchall()
    conn.close()

    tables = {}
    for src, table_name, profiled_at, row_count, profile_json in rows:
        # Namespace key: "source.table" when multi-source, just "table" for single
        key = f"{src}.{table_name}" if not source else table_name
        profile = json.loads(profile_json)
        record = {"profiled_at": profiled_at, "row_count": row_count, "source": src}

        for col_name, col_data in profile.get("columns", {}).items():
            if isinstance(col_data, dict):
                for metric in ["null_pct", "mean", "std", "distinct_count"]:
                    if metric in col_data and col_data[metric] is not None:
                        record[f"{metric}.{col_name}"] = float(col_data[metric])

        tables.setdefault(key, []).append(record)

    result = {}
    for key, records in tables.items():
        df = pd.DataFrame(records)
        df["profiled_at"] = pd.to_datetime(df["profiled_at"], format="ISO8601")
        df = df.sort_values("profiled_at").reset_index(drop=True)
        result[key] = df

    return result

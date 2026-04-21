import pandas as pd
from datetime import datetime

class TableProfiler:
    def __init__(self, engine):
        self.engine = engine

    def profile_table(self, table_name):
        with self.engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        profile = {
            "table": table_name,
            "profiled_at": datetime.utcnow().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {}
        }

        for col in df.columns:
            s = df[col]
            cp = {
                "dtype": str(s.dtype),
                "null_count": int(s.isna().sum()),
                "null_pct": round(s.isna().mean() * 100, 2),
                "distinct_count": int(s.nunique()),
                "distinct_pct": round(s.nunique() / len(df) * 100, 2) if len(df) else 0,
            }
            if pd.api.types.is_numeric_dtype(s) and not s.isna().all():
                try:
                    cp["min"]  = round(float(s.min()), 4)
                    cp["max"]  = round(float(s.max()), 4)
                    cp["mean"] = round(float(s.mean()), 4)
                    cp["std"]  = round(float(s.std()), 4)
                except (TypeError, ValueError):
                    pass
            if pd.api.types.is_object_dtype(s):
                cp["top_values"] = s.value_counts().head(5).to_dict()
            profile["columns"][col] = cp

        return profile

from sqlalchemy import inspect
import hashlib, json

class SchemaFingerprinter:
    def __init__(self, engine):
        self.inspector = inspect(engine)

    def fingerprint(self, table):
        cols = self.inspector.get_columns(table)
        schema = {c["name"]: str(c["type"]) for c in cols}
        raw = json.dumps(schema, sort_keys=True)
        return {
            "table": table,
            "columns": schema,
            "fingerprint": hashlib.md5(raw.encode()).hexdigest()
        }

    def detect_drift(self, old_fp, new_fp):
        issues = []
        old_cols = set(old_fp["columns"])
        new_cols = set(new_fp["columns"])
        for col in new_cols - old_cols:
            issues.append(f"ADDED column: {col}")
        for col in old_cols - new_cols:
            issues.append(f"DROPPED column: {col}")
        for col in old_cols & new_cols:
            if old_fp["columns"][col] != new_fp["columns"][col]:
                issues.append(f"TYPE CHANGED: {col}  {old_fp['columns'][col]} -> {new_fp['columns'][col]}")
        return issues

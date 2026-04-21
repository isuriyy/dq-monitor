import sqlite3, json

class MetadataStore:
    def __init__(self, db_path="./metadata.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS profile_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                table_name TEXT,
                profiled_at TEXT,
                row_count INTEGER,
                profile_json TEXT,
                schema_fingerprint TEXT
            )
        """)
        self.conn.commit()

    def save_snapshot(self, source, profile, fingerprint):
        self.conn.execute("""
            INSERT INTO profile_snapshots
            (source, table_name, profiled_at, row_count, profile_json, schema_fingerprint)
            VALUES (?,?,?,?,?,?)
        """, (source, profile["table"], profile["profiled_at"],
              profile["row_count"], json.dumps(profile), fingerprint))
        self.conn.commit()

    def get_last_fingerprint(self, source, table):
        row = self.conn.execute("""
            SELECT schema_fingerprint FROM profile_snapshots
            WHERE source=? AND table_name=?
            ORDER BY profiled_at DESC LIMIT 1
        """, (source, table)).fetchone()
        return json.loads(row[0]) if row else None

    def get_history(self, source, table, limit=10):
        rows = self.conn.execute("""
            SELECT profiled_at, row_count FROM profile_snapshots
            WHERE source=? AND table_name=?
            ORDER BY profiled_at DESC LIMIT ?
        """, (source, table, limit)).fetchall()
        return [{"profiled_at": r[0], "row_count": r[1]} for r in rows]

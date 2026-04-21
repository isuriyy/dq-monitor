"""
Alert deduplication store.
Tracks which alerts have already been sent so we don't
spam the same notification every time the pipeline runs.

Uses a simple SQLite table with a time-window check.
If the same alert was sent within DEDUP_WINDOW_MINUTES, skip it.
"""
import sqlite3
import hashlib
from datetime import datetime, timedelta


class DeduplicationStore:
    def __init__(self, db_path="./metadata.db", window_minutes=60):
        self.db_path       = db_path
        self.window        = timedelta(minutes=window_minutes)
        self._setup()

    def _setup(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alert_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_key   TEXT NOT NULL,
                channel     TEXT NOT NULL,
                sent_at     TEXT NOT NULL,
                severity    TEXT,
                summary     TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _make_key(self, table: str, metric: str, severity: str) -> str:
        raw = f"{table}|{metric}|{severity}"
        return hashlib.md5(raw.encode()).hexdigest()

    def already_sent(self, table: str, metric: str, severity: str, channel: str) -> bool:
        key   = self._make_key(table, metric, severity)
        since = (datetime.now() - self.window).isoformat()
        conn  = sqlite3.connect(self.db_path)
        row   = conn.execute("""
            SELECT id FROM alert_log
            WHERE alert_key = ? AND channel = ? AND sent_at > ?
            ORDER BY sent_at DESC LIMIT 1
        """, (key, channel, since)).fetchone()
        conn.close()
        return row is not None

    def record(self, table: str, metric: str, severity: str,
               channel: str, summary: str):
        key  = self._make_key(table, metric, severity)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO alert_log (alert_key, channel, sent_at, severity, summary)
            VALUES (?, ?, ?, ?, ?)
        """, (key, channel, datetime.now().isoformat(), severity, summary))
        conn.commit()
        conn.close()

    def get_recent_alerts(self, limit=20) -> list:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT sent_at, channel, severity, summary
            FROM alert_log
            ORDER BY sent_at DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [{"sent_at": r[0], "channel": r[1],
                 "severity": r[2], "summary": r[3]} for r in rows]

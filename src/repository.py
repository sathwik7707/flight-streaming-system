"""SQLite persistence for observations, analytics and alerts."""
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from src.config import settings


class Repository:
    def __init__(self, path: str | None = None) -> None:
        self.path = path or settings.db_path
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                event_id TEXT PRIMARY KEY, icao24 TEXT NOT NULL, event_timestamp TEXT NOT NULL,
                latitude REAL NOT NULL, longitude REAL NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS observations_time_idx ON observations(event_timestamp);
            CREATE INDEX IF NOT EXISTS observations_aircraft_idx ON observations(icao24,event_timestamp);
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT, event_timestamp TEXT NOT NULL,
                zone_id TEXT NOT NULL, aircraft_count INTEGER NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS analytics_time_idx ON analytics(event_timestamp);
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY, event_timestamp TEXT NOT NULL, alert_type TEXT NOT NULL,
                severity TEXT NOT NULL, icao24 TEXT, zone_id TEXT, description TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS alerts_time_idx ON alerts(event_timestamp);
        """)
        self.connection.commit()

    def save_observation(self, event: dict[str, Any]) -> None:
        event_id = f"{event.get('icao24')}:{event.get('source_timestamp') or event.get('snapshot_timestamp')}"
        self.connection.execute(
            "INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?)",
            (event_id, event["icao24"], event.get("source_timestamp") or event.get("snapshot_timestamp"),
             event["latitude"], event["longitude"], json.dumps(event)),
        )
        self.connection.commit()

    def save_analytics(self, result: dict[str, Any]) -> None:
        self.connection.execute("INSERT INTO analytics(event_timestamp,zone_id,aircraft_count,payload) VALUES(?,?,?,?)",
                                (result["event_timestamp"], result["zone_id"], result["aircraft_count"], json.dumps(result)))
        self.connection.commit()

    def save_alert(self, alert: dict[str, Any]) -> None:
        self.connection.execute("INSERT OR IGNORE INTO alerts VALUES(?,?,?,?,?,?,?,?)",
                                (alert["alert_id"], alert["event_timestamp"], alert["alert_type"], alert["severity"],
                                 alert.get("icao24"), alert.get("zone_id"), alert["description"], json.dumps(alert)))
        self.connection.commit()

    def rows(self, table: str, limit: int = 500) -> list[dict[str, Any]]:
        order_columns = {
            "observations": "rowid",
            # rowid is present even when an older database has no explicit id
            # column on this table.
            "analytics": "rowid",
            "alerts": "event_timestamp",
        }
        if table not in order_columns:
            raise ValueError("Unsupported table")
        order_column = order_columns[table]
        return [
            dict(row)
            for row in self.connection.execute(
                f"SELECT * FROM {table} ORDER BY {order_column} DESC LIMIT ?",
                (limit,),
            )
        ]

    def close(self) -> None:
        self.connection.close()

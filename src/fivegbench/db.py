"""SQLite database writer for 5gbench.

This is the *only* component that writes to the database.  All other components
publish messages to the event bus; this consumer reads from its queue and
persists records in batches.

Features:
  - WAL mode for concurrent reads during collection
  - Batch writes (flush every 1 second or every 100 messages)
  - One database file per calendar day: 5gbench_YYYYMMDD.db
  - Schema auto-creation on first open
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

BATCH_SIZE = 100
FLUSH_INTERVAL = 1.0  # seconds

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    start_time TEXT NOT NULL,
    end_time TEXT,
    operator TEXT,
    vehicle_id TEXT,
    route_description TEXT,
    notes TEXT,
    config_snapshot TEXT
);

CREATE TABLE IF NOT EXISTS gnss (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    source_modem TEXT NOT NULL,
    fix_type TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    altitude REAL,
    speed_kmh REAL,
    course REAL,
    hdop REAL,
    satellites INTEGER,
    utc_time TEXT
);
CREATE INDEX IF NOT EXISTS idx_gnss_session_time ON gnss(session_id, timestamp);

CREATE TABLE IF NOT EXISTS rf_telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    modem_imei TEXT NOT NULL,
    carrier TEXT NOT NULL,
    access_technology TEXT,
    band TEXT,
    earfcn INTEGER,
    pci INTEGER,
    cell_id TEXT,
    rsrp REAL,
    rsrq REAL,
    sinr REAL,
    rssi REAL,
    registered_network TEXT,
    raw_response TEXT
);
CREATE INDEX IF NOT EXISTS idx_rf_session_time ON rf_telemetry(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_rf_carrier ON rf_telemetry(carrier);

CREATE TABLE IF NOT EXISTS neighbor_cells (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    modem_imei TEXT NOT NULL,
    carrier TEXT NOT NULL,
    neighbor_technology TEXT,
    earfcn INTEGER,
    pci INTEGER,
    rsrp REAL,
    rsrq REAL
);
CREATE INDEX IF NOT EXISTS idx_neighbor_session_time ON neighbor_cells(session_id, timestamp);

CREATE TABLE IF NOT EXISTS throughput_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    modem_imei TEXT NOT NULL,
    carrier TEXT NOT NULL,
    method TEXT NOT NULL,
    direction TEXT NOT NULL,
    bandwidth_bps REAL,
    retransmits INTEGER,
    duration_seconds REAL,
    server TEXT,
    raw_result TEXT
);
CREATE INDEX IF NOT EXISTS idx_tp_session_time ON throughput_results(session_id, timestamp);

CREATE TABLE IF NOT EXISTS latency_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    modem_imei TEXT NOT NULL,
    carrier TEXT NOT NULL,
    method TEXT NOT NULL,
    target TEXT NOT NULL,
    min_ms REAL,
    avg_ms REAL,
    max_ms REAL,
    stddev_ms REAL,
    packet_loss_pct REAL,
    ttfb_ms REAL,
    raw_result TEXT
);
CREATE INDEX IF NOT EXISTS idx_lat_session_time ON latency_results(session_id, timestamp);

CREATE TABLE IF NOT EXISTS modem_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    timestamp TEXT NOT NULL,
    modem_imei TEXT,
    carrier TEXT,
    event_type TEXT NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_session_time ON modem_events(session_id, timestamp);
"""

# ---------------------------------------------------------------------------
# Database connection management
# ---------------------------------------------------------------------------

def _db_filename(date: datetime, db_path: Path) -> Path:
    return db_path / f"5gbench_{date.strftime('%Y%m%d')}.db"


def open_db(db_path: Path, date: datetime | None = None) -> sqlite3.Connection:
    """Open (or create) the SQLite database for *date* (defaults to today)."""
    if date is None:
        date = datetime.now()
    db_path.mkdir(parents=True, exist_ok=True)
    path = _db_filename(date, db_path)
    log.info("Opening database: %s", path)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Insert helpers (sync, called from writer task)
# ---------------------------------------------------------------------------

def insert_session(conn: sqlite3.Connection, data: dict[str, Any]) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO sessions
           (session_id, start_time, end_time, operator, vehicle_id,
            route_description, notes, config_snapshot)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            data["session_id"],
            data["start_time"],
            data.get("end_time"),
            data.get("operator", ""),
            data.get("vehicle_id", ""),
            data.get("route_description", ""),
            data.get("notes", ""),
            json.dumps(data.get("config_snapshot", {})),
        ),
    )


def update_session_end(conn: sqlite3.Connection, session_id: str, end_time: str) -> None:
    conn.execute(
        "UPDATE sessions SET end_time=? WHERE session_id=?",
        (end_time, session_id),
    )


def insert_gnss(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    conn.execute(
        """INSERT INTO gnss
           (session_id, timestamp, source_modem, fix_type, latitude, longitude,
            altitude, speed_kmh, course, hdop, satellites, utc_time)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            session_id, ts,
            data.get("source_modem", ""),
            data.get("fix_type", "no_fix"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("altitude"),
            data.get("speed_kmh"),
            data.get("course"),
            data.get("hdop"),
            data.get("satellites"),
            data.get("utc_time"),
        ),
    )


def insert_rf_telemetry(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    conn.execute(
        """INSERT INTO rf_telemetry
           (session_id, timestamp, modem_imei, carrier, access_technology, band,
            earfcn, pci, cell_id, rsrp, rsrq, sinr, rssi, registered_network, raw_response)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            session_id, ts,
            data.get("modem_imei", ""),
            data.get("carrier", ""),
            data.get("access_technology"),
            data.get("band"),
            data.get("earfcn"),
            data.get("pci"),
            data.get("cell_id"),
            data.get("rsrp"),
            data.get("rsrq"),
            data.get("sinr"),
            data.get("rssi"),
            data.get("registered_network"),
            data.get("raw_response"),
        ),
    )


def insert_neighbor_cells(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    neighbors = data.get("neighbors", [])
    for nb in neighbors:
        conn.execute(
            """INSERT INTO neighbor_cells
               (session_id, timestamp, modem_imei, carrier, neighbor_technology,
                earfcn, pci, rsrp, rsrq)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                session_id, ts,
                data.get("modem_imei", ""),
                data.get("carrier", ""),
                nb.get("neighbor_technology"),
                nb.get("earfcn"),
                nb.get("pci"),
                nb.get("rsrp"),
                nb.get("rsrq"),
            ),
        )


def insert_throughput(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    conn.execute(
        """INSERT INTO throughput_results
           (session_id, timestamp, modem_imei, carrier, method, direction,
            bandwidth_bps, retransmits, duration_seconds, server, raw_result)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            session_id, ts,
            data.get("modem_imei", ""),
            data.get("carrier", ""),
            data.get("method", ""),
            data.get("direction", ""),
            data.get("bandwidth_bps"),
            data.get("retransmits"),
            data.get("duration_seconds"),
            data.get("server", ""),
            json.dumps(data.get("raw_result", {})),
        ),
    )


def insert_latency(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    conn.execute(
        """INSERT INTO latency_results
           (session_id, timestamp, modem_imei, carrier, method, target,
            min_ms, avg_ms, max_ms, stddev_ms, packet_loss_pct, ttfb_ms, raw_result)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            session_id, ts,
            data.get("modem_imei", ""),
            data.get("carrier", ""),
            data.get("method", ""),
            data.get("target", ""),
            data.get("min_ms"),
            data.get("avg_ms"),
            data.get("max_ms"),
            data.get("stddev_ms"),
            data.get("packet_loss_pct"),
            data.get("ttfb_ms"),
            json.dumps(data.get("raw_result", {})),
        ),
    )


def insert_modem_event(conn: sqlite3.Connection, session_id: str, ts: str, data: dict) -> None:
    conn.execute(
        """INSERT INTO modem_events
           (session_id, timestamp, modem_imei, carrier, event_type, detail)
           VALUES (?,?,?,?,?,?)""",
        (
            session_id, ts,
            data.get("modem_imei"),
            data.get("carrier"),
            data.get("event_type", ""),
            data.get("detail", ""),
        ),
    )


# ---------------------------------------------------------------------------
# Dispatcher table
# ---------------------------------------------------------------------------

_INSERTERS = {
    "gnss": insert_gnss,
    "rf_telemetry": insert_rf_telemetry,
    "neighbor_cells": insert_neighbor_cells,
    "throughput": insert_throughput,
    "latency": insert_latency,
    "modem_event": insert_modem_event,
}


def _dispatch_message(conn: sqlite3.Connection, msg: dict[str, Any]) -> None:
    """Write a single bus message to the appropriate table."""
    msg_type = msg.get("type")
    session_id = msg.get("session_id", "")
    ts = msg.get("timestamp", "")
    data = msg.get("data", {})

    if msg_type == "session_event":
        ev = data.get("event")
        if ev == "start":
            insert_session(conn, data)
        elif ev == "stop":
            update_session_end(conn, session_id, data.get("end_time", ts))
        return

    inserter = _INSERTERS.get(msg_type)
    if inserter:
        try:
            inserter(conn, session_id, ts, data)
        except sqlite3.Error as exc:
            log.error("DB insert error [%s]: %s — data=%r", msg_type, exc, data)
    else:
        log.debug("DB: unknown message type '%s' — ignored", msg_type)


# ---------------------------------------------------------------------------
# Async writer task
# ---------------------------------------------------------------------------

class SQLiteWriter:
    """Async consumer task that drains the SQLite queue and writes in batches."""

    def __init__(self, db_path: Path, queue: asyncio.Queue) -> None:
        self._db_path = db_path
        self._queue = queue
        self._conn: sqlite3.Connection | None = None
        self._running = False

    def _get_conn(self) -> sqlite3.Connection:
        """Return connection for today, opening/rotating if needed."""
        today = datetime.now()
        expected_path = _db_filename(today, self._db_path)
        if self._conn is None:
            self._conn = open_db(self._db_path, today)
        else:
            # Check if we've crossed midnight
            current_path = _db_filename(today, self._db_path)
            if current_path != expected_path:
                self._conn.close()
                self._conn = open_db(self._db_path, today)
        return self._conn

    async def run(self) -> None:
        """Main writer loop. Batches commits for efficiency."""
        self._running = True
        log.info("SQLiteWriter: started")
        pending: list[dict] = []
        last_flush = asyncio.get_event_loop().time()

        try:
            while self._running:
                # Collect messages up to BATCH_SIZE or FLUSH_INTERVAL timeout
                try:
                    deadline = last_flush + FLUSH_INTERVAL
                    remaining = max(0.01, deadline - asyncio.get_event_loop().time())
                    msg = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    pending.append(msg)
                    self._queue.task_done()
                except asyncio.TimeoutError:
                    pass

                # Drain any immediately available messages
                while len(pending) < BATCH_SIZE:
                    try:
                        msg = self._queue.get_nowait()
                        pending.append(msg)
                        self._queue.task_done()
                    except asyncio.QueueEmpty:
                        break

                should_flush = (
                    len(pending) >= BATCH_SIZE
                    or (pending and asyncio.get_event_loop().time() >= last_flush + FLUSH_INTERVAL)
                )
                if should_flush and pending:
                    await asyncio.to_thread(self._flush_sync, list(pending))
                    pending.clear()
                    last_flush = asyncio.get_event_loop().time()

        except asyncio.CancelledError:
            # Flush remaining on shutdown
            if pending:
                await asyncio.to_thread(self._flush_sync, pending)
            if self._conn:
                await asyncio.to_thread(self._conn.close)
            self._running = False
            log.info("SQLiteWriter: stopped, flushed %d remaining messages", len(pending))
            raise

    def _flush_sync(self, messages: list[dict]) -> None:
        conn = self._get_conn()
        try:
            with conn:  # transaction
                for msg in messages:
                    _dispatch_message(conn, msg)
            log.debug("SQLiteWriter: committed %d messages", len(messages))
        except sqlite3.Error as exc:
            log.error("SQLiteWriter: commit failed: %s", exc)

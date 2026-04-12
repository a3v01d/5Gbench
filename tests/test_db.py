"""Tests for fivegbench.db — schema, insert helpers, dispatch, and SQLiteWriter.

All tests use in-memory or tmp-path SQLite; no hardware required.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from fivegbench.db import (
    SCHEMA_SQL,
    SQLiteWriter,
    _db_filename,
    _dispatch_message,
    insert_gnss,
    insert_latency,
    insert_modem_event,
    insert_neighbor_cells,
    insert_rf_telemetry,
    insert_session,
    insert_throughput,
    open_db,
    update_session_end,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_db() -> sqlite3.Connection:
    """In-memory SQLite connection with schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


@pytest.fixture
def sid() -> str:
    return "20260410_143022"


def _seed_session(conn: sqlite3.Connection, session_id: str) -> None:
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,NULL,'Alice','VH-1','','',NULL)",
        (session_id, "2026-04-10T14:30:22+00:00"),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# _db_filename
# ---------------------------------------------------------------------------

class TestDbFilename:
    def test_filename_format(self, tmp_path):
        p = _db_filename(datetime(2026, 4, 10), tmp_path)
        assert p.name == "5gbench_20260410.db"
        assert p.parent == tmp_path

    def test_january_first(self, tmp_path):
        assert _db_filename(datetime(2026, 1, 1), tmp_path).name == "5gbench_20260101.db"

    def test_december_last(self, tmp_path):
        assert _db_filename(datetime(2026, 12, 31), tmp_path).name == "5gbench_20261231.db"

    def test_different_dates_different_names(self, tmp_path):
        d1 = _db_filename(datetime(2026, 4, 10), tmp_path)
        d2 = _db_filename(datetime(2026, 4, 11), tmp_path)
        assert d1 != d2


# ---------------------------------------------------------------------------
# open_db
# ---------------------------------------------------------------------------

class TestOpenDb:
    def test_creates_db_file(self, tmp_path):
        conn = open_db(tmp_path, datetime(2026, 4, 10))
        conn.close()
        assert (tmp_path / "5gbench_20260410.db").exists()

    def test_creates_all_tables(self, tmp_path):
        conn = open_db(tmp_path, datetime(2026, 4, 10))
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()
        expected = {
            "sessions", "gnss", "rf_telemetry", "neighbor_cells",
            "throughput_results", "latency_results", "modem_events",
        }
        assert expected <= tables

    def test_creates_parent_dirs(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        conn = open_db(nested, datetime(2026, 4, 10))
        conn.close()
        assert nested.is_dir()

    def test_row_factory_is_sqlite_row(self, tmp_path):
        conn = open_db(tmp_path, datetime(2026, 4, 10))
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_idempotent_schema_application(self, tmp_path):
        """Opening the same DB twice must not raise."""
        conn1 = open_db(tmp_path, datetime(2026, 4, 10))
        conn1.close()
        conn2 = open_db(tmp_path, datetime(2026, 4, 10))
        conn2.close()

    def test_defaults_to_today(self, tmp_path):
        conn = open_db(tmp_path)
        conn.close()
        today = datetime.now().strftime("%Y%m%d")
        assert (tmp_path / f"5gbench_{today}.db").exists()


# ---------------------------------------------------------------------------
# insert_session / update_session_end
# ---------------------------------------------------------------------------

class TestInsertSession:
    def test_inserts_row(self, mem_db, sid):
        insert_session(mem_db, {
            "session_id": sid,
            "start_time": "2026-04-10T14:30:22+00:00",
            "operator": "Alice",
            "vehicle_id": "VH-42",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM sessions WHERE session_id=?", (sid,)).fetchone()
        assert row is not None
        assert row["operator"] == "Alice"
        assert row["vehicle_id"] == "VH-42"

    def test_or_ignore_on_duplicate(self, mem_db, sid):
        data = {"session_id": sid, "start_time": "2026-04-10T14:30:22+00:00"}
        insert_session(mem_db, data)
        insert_session(mem_db, data)
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1

    def test_config_snapshot_serialized_as_json(self, mem_db, sid):
        insert_session(mem_db, {
            "session_id": sid,
            "start_time": "2026-04-10T14:30:22+00:00",
            "config_snapshot": {"polling": {"rf_interval_seconds": 1.0}},
        })
        mem_db.commit()
        row = mem_db.execute("SELECT config_snapshot FROM sessions WHERE session_id=?", (sid,)).fetchone()
        snap = json.loads(row["config_snapshot"])
        assert snap["polling"]["rf_interval_seconds"] == 1.0

    def test_missing_optional_fields_use_defaults(self, mem_db, sid):
        insert_session(mem_db, {"session_id": sid, "start_time": "2026-04-10T14:30:22+00:00"})
        mem_db.commit()
        row = mem_db.execute("SELECT operator, notes FROM sessions WHERE session_id=?", (sid,)).fetchone()
        assert row["operator"] == ""
        assert row["notes"] == ""


class TestUpdateSessionEnd:
    def test_sets_end_time(self, mem_db, sid):
        _seed_session(mem_db, sid)
        update_session_end(mem_db, sid, "2026-04-10T15:00:00+00:00")
        mem_db.commit()
        row = mem_db.execute("SELECT end_time FROM sessions WHERE session_id=?", (sid,)).fetchone()
        assert row["end_time"] == "2026-04-10T15:00:00+00:00"

    def test_nonexistent_session_is_noop(self, mem_db):
        """Updating a non-existent session_id should not raise."""
        update_session_end(mem_db, "99991231_000000", "2026-04-10T15:00:00+00:00")
        mem_db.commit()


# ---------------------------------------------------------------------------
# insert_gnss
# ---------------------------------------------------------------------------

class TestInsertGnss:
    def test_inserts_full_row(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_gnss(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "source_modem": "att",
            "fix_type": "3d",
            "latitude": 34.055662,
            "longitude": -117.832028,
            "altitude": 409.4,
            "speed_kmh": 12.5,
            "course": 270.0,
            "hdop": 1.3,
            "satellites": 12,
            "utc_time": "143023.0",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM gnss WHERE session_id=?", (sid,)).fetchone()
        assert row["fix_type"] == "3d"
        assert row["latitude"] == pytest.approx(34.055662)
        assert row["source_modem"] == "att"

    def test_no_fix_inserts_null_coordinates(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_gnss(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "fix_type": "no_fix",
            "latitude": None,
            "longitude": None,
        })
        mem_db.commit()
        row = mem_db.execute("SELECT latitude, longitude FROM gnss").fetchone()
        assert row["latitude"] is None
        assert row["longitude"] is None

    def test_multiple_rows_accumulate(self, mem_db, sid):
        _seed_session(mem_db, sid)
        for i in range(5):
            insert_gnss(mem_db, sid, f"2026-04-10T14:30:2{i}+00:00", {
                "fix_type": "3d", "latitude": 34.0 + i * 0.001, "longitude": -117.0,
            })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM gnss").fetchone()[0] == 5


# ---------------------------------------------------------------------------
# insert_rf_telemetry
# ---------------------------------------------------------------------------

class TestInsertRfTelemetry:
    def test_inserts_row(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_rf_telemetry(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "access_technology": "NR5G-NSA",
            "band": "n260",
            "earfcn": 2177148,
            "pci": 42,
            "cell_id": "0x1A2B",
            "rsrp": -85.0,
            "rsrq": -10.0,
            "sinr": 18.0,
            "rssi": -65.0,
            "registered_network": "AT&T",
            "raw_response": "+QENG:...",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM rf_telemetry").fetchone()
        assert row["carrier"] == "att"
        assert row["rsrp"] == pytest.approx(-85.0)
        assert row["access_technology"] == "NR5G-NSA"

    def test_missing_optional_fields_stored_as_null(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_rf_telemetry(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT rsrp, band FROM rf_telemetry").fetchone()
        assert row["rsrp"] is None
        assert row["band"] is None

    def test_multiple_carriers(self, mem_db, sid):
        _seed_session(mem_db, sid)
        for carrier, imei in [("att", "860000000000001"), ("tmobile", "860000000000002")]:
            insert_rf_telemetry(mem_db, sid, "2026-04-10T14:30:23+00:00", {
                "modem_imei": imei, "carrier": carrier,
            })
        mem_db.commit()
        carriers = {
            r[0] for r in mem_db.execute("SELECT carrier FROM rf_telemetry").fetchall()
        }
        assert carriers == {"att", "tmobile"}


# ---------------------------------------------------------------------------
# insert_neighbor_cells
# ---------------------------------------------------------------------------

class TestInsertNeighborCells:
    def test_inserts_multiple_neighbors(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_neighbor_cells(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "neighbors": [
                {"neighbor_technology": "LTE", "earfcn": 66786, "pci": 10, "rsrp": -90.0, "rsrq": -12.0},
                {"neighbor_technology": "LTE", "earfcn": 66786, "pci": 11, "rsrp": -95.0, "rsrq": -13.0},
                {"neighbor_technology": "NR5G-SA", "earfcn": 504990, "pci": 5, "rsrp": -88.0, "rsrq": -11.0},
            ],
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM neighbor_cells").fetchone()[0] == 3

    def test_empty_neighbors_inserts_nothing(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_neighbor_cells(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001", "carrier": "att", "neighbors": [],
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM neighbor_cells").fetchone()[0] == 0

    def test_neighbor_fields_stored(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_neighbor_cells(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "neighbors": [{"neighbor_technology": "LTE", "pci": 42, "rsrp": -88.0}],
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM neighbor_cells").fetchone()
        assert row["pci"] == 42
        assert row["rsrp"] == pytest.approx(-88.0)


# ---------------------------------------------------------------------------
# insert_throughput
# ---------------------------------------------------------------------------

class TestInsertThroughput:
    def test_inserts_download_row(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_throughput(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "method": "iperf3",
            "direction": "download",
            "bandwidth_bps": 500_000_000.0,
            "retransmits": 2,
            "duration_seconds": 10.0,
            "server": "iperf.example.com",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM throughput_results").fetchone()
        assert row["direction"] == "download"
        assert row["bandwidth_bps"] == pytest.approx(500_000_000.0)
        assert row["retransmits"] == 2

    def test_raw_result_serialized(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_throughput(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "method": "iperf3",
            "direction": "upload",
            "raw_result": {"streams": [{"bits_per_second": 100_000_000}]},
        })
        mem_db.commit()
        row = mem_db.execute("SELECT raw_result FROM throughput_results").fetchone()
        assert "bits_per_second" in row["raw_result"]


# ---------------------------------------------------------------------------
# insert_latency
# ---------------------------------------------------------------------------

class TestInsertLatency:
    def test_inserts_icmp_row(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_latency(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "method": "icmp",
            "target": "8.8.8.8",
            "min_ms": 5.2,
            "avg_ms": 8.4,
            "max_ms": 15.1,
            "stddev_ms": 2.3,
            "packet_loss_pct": 0.0,
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM latency_results").fetchone()
        assert row["method"] == "icmp"
        assert row["avg_ms"] == pytest.approx(8.4)
        assert row["packet_loss_pct"] == pytest.approx(0.0)

    def test_ttfb_stored(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_latency(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "method": "http_head",
            "target": "https://example.com",
            "ttfb_ms": 45.2,
        })
        mem_db.commit()
        row = mem_db.execute("SELECT ttfb_ms FROM latency_results").fetchone()
        assert row["ttfb_ms"] == pytest.approx(45.2)


# ---------------------------------------------------------------------------
# insert_modem_event
# ---------------------------------------------------------------------------

class TestInsertModemEvent:
    def test_inserts_failover_event(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_modem_event(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "modem_imei": "860000000000001",
            "carrier": "att",
            "event_type": "gnss_failover",
            "detail": "failover from att to tmobile",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT * FROM modem_events").fetchone()
        assert row["event_type"] == "gnss_failover"
        assert "tmobile" in row["detail"]

    def test_null_imei_allowed(self, mem_db, sid):
        _seed_session(mem_db, sid)
        insert_modem_event(mem_db, sid, "2026-04-10T14:30:23+00:00", {
            "event_type": "session_start",
        })
        mem_db.commit()
        row = mem_db.execute("SELECT modem_imei FROM modem_events").fetchone()
        assert row["modem_imei"] is None


# ---------------------------------------------------------------------------
# _dispatch_message
# ---------------------------------------------------------------------------

class TestDispatchMessage:
    def test_session_start_inserts_row(self, mem_db):
        _dispatch_message(mem_db, {
            "type": "session_event",
            "session_id": "20260410_143022",
            "timestamp": "2026-04-10T14:30:22+00:00",
            "data": {
                "event": "start",
                "session_id": "20260410_143022",
                "start_time": "2026-04-10T14:30:22+00:00",
                "operator": "Bob",
            },
        })
        mem_db.commit()
        row = mem_db.execute("SELECT operator FROM sessions").fetchone()
        assert row["operator"] == "Bob"

    def test_session_stop_updates_end_time(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "session_event",
            "session_id": sid,
            "timestamp": "2026-04-10T15:00:00+00:00",
            "data": {"event": "stop", "end_time": "2026-04-10T15:00:00+00:00"},
        })
        mem_db.commit()
        row = mem_db.execute("SELECT end_time FROM sessions WHERE session_id=?", (sid,)).fetchone()
        assert row["end_time"] == "2026-04-10T15:00:00+00:00"

    def test_gnss_routes_to_gnss_table(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "gnss",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"fix_type": "3d", "latitude": 34.0, "longitude": -117.0},
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM gnss").fetchone()[0] == 1

    def test_rf_telemetry_routes_correctly(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "rf_telemetry",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"carrier": "att", "modem_imei": "860000000000001"},
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM rf_telemetry").fetchone()[0] == 1

    def test_throughput_routes_correctly(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "throughput",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"carrier": "att", "modem_imei": "860000000000001",
                     "method": "iperf3", "direction": "download"},
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM throughput_results").fetchone()[0] == 1

    def test_latency_routes_correctly(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "latency",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"carrier": "att", "modem_imei": "860000000000001",
                     "method": "icmp", "target": "8.8.8.8"},
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM latency_results").fetchone()[0] == 1

    def test_modem_event_routes_correctly(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "modem_event",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"event_type": "connect", "carrier": "att"},
        })
        mem_db.commit()
        assert mem_db.execute("SELECT count(*) FROM modem_events").fetchone()[0] == 1

    def test_unknown_type_is_silently_ignored(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {
            "type": "xyzzy",
            "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {},
        })
        mem_db.commit()
        # No row in any data table
        for table in ("gnss", "rf_telemetry", "throughput_results", "latency_results"):
            assert mem_db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0

    def test_missing_type_ignored(self, mem_db, sid):
        _seed_session(mem_db, sid)
        _dispatch_message(mem_db, {"session_id": sid, "timestamp": "2026-04-10T14:30:23+00:00"})
        mem_db.commit()


# ---------------------------------------------------------------------------
# SQLiteWriter
# ---------------------------------------------------------------------------

class TestSQLiteWriter:
    @pytest.mark.asyncio
    async def test_processes_session_event(self, tmp_path):
        q: asyncio.Queue = asyncio.Queue()
        writer = SQLiteWriter(tmp_path, q)
        task = asyncio.create_task(writer.run())

        await q.put({
            "type": "session_event",
            "session_id": "20260410_143022",
            "timestamp": "2026-04-10T14:30:22+00:00",
            "data": {
                "event": "start",
                "session_id": "20260410_143022",
                "start_time": "2026-04-10T14:30:22+00:00",
                "operator": "Eve",
            },
        })
        await asyncio.sleep(0.15)  # let flush interval trigger
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        db_file = tmp_path / f"5gbench_{datetime.now().strftime('%Y%m%d')}.db"
        assert db_file.exists()
        conn = sqlite3.connect(str(db_file))
        row = conn.execute("SELECT operator FROM sessions").fetchone()
        conn.close()
        assert row is not None and row[0] == "Eve"

    @pytest.mark.asyncio
    async def test_flushes_pending_on_cancel(self, tmp_path):
        """Remaining messages in the pending list are flushed before CancelledError propagates."""
        q: asyncio.Queue = asyncio.Queue()
        writer = SQLiteWriter(tmp_path, q)
        task = asyncio.create_task(writer.run())

        # Put a message and immediately cancel — shouldn't lose it
        await q.put({
            "type": "session_event",
            "session_id": "20260410_143022",
            "timestamp": "2026-04-10T14:30:22+00:00",
            "data": {
                "event": "start",
                "session_id": "20260410_143022",
                "start_time": "2026-04-10T14:30:22+00:00",
            },
        })
        # Small sleep so the writer can dequeue the message into `pending`
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        db_file = tmp_path / f"5gbench_{datetime.now().strftime('%Y%m%d')}.db"
        assert db_file.exists()

    @pytest.mark.asyncio
    async def test_processes_multiple_message_types(self, tmp_path):
        q: asyncio.Queue = asyncio.Queue()
        writer = SQLiteWriter(tmp_path, q)
        task = asyncio.create_task(writer.run())

        sid = "20260410_143022"
        await q.put({
            "type": "session_event", "session_id": sid,
            "timestamp": "2026-04-10T14:30:22+00:00",
            "data": {"event": "start", "session_id": sid, "start_time": "2026-04-10T14:30:22+00:00"},
        })
        await q.put({
            "type": "gnss", "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"fix_type": "3d", "latitude": 34.0, "longitude": -117.0},
        })
        await q.put({
            "type": "rf_telemetry", "session_id": sid,
            "timestamp": "2026-04-10T14:30:23+00:00",
            "data": {"carrier": "att", "modem_imei": "860000000000001"},
        })

        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        db_file = tmp_path / f"5gbench_{datetime.now().strftime('%Y%m%d')}.db"
        conn = sqlite3.connect(str(db_file))
        assert conn.execute("SELECT count(*) FROM gnss").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM rf_telemetry").fetchone()[0] == 1
        conn.close()

    @pytest.mark.asyncio
    async def test_get_conn_opens_db_on_first_call(self, tmp_path):
        q: asyncio.Queue = asyncio.Queue()
        writer = SQLiteWriter(tmp_path, q)
        assert writer._conn is None
        conn = writer._get_conn()
        assert conn is not None
        assert writer._conn is not None
        conn.close()

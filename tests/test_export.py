"""Tests for fivegbench.export — GIS export to GeoJSON, KML, and CSV.

Uses an in-memory SQLite database populated with representative data.
No hardware or filesystem I/O required (except the tmp_path fixture).
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from fivegbench.export import (
    CSV_FIELDS,
    export,
    export_csv,
    export_geojson,
    export_kml,
    list_sessions,
)
from fivegbench.db import SCHEMA_SQL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_test_db(path: Path) -> Path:
    """Create a minimal 5gbench_YYYYMMDD.db at *path* with sample data."""
    db_file = path / "5gbench_20260410.db"
    con = sqlite3.connect(str(db_file))
    con.executescript(SCHEMA_SQL)

    con.execute("""
        INSERT INTO sessions VALUES (
            '20260410_143022', '2026-04-10T14:30:22.000+00:00',
            '2026-04-10T15:00:00.000+00:00',
            'Alice', 'VH-42', 'Route 66', '', NULL
        )
    """)

    # Three GNSS fixes
    for i, (lat, lon, ts) in enumerate([
        (34.055662, -117.832028, "2026-04-10T14:30:23.000+00:00"),
        (34.056000, -117.833000, "2026-04-10T14:30:33.000+00:00"),
        (34.056500, -117.834000, "2026-04-10T14:30:43.000+00:00"),
    ]):
        con.execute("""
            INSERT INTO gnss
            (session_id, timestamp, source_modem, fix_type, latitude, longitude,
             altitude, speed_kmh, course, hdop, satellites, utc_time)
            VALUES (?, ?, 'att', '3d', ?, ?, 409.4, 12.5, 270.0, 1.3, 12, '143023.0')
        """, ("20260410_143022", ts, lat, lon))

    # Three RF telemetry rows (att, tmobile, verizon)
    carriers = [
        ("att",     "866000000001", "LTE",      "B66",  "-85", "-10", "18"),
        ("tmobile", "866000000002", "NR5G-NSA", "n71",  "-92", "-12", "15"),
        ("verizon", "866000000003", "LTE",      "B13",  "-105","-14",  "5"),
    ]
    for i, (carrier, imei, tech, band, rsrp, rsrq, sinr) in enumerate(carriers):
        ts = f"2026-04-10T14:30:2{i + 3}.000+00:00"
        con.execute("""
            INSERT INTO rf_telemetry
            (session_id, timestamp, modem_imei, carrier, access_technology,
             band, earfcn, pci, cell_id, rsrp, rsrq, sinr, rssi,
             registered_network, raw_response)
            VALUES (?, ?, ?, ?, ?, ?, 66786, 42, '0x1234', ?, ?, ?, -70, 'LTE', '')
        """, ("20260410_143022", ts, imei, carrier, tech, band, rsrp, rsrq, sinr))

    # One RF row with no matching GNSS (different session — no gnss rows for it)
    con.execute("""
        INSERT INTO sessions VALUES (
            '20260410_160000', '2026-04-10T16:00:00.000+00:00',
            NULL, 'Bob', 'VH-01', '', '', NULL
        )
    """)
    con.execute("""
        INSERT INTO rf_telemetry
        (session_id, timestamp, modem_imei, carrier, access_technology,
         band, earfcn, pci, cell_id, rsrp, rsrq, sinr, rssi,
         registered_network, raw_response)
        VALUES ('20260410_160000', '2026-04-10T16:00:01.000+00:00',
                '866000000001', 'att', 'LTE', 'B66', 66786, 42, '0x1234',
                -88, -11, 16, -70, 'LTE', '')
    """)

    con.commit()
    con.close()
    return db_file


@pytest.fixture
def db_dir(tmp_path):
    _create_test_db(tmp_path)
    return tmp_path


@pytest.fixture
def db_file(db_dir):
    return db_dir / "5gbench_20260410.db"


# ---------------------------------------------------------------------------
# GeoJSON
# ---------------------------------------------------------------------------

class TestExportGeoJSON:
    def test_returns_valid_json(self, db_dir):
        text = export_geojson(db_dir)
        obj = json.loads(text)
        assert obj["type"] == "FeatureCollection"

    def test_feature_count_matches_rows_with_gnss(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        # 3 RF rows from session 1 (all have GNSS matches),
        # 1 RF row from session 2 (no GNSS) — should be excluded
        features = obj["features"]
        assert len(features) == 3

    def test_geometry_type_point(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        for f in obj["features"]:
            assert f["geometry"]["type"] == "Point"
            assert len(f["geometry"]["coordinates"]) == 2

    def test_coordinates_lon_lat_order(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        # GeoJSON is [longitude, latitude]
        for f in obj["features"]:
            lon, lat = f["geometry"]["coordinates"]
            assert -180 <= lon <= 180
            assert -90 <= lat <= 90

    def test_properties_contain_rsrp(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        for f in obj["features"]:
            assert "rsrp" in f["properties"]

    def test_properties_contain_carrier(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        carriers = {f["properties"]["carrier"] for f in obj["features"]}
        assert "att" in carriers
        assert "tmobile" in carriers
        assert "verizon" in carriers

    def test_session_filter(self, db_dir):
        obj = json.loads(export_geojson(db_dir, session_id="20260410_143022"))
        assert len(obj["features"]) == 3

    def test_session_filter_other_session_no_gnss(self, db_dir):
        obj = json.loads(export_geojson(db_dir, session_id="20260410_160000"))
        # No GNSS data for this session
        assert len(obj["features"]) == 0

    def test_writes_to_file(self, db_dir, tmp_path):
        out = tmp_path / "out.geojson"
        export_geojson(db_dir, output=out)
        assert out.exists()
        obj = json.loads(out.read_text())
        assert obj["type"] == "FeatureCollection"

    def test_single_db_file_accepted(self, db_file):
        text = export_geojson(db_file)
        obj = json.loads(text)
        assert obj["type"] == "FeatureCollection"

    def test_include_no_fix_adds_null_geometry(self, db_dir):
        obj = json.loads(export_geojson(db_dir, include_no_fix=True))
        # The no-GNSS row should appear with null geometry
        null_geom = [f for f in obj["features"] if f["geometry"] is None]
        assert len(null_geom) == 1

    def test_properties_have_fix_type(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        for f in obj["features"]:
            assert "fix_type" in f["properties"]

    def test_properties_have_gnss_timestamp(self, db_dir):
        obj = json.loads(export_geojson(db_dir))
        for f in obj["features"]:
            assert "gnss_timestamp" in f["properties"]


# ---------------------------------------------------------------------------
# KML
# ---------------------------------------------------------------------------

class TestExportKML:
    def test_returns_xml_string(self, db_dir):
        text = export_kml(db_dir)
        assert text.startswith("<?xml")
        assert "<kml" in text

    def test_contains_placemarks(self, db_dir):
        text = export_kml(db_dir)
        assert text.count("<Placemark>") == 3

    def test_coordinates_present(self, db_dir):
        text = export_kml(db_dir)
        assert "<coordinates>" in text

    def test_carrier_names_in_output(self, db_dir):
        text = export_kml(db_dir)
        assert "att" in text
        assert "tmobile" in text
        assert "verizon" in text

    def test_style_ids_defined(self, db_dir):
        text = export_kml(db_dir)
        for level in ("good", "ok", "poor", "unknown"):
            assert f'id="rsrp_{level}"' in text

    def test_session_filter(self, db_dir):
        text = export_kml(db_dir, session_id="20260410_143022")
        assert text.count("<Placemark>") == 3

    def test_empty_session_no_placemarks(self, db_dir):
        text = export_kml(db_dir, session_id="20260410_160000")
        assert "<Placemark>" not in text

    def test_writes_to_file(self, db_dir, tmp_path):
        out = tmp_path / "out.kml"
        export_kml(db_dir, output=out)
        assert out.exists()
        assert "<kml" in out.read_text()

    def test_xml_special_chars_escaped(self, tmp_path):
        """Carrier name with XML special chars should be escaped."""
        db = tmp_path / "5gbench_20260411.db"
        con = sqlite3.connect(str(db))
        con.executescript(SCHEMA_SQL)
        con.execute("""
            INSERT INTO sessions VALUES (
                'test_session', '2026-04-11T00:00:00+00:00', NULL,
                'A & B', '', '', '', NULL
            )
        """)
        con.execute("""
            INSERT INTO gnss
            (session_id, timestamp, source_modem, fix_type, latitude, longitude,
             altitude, speed_kmh, course, hdop, satellites, utc_time)
            VALUES ('test_session', '2026-04-11T00:00:01+00:00',
                    'att', '3d', 34.0, -117.0, 0, 0, 0, 1.0, 8, '000001.0')
        """)
        con.execute("""
            INSERT INTO rf_telemetry
            (session_id, timestamp, modem_imei, carrier, access_technology,
             band, earfcn, pci, cell_id, rsrp, rsrq, sinr, rssi,
             registered_network, raw_response)
            VALUES ('test_session', '2026-04-11T00:00:01+00:00',
                    '000000000000001', 'a&b<c>', 'LTE', 'B66',
                    66786, 42, '0x1', -80, -10, 18, -65, 'LTE', '')
        """)
        con.commit()
        con.close()

        text = export_kml(tmp_path)
        assert "&amp;" in text or "a&amp;b" in text
        assert "<Placemark>" in text


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

class TestExportCSV:
    def test_returns_string(self, db_dir):
        text = export_csv(db_dir)
        assert isinstance(text, str)

    def test_header_contains_expected_fields(self, db_dir):
        text = export_csv(db_dir)
        reader = csv.DictReader(io.StringIO(text))
        for field in ("session_id", "carrier", "rsrp", "latitude", "longitude"):
            assert field in (reader.fieldnames or [])

    def test_row_count(self, db_dir):
        text = export_csv(db_dir)
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        # 3 rows from session 1 + 1 row from session 2
        assert len(rows) == 4

    def test_session_filter_row_count(self, db_dir):
        text = export_csv(db_dir, session_id="20260410_143022")
        rows = list(csv.DictReader(io.StringIO(text)))
        assert len(rows) == 3

    def test_rsrp_values_present(self, db_dir):
        text = export_csv(db_dir, session_id="20260410_143022")
        rows = list(csv.DictReader(io.StringIO(text)))
        rsrps = [float(r["rsrp"]) for r in rows]
        assert -85.0 in rsrps
        assert -92.0 in rsrps
        assert -105.0 in rsrps

    def test_latitude_present_for_matched_rows(self, db_dir):
        text = export_csv(db_dir, session_id="20260410_143022")
        rows = list(csv.DictReader(io.StringIO(text)))
        for r in rows:
            assert r["latitude"] != ""

    def test_latitude_empty_when_no_gnss(self, db_dir):
        text = export_csv(db_dir, session_id="20260410_160000")
        rows = list(csv.DictReader(io.StringIO(text)))
        assert len(rows) == 1
        assert rows[0]["latitude"] == ""

    def test_writes_to_file(self, db_dir, tmp_path):
        out = tmp_path / "out.csv"
        export_csv(db_dir, output=out)
        assert out.exists()
        assert "session_id" in out.read_text()

    def test_all_csv_fields_present(self, db_dir):
        text = export_csv(db_dir)
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = set(reader.fieldnames or [])
        for field in CSV_FIELDS:
            assert field in fieldnames, f"Missing CSV field: {field}"


# ---------------------------------------------------------------------------
# export() dispatcher
# ---------------------------------------------------------------------------

class TestExportDispatcher:
    def test_geojson_format(self, db_dir):
        text = export(db_dir, "geojson")
        obj = json.loads(text)
        assert obj["type"] == "FeatureCollection"

    def test_kml_format(self, db_dir):
        text = export(db_dir, "kml")
        assert "<kml" in text

    def test_csv_format(self, db_dir):
        text = export(db_dir, "csv")
        assert "session_id" in text

    def test_uppercase_format_accepted(self, db_dir):
        text = export(db_dir, "GeoJSON")
        assert "FeatureCollection" in text

    def test_unknown_format_raises(self, db_dir):
        with pytest.raises(ValueError, match="Unknown export format"):
            export(db_dir, "shapefile")

    def test_output_file_written(self, db_dir, tmp_path):
        out = tmp_path / "result.geojson"
        export(db_dir, "geojson", output=out)
        assert out.exists()


# ---------------------------------------------------------------------------
# list_sessions
# ---------------------------------------------------------------------------

class TestListSessions:
    def test_returns_list(self, db_dir):
        sessions = list_sessions(db_dir)
        assert isinstance(sessions, list)

    def test_finds_both_sessions(self, db_dir):
        sessions = list_sessions(db_dir)
        ids = [s["session_id"] for s in sessions]
        assert "20260410_143022" in ids
        assert "20260410_160000" in ids

    def test_filter_by_session_id(self, db_dir):
        sessions = list_sessions(db_dir, session_id="20260410_143022")
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == "20260410_143022"

    def test_session_has_metadata(self, db_dir):
        sessions = list_sessions(db_dir, session_id="20260410_143022")
        s = sessions[0]
        assert s["operator"] == "Alice"
        assert s["vehicle_id"] == "VH-42"
        assert s["start_time"] is not None

    def test_nonexistent_session_empty(self, db_dir):
        sessions = list_sessions(db_dir, session_id="99991231_999999")
        assert sessions == []

    def test_empty_dir_returns_empty_list(self, tmp_path):
        sessions = list_sessions(tmp_path)
        assert sessions == []

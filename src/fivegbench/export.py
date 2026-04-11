"""GIS export tool for 5gbench drive-test data.

Joins rf_telemetry with gnss (nearest-timestamp match) and exports to:
  - GeoJSON (Feature Collection, one Feature per RF observation)
  - KML     (Placemarks with Point geometry and RF metadata)
  - CSV     (flat table with all fields)

Usage (via CLI):
    5gbench export --db ./data --session 20260410_143022 \\
                   --format geojson --output drive_test.geojson

    5gbench export --db ./data  # exports all sessions from all .db files
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_JOIN_SQL = """
SELECT
    rf.id            AS rf_id,
    rf.session_id,
    rf.timestamp     AS rf_timestamp,
    rf.modem_imei,
    rf.carrier,
    rf.access_technology,
    rf.band,
    rf.earfcn,
    rf.pci,
    rf.cell_id,
    rf.rsrp,
    rf.rsrq,
    rf.sinr,
    rf.rssi,
    rf.registered_network,
    g.timestamp      AS gnss_timestamp,
    g.fix_type,
    g.latitude,
    g.longitude,
    g.altitude,
    g.speed_kmh,
    g.course,
    g.hdop,
    g.satellites,
    g.source_modem  AS gnss_source
FROM rf_telemetry rf
LEFT JOIN gnss g
    ON  g.session_id = rf.session_id
    AND g.latitude   IS NOT NULL
    AND ABS(julianday(g.timestamp) - julianday(rf.timestamp)) = (
        SELECT MIN(ABS(julianday(g2.timestamp) - julianday(rf.timestamp)))
        FROM   gnss g2
        WHERE  g2.session_id = rf.session_id
        AND    g2.latitude   IS NOT NULL
    )
{where_clause}
ORDER BY rf.timestamp, rf.carrier
"""

_SESSION_SQL = """
SELECT session_id, start_time, end_time, operator, vehicle_id, route_description, notes
FROM sessions
{where_clause}
ORDER BY start_time
"""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _iter_db_files(db_path: Path) -> list[Path]:
    """Return all 5gbench_*.db files under db_path, sorted by name."""
    if db_path.is_file():
        return [db_path]
    return sorted(db_path.glob("5gbench_*.db"))


def _open_ro(db_file: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _query_rows(
    db_file: Path,
    session_id: str | None = None,
) -> Iterator[sqlite3.Row]:
    """Yield joined RF+GNSS rows from *db_file*, optionally filtered by session."""
    try:
        con = _open_ro(db_file)
    except sqlite3.OperationalError as exc:
        raise FileNotFoundError(f"Cannot open database {db_file}: {exc}") from exc

    with con:
        if session_id:
            where = "WHERE rf.session_id = ?"
            rows = con.execute(_JOIN_SQL.format(where_clause=where), (session_id,))
        else:
            rows = con.execute(_JOIN_SQL.format(where_clause=""))
        yield from rows


def _query_sessions(
    db_file: Path,
    session_id: str | None = None,
) -> list[dict]:
    """Return session metadata rows from *db_file*."""
    try:
        con = _open_ro(db_file)
    except sqlite3.OperationalError:
        return []

    with con:
        if session_id:
            rows = con.execute(
                _SESSION_SQL.format(where_clause="WHERE session_id = ?"),
                (session_id,),
            )
        else:
            rows = con.execute(_SESSION_SQL.format(where_clause=""))
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GeoJSON export
# ---------------------------------------------------------------------------

def _row_to_geojson_feature(row: sqlite3.Row) -> dict | None:
    """Convert one joined row to a GeoJSON Feature, or None if no coordinates."""
    lat = row["latitude"]
    lon = row["longitude"]
    if lat is None or lon is None:
        return None

    properties: dict[str, Any] = {
        "session_id": row["session_id"],
        "rf_timestamp": row["rf_timestamp"],
        "carrier": row["carrier"],
        "access_technology": row["access_technology"],
        "band": row["band"],
        "earfcn": row["earfcn"],
        "pci": row["pci"],
        "cell_id": row["cell_id"],
        "rsrp": row["rsrp"],
        "rsrq": row["rsrq"],
        "sinr": row["sinr"],
        "rssi": row["rssi"],
        "registered_network": row["registered_network"],
        "gnss_timestamp": row["gnss_timestamp"],
        "fix_type": row["fix_type"],
        "altitude": row["altitude"],
        "speed_kmh": row["speed_kmh"],
        "course": row["course"],
        "hdop": row["hdop"],
        "satellites": row["satellites"],
        "gnss_source": row["gnss_source"],
        "modem_imei": row["modem_imei"],
    }

    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat],  # GeoJSON is [lon, lat]
        },
        "properties": properties,
    }


def export_geojson(
    db_path: Path,
    output: Path | None = None,
    session_id: str | None = None,
    include_no_fix: bool = False,
) -> str:
    """Export to GeoJSON string; optionally write to *output* file."""
    features: list[dict] = []

    for db_file in _iter_db_files(db_path):
        for row in _query_rows(db_file, session_id):
            feat = _row_to_geojson_feature(row)
            if feat is not None:
                features.append(feat)
            elif include_no_fix:
                # Include as null-geometry feature
                features.append({
                    "type": "Feature",
                    "geometry": None,
                    "properties": dict(row),
                })

    collection = {
        "type": "FeatureCollection",
        "features": features,
    }
    text = json.dumps(collection, indent=2, default=str)

    if output is not None:
        output.write_text(text, encoding="utf-8")

    return text


# ---------------------------------------------------------------------------
# KML export
# ---------------------------------------------------------------------------

def _escape_xml(s: str | None) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _rsrp_color(rsrp: float | None) -> str:
    """Return KML ABGR color string based on RSRP quality."""
    if rsrp is None:
        return "ff888888"  # grey
    if rsrp >= -80:
        return "ff00ff00"  # green
    if rsrp >= -100:
        return "ff00ffff"  # yellow
    return "ff0000ff"     # red


def export_kml(
    db_path: Path,
    output: Path | None = None,
    session_id: str | None = None,
) -> str:
    """Export to KML string; optionally write to *output* file."""
    lines: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<kml xmlns="http://www.opengis.net/kml/2.2">',
        "<Document>",
        "  <name>5gbench Drive Test</name>",
    ]

    # One style per RSRP level
    for label, color in [
        ("good", "ff00ff00"),
        ("ok", "ff00ffff"),
        ("poor", "ff0000ff"),
        ("unknown", "ff888888"),
    ]:
        lines += [
            f'  <Style id="rsrp_{label}">',
            "    <IconStyle>",
            f"      <color>{color}</color>",
            "      <scale>0.6</scale>",
            "    </IconStyle>",
            "  </Style>",
        ]

    def _style_url(rsrp: float | None) -> str:
        if rsrp is None:
            return "#rsrp_unknown"
        if rsrp >= -80:
            return "#rsrp_good"
        if rsrp >= -100:
            return "#rsrp_ok"
        return "#rsrp_poor"

    for db_file in _iter_db_files(db_path):
        for row in _query_rows(db_file, session_id):
            lat = row["latitude"]
            lon = row["longitude"]
            if lat is None or lon is None:
                continue

            alt = row["altitude"] or 0.0
            rsrp = row["rsrp"]
            carrier = _escape_xml(row["carrier"])
            tech = _escape_xml(row["access_technology"])
            band = _escape_xml(row["band"])
            ts = _escape_xml(row["rf_timestamp"])

            desc_lines = [
                f"Carrier: {carrier}",
                f"Tech: {tech}",
                f"Band: {band}",
                f"RSRP: {rsrp} dBm" if rsrp is not None else "RSRP: —",
                f"RSRQ: {row['rsrq']} dB" if row["rsrq"] is not None else "RSRQ: —",
                f"SINR: {row['sinr']} dB" if row["sinr"] is not None else "SINR: —",
                f"Speed: {row['speed_kmh']} km/h" if row["speed_kmh"] is not None else "",
                f"Time: {ts}",
            ]
            description = _escape_xml("\n".join(l for l in desc_lines if l))

            lines += [
                "  <Placemark>",
                f"    <name>{carrier} {tech}</name>",
                f"    <description>{description}</description>",
                f"    <styleUrl>{_style_url(rsrp)}</styleUrl>",
                "    <Point>",
                f"      <coordinates>{lon},{lat},{alt}</coordinates>",
                "    </Point>",
                "  </Placemark>",
            ]

    lines += ["</Document>", "</kml>"]
    text = "\n".join(lines)

    if output is not None:
        output.write_text(text, encoding="utf-8")

    return text


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "session_id",
    "rf_timestamp",
    "carrier",
    "modem_imei",
    "access_technology",
    "band",
    "earfcn",
    "pci",
    "cell_id",
    "rsrp",
    "rsrq",
    "sinr",
    "rssi",
    "registered_network",
    "latitude",
    "longitude",
    "altitude",
    "speed_kmh",
    "course",
    "hdop",
    "satellites",
    "fix_type",
    "gnss_timestamp",
    "gnss_source",
]


def export_csv(
    db_path: Path,
    output: Path | None = None,
    session_id: str | None = None,
) -> str:
    """Export to CSV string; optionally write to *output* file."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()

    for db_file in _iter_db_files(db_path):
        for row in _query_rows(db_file, session_id):
            writer.writerow({f: row[f] for f in CSV_FIELDS})

    text = buf.getvalue()

    if output is not None:
        output.write_text(text, encoding="utf-8")

    return text


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def export(
    db_path: Path,
    fmt: str,
    output: Path | None = None,
    session_id: str | None = None,
) -> str:
    """Export data in *fmt* ('geojson', 'kml', or 'csv').

    Returns the exported content as a string and optionally writes to *output*.
    Raises ValueError for unknown formats.
    """
    fmt = fmt.lower()
    if fmt == "geojson":
        return export_geojson(db_path, output=output, session_id=session_id)
    if fmt == "kml":
        return export_kml(db_path, output=output, session_id=session_id)
    if fmt == "csv":
        return export_csv(db_path, output=output, session_id=session_id)
    raise ValueError(f"Unknown export format: {fmt!r} (choose: geojson, kml, csv)")


def list_sessions(db_path: Path, session_id: str | None = None) -> list[dict]:
    """Return session metadata from all database files under *db_path*."""
    sessions: list[dict] = []
    for db_file in _iter_db_files(db_path):
        sessions.extend(_query_sessions(db_file, session_id))
    return sessions

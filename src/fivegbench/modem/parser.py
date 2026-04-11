"""AT response parsers for Quectel RM520N-GL modems.

Parses responses from:
  AT+QENG="servingcell"   — RF serving cell metrics
  AT+QENG="neighbourcell" — neighbor cell list
  AT+QNWINFO              — registered network / band
  AT+QGPSLOC=2            — GNSS position fix

All parsers return dicts with typed values.  Unknown/missing fields are None.
The raw response string is always preserved so callers can log it for debugging.
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        v = float(value.strip())
        return None if math.isnan(v) else v
    except (ValueError, AttributeError):
        return None


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip(), 0)  # handles "0x..." hex values too
    except (ValueError, AttributeError):
        return None


def _strip_quotes(s: str) -> str:
    return s.strip().strip('"')


# ---------------------------------------------------------------------------
# AT+QENG="servingcell" parser
# ---------------------------------------------------------------------------

def parse_serving_cell(response: str) -> dict[str, Any]:
    """Parse AT+QENG="servingcell" response.

    The response format varies by access technology:

    LTE:
      +QENG: "servingcell","<state>","LTE","<duplex>",<mcc>,<mnc>,<cellid>,
             <pcid>,<earfcn>,<freq_band_ind>,<ul_bw>,<dl_bw>,<tac>,
             <rsrp>,<rsrq>,<rssi>,<sinr>,<srxlev>

    NR5G-NSA (two sub-lines: LTE anchor + NR layer):
      +QENG: "servingcell","<state>","NR5G-NSA"
      +QENG: "LTE",<mcc>,<mnc>,<cellid>,<pcid>,<earfcn>,<freq_band_ind>,
             <ul_bw>,<dl_bw>,<tac>,<rsrp>,<rsrq>,<rssi>,<sinr>,<srxlev>
      +QENG: "NR5G",<mcc>,<mnc>,<cellid>,<pcid>,<nr_arfcn>,<band>,
             <nr_dl_bw>,<rsrp>,<rsrq>,<sinr>

    NR5G-SA:
      +QENG: "servingcell","<state>","NR5G-SA",<mcc>,<mnc>,<cellid>,
             <pcid>,<nr_arfcn>,<band>,<nr_dl_bw>,<rsrp>,<rsrq>,<sinr>

    Returns a unified dict regardless of technology.
    """
    result: dict[str, Any] = {
        "raw_response": response,
        "access_technology": None,
        "state": None,
        "band": None,
        "earfcn": None,
        "pci": None,
        "cell_id": None,
        "rsrp": None,
        "rsrq": None,
        "sinr": None,
        "rssi": None,
        "mcc": None,
        "mnc": None,
    }

    lines = [ln.strip() for ln in response.splitlines() if ln.strip()]

    # Find the primary "servingcell" line
    sc_line = None
    extra_lines: list[str] = []
    for line in lines:
        if '"servingcell"' in line.lower():
            sc_line = line
        elif line.startswith("+QENG:") and sc_line is not None:
            extra_lines.append(line)

    if not sc_line:
        log.debug("parse_serving_cell: no +QENG: servingcell line found in: %r", response[:300])
        return result

    # Extract fields after +QENG:
    sc_match = re.match(r'\+QENG:\s*(.*)', sc_line)
    if not sc_match:
        return result
    sc_content = sc_match.group(1)

    # Split CSV but respect quoted strings
    fields = _csv_split(sc_content)
    # fields[0] = "servingcell", fields[1] = state, fields[2] = technology
    if len(fields) < 3:
        return result

    state = _strip_quotes(fields[1])
    technology = _strip_quotes(fields[2])
    result["state"] = state
    result["access_technology"] = technology

    if technology == "LTE":
        # fields: "servingcell",state,"LTE","FDD/TDD",mcc,mnc,cellid,pcid,
        #          earfcn,freq_band,ul_bw,dl_bw,tac,rsrp,rsrq,rssi,sinr,srxlev
        _parse_lte_fields(fields, result, offset=3)

    elif technology == "NR5G-NSA":
        # Primary line has no signal data — parse sub-lines
        for extra in extra_lines:
            em = re.match(r'\+QENG:\s*(.*)', extra)
            if not em:
                continue
            ef = _csv_split(em.group(1))
            if not ef:
                continue
            rat = _strip_quotes(ef[0])
            if rat == "LTE":
                # ef: "LTE",mcc,mnc,cellid,pcid,earfcn,band,ul_bw,dl_bw,tac,rsrp,rsrq,rssi,sinr,srxlev
                _parse_nsa_lte_fields(ef, result, offset=1)
            elif rat == "NR5G":
                # ef: "NR5G",mcc,mnc,cellid,pcid,nr_arfcn,band,nr_dl_bw,rsrp,rsrq,sinr
                _parse_nsa_nr_fields(ef, result, offset=1)

    elif technology == "NR5G-SA":
        # fields: "servingcell",state,"NR5G-SA",mcc,mnc,cellid,pcid,nr_arfcn,band,nr_dl_bw,rsrp,rsrq,sinr
        _parse_nrsa_fields(fields, result, offset=3)

    return result


def _parse_lte_fields(fields: list[str], result: dict, offset: int) -> None:
    # offset: index of the duplex-mode field ("FDD" or "TDD").
    # Field layout after duplex: mcc, mnc, cellid, pcid, earfcn, freq_band,
    #   ul_bw, dl_bw, tac, rsrp, rsrq, rssi, sinr, srxlev
    try:
        result["mcc"] = fields[offset + 1] if offset + 1 < len(fields) else None
        result["mnc"] = fields[offset + 2] if offset + 2 < len(fields) else None
        result["cell_id"] = _strip_quotes(fields[offset + 3]) if offset + 3 < len(fields) else None
        result["pci"] = _int(fields[offset + 4]) if offset + 4 < len(fields) else None
        result["earfcn"] = _int(fields[offset + 5]) if offset + 5 < len(fields) else None
        band_idx = offset + 6
        if band_idx < len(fields):
            result["band"] = f"B{fields[band_idx].strip()}"
        # rsrp is at duplex+10: skip duplex(0)+mcc(1)+mnc(2)+cellid(3)+pcid(4)+
        #   earfcn(5)+band(6)+ul_bw(7)+dl_bw(8)+tac(9) = index offset+10
        rsrp_idx = offset + 10
        result["rsrp"] = _float(fields[rsrp_idx]) if rsrp_idx < len(fields) else None
        result["rsrq"] = _float(fields[rsrp_idx + 1]) if rsrp_idx + 1 < len(fields) else None
        result["rssi"] = _float(fields[rsrp_idx + 2]) if rsrp_idx + 2 < len(fields) else None
        result["sinr"] = _float(fields[rsrp_idx + 3]) if rsrp_idx + 3 < len(fields) else None
    except (IndexError, ValueError) as exc:
        log.debug("_parse_lte_fields error: %s", exc)


def _parse_nsa_lte_fields(fields: list[str], result: dict, offset: int) -> None:
    try:
        result["mcc"] = fields[offset] if offset < len(fields) else None
        result["mnc"] = fields[offset + 1] if offset + 1 < len(fields) else None
        result["cell_id"] = _strip_quotes(fields[offset + 2]) if offset + 2 < len(fields) else None
        result["pci"] = _int(fields[offset + 3]) if offset + 3 < len(fields) else None
        result["earfcn"] = _int(fields[offset + 4]) if offset + 4 < len(fields) else None
        if offset + 5 < len(fields):
            result["band"] = f"B{fields[offset + 5].strip()}"
        # rsrp at offset+9, rsrq, rssi, sinr
        rsrp_idx = offset + 9
        result["rsrp"] = _float(fields[rsrp_idx]) if rsrp_idx < len(fields) else None
        result["rsrq"] = _float(fields[rsrp_idx + 1]) if rsrp_idx + 1 < len(fields) else None
        result["rssi"] = _float(fields[rsrp_idx + 2]) if rsrp_idx + 2 < len(fields) else None
        result["sinr"] = _float(fields[rsrp_idx + 3]) if rsrp_idx + 3 < len(fields) else None
    except (IndexError, ValueError) as exc:
        log.debug("_parse_nsa_lte_fields error: %s", exc)


def _parse_nsa_nr_fields(fields: list[str], result: dict, offset: int) -> None:
    # NR layer: overwrite RSRP/RSRQ/SINR with NR values (stronger signal source)
    try:
        if not result.get("cell_id"):  # prefer NR cell ID if LTE wasn't set
            result["cell_id"] = _strip_quotes(fields[offset + 2]) if offset + 2 < len(fields) else None
        result["pci"] = _int(fields[offset + 3]) if offset + 3 < len(fields) else None
        result["earfcn"] = _int(fields[offset + 4]) if offset + 4 < len(fields) else None
        if offset + 5 < len(fields):
            result["band"] = f"n{fields[offset + 5].strip()}"
        rsrp_idx = offset + 7
        result["rsrp"] = _float(fields[rsrp_idx]) if rsrp_idx < len(fields) else None
        result["rsrq"] = _float(fields[rsrp_idx + 1]) if rsrp_idx + 1 < len(fields) else None
        result["sinr"] = _float(fields[rsrp_idx + 2]) if rsrp_idx + 2 < len(fields) else None
    except (IndexError, ValueError) as exc:
        log.debug("_parse_nsa_nr_fields error: %s", exc)


def _parse_nrsa_fields(fields: list[str], result: dict, offset: int) -> None:
    try:
        result["mcc"] = fields[offset] if offset < len(fields) else None
        result["mnc"] = fields[offset + 1] if offset + 1 < len(fields) else None
        result["cell_id"] = _strip_quotes(fields[offset + 2]) if offset + 2 < len(fields) else None
        result["pci"] = _int(fields[offset + 3]) if offset + 3 < len(fields) else None
        result["earfcn"] = _int(fields[offset + 4]) if offset + 4 < len(fields) else None
        if offset + 5 < len(fields):
            result["band"] = f"n{fields[offset + 5].strip()}"
        rsrp_idx = offset + 7
        result["rsrp"] = _float(fields[rsrp_idx]) if rsrp_idx < len(fields) else None
        result["rsrq"] = _float(fields[rsrp_idx + 1]) if rsrp_idx + 1 < len(fields) else None
        result["sinr"] = _float(fields[rsrp_idx + 2]) if rsrp_idx + 2 < len(fields) else None
    except (IndexError, ValueError) as exc:
        log.debug("_parse_nrsa_fields error: %s", exc)


# ---------------------------------------------------------------------------
# AT+QENG="neighbourcell" parser
# ---------------------------------------------------------------------------

def parse_neighbor_cells(response: str) -> list[dict[str, Any]]:
    """Parse AT+QENG="neighbourcell" response.

    Returns a list of neighbor cell dicts.
    Each dict has: neighbor_technology, earfcn, pci, rsrp, rsrq
    """
    neighbors: list[dict[str, Any]] = []
    for line in response.splitlines():
        line = line.strip()
        if not line.startswith("+QENG:"):
            continue
        m = re.match(r'\+QENG:\s*(.*)', line)
        if not m:
            continue
        fields = _csv_split(m.group(1))
        if len(fields) < 2:
            continue
        rat = _strip_quotes(fields[0])
        if rat in ("neighbourcell intra", "neighbourcell inter"):
            # LTE intra/inter: "neighbourcell intra","LTE",earfcn,pcid,rsrq,rsrp,rssi,dltq
            try:
                cell: dict[str, Any] = {
                    "neighbor_technology": "LTE",
                    "earfcn": _int(fields[2]) if len(fields) > 2 else None,
                    "pci": _int(fields[3]) if len(fields) > 3 else None,
                    "rsrq": _float(fields[4]) if len(fields) > 4 else None,
                    "rsrp": _float(fields[5]) if len(fields) > 5 else None,
                }
                neighbors.append(cell)
            except (IndexError, ValueError):
                continue
        elif rat == "neighbourcell":
            # NR5G neighbor: "neighbourcell","NR5G",nr_arfcn,pcid,rsrp,rsrq,sinr
            try:
                nr_tech = _strip_quotes(fields[1]) if len(fields) > 1 else "NR5G"
                cell = {
                    "neighbor_technology": nr_tech,
                    "earfcn": _int(fields[2]) if len(fields) > 2 else None,
                    "pci": _int(fields[3]) if len(fields) > 3 else None,
                    "rsrp": _float(fields[4]) if len(fields) > 4 else None,
                    "rsrq": _float(fields[5]) if len(fields) > 5 else None,
                }
                neighbors.append(cell)
            except (IndexError, ValueError):
                continue
    return neighbors


# ---------------------------------------------------------------------------
# AT+QNWINFO parser
# ---------------------------------------------------------------------------

def parse_qnwinfo(response: str) -> dict[str, Any]:
    """Parse AT+QNWINFO response.

    Format: +QNWINFO: <access_tech>,<MCC><MNC>,<band>,<channel>

    Example: +QNWINFO: "LTE","310260","LTE BAND 66",66786
    """
    result: dict[str, Any] = {
        "registered_network": None,
        "band_info": None,
        "channel": None,
    }
    for line in response.splitlines():
        m = re.match(r'\+QNWINFO:\s*(.*)', line.strip())
        if not m:
            continue
        fields = _csv_split(m.group(1))
        if len(fields) >= 1:
            result["registered_network"] = _strip_quotes(fields[0])
        if len(fields) >= 3:
            result["band_info"] = _strip_quotes(fields[2])
        if len(fields) >= 4:
            result["channel"] = _int(fields[3])
        break
    return result


# ---------------------------------------------------------------------------
# AT+QGPSLOC=2 parser
# ---------------------------------------------------------------------------

def parse_gnss_fix(response: str) -> dict[str, Any]:
    """Parse AT+QGPSLOC=2 response.

    Format (mode=2, decimal degrees):
      +QGPSLOC: <UTC>,<lat>,<lon>,<hdop>,<alt>,<fix_mode>,<course>,<speed_km>,<date>,<nsat>

    Example:
      +QGPSLOC: 093441.0,34.055662,-117.832028,1.3,409.4,3,270.0,0.0,100423,12

    fix_mode: 0=no fix, 1=dead reckoning, 2=2D fix, 3=3D fix
    """
    result: dict[str, Any] = {
        "fix_type": "no_fix",
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "speed_kmh": None,
        "course": None,
        "hdop": None,
        "satellites": None,
        "utc_time": None,
    }

    for line in response.splitlines():
        line = line.strip()
        if not line.startswith("+QGPSLOC:"):
            continue
        m = re.match(r'\+QGPSLOC:\s*(.*)', line)
        if not m:
            continue

        fields = [f.strip() for f in m.group(1).split(",")]
        if len(fields) < 9:
            log.debug("GNSS: too few fields (%d): %r", len(fields), line)
            return result

        try:
            result["utc_time"] = fields[0]
            result["latitude"] = _float(fields[1])
            result["longitude"] = _float(fields[2])
            result["hdop"] = _float(fields[3])
            result["altitude"] = _float(fields[4])
            fix_mode = _int(fields[5])
            result["course"] = _float(fields[6])
            result["speed_kmh"] = _float(fields[7])
            # fields[8] = date, fields[9] = nsat (optional)
            if len(fields) > 9:
                result["satellites"] = _int(fields[9])

            if fix_mode == 2:
                result["fix_type"] = "2d"
            elif fix_mode == 3:
                result["fix_type"] = "3d"
            else:
                result["fix_type"] = "no_fix"
        except (IndexError, ValueError) as exc:
            log.debug("parse_gnss_fix: parse error: %s on line: %r", exc, line)
        break

    return result


# ---------------------------------------------------------------------------
# CSV field splitter (handles quoted strings with commas)
# ---------------------------------------------------------------------------

def _csv_split(s: str) -> list[str]:
    """Split a comma-separated string, respecting double-quoted fields."""
    fields: list[str] = []
    current: list[str] = []
    in_quotes = False
    for ch in s:
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
        elif ch == ',' and not in_quotes:
            fields.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        fields.append("".join(current))
    return fields

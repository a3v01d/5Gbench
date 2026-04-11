"""Tests for fivegbench.modem.parser — AT response parsing.

All test inputs are representative real-world responses from the
Quectel RM520N-GL firmware.  No hardware required.
"""

import pytest

from fivegbench.modem.parser import (
    parse_serving_cell,
    parse_neighbor_cells,
    parse_qnwinfo,
    parse_gnss_fix,
    _csv_split,
)


# ---------------------------------------------------------------------------
# _csv_split
# ---------------------------------------------------------------------------

class TestCsvSplit:
    def test_simple(self):
        assert _csv_split("a,b,c") == ["a", "b", "c"]

    def test_quoted_with_comma(self):
        assert _csv_split('"hello, world",b') == ['"hello, world"', "b"]

    def test_empty_string(self):
        assert _csv_split("") == []

    def test_quoted_empty(self):
        assert _csv_split('"",a') == ['""', "a"]

    def test_all_quoted(self):
        result = _csv_split('"servingcell","NOCONN","LTE"')
        assert result == ['"servingcell"', '"NOCONN"', '"LTE"']

    def test_no_quotes(self):
        assert _csv_split("310,260,0x1234") == ["310", "260", "0x1234"]


# ---------------------------------------------------------------------------
# parse_serving_cell — LTE
# ---------------------------------------------------------------------------

class TestServingCellLTE:
    # Typical RM520N-GL LTE response (includes TAC between dl_bw and rsrp)
    # Layout after FDD: mcc,mnc,cellid,pcid,earfcn,freq_band,ul_bw,dl_bw,tac,rsrp,rsrq,rssi,sinr,srxlev
    RESPONSE = (
        '+QENG: "servingcell","NOCONN","LTE","FDD",'
        '310,260,0x1234ABCD,123,4,66,3,3,4660,-85,-10,-65,18,17\r\n'
        "OK"
    )

    def setup_method(self):
        self.result = parse_serving_cell(self.RESPONSE)

    def test_access_technology(self):
        assert self.result["access_technology"] == "LTE"

    def test_state(self):
        assert self.result["state"] == "NOCONN"

    def test_rsrp(self):
        assert self.result["rsrp"] == -85.0

    def test_rsrq(self):
        assert self.result["rsrq"] == -10.0

    def test_sinr(self):
        assert self.result["sinr"] == 18.0

    def test_pci(self):
        assert self.result["pci"] == 123

    def test_earfcn(self):
        assert self.result["earfcn"] == 4

    def test_band_prefix(self):
        # Band field should start with "B" for LTE
        assert self.result["band"] is not None
        assert self.result["band"].startswith("B")

    def test_raw_response_preserved(self):
        assert self.result["raw_response"] == self.RESPONSE

    def test_no_exception_on_valid_input(self):
        # Parsing should never raise
        assert isinstance(self.result, dict)


# ---------------------------------------------------------------------------
# parse_serving_cell — NR5G-NSA (two sub-lines)
# ---------------------------------------------------------------------------

class TestServingCellNR5GNSA:
    # NSA LTE sub-line: "LTE",mcc,mnc,cellid,pcid,earfcn,band,ul_bw,dl_bw,tac,rsrp,rsrq,rssi,sinr,srxlev
    # NR5G sub-line:    "NR5G",mcc,mnc,cellid,pcid,nr_arfcn,band,nr_dl_bw,rsrp,rsrq,sinr
    RESPONSE = (
        '+QENG: "servingcell","NOCONN","NR5G-NSA"\r\n'
        '+QENG: "LTE",310,260,0xABCD1234,42,66786,66,3,3,5000,-90,-12,-70,15,15\r\n'
        '+QENG: "NR5G",310,260,0xDEADBEEF,87,520111,260,4,-72,-8,22\r\n'
        "OK"
    )

    def setup_method(self):
        self.result = parse_serving_cell(self.RESPONSE)

    def test_access_technology(self):
        assert self.result["access_technology"] == "NR5G-NSA"

    def test_state(self):
        assert self.result["state"] == "NOCONN"

    def test_rsrp_comes_from_nr_layer(self):
        # NR values should overwrite LTE values in NSA mode
        assert self.result["rsrp"] == -72.0

    def test_rsrq_from_nr(self):
        assert self.result["rsrq"] == -8.0

    def test_sinr_from_nr(self):
        assert self.result["sinr"] == 22.0

    def test_band_nr_prefix(self):
        # NR band uses "n" prefix
        assert self.result["band"] is not None
        assert self.result["band"].startswith("n")

    def test_earfcn_is_nr_arfcn(self):
        assert self.result["earfcn"] == 520111

    def test_pci_from_nr(self):
        assert self.result["pci"] == 87


# ---------------------------------------------------------------------------
# parse_serving_cell — NR5G-SA
# ---------------------------------------------------------------------------

class TestServingCellNR5GSA:
    RESPONSE = (
        '+QENG: "servingcell","CONNECT","NR5G-SA",'
        '310,260,0xCAFEBABE,55,123456,71,3,-95,-13,14\r\n'
        "OK"
    )

    def setup_method(self):
        self.result = parse_serving_cell(self.RESPONSE)

    def test_access_technology(self):
        assert self.result["access_technology"] == "NR5G-SA"

    def test_state(self):
        assert self.result["state"] == "CONNECT"

    def test_rsrp(self):
        assert self.result["rsrp"] == -95.0

    def test_rsrq(self):
        assert self.result["rsrq"] == -13.0

    def test_sinr(self):
        assert self.result["sinr"] == 14.0

    def test_earfcn(self):
        assert self.result["earfcn"] == 123456

    def test_pci(self):
        assert self.result["pci"] == 55

    def test_band_nr_prefix(self):
        assert self.result["band"].startswith("n")


# ---------------------------------------------------------------------------
# parse_serving_cell — edge cases
# ---------------------------------------------------------------------------

class TestServingCellEdgeCases:
    def test_empty_response_returns_dict(self):
        result = parse_serving_cell("")
        assert isinstance(result, dict)
        assert result["access_technology"] is None

    def test_ok_only_returns_dict(self):
        result = parse_serving_cell("OK")
        assert result["access_technology"] is None

    def test_cme_error_returns_dict(self):
        result = parse_serving_cell("+CME ERROR: 3\r\nERROR")
        assert result["access_technology"] is None

    def test_truncated_lte_response_no_crash(self):
        # Missing signal fields — should return partial result, not raise
        result = parse_serving_cell('+QENG: "servingcell","NOCONN","LTE","FDD",310,260\r\nOK')
        assert result["access_technology"] == "LTE"
        # Signal fields absent → None
        assert result["rsrp"] is None

    def test_raw_response_always_present(self):
        resp = "some garbage"
        result = parse_serving_cell(resp)
        assert result["raw_response"] == resp


# ---------------------------------------------------------------------------
# parse_neighbor_cells
# ---------------------------------------------------------------------------

class TestNeighborCells:
    LTE_RESPONSE = (
        '+QENG: "neighbourcell intra","LTE",1300,100,-12,-95,-65,0\r\n'
        '+QENG: "neighbourcell inter","LTE",6200,200,-14,-102,-70,0\r\n'
        "OK"
    )

    NR_RESPONSE = (
        '+QENG: "neighbourcell","NR5G",431000,44,-88,-10,15\r\n'
        "OK"
    )

    def test_lte_intra_parsed(self):
        cells = parse_neighbor_cells(self.LTE_RESPONSE)
        assert len(cells) >= 1
        first = cells[0]
        assert first["neighbor_technology"] == "LTE"
        assert first["earfcn"] == 1300
        assert first["pci"] == 100

    def test_lte_rsrp_rsrq(self):
        cells = parse_neighbor_cells(self.LTE_RESPONSE)
        first = cells[0]
        assert first["rsrq"] == -12.0
        assert first["rsrp"] == -95.0

    def test_lte_two_cells(self):
        cells = parse_neighbor_cells(self.LTE_RESPONSE)
        assert len(cells) == 2

    def test_nr_cell_parsed(self):
        cells = parse_neighbor_cells(self.NR_RESPONSE)
        assert len(cells) == 1
        cell = cells[0]
        assert cell["neighbor_technology"] == "NR5G"
        assert cell["earfcn"] == 431000
        assert cell["pci"] == 44
        assert cell["rsrp"] == -88.0

    def test_empty_response_returns_empty_list(self):
        assert parse_neighbor_cells("OK") == []

    def test_no_exception_on_garbage(self):
        result = parse_neighbor_cells("garbage data\nOK")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# parse_qnwinfo
# ---------------------------------------------------------------------------

class TestQnwinfo:
    def test_lte_response(self):
        resp = '+QNWINFO: "LTE","310260","LTE BAND 66",66786\r\nOK'
        result = parse_qnwinfo(resp)
        assert result["registered_network"] == "LTE"
        assert result["band_info"] == "LTE BAND 66"
        assert result["channel"] == 66786

    def test_nr_response(self):
        resp = '+QNWINFO: "NR5G-NSA","310260","NR5G BAND 260",520111\r\nOK'
        result = parse_qnwinfo(resp)
        assert result["registered_network"] == "NR5G-NSA"
        assert result["channel"] == 520111

    def test_empty_returns_nones(self):
        result = parse_qnwinfo("OK")
        assert result["registered_network"] is None
        assert result["band_info"] is None
        assert result["channel"] is None

    def test_no_exception_on_garbage(self):
        result = parse_qnwinfo("garbage")
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# parse_gnss_fix
# ---------------------------------------------------------------------------

class TestGnssFix:
    # AT+QGPSLOC=2 response: decimal degrees, 3D fix
    FIX_3D = (
        "+QGPSLOC: 093441.0,34.055662,-117.832028,1.3,409.4,3,270.0,12.5,100423,12\r\n"
        "OK"
    )
    # 2D fix
    FIX_2D = (
        "+QGPSLOC: 093441.0,34.055662,-117.832028,2.1,0.0,2,0.0,0.0,100423,8\r\n"
        "OK"
    )
    # No fix (CME ERROR 516 = no fix)
    NO_FIX = "+CME ERROR: 516\r\nERROR"

    def test_3d_fix_type(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["fix_type"] == "3d"

    def test_3d_latitude(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert abs(result["latitude"] - 34.055662) < 1e-5

    def test_3d_longitude(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert abs(result["longitude"] - (-117.832028)) < 1e-5

    def test_3d_altitude(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["altitude"] == pytest.approx(409.4)

    def test_3d_hdop(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["hdop"] == pytest.approx(1.3)

    def test_3d_satellites(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["satellites"] == 12

    def test_3d_speed_kmh(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["speed_kmh"] == pytest.approx(12.5)

    def test_3d_course(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["course"] == pytest.approx(270.0)

    def test_3d_utc_time(self):
        result = parse_gnss_fix(self.FIX_3D)
        assert result["utc_time"] == "093441.0"

    def test_2d_fix_type(self):
        result = parse_gnss_fix(self.FIX_2D)
        assert result["fix_type"] == "2d"

    def test_no_fix_response(self):
        result = parse_gnss_fix(self.NO_FIX)
        assert result["fix_type"] == "no_fix"
        assert result["latitude"] is None
        assert result["longitude"] is None

    def test_empty_response_no_fix(self):
        result = parse_gnss_fix("OK")
        assert result["fix_type"] == "no_fix"

    def test_no_exception_on_garbage(self):
        result = parse_gnss_fix("totally wrong")
        assert isinstance(result, dict)
        assert result["fix_type"] == "no_fix"

    def test_missing_sat_field_graceful(self):
        # Only 9 fields (no nsat)
        resp = "+QGPSLOC: 093441.0,34.055662,-117.832028,1.3,409.4,3,270.0,0.0,100423\r\nOK"
        result = parse_gnss_fix(resp)
        assert result["fix_type"] == "3d"
        assert result["satellites"] is None

"""Tests for fivegbench.config — loading, validation, and defaults."""

import textwrap
import tempfile
from pathlib import Path

import pytest

from fivegbench.config import load, Config, GnssConfig, ModemConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_toml(content: str) -> Path:
    """Write *content* to a temp file and return the path."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".toml", delete=False, encoding="utf-8"
    )
    tmp.write(textwrap.dedent(content))
    tmp.flush()
    return Path(tmp.name)


MINIMAL_TOML = """\
    [[modems]]
    imei = "860123456789001"
    carrier = "att"
    label = "AT&T"
    apn = "broadband"
"""

FULL_TOML = """\
    [general]
    db_path = "/tmp/5gbench_test/"
    log_path = "/tmp/5gbench_test/logs/"
    log_level = "debug"

    [general.dns]
    nameservers = ["1.1.1.1", "8.8.4.4"]

    [session_defaults]
    operator = "Alice"
    vehicle_id = "VH-001"
    route_description = "Sierra Ave"
    notes = "Test run"

    [gnss]
    primary_modem = "att"
    failover_order = ["tmobile"]
    movement_threshold_meters = 10.0
    interpolate_on_fix_loss = false

    [polling]
    rf_interval_seconds = 2.0
    neighbor_cells = false
    throughput_interval_seconds = 120.0
    gnss_interval_seconds = 2.0

    [throughput]
    method = "http"
    iperf3_server = "iperf.example.com"
    iperf3_port = 5202
    iperf3_duration_seconds = 20

    [latency]
    methods = ["icmp"]
    targets = ["1.1.1.1"]
    icmp_count = 5
    tcp_port = 80
    jitter = false

    [api]
    enabled = true
    bind = "0.0.0.0"
    port = 9090
    auth = "apikey"
    api_key = "secret123"

    [tui]
    enabled = false

    [[modems]]
    imei = "860123456789001"
    carrier = "att"
    label = "AT&T"
    apn = "broadband"

    [[modems]]
    imei = "860123456789002"
    carrier = "tmobile"
    label = "T-Mobile"
    apn = "fast.t-mobile.com"
"""


# ---------------------------------------------------------------------------
# Basic loading
# ---------------------------------------------------------------------------

class TestLoad:
    def test_minimal_config_loads(self):
        cfg = load(_write_toml(MINIMAL_TOML))
        assert isinstance(cfg, Config)
        assert len(cfg.modems) == 1

    def test_full_config_loads(self):
        cfg = load(_write_toml(FULL_TOML))
        assert len(cfg.modems) == 2

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load(Path("/nonexistent/path/config.toml"))

    def test_invalid_toml_raises(self):
        import tomllib
        path = _write_toml("this is not valid [[[toml")
        with pytest.raises(tomllib.TOMLDecodeError):
            load(path)


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

class TestDefaults:
    def setup_method(self):
        self.cfg = load(_write_toml(MINIMAL_TOML))

    def test_default_log_level(self):
        assert self.cfg.general.log_level == "warning"

    def test_default_dns(self):
        assert self.cfg.general.dns.nameservers == ["8.8.8.8", "1.1.1.1"]

    def test_default_rf_interval(self):
        assert self.cfg.polling.rf_interval_seconds == 1.0

    def test_default_throughput_interval(self):
        assert self.cfg.polling.throughput_interval_seconds == 60.0

    def test_default_gnss_interval(self):
        assert self.cfg.polling.gnss_interval_seconds == 1.0

    def test_default_neighbor_cells_on(self):
        assert self.cfg.polling.neighbor_cells is True

    def test_default_throughput_method_iperf3(self):
        assert self.cfg.throughput.method == "iperf3"

    def test_default_latency_methods(self):
        assert set(self.cfg.latency.methods) == {"icmp", "tcp", "http_head"}

    def test_default_latency_targets(self):
        assert self.cfg.latency.targets == ["8.8.8.8", "1.1.1.1"]

    def test_default_icmp_count(self):
        assert self.cfg.latency.icmp_count == 10

    def test_default_tui_enabled(self):
        assert self.cfg.tui.enabled is True

    def test_default_api_disabled(self):
        assert self.cfg.api.enabled is False

    def test_default_gnss_primary(self):
        assert self.cfg.gnss.primary_modem == ""

    def test_default_movement_threshold(self):
        assert self.cfg.gnss.movement_threshold_meters == 5.0

    def test_default_interpolate_on_fix_loss(self):
        assert self.cfg.gnss.interpolate_on_fix_loss is True

    def test_default_session_operator_empty(self):
        assert self.cfg.session_defaults.operator == ""


# ---------------------------------------------------------------------------
# Full config round-trip
# ---------------------------------------------------------------------------

class TestFullConfig:
    def setup_method(self):
        self.cfg = load(_write_toml(FULL_TOML))

    def test_log_level(self):
        assert self.cfg.general.log_level == "debug"

    def test_custom_dns(self):
        assert self.cfg.general.dns.nameservers == ["1.1.1.1", "8.8.4.4"]

    def test_session_defaults(self):
        assert self.cfg.session_defaults.operator == "Alice"
        assert self.cfg.session_defaults.vehicle_id == "VH-001"

    def test_gnss_movement_threshold(self):
        assert self.cfg.gnss.movement_threshold_meters == 10.0

    def test_gnss_interpolate_off(self):
        assert self.cfg.gnss.interpolate_on_fix_loss is False

    def test_polling_custom(self):
        assert self.cfg.polling.rf_interval_seconds == 2.0
        assert self.cfg.polling.neighbor_cells is False
        assert self.cfg.polling.throughput_interval_seconds == 120.0

    def test_throughput_method_http(self):
        assert self.cfg.throughput.method == "http"

    def test_latency_methods_icmp_only(self):
        assert self.cfg.latency.methods == ["icmp"]

    def test_api_enabled(self):
        assert self.cfg.api.enabled is True
        assert self.cfg.api.auth == "apikey"
        assert self.cfg.api.api_key == "secret123"

    def test_tui_disabled(self):
        assert self.cfg.tui.enabled is False

    def test_two_modems(self):
        assert len(self.cfg.modems) == 2
        carriers = {m.carrier for m in self.cfg.modems}
        assert carriers == {"att", "tmobile"}


# ---------------------------------------------------------------------------
# Modem config
# ---------------------------------------------------------------------------

class TestModemConfig:
    def test_namespace_derived_from_carrier(self):
        cfg = load(_write_toml(MINIMAL_TOML))
        modem = cfg.modems[0]
        assert modem.namespace == "ns_att"

    def test_carrier_lowercased(self):
        toml = """\
            [[modems]]
            imei = "860123456789001"
            carrier = "ATT"
            label = "AT&T"
            apn = "broadband"
        """
        cfg = load(_write_toml(toml))
        assert cfg.modems[0].carrier == "att"
        assert cfg.modems[0].namespace == "ns_att"

    def test_runtime_fields_empty_by_default(self):
        cfg = load(_write_toml(MINIMAL_TOML))
        m = cfg.modems[0]
        assert m.at_port == ""
        assert m.net_interface == ""
        assert m.mm_index == ""

    def test_imei_stored_as_string(self):
        cfg = load(_write_toml(MINIMAL_TOML))
        assert cfg.modems[0].imei == "860123456789001"


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------

class TestValidationErrors:
    def test_zero_modems_loads_ok(self):
        cfg = load(_write_toml("[general]\nlog_level = 'info'\n"))
        assert cfg.modems == []

    def test_invalid_imei_too_short(self):
        toml = """\
            [[modems]]
            imei = "12345"
            carrier = "att"
            label = "AT&T"
            apn = "broadband"
        """
        with pytest.raises(ValueError, match="IMEI"):
            load(_write_toml(toml))

    def test_invalid_imei_not_digits(self):
        toml = """\
            [[modems]]
            imei = "86012345678900X"
            carrier = "att"
            label = "AT&T"
            apn = "broadband"
        """
        with pytest.raises(ValueError, match="IMEI"):
            load(_write_toml(toml))

    def test_duplicate_imei_raises(self):
        toml = """\
            [[modems]]
            imei = "860123456789001"
            carrier = "att"
            label = "AT&T"
            apn = "broadband"

            [[modems]]
            imei = "860123456789001"
            carrier = "tmobile"
            label = "T-Mobile"
            apn = "fast.t-mobile.com"
        """
        with pytest.raises(ValueError, match="Duplicate modem IMEI"):
            load(_write_toml(toml))

    def test_duplicate_carrier_raises(self):
        toml = """\
            [[modems]]
            imei = "860123456789001"
            carrier = "att"
            label = "AT&T"
            apn = "broadband"

            [[modems]]
            imei = "860123456789002"
            carrier = "att"
            label = "AT&T again"
            apn = "broadband"
        """
        with pytest.raises(ValueError, match="Duplicate modem carrier"):
            load(_write_toml(toml))

    def test_missing_required_modem_field_raises(self):
        toml = """\
            [[modems]]
            imei = "860123456789001"
            carrier = "att"
            label = "AT&T"
        """
        with pytest.raises(ValueError, match="apn"):
            load(_write_toml(toml))

    def test_rf_interval_too_small_raises(self):
        toml = MINIMAL_TOML + "\n[polling]\nrf_interval_seconds = 0.05\n"
        with pytest.raises(ValueError, match="rf_interval_seconds"):
            load(_write_toml(toml))

    def test_rf_interval_at_minimum_ok(self):
        toml = MINIMAL_TOML + "\n[polling]\nrf_interval_seconds = 0.1\n"
        cfg = load(_write_toml(toml))
        assert cfg.polling.rf_interval_seconds == 0.1

    def test_invalid_throughput_method_raises(self):
        toml = MINIMAL_TOML + "\n[throughput]\nmethod = 'ftp'\n"
        with pytest.raises(ValueError, match="throughput.method"):
            load(_write_toml(toml))

    def test_invalid_latency_method_raises(self):
        toml = MINIMAL_TOML + '\n[latency]\nmethods = ["icmp", "udp"]\n'
        with pytest.raises(ValueError, match="unknown method"):
            load(_write_toml(toml))

    def test_invalid_api_auth_raises(self):
        toml = MINIMAL_TOML + "\n[api]\nauth = 'oauth'\n"
        with pytest.raises(ValueError, match="api.auth"):
            load(_write_toml(toml))

    def test_nine_modems_raises(self):
        blocks = ""
        for i in range(9):
            blocks += f"""
[[modems]]
imei = "86012345678{i:04d}"
carrier = "carrier{i}"
label = "Carrier {i}"
apn = "apn{i}.example.com"
"""
        with pytest.raises(ValueError, match="at most 8"):
            load(_write_toml(blocks))

    def test_gnss_primary_not_in_modems_raises(self):
        toml = MINIMAL_TOML + "\n[gnss]\nprimary_modem = 'sprint'\n"
        with pytest.raises(ValueError, match="gnss.primary_modem"):
            load(_write_toml(toml))

    def test_gnss_failover_not_in_modems_raises(self):
        toml = MINIMAL_TOML + '\n[gnss]\nfailover_order = ["sprint"]\n'
        with pytest.raises(ValueError, match="gnss.failover_order"):
            load(_write_toml(toml))

    def test_gnss_empty_primary_valid(self):
        toml = MINIMAL_TOML + '\n[gnss]\nprimary_modem = ""\n'
        cfg = load(_write_toml(toml))
        assert cfg.gnss.primary_modem == ""

    def test_gnss_defaults_no_hardcoded_carriers(self):
        g = GnssConfig()
        assert g.primary_modem == ""
        assert g.failover_order == []

"""Tests for fivegbench.preflight — non-hardware checks and 0-modem paths.

Hardware-dependent checks (modem detection, AT ports, SIM, etc.) are not
tested here — they require real devices.  We test:
  - check_config: pure logic against cfg.modems
  - check_disk_space: filesystem query (uses tmp_path)
  - check_gnss: early-return paths that don't touch hardware
  - run_preflight with skip_hardware=True: integration of the above
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from fivegbench.config import (
    Config,
    GeneralConfig,
    GnssConfig,
    ModemConfig,
)
from fivegbench.preflight import (
    check_config,
    check_disk_space,
    check_gnss,
    run_preflight,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_modem(carrier: str, idx: int = 1) -> ModemConfig:
    return ModemConfig(
        imei=f"86000000000{idx:04d}",
        carrier=carrier,
        label=carrier.upper(),
        apn=f"{carrier}.apn",
        namespace=f"ns_{carrier}",
    )


def _make_cfg(num_modems: int = 1, primary_modem: str = "") -> Config:
    carriers = ["att", "tmobile", "verizon", "c3", "c4", "c5", "c6", "c7"]
    cfg = Config()
    cfg.modems = [_make_modem(carriers[i], i + 1) for i in range(num_modems)]
    cfg.gnss = GnssConfig(primary_modem=primary_modem)
    return cfg


# ---------------------------------------------------------------------------
# check_config
# ---------------------------------------------------------------------------

class TestCheckConfig:
    @pytest.mark.asyncio
    async def test_zero_modems_returns_warn(self):
        cfg = _make_cfg(0)
        ok, detail = await check_config(cfg)
        assert ok is None
        assert "test mode" in detail.lower() or "no-hardware" in detail.lower()

    @pytest.mark.asyncio
    async def test_one_modem_passes(self):
        cfg = _make_cfg(1)
        ok, detail = await check_config(cfg)
        assert ok is True
        assert "1" in detail

    @pytest.mark.asyncio
    async def test_three_modems_passes(self):
        cfg = _make_cfg(3)
        ok, detail = await check_config(cfg)
        assert ok is True
        assert "3" in detail

    @pytest.mark.asyncio
    async def test_modem_missing_imei_fails(self):
        cfg = _make_cfg(1)
        cfg.modems[0].imei = ""
        ok, detail = await check_config(cfg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_modem_missing_carrier_fails(self):
        cfg = _make_cfg(1)
        cfg.modems[0].carrier = ""
        ok, detail = await check_config(cfg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_modem_missing_apn_fails(self):
        cfg = _make_cfg(1)
        cfg.modems[0].apn = ""
        ok, detail = await check_config(cfg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_return_type_is_tuple(self):
        cfg = _make_cfg(1)
        result = await check_config(cfg)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# check_disk_space
# ---------------------------------------------------------------------------

class TestCheckDiskSpace:
    @pytest.mark.asyncio
    async def test_existing_path_returns_result(self, tmp_path):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        ok, detail = await check_disk_space(cfg)
        # Pass or warn — depends on available space; either way not False
        assert ok is True or ok is None

    @pytest.mark.asyncio
    async def test_nonexistent_path_fails(self):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=Path("/nonexistent/xyz/abc"))
        ok, detail = await check_disk_space(cfg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_detail_contains_gb(self, tmp_path):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        ok, detail = await check_disk_space(cfg)
        if ok is not False:
            assert "GB" in detail


# ---------------------------------------------------------------------------
# check_gnss — early-return paths only (no hardware)
# ---------------------------------------------------------------------------

class TestCheckGnssEarlyReturn:
    @pytest.mark.asyncio
    async def test_zero_modems_returns_true_skipped(self):
        cfg = _make_cfg(0)
        ok, detail = await check_gnss(cfg)
        assert ok is True
        assert "skipped" in detail.lower()

    @pytest.mark.asyncio
    async def test_primary_modem_not_in_list_returns_false(self):
        """When primary_modem is set but doesn't match any modem, fail before hardware access."""
        cfg = _make_cfg(1)  # only "att"
        cfg.gnss = GnssConfig(primary_modem="verizon")
        ok, detail = await check_gnss(cfg)
        assert ok is False
        assert "verizon" in detail

    @pytest.mark.asyncio
    async def test_primary_modem_not_found_message_includes_carrier(self):
        cfg = _make_cfg(2)  # att + tmobile
        cfg.gnss = GnssConfig(primary_modem="sprint")
        ok, detail = await check_gnss(cfg)
        assert ok is False
        assert "sprint" in detail


# ---------------------------------------------------------------------------
# run_preflight — skip_hardware=True
# ---------------------------------------------------------------------------

class TestRunPreflight:
    @pytest.mark.asyncio
    async def test_skip_hardware_completes(self, tmp_path, capsys):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        result = await run_preflight(cfg, skip_hardware=True)
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_output_contains_preflight_header(self, tmp_path, capsys):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        await run_preflight(cfg, skip_hardware=True)
        out = capsys.readouterr().out
        assert "preflight" in out.lower()

    @pytest.mark.asyncio
    async def test_zero_modems_produces_warn_output(self, tmp_path, capsys):
        cfg = _make_cfg(0)
        cfg.general = GeneralConfig(db_path=tmp_path)
        await run_preflight(cfg, skip_hardware=True)
        out = capsys.readouterr().out
        # The warn icon or "test mode" text should appear
        assert "WARN" in out or "test mode" in out.lower() or "no-hardware" in out.lower()

    @pytest.mark.asyncio
    async def test_results_summary_printed(self, tmp_path, capsys):
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        await run_preflight(cfg, skip_hardware=True)
        out = capsys.readouterr().out
        assert "Results:" in out

    @pytest.mark.asyncio
    async def test_one_modem_skip_hardware_passes(self, tmp_path):
        """With 1 modem, skip_hardware, and mocked system tools — all checks pass."""
        from unittest.mock import patch, AsyncMock
        cfg = _make_cfg(1)
        cfg.general = GeneralConfig(db_path=tmp_path)
        with patch("fivegbench.preflight.check_system_tools", return_value=(True, "mocked")):
            result = await run_preflight(cfg, skip_hardware=True)
        assert result is True

    @pytest.mark.asyncio
    async def test_zero_modems_skip_hardware_still_passes(self, tmp_path):
        """0 modems in test mode: WARN is counted as pass in run_preflight."""
        from unittest.mock import patch
        cfg = _make_cfg(0)
        cfg.general = GeneralConfig(db_path=tmp_path)
        with patch("fivegbench.preflight.check_system_tools", return_value=(True, "mocked")):
            result = await run_preflight(cfg, skip_hardware=True)
        assert result is True

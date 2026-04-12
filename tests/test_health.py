"""Tests for fivegbench.modem.health — ModemHealthMonitor state machine.

All external I/O (mmcli, namespace ops, bus publish) is mocked.
No hardware required.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fivegbench.modem.health import (
    CarrierHealth,
    ModemHealthMonitor,
    POLL_INTERVAL,
    RECONNECT_HOLDDOWN,
)
from fivegbench.modem.manager import ModemInfo, ModemState


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_modem_config(carrier: str = "att", imei: str = "123456789012345"):
    """Return a minimal mock ModemConfig."""
    m = MagicMock()
    m.carrier = carrier
    m.imei = imei
    m.apn = f"{carrier}.example.com"
    m.namespace = f"ns_{carrier}"
    m.net_interface = ""
    m.mm_index = ""
    return m


def _make_modem_info(
    mm_index: str = "0",
    imei: str = "123456789012345",
    state: ModemState = ModemState.CONNECTED,
    bearer_interface: str = "wwan0",
    ip_address: str = "192.168.1.100/24",
    gateway: str = "192.168.1.1",
) -> ModemInfo:
    return ModemInfo(
        mm_index=mm_index,
        imei=imei,
        state=state,
        primary_port="ttyUSB2",
        bearer_interface=bearer_interface,
        ip_address=ip_address,
        gateway=gateway,
    )


def _make_monitor(modems=None) -> tuple[ModemHealthMonitor, MagicMock, MagicMock]:
    """
    Build a ModemHealthMonitor wired to mock bus, session, and RF collectors.

    Returns (monitor, bus_mock, rf_mock_map).
    """
    if modems is None:
        modems = [_make_modem_config("att")]

    cfg = MagicMock()
    cfg.modems = modems
    cfg.general.dns.nameservers = ["8.8.8.8"]

    bus = MagicMock()
    bus.publish = AsyncMock()

    session = MagicMock()
    session.session_id = "20260410_143022"

    # Build one RF collector mock per modem
    rf_collectors = []
    rf_map = {}
    for m in modems:
        rf = MagicMock()
        rf.carrier = m.carrier
        rf.pause = MagicMock()
        rf.resume = MagicMock()
        rf_collectors.append(rf)
        rf_map[m.carrier] = rf

    monitor = ModemHealthMonitor(cfg, bus, session, rf_collectors)
    return monitor, bus, rf_map


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

class TestInitialState:
    def test_all_modems_start_unknown(self):
        modems = [_make_modem_config("att"), _make_modem_config("tmobile", "999999999999999")]
        monitor, _, _ = _make_monitor(modems)
        for m in modems:
            assert monitor.health_state(m.carrier) == CarrierHealth.UNKNOWN

    def test_is_healthy_false_when_unknown(self):
        monitor, _, _ = _make_monitor()
        assert monitor.is_healthy("att") is False

    def test_all_health_returns_all_carriers(self):
        modems = [_make_modem_config("att"), _make_modem_config("tmobile", "999999999999999")]
        monitor, _, _ = _make_monitor(modems)
        h = monitor.all_health()
        assert "att" in h
        assert "tmobile" in h

    def test_is_healthy_for_unknown_carrier_false(self):
        monitor, _, _ = _make_monitor()
        assert monitor.is_healthy("verizon") is False

    def test_health_state_for_unknown_carrier(self):
        monitor, _, _ = _make_monitor()
        assert monitor.health_state("verizon") == CarrierHealth.UNKNOWN


# ---------------------------------------------------------------------------
# _on_disconnect
# ---------------------------------------------------------------------------

class TestOnDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_sets_disconnected_state(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock) as td:
            td.return_value = True
            await monitor._on_disconnect(modem)
        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_disconnect_pauses_rf_collector(self):
        monitor, _, rf_map = _make_monitor()
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock):
            await monitor._on_disconnect(modem)
        rf_map["att"].pause.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_tears_down_namespace(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock) as td:
            td.return_value = True
            await monitor._on_disconnect(modem)
        td.assert_called_once_with(modem.namespace)

    @pytest.mark.asyncio
    async def test_disconnect_publishes_event(self):
        monitor, bus, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock):
            await monitor._on_disconnect(modem)
        bus.publish.assert_called_once()
        msg = bus.publish.call_args[0][0]
        assert msg["type"] == "modem_event"
        assert msg["data"]["event_type"] == "disconnect"
        assert msg["carrier"] == "att"

    @pytest.mark.asyncio
    async def test_disconnect_no_rf_collector_does_not_crash(self):
        """If a carrier has no RF collector, disconnect should still work."""
        monitor, _, _ = _make_monitor()
        monitor._rf_map.pop("att", None)  # remove collector
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock):
            await monitor._on_disconnect(modem)  # must not raise
        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_disconnect_skips_publish_when_no_session(self):
        monitor, bus, _ = _make_monitor()
        monitor._session.session_id = None  # no active session
        modem = monitor._cfg.modems[0]
        with patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock):
            await monitor._on_disconnect(modem)
        bus.publish.assert_not_called()


# ---------------------------------------------------------------------------
# _on_reconnect
# ---------------------------------------------------------------------------

class TestOnReconnect:
    @pytest.mark.asyncio
    async def test_reconnect_sets_healthy_on_success(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        assert monitor.health_state("att") == CarrierHealth.HEALTHY

    @pytest.mark.asyncio
    async def test_reconnect_resumes_rf_collector(self):
        monitor, _, rf_map = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        rf_map["att"].resume.assert_called_once()

    @pytest.mark.asyncio
    async def test_reconnect_publishes_reconnect_event(self):
        monitor, bus, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED  # real recovery

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        bus.publish.assert_called_once()
        msg = bus.publish.call_args[0][0]
        assert msg["data"]["event_type"] == "reconnect"

    @pytest.mark.asyncio
    async def test_reconnect_from_unknown_does_not_publish(self):
        """First-time setup (UNKNOWN→HEALTHY) should not publish a reconnect event."""
        monitor, bus, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        # default is UNKNOWN — first-time baseline, no event expected

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        bus.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconnect_namespace_failure_stays_disconnected(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = False  # namespace setup fails
            await monitor._on_reconnect(modem, "0")

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_reconnect_no_bearer_info_stays_disconnected(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = None  # MM gives no bearer info yet
            await monitor._on_reconnect(modem, "0")

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_reconnect_empty_bearer_interface_stays_disconnected(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info(bearer_interface="")  # no interface
            await monitor._on_reconnect(modem, "0")

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_reconnect_calls_connect_when_not_connected(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.connect_modem", new_callable=AsyncMock) as cm,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.REGISTERED  # not yet connected
            cm.return_value = True
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        cm.assert_called_once_with("0", modem.apn)
        assert monitor.health_state("att") == CarrierHealth.HEALTHY

    @pytest.mark.asyncio
    async def test_reconnect_connect_failure_stays_disconnected(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.connect_modem", new_callable=AsyncMock) as cm,
        ):
            gms.return_value = ModemState.REGISTERED
            cm.return_value = False  # MM connect fails
            await monitor._on_reconnect(modem, "0")

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_reconnect_updates_net_interface(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED
        modem.net_interface = "old_wwan0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info(bearer_interface="new_wwan1")
            sn.return_value = True
            await monitor._on_reconnect(modem, "0")

        assert modem.net_interface == "new_wwan1"


# ---------------------------------------------------------------------------
# _check_modem — state machine transitions
# ---------------------------------------------------------------------------

class TestCheckModem:
    @pytest.mark.asyncio
    async def test_unknown_connected_becomes_healthy(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._mm_index["att"] = "0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.HEALTHY

    @pytest.mark.asyncio
    async def test_healthy_stays_healthy_when_connected(self):
        monitor, bus, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.HEALTHY
        monitor._mm_index["att"] = "0"

        with patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms:
            gms.return_value = ModemState.CONNECTED
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.HEALTHY
        bus.publish.assert_not_called()  # no events for steady state

    @pytest.mark.asyncio
    async def test_healthy_to_disconnected_on_down_state(self):
        monitor, _, rf_map = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.HEALTHY
        monitor._mm_index["att"] = "0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock),
        ):
            gms.return_value = ModemState.DISCONNECTED
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED
        rf_map["att"].pause.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnected_recovers_to_healthy(self):
        monitor, _, rf_map = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.DISCONNECTED
        monitor._mm_index["att"] = "0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.HEALTHY
        rf_map["att"].resume.assert_called_once()

    @pytest.mark.asyncio
    async def test_mm_not_found_triggers_disconnect(self):
        """When mmcli can't find the modem at all, treat it as disconnected."""
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.HEALTHY

        with (
            patch("fivegbench.modem.health.find_modem_by_imei", new_callable=AsyncMock) as fmbi,
            patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock),
        ):
            fmbi.return_value = None  # modem vanished from MM
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_failed_state_triggers_disconnect(self):
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.HEALTHY
        monitor._mm_index["att"] = "0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock),
        ):
            gms.return_value = ModemState.FAILED
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_transient_state_no_action(self):
        """CONNECTING / REGISTERED (transient) — health should not change."""
        monitor, bus, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.HEALTHY
        monitor._mm_index["att"] = "0"

        with patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms:
            gms.return_value = ModemState.CONNECTING
            await monitor._check_modem(modem)

        # State unchanged, no events
        assert monitor.health_state("att") == CarrierHealth.HEALTHY
        bus.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconnecting_lost_again_becomes_disconnected(self):
        """If modem goes down while in RECONNECTING state, revert to DISCONNECTED."""
        monitor, _, _ = _make_monitor()
        modem = monitor._cfg.modems[0]
        monitor._health["att"] = CarrierHealth.RECONNECTING
        monitor._mm_index["att"] = "0"

        with (
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
        ):
            gms.return_value = ModemState.DISCONNECTED
            await monitor._check_modem(modem)

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    @pytest.mark.asyncio
    async def test_exception_in_check_does_not_crash_run_loop(self):
        """run() catches per-modem errors and continues the poll loop."""
        monitor, _, _ = _make_monitor()
        monitor._mm_index["att"] = "0"

        call_count = 0

        async def _exploding_check(modem):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("mmcli exploded")
            # Second call: stop the loop cleanly
            monitor._running = False

        with (
            patch("fivegbench.modem.health.find_modem_by_imei", new_callable=AsyncMock) as fmbi,
            patch.object(monitor, "_check_modem", side_effect=_exploding_check),
            patch("fivegbench.modem.health.asyncio.sleep", new_callable=AsyncMock),
        ):
            fmbi.return_value = None
            await monitor.run()  # must not raise

        assert call_count >= 2  # loop continued after the exception


# ---------------------------------------------------------------------------
# is_healthy / health_state
# ---------------------------------------------------------------------------

class TestPublicApi:
    def test_is_healthy_true_only_for_healthy(self):
        monitor, _, _ = _make_monitor()
        monitor._health["att"] = CarrierHealth.HEALTHY
        assert monitor.is_healthy("att") is True

    def test_is_healthy_false_for_disconnected(self):
        monitor, _, _ = _make_monitor()
        monitor._health["att"] = CarrierHealth.DISCONNECTED
        assert monitor.is_healthy("att") is False

    def test_is_healthy_false_for_reconnecting(self):
        monitor, _, _ = _make_monitor()
        monitor._health["att"] = CarrierHealth.RECONNECTING
        assert monitor.is_healthy("att") is False

    def test_health_state_returns_enum(self):
        monitor, _, _ = _make_monitor()
        monitor._health["att"] = CarrierHealth.DISCONNECTED
        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED

    def test_all_health_returns_copy(self):
        monitor, _, _ = _make_monitor()
        h = monitor.all_health()
        h["att"] = CarrierHealth.HEALTHY  # mutate returned copy
        # Original must be unchanged
        assert monitor.health_state("att") == CarrierHealth.UNKNOWN

    def test_stop_sets_running_false(self):
        monitor, _, _ = _make_monitor()
        monitor._running = True
        monitor.stop()
        assert monitor._running is False


# ---------------------------------------------------------------------------
# Multi-carrier
# ---------------------------------------------------------------------------

class TestMultiCarrier:
    @pytest.mark.asyncio
    async def test_two_carriers_independent(self):
        """ATT disconnect should not affect T-Mobile health."""
        att = _make_modem_config("att", "111111111111111")
        tmob = _make_modem_config("tmobile", "222222222222222")
        monitor, _, rf_map = _make_monitor([att, tmob])

        monitor._health["att"] = CarrierHealth.HEALTHY
        monitor._health["tmobile"] = CarrierHealth.HEALTHY
        monitor._mm_index["att"] = "0"
        monitor._mm_index["tmobile"] = "1"

        async def _mock_state(idx: str) -> ModemState:
            return ModemState.DISCONNECTED if idx == "0" else ModemState.CONNECTED

        with (
            patch("fivegbench.modem.health.get_modem_state", side_effect=_mock_state),
            patch("fivegbench.modem.health.teardown_namespace", new_callable=AsyncMock),
        ):
            await monitor._check_modem(att)
            # T-Mobile still healthy; do not check it here to avoid reconnect side-effects

        assert monitor.health_state("att") == CarrierHealth.DISCONNECTED
        assert monitor.health_state("tmobile") == CarrierHealth.HEALTHY

    @pytest.mark.asyncio
    async def test_all_three_carriers_independent(self):
        attm = _make_modem_config("att", "111111111111111")
        tmobm = _make_modem_config("tmobile", "222222222222222")
        vzwm = _make_modem_config("verizon", "333333333333333")
        monitor, _, _ = _make_monitor([attm, tmobm, vzwm])
        h = monitor.all_health()
        assert set(h.keys()) == {"att", "tmobile", "verizon"}
        assert all(v == CarrierHealth.UNKNOWN for v in h.values())


# ---------------------------------------------------------------------------
# run() lifecycle
# ---------------------------------------------------------------------------

class TestRunLifecycle:
    @pytest.mark.asyncio
    async def test_run_cancels_cleanly(self):
        monitor, _, _ = _make_monitor()
        with (
            patch("fivegbench.modem.health.find_modem_by_imei", new_callable=AsyncMock) as fmbi,
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            fmbi.return_value = "0"
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True

            task = asyncio.create_task(monitor.run())
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert monitor._running is False

    @pytest.mark.asyncio
    async def test_run_sets_running_true(self):
        monitor, _, _ = _make_monitor()
        with (
            patch("fivegbench.modem.health.find_modem_by_imei", new_callable=AsyncMock) as fmbi,
            patch("fivegbench.modem.health.get_modem_state", new_callable=AsyncMock) as gms,
            patch("fivegbench.modem.health.build_modem_info", new_callable=AsyncMock) as bmi,
            patch("fivegbench.modem.health.setup_namespace", new_callable=AsyncMock) as sn,
        ):
            fmbi.return_value = "0"
            gms.return_value = ModemState.CONNECTED
            bmi.return_value = _make_modem_info()
            sn.return_value = True

            task = asyncio.create_task(monitor.run())
            await asyncio.sleep(0.02)
            assert monitor._running is True
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

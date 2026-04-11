"""Tests for fivegbench.session — ID generation, sanitization, and lifecycle."""

import asyncio
import re
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fivegbench.session import (
    SessionManager,
    SessionStatus,
    _sanitize_name,
    generate_session_id,
    now_iso,
)


# ---------------------------------------------------------------------------
# generate_session_id
# ---------------------------------------------------------------------------

class TestGenerateSessionId:
    def test_format_without_name(self):
        sid = generate_session_id()
        # Must match YYYYMMDD_HHMMSS exactly
        assert re.fullmatch(r"\d{8}_\d{6}", sid), f"Got: {sid!r}"

    def test_format_with_name(self):
        sid = generate_session_id("Sierra Ave")
        # Must start with timestamp then underscore-separated sanitized name
        assert re.fullmatch(r"\d{8}_\d{6}_[a-z0-9_]+", sid), f"Got: {sid!r}"

    def test_name_embedded_in_id(self):
        sid = generate_session_id("sierra")
        assert sid.endswith("_sierra")

    def test_empty_name_no_suffix(self):
        sid = generate_session_id("")
        assert re.fullmatch(r"\d{8}_\d{6}", sid)

    def test_whitespace_only_name_no_suffix(self):
        sid = generate_session_id("   ")
        assert re.fullmatch(r"\d{8}_\d{6}", sid)

    def test_two_calls_different_ids(self):
        # Unless called in the same second, IDs differ
        import time
        a = generate_session_id()
        time.sleep(1.01)
        b = generate_session_id()
        assert a != b

    def test_timestamp_is_recent(self):
        before = datetime.now()
        sid = generate_session_id()
        after = datetime.now()
        date_part = sid[:8]
        assert date_part == before.strftime("%Y%m%d") or date_part == after.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# _sanitize_name
# ---------------------------------------------------------------------------

class TestSanitizeName:
    def test_lowercase(self):
        assert _sanitize_name("Sierra") == "sierra"

    def test_spaces_become_underscores(self):
        assert _sanitize_name("Sierra Ave") == "sierra_ave"

    def test_multiple_spaces_collapse(self):
        assert _sanitize_name("a  b") == "a_b"

    def test_special_chars_stripped(self):
        result = _sanitize_name("Route #1 (North)")
        assert re.fullmatch(r"[a-z0-9_]+", result), f"Got: {result!r}"

    def test_leading_trailing_underscores_stripped(self):
        result = _sanitize_name("!hello!")
        assert not result.startswith("_")
        assert not result.endswith("_")

    def test_empty_string(self):
        assert _sanitize_name("") == ""

    def test_only_special_chars(self):
        assert _sanitize_name("!@#$") == ""

    def test_length_capped_at_64(self):
        long_name = "a" * 200
        result = _sanitize_name(long_name)
        assert len(result) <= 64

    def test_numeric_preserved(self):
        assert _sanitize_name("route 66") == "route_66"

    def test_mixed_case_numbers_special(self):
        result = _sanitize_name("Foothill Blvd @ Sierra Ave #2")
        assert "foothill" in result
        assert "sierra" in result
        # No special chars other than underscore
        assert re.fullmatch(r"[a-z0-9_]+", result)


# ---------------------------------------------------------------------------
# now_iso
# ---------------------------------------------------------------------------

class TestNowIso:
    def test_format(self):
        ts = now_iso()
        # ISO 8601 with milliseconds: "2026-04-10T14:30:22.456+00:00" or "-07:00"
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}",
            ts,
        ), f"Got: {ts!r}"

    def test_has_milliseconds(self):
        ts = now_iso()
        # The milliseconds part is after the dot before the timezone
        assert "." in ts

    def test_recent(self):
        before = datetime.now()
        ts = now_iso()
        after = datetime.now()
        # Parse just the date portion
        date_str = ts[:10]
        assert date_str in (before.strftime("%Y-%m-%d"), after.strftime("%Y-%m-%d"))


# ---------------------------------------------------------------------------
# SessionManager lifecycle
# ---------------------------------------------------------------------------

def _make_manager() -> tuple[SessionManager, MagicMock]:
    """Return a SessionManager wired to a mock bus and a mock config."""
    from fivegbench.bus import EventBus

    bus = MagicMock(spec=EventBus)
    bus.publish = AsyncMock()

    cfg = MagicMock()
    cfg.session_defaults.operator = "default_op"
    cfg.session_defaults.vehicle_id = "VH-00"
    cfg.session_defaults.route_description = ""
    cfg.session_defaults.notes = ""
    cfg.general.db_path = MagicMock()
    cfg.general.log_path = MagicMock()
    cfg.general.log_level = "warning"
    cfg.general.dns.nameservers = ["8.8.8.8"]
    cfg.gnss.primary_modem = "att"
    cfg.gnss.failover_order = []
    cfg.gnss.movement_threshold_meters = 5.0
    cfg.gnss.interpolate_on_fix_loss = True
    cfg.polling.rf_interval_seconds = 1.0
    cfg.polling.neighbor_cells = True
    cfg.polling.throughput_interval_seconds = 60.0
    cfg.polling.gnss_interval_seconds = 1.0
    cfg.throughput.method = "iperf3"
    cfg.throughput.iperf3_server = "iperf.example.com"
    cfg.throughput.iperf3_port = 5201
    cfg.throughput.iperf3_duration_seconds = 10
    cfg.latency.methods = ["icmp"]
    cfg.latency.targets = ["8.8.8.8"]
    cfg.latency.icmp_count = 10
    cfg.modems = []

    mgr = SessionManager(bus, cfg)
    return mgr, bus


class TestSessionManagerStart:
    @pytest.mark.asyncio
    async def test_start_creates_session(self):
        mgr, _ = _make_manager()
        session = await mgr.start()
        assert session is not None
        assert session.session_id != ""

    @pytest.mark.asyncio
    async def test_start_sets_active_status(self):
        mgr, _ = _make_manager()
        await mgr.start()
        assert mgr.status == SessionStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_start_publishes_event(self):
        mgr, bus = _make_manager()
        await mgr.start()
        bus.publish.assert_called_once()
        msg = bus.publish.call_args[0][0]
        assert msg["type"] == "session_event"
        assert msg["data"]["event"] == "start"

    @pytest.mark.asyncio
    async def test_start_uses_provided_metadata(self):
        mgr, _ = _make_manager()
        session = await mgr.start(operator="Alice", vehicle_id="VH-42")
        assert session.operator == "Alice"
        assert session.vehicle_id == "VH-42"

    @pytest.mark.asyncio
    async def test_start_uses_config_defaults_when_not_provided(self):
        mgr, _ = _make_manager()
        session = await mgr.start()
        assert session.operator == "default_op"
        assert session.vehicle_id == "VH-00"

    @pytest.mark.asyncio
    async def test_start_generates_valid_session_id(self):
        mgr, _ = _make_manager()
        session = await mgr.start()
        assert re.fullmatch(r"\d{8}_\d{6}", session.session_id)

    @pytest.mark.asyncio
    async def test_start_with_name_suffix(self):
        mgr, _ = _make_manager()
        session = await mgr.start(name_suffix="sierra ave")
        assert "sierra_ave" in session.session_id

    @pytest.mark.asyncio
    async def test_session_id_property(self):
        mgr, _ = _make_manager()
        await mgr.start()
        assert mgr.session_id == mgr.current.session_id

    @pytest.mark.asyncio
    async def test_active_property_when_active(self):
        mgr, _ = _make_manager()
        await mgr.start()
        assert mgr.active is not None

    @pytest.mark.asyncio
    async def test_second_start_stops_first(self):
        mgr, bus = _make_manager()
        first = await mgr.start(operator="First")
        second = await mgr.start(operator="Second")
        # Two publish calls: start of first, stop of first, start of second
        calls = [c[0][0] for c in bus.publish.call_args_list]
        events = [c["data"]["event"] for c in calls if c["type"] == "session_event"]
        assert "stop" in events
        assert second.operator == "Second"


class TestSessionManagerStop:
    @pytest.mark.asyncio
    async def test_stop_sets_stopped_status(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.stop()
        assert mgr.status == SessionStatus.STOPPED

    @pytest.mark.asyncio
    async def test_stop_publishes_event(self):
        mgr, bus = _make_manager()
        await mgr.start()
        bus.publish.reset_mock()
        await mgr.stop()
        bus.publish.assert_called_once()
        msg = bus.publish.call_args[0][0]
        assert msg["data"]["event"] == "stop"

    @pytest.mark.asyncio
    async def test_stop_sets_end_time(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.stop()
        assert mgr.current.end_time is not None

    @pytest.mark.asyncio
    async def test_stop_with_no_session_is_safe(self):
        mgr, _ = _make_manager()
        await mgr.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_active_is_none_after_stop(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.stop()
        assert mgr.active is None

    @pytest.mark.asyncio
    async def test_session_id_still_readable_after_stop(self):
        mgr, _ = _make_manager()
        await mgr.start()
        sid = mgr.session_id
        await mgr.stop()
        assert mgr.session_id == sid


class TestSessionManagerPauseResume:
    @pytest.mark.asyncio
    async def test_pause_sets_paused_status(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.pause()
        assert mgr.status == SessionStatus.PAUSED

    @pytest.mark.asyncio
    async def test_pause_publishes_event(self):
        mgr, bus = _make_manager()
        await mgr.start()
        bus.publish.reset_mock()
        await mgr.pause()
        msg = bus.publish.call_args[0][0]
        assert msg["data"]["event"] == "pause"

    @pytest.mark.asyncio
    async def test_active_is_none_when_paused(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.pause()
        assert mgr.active is None

    @pytest.mark.asyncio
    async def test_resume_sets_active_status(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.pause()
        await mgr.resume()
        assert mgr.status == SessionStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_resume_publishes_event(self):
        mgr, bus = _make_manager()
        await mgr.start()
        await mgr.pause()
        bus.publish.reset_mock()
        await mgr.resume()
        msg = bus.publish.call_args[0][0]
        assert msg["data"]["event"] == "resume"

    @pytest.mark.asyncio
    async def test_pause_without_session_is_safe(self):
        mgr, _ = _make_manager()
        await mgr.pause()  # no session — should not raise

    @pytest.mark.asyncio
    async def test_resume_without_session_is_safe(self):
        mgr, _ = _make_manager()
        await mgr.resume()  # no session — should not raise

    @pytest.mark.asyncio
    async def test_pause_then_stop(self):
        mgr, _ = _make_manager()
        await mgr.start()
        await mgr.pause()
        await mgr.stop()
        assert mgr.status == SessionStatus.STOPPED


class TestSessionManagerIdle:
    def test_session_id_is_none_before_start(self):
        mgr, _ = _make_manager()
        assert mgr.session_id is None

    def test_status_is_idle_before_start(self):
        mgr, _ = _make_manager()
        assert mgr.status == SessionStatus.IDLE

    def test_active_is_none_before_start(self):
        mgr, _ = _make_manager()
        assert mgr.active is None

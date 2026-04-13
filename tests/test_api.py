"""Tests for fivegbench.api.stub — REST endpoints and WebSocket stream.

Uses FastAPI's TestClient (synchronous httpx-backed client) so no running
server or event loop is needed for HTTP tests.  WebSocket tests use
TestClient.websocket_connect().

All external dependencies (SessionManager, health monitor, collectors, bus)
are replaced with lightweight mocks — no hardware required.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from fivegbench.api.stub import AppState, create_app, _db_for_session
from fivegbench.db import SCHEMA_SQL
from fivegbench.modem.health import CarrierHealth
from fivegbench.session import SessionStatus


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_state(db_path: Path | None = None) -> AppState:
    """Build a fully mocked AppState."""
    state = AppState()

    session = MagicMock()
    session.session_id = "20260410_143022"
    session.start_time = "2026-04-10T14:30:22.000+00:00"
    session.end_time = None

    session_mgr = MagicMock()
    session_mgr.current = session
    session_mgr.session_id = "20260410_143022"
    session_mgr.status = SessionStatus.ACTIVE
    session_mgr.start = AsyncMock(return_value=session)
    session_mgr.stop = AsyncMock()
    session_mgr.pause = AsyncMock()
    session_mgr.resume = AsyncMock()

    health = MagicMock()
    health.all_health.return_value = {
        "att": CarrierHealth.HEALTHY,
        "tmobile": CarrierHealth.HEALTHY,
        "verizon": CarrierHealth.DISCONNECTED,
    }
    health.health_state.side_effect = lambda c: {
        "att": CarrierHealth.HEALTHY,
        "tmobile": CarrierHealth.HEALTHY,
        "verizon": CarrierHealth.DISCONNECTED,
    }.get(c, CarrierHealth.UNKNOWN)

    tp = MagicMock()
    tp.trigger_now = MagicMock()

    lat = MagicMock()
    lat.trigger_now = MagicMock()

    cfg = MagicMock()
    cfg.api.auth = "none"
    cfg.api.api_key = ""
    cfg.api.enabled = True
    cfg.api.host = "127.0.0.1"
    cfg.api.port = 8080
    cfg.polling.rf_interval_seconds = 1.0
    cfg.polling.throughput_interval_seconds = 60.0
    cfg.polling.gnss_interval_seconds = 1.0
    cfg.polling.neighbor_cells = True
    cfg.throughput.method = "iperf3"
    cfg.throughput.iperf3_server = "iperf.example.com"
    cfg.throughput.iperf3_port = 5201
    cfg.throughput.iperf3_duration_seconds = 10
    cfg.latency.methods = ["icmp"]
    cfg.latency.targets = ["8.8.8.8"]

    # Modems
    att = MagicMock()
    att.carrier = "att"
    att.label = "AT&T"
    att.imei = "111111111111111"
    att.at_port = "/dev/ttyUSB2"
    att.net_interface = "wwan0"
    att.namespace = "ns_att"

    tmob = MagicMock()
    tmob.carrier = "tmobile"
    tmob.label = "T-Mobile"
    tmob.imei = "222222222222222"
    tmob.at_port = "/dev/ttyUSB5"
    tmob.net_interface = "wwan1"
    tmob.namespace = "ns_tmobile"

    vzw = MagicMock()
    vzw.carrier = "verizon"
    vzw.label = "Verizon"
    vzw.imei = "333333333333333"
    vzw.at_port = ""
    vzw.net_interface = ""
    vzw.namespace = "ns_verizon"

    cfg.modems = [att, tmob, vzw]

    state.attach(
        session_mgr=session_mgr,
        health_monitor=health,
        throughput_collector=tp,
        latency_collector=lat,
        bus=MagicMock(),
        cfg=cfg,
    )
    if db_path:
        state.db_path = db_path

    # Pre-seed live state
    state._rf["att"] = {"rsrp": -85.0, "access_technology": "LTE"}
    state._rf["tmobile"] = {"rsrp": -92.0, "access_technology": "NR5G-NSA"}
    state._rf["verizon"] = {}
    state._gnss = {
        "fix_type": "3d",
        "latitude": 34.055662,
        "longitude": -117.832028,
    }

    return state


@pytest.fixture
def state():
    return _make_state()


@pytest.fixture
def client(state):
    app = create_app(state)
    return TestClient(app)


def _create_test_db(db_path: Path, session_id: str = "20260410_143022") -> None:
    """Populate a minimal test database for history endpoint tests."""
    date_str = session_id[:8]
    db_file = db_path / f"5gbench_{date_str}.db"
    db_path.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_file))
    con.executescript(SCHEMA_SQL)
    con.execute(
        "INSERT INTO sessions VALUES (?, ?, NULL, 'Alice', 'VH-42', '', '', NULL)",
        (session_id, "2026-04-10T14:30:22.000+00:00"),
    )
    con.execute(
        """INSERT INTO rf_telemetry
           (session_id, timestamp, modem_imei, carrier, access_technology,
            band, earfcn, pci, cell_id, rsrp, rsrq, sinr, rssi,
            registered_network, raw_response)
           VALUES (?, '2026-04-10T14:30:23+00:00', '111111111111111',
                   'att', 'LTE', 'B66', 66786, 42, '0x1', -85, -10, 18, -65, 'LTE', '')""",
        (session_id,),
    )
    con.execute(
        """INSERT INTO throughput_results
           (session_id, timestamp, modem_imei, carrier, method, direction,
            bandwidth_bps, retransmits, duration_seconds, server, raw_result)
           VALUES (?, '2026-04-10T14:31:00+00:00', '111111111111111',
                   'att', 'iperf3', 'download', 500000000, 0, 10.0,
                   'iperf.example.com', '{}')""",
        (session_id,),
    )
    con.execute(
        """INSERT INTO latency_results
           (session_id, timestamp, modem_imei, carrier, method, target,
            min_ms, avg_ms, max_ms, stddev_ms, packet_loss_pct, ttfb_ms, raw_result)
           VALUES (?, '2026-04-10T14:31:10+00:00', '111111111111111',
                   'att', 'icmp', '8.8.8.8',
                   12.0, 15.0, 22.0, 2.1, 0.0, NULL, '{}')""",
        (session_id,),
    )
    con.commit()
    con.close()


@pytest.fixture
def db_state(tmp_path):
    _create_test_db(tmp_path)
    s = _make_state(db_path=tmp_path)
    return s, tmp_path


@pytest.fixture
def db_client(db_state):
    s, _ = db_state
    app = create_app(s)
    return TestClient(app), s


# ---------------------------------------------------------------------------
# GET /v1/status
# ---------------------------------------------------------------------------

class TestStatus:
    def test_returns_200(self, client):
        r = client.get("/v1/status")
        assert r.status_code == 200

    def test_has_session_id(self, client):
        r = client.get("/v1/status")
        assert r.json()["session_id"] == "20260410_143022"

    def test_has_session_status(self, client):
        r = client.get("/v1/status")
        assert r.json()["session_status"] == "active"

    def test_modem_health_present(self, client):
        r = client.get("/v1/status")
        health = r.json()["modem_health"]
        assert health["att"] == "healthy"
        assert health["verizon"] == "disconnected"

    def test_gnss_present(self, client):
        r = client.get("/v1/status")
        gnss = r.json()["gnss"]
        assert gnss["fix_type"] == "3d"

    def test_no_session_mgr_still_200(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.get("/v1/status")
        assert r.status_code == 200
        assert r.json()["session_id"] is None


# ---------------------------------------------------------------------------
# GET /v1/modems
# ---------------------------------------------------------------------------

class TestModems:
    def test_returns_list(self, client):
        r = client.get("/v1/modems")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_three_modems_returned(self, client):
        r = client.get("/v1/modems")
        assert len(r.json()) == 3

    def test_modem_has_expected_fields(self, client):
        r = client.get("/v1/modems")
        m = r.json()[0]
        for field in ("carrier", "label", "imei", "health", "last_rf"):
            assert field in m

    def test_att_health_healthy(self, client):
        r = client.get("/v1/modems")
        att = next(m for m in r.json() if m["carrier"] == "att")
        assert att["health"] == "healthy"

    def test_verizon_health_disconnected(self, client):
        r = client.get("/v1/modems")
        vzw = next(m for m in r.json() if m["carrier"] == "verizon")
        assert vzw["health"] == "disconnected"

    def test_last_rf_included(self, client):
        r = client.get("/v1/modems")
        att = next(m for m in r.json() if m["carrier"] == "att")
        assert att["last_rf"]["rsrp"] == -85.0

    def test_no_config_returns_empty(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.get("/v1/modems")
        assert r.json() == []


# ---------------------------------------------------------------------------
# GET /v1/modems/{carrier}/rf
# ---------------------------------------------------------------------------

class TestModemRF:
    def test_known_carrier_200(self, client):
        r = client.get("/v1/modems/att/rf")
        assert r.status_code == 200

    def test_rf_data_returned(self, client):
        r = client.get("/v1/modems/att/rf")
        assert r.json()["rsrp"] == -85.0

    def test_unknown_carrier_404(self, client):
        r = client.get("/v1/modems/sprint/rf")
        assert r.status_code == 404

    def test_empty_rf_dict_returns_200(self, client):
        r = client.get("/v1/modems/verizon/rf")
        assert r.status_code == 200
        assert r.json() == {}


# ---------------------------------------------------------------------------
# GET /v1/gnss
# ---------------------------------------------------------------------------

class TestGNSS:
    def test_returns_200_with_fix(self, client):
        r = client.get("/v1/gnss")
        assert r.status_code == 200

    def test_fix_type_present(self, client):
        r = client.get("/v1/gnss")
        assert r.json()["fix_type"] == "3d"

    def test_coordinates_present(self, client):
        r = client.get("/v1/gnss")
        d = r.json()
        assert abs(d["latitude"] - 34.055662) < 1e-4
        assert abs(d["longitude"] - (-117.832028)) < 1e-4

    def test_503_when_no_gnss(self):
        s = _make_state()
        s._gnss = {}
        app = create_app(s)
        c = TestClient(app)
        r = c.get("/v1/gnss")
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# GET /v1/sessions and GET /v1/sessions/{id}
# ---------------------------------------------------------------------------

class TestSessions:
    def test_list_returns_list(self, db_client):
        c, _ = db_client
        r = c.get("/v1/sessions")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_session_found(self, db_client):
        c, _ = db_client
        r = c.get("/v1/sessions")
        ids = [s["session_id"] for s in r.json()]
        assert "20260410_143022" in ids

    def test_get_session_by_id(self, db_client):
        c, _ = db_client
        r = c.get("/v1/sessions/20260410_143022")
        assert r.status_code == 200
        assert r.json()["session_id"] == "20260410_143022"
        assert r.json()["operator"] == "Alice"

    def test_get_session_not_found_404(self, db_client):
        c, _ = db_client
        r = c.get("/v1/sessions/99991231_000000")
        assert r.status_code == 404

    def test_no_db_returns_empty_list(self, client):
        r = client.get("/v1/sessions")
        assert r.status_code == 200
        assert r.json() == []


# ---------------------------------------------------------------------------
# POST /v1/sessions/start, stop, pause, resume
# ---------------------------------------------------------------------------

class TestSessionLifecycle:
    def test_start_returns_session_id(self, client, state):
        r = client.post("/v1/sessions/start", json={})
        assert r.status_code == 200
        assert r.json()["session_id"] == "20260410_143022"

    def test_start_calls_session_mgr(self, client, state):
        client.post("/v1/sessions/start", json={"operator": "Bob"})
        state.session_mgr.start.assert_called_once()
        call_kwargs = state.session_mgr.start.call_args[1]
        assert call_kwargs.get("operator") == "Bob"

    def test_start_with_empty_body(self, client):
        r = client.post("/v1/sessions/start")
        assert r.status_code == 200

    def test_stop_returns_200(self, client):
        r = client.post("/v1/sessions/stop")
        assert r.status_code == 200

    def test_stop_calls_session_mgr(self, client, state):
        client.post("/v1/sessions/stop")
        state.session_mgr.stop.assert_called_once()

    def test_pause_returns_status(self, client):
        r = client.post("/v1/sessions/pause")
        assert r.status_code == 200
        assert "status" in r.json()

    def test_pause_calls_session_mgr(self, client, state):
        client.post("/v1/sessions/pause")
        state.session_mgr.pause.assert_called_once()

    def test_resume_calls_session_mgr(self, client, state):
        client.post("/v1/sessions/resume")
        state.session_mgr.resume.assert_called_once()

    def test_no_session_mgr_start_503(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.post("/v1/sessions/start", json={})
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# POST /v1/tests/throughput and /v1/tests/latency
# ---------------------------------------------------------------------------

class TestTriggers:
    def test_trigger_throughput_200(self, client):
        r = client.post("/v1/tests/throughput")
        assert r.status_code == 200
        assert r.json()["triggered"] is True

    def test_trigger_throughput_calls_collector(self, client, state):
        client.post("/v1/tests/throughput")
        state.throughput_collector.trigger_now.assert_called_once()

    def test_trigger_latency_200(self, client):
        r = client.post("/v1/tests/latency")
        assert r.status_code == 200
        assert r.json()["triggered"] is True

    def test_trigger_latency_calls_collector(self, client, state):
        client.post("/v1/tests/latency")
        state.latency_collector.trigger_now.assert_called_once()

    def test_throughput_no_collector_503(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.post("/v1/tests/throughput")
        assert r.status_code == 503

    def test_latency_no_collector_503(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.post("/v1/tests/latency")
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# GET /v1/config and PATCH /v1/config
# ---------------------------------------------------------------------------

class TestConfig:
    def test_get_config_200(self, client):
        r = client.get("/v1/config")
        assert r.status_code == 200

    def test_config_has_polling(self, client):
        r = client.get("/v1/config")
        assert "polling" in r.json()
        assert "rf_interval_seconds" in r.json()["polling"]

    def test_config_has_throughput(self, client):
        r = client.get("/v1/config")
        assert "throughput" in r.json()

    def test_config_has_tui(self, client):
        r = client.get("/v1/config")
        assert "tui" in r.json()
        assert "enabled" in r.json()["tui"]

    def test_config_has_api(self, client):
        r = client.get("/v1/config")
        assert "api" in r.json()

    def test_patch_tui_enabled(self, client, state):
        state.cfg.tui.enabled = True
        r = client.patch("/v1/config", json={"tui_enabled": False})
        assert r.status_code == 200
        assert "tui_enabled" in r.json()["applied"]
        assert state.cfg.tui.enabled is False

    def test_patch_valid_field(self, client, state):
        r = client.patch("/v1/config", json={"rf_interval_seconds": 2.5})
        assert r.status_code == 200
        assert "rf_interval_seconds" in r.json()["applied"]
        assert state.cfg.polling.rf_interval_seconds == 2.5

    def test_patch_invalid_field_rejected(self, client):
        r = client.patch("/v1/config", json={"unknown_key": 99})
        assert r.status_code == 200
        assert "unknown_key" in r.json()["rejected"]

    def test_patch_mixed_applied_and_rejected(self, client):
        r = client.patch("/v1/config", json={
            "rf_interval_seconds": 5.0,
            "evil_field": "bad",
        })
        d = r.json()
        assert "rf_interval_seconds" in d["applied"]
        assert "evil_field" in d["rejected"]

    def test_no_cfg_returns_503(self):
        s = AppState()
        app = create_app(s)
        c = TestClient(app)
        r = c.get("/v1/config")
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# GET /v1/history/rf, /throughput, /latency
# ---------------------------------------------------------------------------

class TestHistory:
    def test_rf_history_200(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/rf", params={"session_id": "20260410_143022"})
        assert r.status_code == 200

    def test_rf_history_has_data(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/rf", params={"session_id": "20260410_143022"})
        rows = r.json()
        assert len(rows) >= 1
        assert rows[0]["carrier"] == "att"

    def test_rf_history_carrier_filter(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/rf", params={
            "session_id": "20260410_143022", "carrier": "att"
        })
        assert all(row["carrier"] == "att" for row in r.json())

    def test_rf_history_unknown_session_empty(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/rf", params={"session_id": "99991231_000000"})
        assert r.status_code == 200
        assert r.json() == []

    def test_throughput_history_200(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/throughput", params={"session_id": "20260410_143022"})
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) >= 1
        assert rows[0]["direction"] == "download"

    def test_latency_history_200(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/latency", params={"session_id": "20260410_143022"})
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) >= 1
        assert rows[0]["method"] == "icmp"

    def test_limit_respected(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/rf", params={
            "session_id": "20260410_143022", "limit": 1
        })
        assert len(r.json()) <= 1

    def test_no_db_returns_empty(self, client):
        r = client.get("/v1/history/rf", params={"session_id": "20260410_143022"})
        assert r.json() == []

    def test_gnss_history_200(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/gnss", params={"session_id": "20260410_143022"})
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_gnss_history_unknown_session_empty(self, db_client):
        c, _ = db_client
        r = c.get("/v1/history/gnss", params={"session_id": "99991231_000000"})
        assert r.json() == []

    def test_gnss_history_no_db_empty(self, client):
        r = client.get("/v1/history/gnss", params={"session_id": "20260410_143022"})
        assert r.json() == []


# ---------------------------------------------------------------------------
# Auth (API key)
# ---------------------------------------------------------------------------

class TestAuth:
    def _make_keyed_client(self, key: str = "secret123"):
        s = _make_state()
        s.cfg.api.auth = "apikey"
        s.cfg.api.api_key = key
        app = create_app(s)
        return TestClient(app, raise_server_exceptions=False)

    def test_no_key_returns_403(self):
        c = self._make_keyed_client()
        r = c.get("/v1/status")
        assert r.status_code == 403

    def test_correct_key_returns_200(self):
        c = self._make_keyed_client("secret123")
        r = c.get("/v1/status", headers={"Authorization": "Bearer secret123"})
        assert r.status_code == 200

    def test_wrong_key_returns_403(self):
        c = self._make_keyed_client("secret123")
        r = c.get("/v1/status", headers={"Authorization": "Bearer wrongkey"})
        assert r.status_code == 403

    def test_malformed_header_returns_403(self):
        c = self._make_keyed_client()
        r = c.get("/v1/status", headers={"Authorization": "secret123"})
        assert r.status_code == 403

    def test_no_auth_config_allows_all(self, client):
        # state has auth="none"
        r = client.get("/v1/status")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# WebSocket /v1/ws
# ---------------------------------------------------------------------------

class TestWebSocket:
    def test_connect_accepts(self, client):
        with client.websocket_connect("/v1/ws") as ws:
            # Consume the timeout ping (TestClient drives the WS synchronously)
            # Just verify the connection opened without error
            assert ws is not None

    def test_message_fan_out(self, state):
        """Messages put on a WS queue arrive at the WebSocket client."""
        app = create_app(state)
        c = TestClient(app)
        with c.websocket_connect("/v1/ws") as ws:
            # Manually push a message to all registered WS queues
            msg = {"type": "rf_telemetry", "carrier": "att", "data": {"rsrp": -85.0}}
            for q in list(state._ws_queues):
                q.put_nowait(msg)
            received = ws.receive_json()
            assert received["type"] == "rf_telemetry"
            assert received["carrier"] == "att"

    def test_multiple_clients_both_receive(self, state):
        """Two connected clients both get the same message."""
        app = create_app(state)
        c = TestClient(app)

        msg = {"type": "gnss", "data": {"fix_type": "3d"}}

        with c.websocket_connect("/v1/ws") as ws1:
            with c.websocket_connect("/v1/ws") as ws2:
                for q in list(state._ws_queues):
                    q.put_nowait(msg)
                r1 = ws1.receive_json()
                r2 = ws2.receive_json()
                assert r1["type"] == "gnss"
                assert r2["type"] == "gnss"

    def test_queue_registered_on_connect(self, state):
        app = create_app(state)
        c = TestClient(app)
        before = len(state._ws_queues)
        with c.websocket_connect("/v1/ws"):
            assert len(state._ws_queues) == before + 1
        # After disconnect, queue should be removed
        assert len(state._ws_queues) == before

    def test_ws_auth_correct_key_accepted(self):
        s = _make_state()
        s.cfg.api.auth = "apikey"
        s.cfg.api.api_key = "ws_secret"
        app = create_app(s)
        c = TestClient(app)
        # Correct key → connection accepted, queue registered
        with c.websocket_connect("/v1/ws?api_key=ws_secret") as ws:
            assert ws is not None

    def test_ws_auth_wrong_key_closed(self):
        s = _make_state()
        s.cfg.api.auth = "apikey"
        s.cfg.api.api_key = "ws_secret"
        app = create_app(s)
        c = TestClient(app)
        import pytest as _pytest
        with _pytest.raises(Exception):
            # Wrong key → server closes with 4003 before accepting
            with c.websocket_connect("/v1/ws?api_key=wrong"):
                pass  # should not reach here

    def test_ws_auth_no_key_closed(self):
        s = _make_state()
        s.cfg.api.auth = "apikey"
        s.cfg.api.api_key = "ws_secret"
        app = create_app(s)
        c = TestClient(app)
        import pytest as _pytest
        with _pytest.raises(Exception):
            with c.websocket_connect("/v1/ws"):
                pass  # should not reach here

    def test_ws_no_auth_config_accepted_without_key(self, state):
        # state has auth="none" — no key required
        app = create_app(state)
        c = TestClient(app)
        with c.websocket_connect("/v1/ws") as ws:
            assert ws is not None


# ---------------------------------------------------------------------------
# AppState helpers
# ---------------------------------------------------------------------------

class TestAppState:
    def test_add_remove_ws_queue(self):
        s = AppState()
        q = asyncio.Queue()
        s.add_ws_queue(q)
        assert q in s._ws_queues
        s.remove_ws_queue(q)
        assert q not in s._ws_queues

    def test_remove_nonexistent_queue_safe(self):
        s = AppState()
        q = asyncio.Queue()
        s.remove_ws_queue(q)  # must not raise

    @pytest.mark.asyncio
    async def test_consume_updates_rf_state(self):
        s = _make_state()
        bus = MagicMock()

        msgs = [
            {"type": "rf_telemetry", "carrier": "att",
             "data": {"rsrp": -70.0, "access_technology": "NR5G-SA"}},
        ]
        call_count = 0

        async def _mock_get(name):
            nonlocal call_count
            call_count += 1
            if call_count <= len(msgs):
                return msgs[call_count - 1]
            s.stop()
            raise asyncio.TimeoutError

        bus.get = AsyncMock(side_effect=_mock_get)
        bus.task_done = MagicMock()
        s.bus = bus

        await s.consume()
        assert s._rf["att"]["rsrp"] == -70.0

    @pytest.mark.asyncio
    async def test_consume_updates_gnss_state(self):
        s = _make_state()
        bus = MagicMock()

        msgs = [
            {"type": "gnss", "carrier": "",
             "data": {"fix_type": "2d", "latitude": 34.0, "longitude": -117.0}},
        ]
        call_count = 0

        async def _mock_get(name):
            nonlocal call_count
            call_count += 1
            if call_count <= len(msgs):
                return msgs[call_count - 1]
            s.stop()
            raise asyncio.TimeoutError

        bus.get = AsyncMock(side_effect=_mock_get)
        bus.task_done = MagicMock()
        s.bus = bus

        await s.consume()
        assert s._gnss["fix_type"] == "2d"

    @pytest.mark.asyncio
    async def test_consume_fans_out_to_ws_queues(self):
        s = _make_state()
        bus = MagicMock()

        ws_q: asyncio.Queue = asyncio.Queue()
        s.add_ws_queue(ws_q)

        msg = {"type": "rf_telemetry", "carrier": "att", "data": {}}
        call_count = 0

        async def _mock_get(name):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return msg
            s.stop()
            raise asyncio.TimeoutError

        bus.get = AsyncMock(side_effect=_mock_get)
        bus.task_done = MagicMock()
        s.bus = bus

        await s.consume()
        assert not ws_q.empty()
        received = ws_q.get_nowait()
        assert received["type"] == "rf_telemetry"

    @pytest.mark.asyncio
    async def test_consume_cancels_cleanly(self):
        s = AppState()
        s._running = True
        bus = MagicMock()
        bus.get = AsyncMock(side_effect=asyncio.CancelledError)
        s.bus = bus

        task = asyncio.create_task(s.consume())
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert s._running is False


# ---------------------------------------------------------------------------
# DB helper functions
# ---------------------------------------------------------------------------

class TestDbHelpers:
    def test_db_for_session(self, tmp_path):
        p = _db_for_session(tmp_path, "20260410_143022")
        assert p.name == "5gbench_20260410.db"
        assert p.parent == tmp_path

    def test_db_for_session_date_extraction(self, tmp_path):
        # Different session IDs should map to different DB files
        p1 = _db_for_session(tmp_path, "20260410_143022")
        p2 = _db_for_session(tmp_path, "20260411_090000")
        assert p1.name == "5gbench_20260410.db"
        assert p2.name == "5gbench_20260411.db"
        assert p1 != p2

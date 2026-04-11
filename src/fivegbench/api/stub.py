"""5gbench REST + WebSocket API — Phase 2 implementation.

Architecture
============
AppState holds all runtime objects (SessionManager, collectors, bus, config)
and is attached to ``app.state.registry`` so route handlers can reach it via
a FastAPI dependency without module-level globals (keeps the module testable).

WebSocket fan-out
-----------------
``AppState.consume()`` runs as a background asyncio task reading from the
``api`` bus consumer queue.  For each message it:

  1. Updates in-memory live state (last RF snapshot per carrier, last GNSS fix).
  2. Puts the message onto every connected WS client's own Queue(maxsize=100).
     Full queues are silently dropped — a slow client won't block others.

Each ``/v1/ws`` handler owns one such queue and forwards messages to its
WebSocket connection.  On disconnect, the queue is removed.

Auth
----
When ``cfg.api.auth == "apikey"`` every request must carry::

    Authorization: Bearer <api_key>

Requests without a valid key get HTTP 403.

History queries
---------------
Session IDs encode their creation date (``YYYYMMDD_HHMMSS``), so the correct
database file can be found as ``5gbench_YYYYMMDD.db`` without a full scan.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from fivegbench.bus import EventBus
    from fivegbench.config import Config
    from fivegbench.modem.health import ModemHealthMonitor
    from fivegbench.session import SessionManager
    from fivegbench.collectors.throughput import ThroughputCollector
    from fivegbench.collectors.latency import LatencyCollector

log = logging.getLogger(__name__)

try:
    from fastapi import Body, Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
    from fastapi.responses import JSONResponse
    _FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    _FASTAPI_AVAILABLE = False


# ---------------------------------------------------------------------------
# AppState — runtime object registry
# ---------------------------------------------------------------------------

class AppState:
    """Holds all live objects the API handlers need.

    Call ``attach()`` once at startup to inject dependencies, then launch
    ``consume()`` as an asyncio task.
    """

    def __init__(self) -> None:
        self.session_mgr: "SessionManager | None" = None
        self.health_monitor: "ModemHealthMonitor | None" = None
        self.throughput_collector: "ThroughputCollector | None" = None
        self.latency_collector: "LatencyCollector | None" = None
        self.bus: "EventBus | None" = None
        self.cfg: "Config | None" = None
        self.db_path: Path | None = None

        # Live state (updated by consume())
        self._rf: dict[str, dict] = {}      # carrier → last rf_telemetry data
        self._gnss: dict[str, Any] = {}     # last gnss data

        # WebSocket fan-out: one Queue per connected client
        self._ws_queues: list[asyncio.Queue] = []
        self._running = False

    def attach(
        self,
        *,
        session_mgr: "SessionManager",
        health_monitor: "ModemHealthMonitor | None",
        throughput_collector: "ThroughputCollector | None",
        latency_collector: "LatencyCollector | None",
        bus: "EventBus",
        cfg: "Config",
    ) -> None:
        self.session_mgr = session_mgr
        self.health_monitor = health_monitor
        self.throughput_collector = throughput_collector
        self.latency_collector = latency_collector
        self.bus = bus
        self.cfg = cfg
        self.db_path = cfg.general.db_path
        self._rf = {m.carrier: {} for m in cfg.modems}

    def add_ws_queue(self, q: asyncio.Queue) -> None:
        self._ws_queues.append(q)

    def remove_ws_queue(self, q: asyncio.Queue) -> None:
        try:
            self._ws_queues.remove(q)
        except ValueError:
            pass

    async def consume(self) -> None:
        """Read from the ``api`` bus consumer queue and maintain live state."""
        if self.bus is None:
            return
        self._running = True
        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(self.bus.get("api"), timeout=0.2)
                except asyncio.TimeoutError:
                    continue

                t = msg.get("type")
                carrier = msg.get("carrier", "")
                data = msg.get("data", {})

                if t == "rf_telemetry" and carrier in self._rf:
                    self._rf[carrier] = data
                elif t == "gnss":
                    self._gnss = data

                # Fan-out to all connected WS clients (drop on full queue)
                for q in list(self._ws_queues):
                    try:
                        q.put_nowait(msg)
                    except asyncio.QueueFull:
                        pass

                self.bus.task_done("api")
        except asyncio.CancelledError:
            raise
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_for_session(db_path: Path, session_id: str) -> Path:
    """Return the database file path for the given session ID."""
    date_str = session_id[:8]  # "YYYYMMDD"
    return db_path / f"5gbench_{date_str}.db"


def _open_ro(db_file: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _rows_to_list(rows) -> list[dict]:
    return [dict(r) for r in rows]


def _query_sessions(db_path: Path, session_id: str | None = None) -> list[dict]:
    if session_id:
        # Use the date encoded in the session ID to locate the right file
        db_file = _db_for_session(db_path, session_id)
        if not db_file.exists():
            return []
        try:
            con = _open_ro(db_file)
            with con:
                rows = con.execute(
                    "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
                )
                return _rows_to_list(rows)
        except sqlite3.OperationalError:
            return []
    else:
        # Scan all available daily database files, newest first
        results: list[dict] = []
        for db_file in sorted(db_path.glob("5gbench_*.db"), reverse=True):
            try:
                con = _open_ro(db_file)
                with con:
                    rows = con.execute(
                        "SELECT * FROM sessions ORDER BY start_time DESC"
                    )
                    results.extend(_rows_to_list(rows))
            except sqlite3.OperationalError:
                continue
        return results


def _query_history(
    db_path: Path,
    table: str,
    session_id: str,
    carrier: str | None,
    limit: int,
) -> list[dict]:
    db_file = _db_for_session(db_path, session_id)
    if not db_file.exists():
        return []
    try:
        con = _open_ro(db_file)
        with con:
            if carrier:
                rows = con.execute(
                    f"SELECT * FROM {table} WHERE session_id = ? AND carrier = ?"
                    " ORDER BY timestamp DESC LIMIT ?",
                    (session_id, carrier, limit),
                )
            else:
                rows = con.execute(
                    f"SELECT * FROM {table} WHERE session_id = ?"
                    " ORDER BY timestamp DESC LIMIT ?",
                    (session_id, limit),
                )
            return _rows_to_list(rows)
    except sqlite3.OperationalError:
        return []


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------

def create_app(state: "AppState | None" = None) -> "FastAPI":
    """Create and return the configured FastAPI application.

    Pass an ``AppState`` instance (created and attached by the caller) to wire
    up live handlers.  Omit it (or pass ``None``) to get a standalone app for
    testing without a running collector.
    """
    if not _FASTAPI_AVAILABLE:  # pragma: no cover
        raise RuntimeError("fastapi is not installed. Run: pip install fastapi uvicorn")

    if state is None:
        state = AppState()

    app = FastAPI(
        title="5gbench API",
        description="5G Tri-Carrier Drive-Test Benchmarking — REST + WebSocket API",
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.state.registry = state

    # ------------------------------------------------------------------
    # Auth dependency
    # ------------------------------------------------------------------

    async def _check_auth(request: Request) -> None:
        cfg = request.app.state.registry.cfg
        if cfg is None or getattr(cfg.api, "auth", "none") != "apikey":
            return
        api_key = getattr(cfg.api, "api_key", "")
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer ") or header[7:] != api_key:
            raise HTTPException(status_code=403, detail="Invalid or missing API key")

    # ------------------------------------------------------------------
    # GET /v1/status
    # ------------------------------------------------------------------

    @app.get("/v1/status", dependencies=[Depends(_check_auth)])
    async def get_status(request: Request) -> dict:
        s: AppState = request.app.state.registry
        session = s.session_mgr.current if s.session_mgr else None
        health = (
            {c: h.value for c, h in s.health_monitor.all_health().items()}
            if s.health_monitor else {}
        )
        return {
            "session_id": session.session_id if session else None,
            "session_status": s.session_mgr.status.value if s.session_mgr else "idle",
            "modem_health": health,
            "gnss": s._gnss or None,
        }

    # ------------------------------------------------------------------
    # GET /v1/modems
    # ------------------------------------------------------------------

    @app.get("/v1/modems", dependencies=[Depends(_check_auth)])
    async def list_modems(request: Request) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.cfg is None:
            return []
        result = []
        for m in s.cfg.modems:
            health = (
                s.health_monitor.health_state(m.carrier).value
                if s.health_monitor else "unknown"
            )
            result.append({
                "carrier": m.carrier,
                "label": m.label,
                "imei": m.imei,
                "at_port": m.at_port,
                "net_interface": m.net_interface,
                "namespace": m.namespace,
                "health": health,
                "last_rf": s._rf.get(m.carrier, {}),
            })
        return result

    # ------------------------------------------------------------------
    # GET /v1/modems/{carrier}/rf
    # ------------------------------------------------------------------

    @app.get("/v1/modems/{carrier}/rf", dependencies=[Depends(_check_auth)])
    async def get_modem_rf(carrier: str, request: Request) -> dict:
        s: AppState = request.app.state.registry
        if carrier not in s._rf:
            raise HTTPException(status_code=404, detail=f"Carrier {carrier!r} not found")
        return s._rf[carrier]

    # ------------------------------------------------------------------
    # GET /v1/gnss
    # ------------------------------------------------------------------

    @app.get("/v1/gnss", dependencies=[Depends(_check_auth)])
    async def get_gnss(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if not s._gnss:
            raise HTTPException(status_code=503, detail="No GNSS fix available yet")
        return s._gnss

    # ------------------------------------------------------------------
    # GET /v1/sessions
    # ------------------------------------------------------------------

    @app.get("/v1/sessions", dependencies=[Depends(_check_auth)])
    async def list_sessions_endpoint(request: Request) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            return []
        return _query_sessions(s.db_path)

    # ------------------------------------------------------------------
    # GET /v1/sessions/{session_id}
    # ------------------------------------------------------------------

    @app.get("/v1/sessions/{session_id}", dependencies=[Depends(_check_auth)])
    async def get_session(session_id: str, request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            raise HTTPException(status_code=503, detail="Database not available")
        rows = _query_sessions(s.db_path, session_id=session_id)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Session {session_id!r} not found")
        return rows[0]

    # ------------------------------------------------------------------
    # POST /v1/sessions/start
    # ------------------------------------------------------------------

    @app.post("/v1/sessions/start", dependencies=[Depends(_check_auth)])
    async def start_session(request: Request, body: dict = Body(default={})) -> dict:
        s: AppState = request.app.state.registry
        if s.session_mgr is None:
            raise HTTPException(status_code=503, detail="Session manager not available")
        session = await s.session_mgr.start(
            operator=body.get("operator"),
            vehicle_id=body.get("vehicle_id"),
            route_description=body.get("route_description"),
            notes=body.get("notes"),
            name_suffix=body.get("name_suffix"),
        )
        return {"session_id": session.session_id, "start_time": session.start_time}

    # ------------------------------------------------------------------
    # POST /v1/sessions/stop
    # ------------------------------------------------------------------

    @app.post("/v1/sessions/stop", dependencies=[Depends(_check_auth)])
    async def stop_session(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.session_mgr is None:
            raise HTTPException(status_code=503, detail="Session manager not available")
        session = s.session_mgr.current
        await s.session_mgr.stop()
        return {
            "session_id": session.session_id if session else None,
            "end_time": session.end_time if session else None,
        }

    # ------------------------------------------------------------------
    # POST /v1/sessions/pause
    # ------------------------------------------------------------------

    @app.post("/v1/sessions/pause", dependencies=[Depends(_check_auth)])
    async def pause_session(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.session_mgr is None:
            raise HTTPException(status_code=503, detail="Session manager not available")
        await s.session_mgr.pause()
        return {"status": s.session_mgr.status.value}

    # ------------------------------------------------------------------
    # POST /v1/sessions/resume
    # ------------------------------------------------------------------

    @app.post("/v1/sessions/resume", dependencies=[Depends(_check_auth)])
    async def resume_session(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.session_mgr is None:
            raise HTTPException(status_code=503, detail="Session manager not available")
        await s.session_mgr.resume()
        return {"status": s.session_mgr.status.value}

    # ------------------------------------------------------------------
    # POST /v1/tests/throughput
    # ------------------------------------------------------------------

    @app.post("/v1/tests/throughput", dependencies=[Depends(_check_auth)])
    async def trigger_throughput(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.throughput_collector is None:
            raise HTTPException(status_code=503, detail="Throughput collector not running")
        s.throughput_collector.trigger_now()
        return {"triggered": True}

    # ------------------------------------------------------------------
    # POST /v1/tests/latency
    # ------------------------------------------------------------------

    @app.post("/v1/tests/latency", dependencies=[Depends(_check_auth)])
    async def trigger_latency(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.latency_collector is None:
            raise HTTPException(status_code=503, detail="Latency collector not running")
        s.latency_collector.trigger_now()
        return {"triggered": True}

    # ------------------------------------------------------------------
    # GET /v1/config
    # ------------------------------------------------------------------

    @app.get("/v1/config", dependencies=[Depends(_check_auth)])
    async def get_config(request: Request) -> dict:
        s: AppState = request.app.state.registry
        if s.cfg is None:
            raise HTTPException(status_code=503, detail="Config not available")
        c = s.cfg
        return {
            "polling": {
                "rf_interval_seconds": c.polling.rf_interval_seconds,
                "throughput_interval_seconds": c.polling.throughput_interval_seconds,
                "gnss_interval_seconds": c.polling.gnss_interval_seconds,
                "neighbor_cells": c.polling.neighbor_cells,
            },
            "throughput": {
                "method": c.throughput.method,
                "iperf3_server": c.throughput.iperf3_server,
                "iperf3_port": c.throughput.iperf3_port,
                "iperf3_duration_seconds": c.throughput.iperf3_duration_seconds,
            },
            "latency": {
                "methods": c.latency.methods,
                "targets": c.latency.targets,
            },
            "tui": {
                "enabled": c.tui.enabled,
            },
            "api": {
                "enabled": c.api.enabled,
                "host": c.api.host,
                "port": c.api.port,
            },
        }

    # ------------------------------------------------------------------
    # PATCH /v1/config
    # ------------------------------------------------------------------

    # Maps body key → (config section name, attribute name)
    _PATCHABLE: dict[str, tuple[str, str]] = {
        "rf_interval_seconds":           ("polling", "rf_interval_seconds"),
        "throughput_interval_seconds":   ("polling", "throughput_interval_seconds"),
        "gnss_interval_seconds":         ("polling", "gnss_interval_seconds"),
        "neighbor_cells":                ("polling", "neighbor_cells"),
        "tui_enabled":                   ("tui",     "enabled"),
    }

    @app.patch("/v1/config", dependencies=[Depends(_check_auth)])
    async def patch_config(request: Request, body: dict = Body(default={})) -> dict:
        s: AppState = request.app.state.registry
        if s.cfg is None:
            raise HTTPException(status_code=503, detail="Config not available")
        applied: dict[str, Any] = {}
        rejected: list[str] = []
        for key, val in body.items():
            if key in _PATCHABLE:
                section_name, attr = _PATCHABLE[key]
                section = getattr(s.cfg, section_name, None)
                if section is not None and hasattr(section, attr):
                    setattr(section, attr, val)
                    applied[key] = val
                else:
                    rejected.append(key)
            else:
                rejected.append(key)
        return {"applied": applied, "rejected": rejected}

    # ------------------------------------------------------------------
    # GET /v1/history/rf
    # ------------------------------------------------------------------

    @app.get("/v1/history/rf", dependencies=[Depends(_check_auth)])
    async def get_rf_history(
        request: Request,
        session_id: str,
        carrier: str | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            return []
        return _query_history(s.db_path, "rf_telemetry", session_id, carrier, min(limit, 10000))

    # ------------------------------------------------------------------
    # GET /v1/history/throughput
    # ------------------------------------------------------------------

    @app.get("/v1/history/throughput", dependencies=[Depends(_check_auth)])
    async def get_throughput_history(
        request: Request,
        session_id: str,
        carrier: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            return []
        return _query_history(s.db_path, "throughput_results", session_id, carrier, min(limit, 1000))

    # ------------------------------------------------------------------
    # GET /v1/history/latency
    # ------------------------------------------------------------------

    @app.get("/v1/history/latency", dependencies=[Depends(_check_auth)])
    async def get_latency_history(
        request: Request,
        session_id: str,
        carrier: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            return []
        return _query_history(s.db_path, "latency_results", session_id, carrier, min(limit, 1000))

    # ------------------------------------------------------------------
    # GET /v1/history/gnss
    # ------------------------------------------------------------------

    @app.get("/v1/history/gnss", dependencies=[Depends(_check_auth)])
    async def get_gnss_history(
        request: Request,
        session_id: str,
        limit: int = 1000,
    ) -> list[dict]:
        s: AppState = request.app.state.registry
        if s.db_path is None:
            return []
        db_file = _db_for_session(s.db_path, session_id)
        if not db_file.exists():
            return []
        try:
            con = _open_ro(db_file)
            with con:
                rows = con.execute(
                    "SELECT * FROM gnss WHERE session_id = ?"
                    " ORDER BY timestamp DESC LIMIT ?",
                    (session_id, min(limit, 10000)),
                )
                return _rows_to_list(rows)
        except sqlite3.OperationalError:
            return []

    # ------------------------------------------------------------------
    # WS /v1/ws
    # ------------------------------------------------------------------

    @app.websocket("/v1/ws")
    async def websocket_stream(websocket: WebSocket, api_key: str = "") -> None:
        """Real-time telemetry stream — multiplexes all bus message types.

        When ``api.auth = "apikey"`` is configured, pass the key as a query
        parameter: ``ws://host/v1/ws?api_key=<key>``
        """
        s: AppState = websocket.app.state.registry  # type: ignore[attr-defined]

        # Auth check before accepting — close with 4003 if invalid
        cfg = s.cfg
        if cfg is not None and getattr(cfg.api, "auth", "none") == "apikey":
            expected = getattr(cfg.api, "api_key", "")
            if api_key != expected:
                await websocket.close(code=4003)
                return

        await websocket.accept()
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        s.add_ws_queue(q)
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=30.0)
                    await websocket.send_json(msg)
                except asyncio.TimeoutError:
                    await websocket.send_json({"type": "ping"})
        except (WebSocketDisconnect, Exception):
            pass
        finally:
            s.remove_ws_queue(q)

    return app


# ---------------------------------------------------------------------------
# Module-level helpers (used by cli.py)
# ---------------------------------------------------------------------------

_app_instance: "FastAPI | None" = None
_state_instance: AppState | None = None


def get_app(state: "AppState | None" = None) -> "FastAPI":
    """Return (or lazily create) the module-level FastAPI app."""
    global _app_instance, _state_instance
    if _app_instance is None:
        _state_instance = state or AppState()
        _app_instance = create_app(_state_instance)
    return _app_instance


def get_state() -> AppState:
    """Return the AppState wired to the module-level app."""
    global _state_instance
    if _state_instance is None:
        _state_instance = AppState()
    return _state_instance


async def start_api_server(state: AppState, host: str, port: int) -> None:
    """Start the uvicorn server with the given AppState. Called from cli.py."""
    try:
        import uvicorn
    except ImportError:  # pragma: no cover
        log.error("uvicorn not installed. Run: pip install uvicorn")
        return

    app = create_app(state)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    log.info("API server starting on %s:%s", host, port)
    await server.serve()

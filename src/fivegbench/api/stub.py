"""API stub — Phase 2+.

This module defines the full FastAPI application with all planned endpoints.
In Phase 1, every endpoint raises NotImplementedError.  The internal
architecture (event bus, SQLite schema, session management) is fully wired
to support Phase 2 implementation without refactoring.

To enable:
    [api]
    enabled = true

The API consumer queue is registered in the dispatcher but receives messages
only when api.enabled = true.

Phase 2 implementation notes:
  - Replace raise NotImplementedError() with real handler logic
  - Wire the WebSocket endpoint to the api_queue fan-out consumer
  - Add optional API key auth middleware when auth = "apikey"
"""

from __future__ import annotations

# FastAPI is an optional dependency in Phase 1.
# Import lazily so the app starts without it installed.
try:
    from fastapi import FastAPI, WebSocket, HTTPException, Depends
    from fastapi.responses import JSONResponse
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    FastAPI = None  # type: ignore[assignment,misc]

import asyncio
import logging
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App creation
# ---------------------------------------------------------------------------

def create_app() -> "FastAPI":
    """Create and configure the FastAPI application instance."""
    if not _FASTAPI_AVAILABLE:
        raise RuntimeError(
            "fastapi is not installed. Run: pip install fastapi uvicorn"
        )

    app = FastAPI(
        title="5gbench API",
        description="5G Tri-Carrier Drive-Test Benchmarking Tool — REST + WebSocket API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ---------------------------------------------------------------------------
    # GET /v1/status
    # ---------------------------------------------------------------------------
    @app.get("/v1/status")
    async def get_status() -> dict:
        """
        Returns overall system status: modem connection states, active session
        info, GNSS fix state, collection running state.

        Phase 2: Return live state from SessionManager and ModemManager.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/modems
    # ---------------------------------------------------------------------------
    @app.get("/v1/modems")
    async def list_modems() -> list[dict]:
        """
        Returns list of configured modems with their current state:
        carrier, label, IMEI, connection status, namespace, last RF metrics.

        Phase 2: Query in-memory modem state registry.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/modems/{carrier}/rf
    # ---------------------------------------------------------------------------
    @app.get("/v1/modems/{carrier}/rf")
    async def get_modem_rf(carrier: str) -> dict:
        """
        Returns latest RF telemetry snapshot for the specified carrier.
        carrier: "att" | "tmobile" | "verizon"

        Phase 2: Return most recent rf_telemetry message from in-memory state.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/gnss
    # ---------------------------------------------------------------------------
    @app.get("/v1/gnss")
    async def get_gnss() -> dict:
        """
        Returns latest GNSS fix: lat, lon, altitude, speed, course, fix_type,
        satellites, HDOP, source modem.

        Phase 2: Return last gnss message from GNSSCollector state.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/sessions
    # ---------------------------------------------------------------------------
    @app.get("/v1/sessions")
    async def list_sessions() -> list[dict]:
        """
        Returns list of all sessions in today's database (session_id, start_time,
        end_time, operator, vehicle_id, route_description).

        Phase 2: Query sessions table in SQLite for today's db file.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/sessions/{session_id}
    # ---------------------------------------------------------------------------
    @app.get("/v1/sessions/{session_id}")
    async def get_session(session_id: str) -> dict:
        """
        Returns full session metadata including config_snapshot.

        Phase 2: Query sessions table by session_id.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/sessions/start
    # ---------------------------------------------------------------------------
    @app.post("/v1/sessions/start")
    async def start_session(body: dict) -> dict:
        """
        Start a new data collection session.

        Request body (all optional):
          { "operator": "...", "vehicle_id": "...", "route_description": "...", "notes": "..." }

        Returns: { "session_id": "...", "start_time": "..." }

        Phase 2: Call SessionManager.start() with provided metadata.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/sessions/stop
    # ---------------------------------------------------------------------------
    @app.post("/v1/sessions/stop")
    async def stop_session() -> dict:
        """
        Stop the current active session.

        Returns: { "session_id": "...", "end_time": "..." }

        Phase 2: Call SessionManager.stop().
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/sessions/pause
    # ---------------------------------------------------------------------------
    @app.post("/v1/sessions/pause")
    async def pause_session() -> dict:
        """
        Pause the current session (halt data collection without closing).

        Phase 2: Call SessionManager.pause().
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/sessions/resume
    # ---------------------------------------------------------------------------
    @app.post("/v1/sessions/resume")
    async def resume_session() -> dict:
        """
        Resume a paused session.

        Phase 2: Call SessionManager.resume().
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/tests/throughput
    # ---------------------------------------------------------------------------
    @app.post("/v1/tests/throughput")
    async def trigger_throughput() -> dict:
        """
        Trigger an on-demand throughput test run across all modems.
        Returns immediately; test runs asynchronously.

        Phase 2: Call ThroughputCollector.trigger_now().
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # POST /v1/tests/latency
    # ---------------------------------------------------------------------------
    @app.post("/v1/tests/latency")
    async def trigger_latency() -> dict:
        """
        Trigger an on-demand latency test run across all modems.
        Returns immediately; test runs asynchronously.

        Phase 2: Call LatencyCollector.trigger_now().
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/config
    # ---------------------------------------------------------------------------
    @app.get("/v1/config")
    async def get_config() -> dict:
        """
        Returns the active runtime configuration (as loaded at startup).

        Phase 2: Serialize active Config object to dict.
        Note: config is read-only in Phase 1; PATCH is for runtime overrides only.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # PATCH /v1/config
    # ---------------------------------------------------------------------------
    @app.patch("/v1/config")
    async def patch_config(body: dict) -> dict:
        """
        Update runtime configuration (polling intervals, toggles).
        Changes are in-memory only — the TOML file is NOT modified.

        Patchable fields: rf_interval_seconds, throughput_interval_seconds,
        gnss_interval_seconds, neighbor_cells, tui.enabled

        Phase 2: Apply overrides to in-memory Config and notify collectors.
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/history/rf
    # ---------------------------------------------------------------------------
    @app.get("/v1/history/rf")
    async def get_rf_history(session_id: str, carrier: str | None = None, limit: int = 1000) -> list[dict]:
        """
        Query historical RF telemetry.

        Parameters:
          session_id: required
          carrier: optional filter ("att", "tmobile", "verizon")
          limit: max rows (default 1000)

        Phase 2: SELECT from rf_telemetry WHERE session_id=? [AND carrier=?] ORDER BY timestamp DESC LIMIT ?
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/history/throughput
    # ---------------------------------------------------------------------------
    @app.get("/v1/history/throughput")
    async def get_throughput_history(session_id: str, carrier: str | None = None, limit: int = 100) -> list[dict]:
        """
        Query historical throughput results.

        Phase 2: SELECT from throughput_results WHERE session_id=? ORDER BY timestamp DESC LIMIT ?
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # GET /v1/history/latency
    # ---------------------------------------------------------------------------
    @app.get("/v1/history/latency")
    async def get_latency_history(session_id: str, carrier: str | None = None, limit: int = 100) -> list[dict]:
        """
        Query historical latency results.

        Phase 2: SELECT from latency_results WHERE session_id=? ORDER BY timestamp DESC LIMIT ?
        """
        raise NotImplementedError("API Phase 2")

    # ---------------------------------------------------------------------------
    # WS /v1/ws
    # ---------------------------------------------------------------------------
    @app.websocket("/v1/ws")
    async def websocket_stream(websocket: WebSocket) -> None:
        """
        Real-time telemetry WebSocket stream.

        All event types are multiplexed on a single connection, each message
        tagged with a "type" field matching the event bus message format:
          { "type": "rf_telemetry"|"gnss"|"throughput"|"latency"|"modem_event", ... }

        Phase 2 implementation:
          1. await websocket.accept()
          2. Subscribe a new consumer queue to the event bus dispatcher
          3. Loop: msg = await queue.get(); await websocket.send_json(msg)
          4. On disconnect: unregister consumer queue from dispatcher
        """
        raise NotImplementedError("API Phase 2")

    return app


# Module-level app instance (created lazily)
_app_instance: "FastAPI | None" = None


def get_app() -> "FastAPI":
    global _app_instance
    if _app_instance is None:
        _app_instance = create_app()
    return _app_instance


async def start_api_server(host: str, port: int) -> None:
    """Start the uvicorn server. Called from cli.py when api.enabled=True."""
    try:
        import uvicorn
    except ImportError:
        log.error("uvicorn not installed. Run: pip install uvicorn")
        return

    app = get_app()
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    log.info("API server starting on %s:%s", host, port)
    await server.serve()

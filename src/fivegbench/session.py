"""Session lifecycle management for 5gbench.

A session represents one continuous drive-test run.  Multiple sessions may
exist in a single database file (same calendar day).  Sessions within a day
share the same file, distinguished by session_id.

Session ID format:
  YYYYMMDD_HHMMSS                       — auto-generated
  YYYYMMDD_HHMMSS_<sanitized_name>      — if operator provides a name

All session state changes (start, stop, pause, resume) publish events to the
event bus so the SQLite writer and TUI are notified.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from fivegbench.bus import EventBus
from fivegbench.config import Config

log = logging.getLogger(__name__)


class SessionStatus(Enum):
    IDLE = "idle"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class Session:
    session_id: str
    start_time: str                    # ISO 8601 with milliseconds
    end_time: str | None = None
    operator: str = ""
    vehicle_id: str = ""
    route_description: str = ""
    notes: str = ""
    config_snapshot: dict = field(default_factory=dict)
    status: SessionStatus = SessionStatus.IDLE


# ---------------------------------------------------------------------------
# Session ID generation
# ---------------------------------------------------------------------------

def _sanitize_name(name: str) -> str:
    """Convert a free-text name to a safe session ID suffix."""
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name[:64]  # cap length


def generate_session_id(name: str = "") -> str:
    """Generate a session ID from current local time and optional name."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if name:
        safe = _sanitize_name(name)
        if safe:
            return f"{ts}_{safe}"
    return ts


def now_iso() -> str:
    """Return current local time as ISO 8601 with milliseconds."""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Config snapshot
# ---------------------------------------------------------------------------

def _config_to_dict(cfg: Config) -> dict:
    """Serialize the active config to a plain dict for the session snapshot."""
    return {
        "general": {
            "db_path": str(cfg.general.db_path),
            "log_path": str(cfg.general.log_path),
            "log_level": cfg.general.log_level,
            "dns": {"nameservers": cfg.general.dns.nameservers},
        },
        "gnss": {
            "primary_modem": cfg.gnss.primary_modem,
            "failover_order": cfg.gnss.failover_order,
            "movement_threshold_meters": cfg.gnss.movement_threshold_meters,
            "interpolate_on_fix_loss": cfg.gnss.interpolate_on_fix_loss,
        },
        "polling": {
            "rf_interval_seconds": cfg.polling.rf_interval_seconds,
            "neighbor_cells": cfg.polling.neighbor_cells,
            "throughput_interval_seconds": cfg.polling.throughput_interval_seconds,
            "gnss_interval_seconds": cfg.polling.gnss_interval_seconds,
        },
        "throughput": {
            "method": cfg.throughput.method,
            "iperf3_server": cfg.throughput.iperf3_server,
            "iperf3_port": cfg.throughput.iperf3_port,
            "iperf3_duration_seconds": cfg.throughput.iperf3_duration_seconds,
        },
        "latency": {
            "methods": cfg.latency.methods,
            "targets": cfg.latency.targets,
            "icmp_count": cfg.latency.icmp_count,
        },
        "modems": [
            {
                "imei": m.imei,
                "carrier": m.carrier,
                "label": m.label,
                "apn": m.apn,
            }
            for m in cfg.modems
        ],
    }


# ---------------------------------------------------------------------------
# Session manager
# ---------------------------------------------------------------------------

class SessionManager:
    """Manages the active session lifecycle and publishes events to the bus."""

    def __init__(self, bus: EventBus, cfg: Config) -> None:
        self._bus = bus
        self._cfg = cfg
        self._session: Session | None = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def active(self) -> Session | None:
        return self._session if self._session and self._session.status == SessionStatus.ACTIVE else None

    @property
    def current(self) -> Session | None:
        return self._session

    @property
    def session_id(self) -> str | None:
        return self._session.session_id if self._session else None

    @property
    def status(self) -> SessionStatus:
        return self._session.status if self._session else SessionStatus.IDLE

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    async def start(
        self,
        operator: str = "",
        vehicle_id: str = "",
        route_description: str = "",
        notes: str = "",
        name_suffix: str = "",
    ) -> Session:
        """Start a new session.  Stops any existing active session first."""
        if self._session and self._session.status == SessionStatus.ACTIVE:
            await self.stop()

        # Use config defaults if not provided
        operator = operator or self._cfg.session_defaults.operator
        vehicle_id = vehicle_id or self._cfg.session_defaults.vehicle_id
        route_description = route_description or self._cfg.session_defaults.route_description
        notes = notes or self._cfg.session_defaults.notes

        session_id = generate_session_id(name_suffix or route_description)
        start_time = now_iso()

        session = Session(
            session_id=session_id,
            start_time=start_time,
            operator=operator,
            vehicle_id=vehicle_id,
            route_description=route_description,
            notes=notes,
            config_snapshot=_config_to_dict(self._cfg),
            status=SessionStatus.ACTIVE,
        )
        self._session = session
        log.info("Session started: %s", session_id)

        await self._bus.publish({
            "type": "session_event",
            "timestamp": start_time,
            "session_id": session_id,
            "data": {
                "event": "start",
                "session_id": session_id,
                "start_time": start_time,
                "operator": operator,
                "vehicle_id": vehicle_id,
                "route_description": route_description,
                "notes": notes,
                "config_snapshot": session.config_snapshot,
            },
        })
        return session

    # ------------------------------------------------------------------
    # Stop
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        """Stop the current session."""
        if not self._session:
            return
        end_time = now_iso()
        self._session.end_time = end_time
        self._session.status = SessionStatus.STOPPED
        session_id = self._session.session_id
        log.info("Session stopped: %s at %s", session_id, end_time)

        await self._bus.publish({
            "type": "session_event",
            "timestamp": end_time,
            "session_id": session_id,
            "data": {
                "event": "stop",
                "session_id": session_id,
                "end_time": end_time,
            },
        })

    # ------------------------------------------------------------------
    # Pause / Resume
    # ------------------------------------------------------------------

    async def pause(self) -> None:
        """Pause the current session (stops data collection without closing)."""
        if not self._session or self._session.status != SessionStatus.ACTIVE:
            return
        self._session.status = SessionStatus.PAUSED
        ts = now_iso()
        log.info("Session paused: %s", self._session.session_id)
        await self._bus.publish({
            "type": "session_event",
            "timestamp": ts,
            "session_id": self._session.session_id,
            "data": {"event": "pause", "session_id": self._session.session_id},
        })

    async def resume(self) -> None:
        """Resume a paused session."""
        if not self._session or self._session.status != SessionStatus.PAUSED:
            return
        self._session.status = SessionStatus.ACTIVE
        ts = now_iso()
        log.info("Session resumed: %s", self._session.session_id)
        await self._bus.publish({
            "type": "session_event",
            "timestamp": ts,
            "session_id": self._session.session_id,
            "data": {"event": "resume", "session_id": self._session.session_id},
        })

    # ------------------------------------------------------------------
    # Modem event helper
    # ------------------------------------------------------------------

    async def publish_modem_event(
        self,
        event_type: str,
        modem_imei: str = "",
        carrier: str = "",
        detail: str = "",
    ) -> None:
        """Convenience: publish a modem_event to the bus."""
        if not self._session:
            return
        ts = now_iso()
        await self._bus.publish({
            "type": "modem_event",
            "timestamp": ts,
            "session_id": self._session.session_id,
            "carrier": carrier,
            "modem_imei": modem_imei,
            "data": {
                "event_type": event_type,
                "modem_imei": modem_imei,
                "carrier": carrier,
                "detail": detail,
            },
        })

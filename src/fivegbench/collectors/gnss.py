"""GNSS collector with failover chain and motion-aware logging.

One GNSS source is active at a time.  The primary modem is queried via
AT+QGPSLOC=2.  If it loses fix, the collector falls over to the next modem
in gnss.failover_order.  If all modems lose fix and interpolate_on_fix_loss
is enabled, positions are interpolated from last known speed and heading.

Motion-aware logging:
  - Write a new row only when position changes by >= movement_threshold_meters
  - Write a heartbeat row every 30 seconds even when stationary (to confirm
    the receiver is alive)
  - Interpolated positions are tagged fix_type="interpolated"
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime
from typing import Any

from fivegbench.bus import EventBus
from fivegbench.config import Config, GnssConfig, ModemConfig
from fivegbench.modem import parser as at_parser
from fivegbench.modem.serial import ATSerial, ATError, ATTimeout
from fivegbench.session import SessionManager, SessionStatus

import serial

log = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 30.0   # seconds: minimum write interval when stationary
REOPEN_DELAY = 5.0          # seconds before retrying a failed port
EARTH_RADIUS_M = 6_371_000.0


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in metres between two lat/lon points."""
    r = EARTH_RADIUS_M
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.asin(math.sqrt(a))


def _interpolate_position(
    lat: float, lon: float, speed_kmh: float, course_deg: float, elapsed_s: float
) -> tuple[float, float]:
    """Dead-reckoning: advance lat/lon by speed × elapsed along course."""
    distance_m = (speed_kmh / 3.6) * elapsed_s
    bearing = math.radians(course_deg)
    r = EARTH_RADIUS_M
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_m / r)
        + math.cos(lat1) * math.sin(distance_m / r) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(distance_m / r) * math.cos(lat1),
        math.cos(distance_m / r) - math.sin(lat1) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)


class GNSSCollector:
    """Single-instance GNSS collector with failover and motion-aware logging."""

    def __init__(
        self,
        cfg: Config,
        bus: EventBus,
        session_mgr: SessionManager,
    ) -> None:
        self._cfg = cfg
        self._gnss_cfg: GnssConfig = cfg.gnss
        self._bus = bus
        self._session = session_mgr
        self._running = False

        # Build ordered list of modems for failover: primary first, then chain
        modem_map = {m.carrier: m for m in cfg.modems}
        if not modem_map:
            self._modem_order: list[ModemConfig] = []
        else:
            primary = self._gnss_cfg.primary_modem or next(iter(modem_map))
            if self._gnss_cfg.failover_order:
                order = [primary] + [c for c in self._gnss_cfg.failover_order if c != primary]
            else:
                # No explicit order — primary first, then remaining in config order
                order = [primary] + [c for c in modem_map if c != primary]
            self._modem_order = [modem_map[c] for c in order if c in modem_map]

        self._active_index = 0   # index into _modem_order

        # State for motion-aware logging
        self._last_lat: float | None = None
        self._last_lon: float | None = None
        self._last_write_time: float = 0.0   # monotonic
        self._last_fix_time: float | None = None  # monotonic
        self._last_speed_kmh: float = 0.0
        self._last_course: float = 0.0

        # AT port connections — one per modem (lazy open)
        self._ports: dict[str, ATSerial | None] = {m.carrier: None for m in self._modem_order}

    # ------------------------------------------------------------------
    # Control
    # ------------------------------------------------------------------

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self._running = True
        interval = self._cfg.polling.gnss_interval_seconds
        log.info("GNSSCollector: started (interval=%.1fs)", interval)

        try:
            while self._running:
                if self._session.status != SessionStatus.ACTIVE:
                    await asyncio.sleep(0.5)
                    continue

                fix = await self._get_fix()
                await self._maybe_publish(fix)
                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            log.debug("GNSSCollector: cancelled")
            raise
        finally:
            await self._close_all_ports()
            self._running = False

    # ------------------------------------------------------------------
    # Fix acquisition with failover
    # ------------------------------------------------------------------

    async def _get_fix(self) -> dict[str, Any]:
        """Try each modem in failover order. Return fix data dict."""
        for offset in range(len(self._modem_order)):
            idx = (self._active_index + offset) % len(self._modem_order)
            modem = self._modem_order[idx]
            fix = await self._query_modem(modem)
            if fix and fix.get("fix_type") in ("2d", "3d"):
                if idx != self._active_index:
                    await self._report_failover(modem.carrier, idx)
                    self._active_index = idx
                self._last_fix_time = asyncio.get_event_loop().time()
                self._last_speed_kmh = fix.get("speed_kmh") or 0.0
                self._last_course = fix.get("course") or 0.0
                fix["source_modem"] = modem.carrier
                return fix

        # All modems failed fix — interpolate or return no_fix
        return await self._handle_fix_loss()

    async def _query_modem(self, modem: ModemConfig) -> dict[str, Any] | None:
        """Query AT+QGPSLOC=2 on *modem*. Returns parsed fix or None on error."""
        at = await self._get_port(modem)
        if at is None:
            return None
        try:
            resp = await at.get_gnss_fix()
            return at_parser.parse_gnss_fix(resp)
        except (ATError, ATTimeout) as exc:
            log.debug("GNSS query failed on %s: %s", modem.carrier, exc)
            return None
        except (serial.SerialException, OSError) as exc:
            log.warning("GNSS serial error on %s: %s", modem.carrier, exc)
            await self._close_port(modem.carrier)
            return None

    async def _handle_fix_loss(self) -> dict[str, Any]:
        """All modems have no fix.  Interpolate if enabled, else return no_fix."""
        if (
            self._gnss_cfg.interpolate_on_fix_loss
            and self._last_lat is not None
            and self._last_lon is not None
            and self._last_fix_time is not None
        ):
            elapsed = asyncio.get_event_loop().time() - self._last_fix_time
            lat, lon = _interpolate_position(
                self._last_lat, self._last_lon,
                self._last_speed_kmh, self._last_course, elapsed,
            )
            log.debug("GNSS: interpolating position (elapsed=%.1fs)", elapsed)
            return {
                "fix_type": "interpolated",
                "latitude": lat,
                "longitude": lon,
                "altitude": None,
                "speed_kmh": self._last_speed_kmh,
                "course": self._last_course,
                "hdop": None,
                "satellites": 0,
                "utc_time": None,
                "source_modem": "interpolated",
            }

        return {
            "fix_type": "no_fix",
            "latitude": None,
            "longitude": None,
            "altitude": None,
            "speed_kmh": None,
            "course": None,
            "hdop": None,
            "satellites": 0,
            "utc_time": None,
            "source_modem": "none",
        }

    # ------------------------------------------------------------------
    # Motion-aware write decision
    # ------------------------------------------------------------------

    async def _maybe_publish(self, fix: dict[str, Any]) -> None:
        """Publish a GNSS row if position changed enough or heartbeat due."""
        now_mono = asyncio.get_event_loop().time()
        session_id = self._session.session_id
        if not session_id:
            return

        lat = fix.get("latitude")
        lon = fix.get("longitude")
        fix_type = fix.get("fix_type", "no_fix")

        should_write = False

        if fix_type in ("2d", "3d"):
            if self._last_lat is None or self._last_lon is None:
                should_write = True  # first fix
            else:
                dist = _haversine_m(self._last_lat, self._last_lon, lat, lon)
                if dist >= self._gnss_cfg.movement_threshold_meters:
                    should_write = True

        # Heartbeat when stationary or no fix
        if now_mono - self._last_write_time >= HEARTBEAT_INTERVAL:
            should_write = True

        if not should_write:
            return

        if lat is not None and lon is not None:
            self._last_lat = lat
            self._last_lon = lon

        self._last_write_time = now_mono
        ts = _now_iso()

        await self._bus.publish({
            "type": "gnss",
            "timestamp": ts,
            "session_id": session_id,
            "data": {
                "source_modem": fix.get("source_modem", ""),
                "fix_type": fix_type,
                "latitude": lat,
                "longitude": lon,
                "altitude": fix.get("altitude"),
                "speed_kmh": fix.get("speed_kmh"),
                "course": fix.get("course"),
                "hdop": fix.get("hdop"),
                "satellites": fix.get("satellites"),
                "utc_time": fix.get("utc_time"),
            },
        })

    # ------------------------------------------------------------------
    # Port management
    # ------------------------------------------------------------------

    async def _get_port(self, modem: ModemConfig) -> ATSerial | None:
        """Return an open ATSerial for *modem*, opening if needed."""
        at = self._ports.get(modem.carrier)
        if at and at.is_open:
            return at
        if not modem.at_port:
            return None
        try:
            at = ATSerial(modem.at_port, timeout=4.0)
            await at.open()
            # Enable GNSS engine (idempotent)
            await at.enable_gnss()
            self._ports[modem.carrier] = at
            return at
        except (serial.SerialException, OSError, ATError) as exc:
            log.debug("GNSSCollector: cannot open %s: %s", modem.at_port, exc)
            self._ports[modem.carrier] = None
            return None

    async def _close_port(self, carrier: str) -> None:
        at = self._ports.get(carrier)
        if at:
            try:
                await at.close()
            except Exception:  # noqa: BLE001
                pass
            self._ports[carrier] = None

    async def _close_all_ports(self) -> None:
        for carrier in list(self._ports):
            await self._close_port(carrier)

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    async def _report_failover(self, new_carrier: str, new_idx: int) -> None:
        old_carrier = self._modem_order[self._active_index].carrier
        detail = f"failover from {old_carrier} to {new_carrier}"
        log.warning("GNSSCollector: %s", detail)
        session_id = self._session.session_id
        if not session_id:
            return
        await self._bus.publish({
            "type": "modem_event",
            "timestamp": _now_iso(),
            "session_id": session_id,
            "carrier": new_carrier,
            "modem_imei": self._modem_order[new_idx].imei,
            "data": {
                "event_type": "gnss_failover",
                "carrier": new_carrier,
                "modem_imei": self._modem_order[new_idx].imei,
                "detail": detail,
            },
        })

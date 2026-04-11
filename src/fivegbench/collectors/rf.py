"""RF telemetry collector — one asyncio task per modem.

Polls AT+QENG="servingcell", AT+QENG="neighbourcell", and AT+QNWINFO on a
configurable interval and publishes results to the event bus.

The AT serial port is kept open for the life of the session and re-opened on
transient failures (port disappears on USB disconnect).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from fivegbench.bus import EventBus
from fivegbench.config import Config, ModemConfig, PollingConfig
from fivegbench.modem import parser as at_parser
from fivegbench.modem.serial import ATSerial, ATError, ATTimeout
from fivegbench.session import SessionManager, SessionStatus

import serial

log = logging.getLogger(__name__)

REOPEN_DELAY = 5.0   # seconds to wait before retrying a failed port open


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class RFCollector:
    """Polls RF metrics for a single modem and publishes to the event bus.

    One instance is created per modem.  All instances run concurrently as
    separate asyncio tasks.
    """

    def __init__(
        self,
        modem_cfg: ModemConfig,
        polling_cfg: PollingConfig,
        bus: EventBus,
        session_mgr: SessionManager,
    ) -> None:
        self._modem = modem_cfg
        self._polling = polling_cfg
        self._bus = bus
        self._session = session_mgr
        self._running = False
        self._paused = False
        self._at: ATSerial | None = None

    # ------------------------------------------------------------------
    # Control
    # ------------------------------------------------------------------

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def stop(self) -> None:
        self._running = False

    @property
    def carrier(self) -> str:
        return self._modem.carrier

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main polling loop.  Runs until cancelled or stop() is called."""
        self._running = True
        log.info(
            "RFCollector[%s]: started on %s (interval=%.1fs)",
            self._modem.carrier, self._modem.at_port, self._polling.rf_interval_seconds,
        )
        try:
            while self._running:
                if self._paused or self._session.status != SessionStatus.ACTIVE:
                    await asyncio.sleep(0.5)
                    continue

                # Ensure port is open
                if not self._at or not self._at.is_open:
                    if not await self._open_port():
                        await asyncio.sleep(REOPEN_DELAY)
                        continue

                # Collect one polling cycle
                try:
                    await self._poll_cycle()
                except (ATError, ATTimeout) as exc:
                    log.warning("RFCollector[%s]: AT error: %s", self._modem.carrier, exc)
                except (serial.SerialException, OSError) as exc:
                    log.warning(
                        "RFCollector[%s]: serial error: %s — will reopen",
                        self._modem.carrier, exc,
                    )
                    await self._close_port()
                    await self._publish_modem_event("disconnect", str(exc))

                await asyncio.sleep(self._polling.rf_interval_seconds)

        except asyncio.CancelledError:
            log.debug("RFCollector[%s]: cancelled", self._modem.carrier)
            raise
        finally:
            await self._close_port()
            self._running = False

    # ------------------------------------------------------------------
    # Port management
    # ------------------------------------------------------------------

    async def _open_port(self) -> bool:
        port = self._modem.at_port
        if not port:
            log.error("RFCollector[%s]: no AT port configured", self._modem.carrier)
            return False
        try:
            self._at = ATSerial(port)
            await self._at.open()
            log.info("RFCollector[%s]: opened %s", self._modem.carrier, port)
            return True
        except (serial.SerialException, OSError) as exc:
            log.warning("RFCollector[%s]: cannot open %s: %s", self._modem.carrier, port, exc)
            self._at = None
            return False

    async def _close_port(self) -> None:
        if self._at:
            try:
                await self._at.close()
            except Exception:  # noqa: BLE001
                pass
            self._at = None

    # ------------------------------------------------------------------
    # Polling cycle
    # ------------------------------------------------------------------

    async def _poll_cycle(self) -> None:
        ts = _now_iso()
        session_id = self._session.session_id
        if not session_id:
            return

        # 1. Serving cell
        sc_resp = await self._at.command('AT+QENG="servingcell"')
        sc = at_parser.parse_serving_cell(sc_resp)

        # 2. Network info
        nw_resp = await self._at.command("AT+QNWINFO")
        nw = at_parser.parse_qnwinfo(nw_resp)

        rf_data: dict[str, Any] = {
            "modem_imei": self._modem.imei,
            "carrier": self._modem.carrier,
            "access_technology": sc.get("access_technology"),
            "band": sc.get("band") or nw.get("band_info"),
            "earfcn": sc.get("earfcn"),
            "pci": sc.get("pci"),
            "cell_id": sc.get("cell_id"),
            "rsrp": sc.get("rsrp"),
            "rsrq": sc.get("rsrq"),
            "sinr": sc.get("sinr"),
            "rssi": sc.get("rssi"),
            "registered_network": nw.get("registered_network"),
            "raw_response": sc_resp,
        }

        await self._bus.publish({
            "type": "rf_telemetry",
            "timestamp": ts,
            "session_id": session_id,
            "carrier": self._modem.carrier,
            "modem_imei": self._modem.imei,
            "data": rf_data,
        })

        # 3. Neighbor cells (optional)
        if self._polling.neighbor_cells:
            nb_resp = await self._at.command('AT+QENG="neighbourcell"', timeout=10.0)
            neighbors = at_parser.parse_neighbor_cells(nb_resp)
            if neighbors:
                await self._bus.publish({
                    "type": "neighbor_cells",
                    "timestamp": ts,
                    "session_id": session_id,
                    "carrier": self._modem.carrier,
                    "modem_imei": self._modem.imei,
                    "data": {
                        "modem_imei": self._modem.imei,
                        "carrier": self._modem.carrier,
                        "neighbors": neighbors,
                    },
                })

    # ------------------------------------------------------------------
    # Event publishing
    # ------------------------------------------------------------------

    async def _publish_modem_event(self, event_type: str, detail: str = "") -> None:
        session_id = self._session.session_id
        if not session_id:
            return
        await self._bus.publish({
            "type": "modem_event",
            "timestamp": _now_iso(),
            "session_id": session_id,
            "carrier": self._modem.carrier,
            "modem_imei": self._modem.imei,
            "data": {
                "event_type": event_type,
                "modem_imei": self._modem.imei,
                "carrier": self._modem.carrier,
                "detail": detail,
            },
        })

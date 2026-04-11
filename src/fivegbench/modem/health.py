"""Central modem health monitor — Phase 6 hardening.

Polls every ModemManager modem every 5 seconds and orchestrates the full
disconnect → reconnect recovery cycle:

  HEALTHY → DISCONNECTED
    • Pause RF collector for that carrier
    • Tear down its network namespace
    • Publish modem_event "disconnect"

  DISCONNECTED → HEALTHY  (when MM reports connected again)
    • Fetch new bearer info (IP/gateway may change after reconnect)
    • Re-establish namespace with new addressing
    • Resume RF collector
    • Publish modem_event "reconnect"

The throughput and latency collectors call is_healthy(carrier) before running
any test for a given carrier, so they skip disconnected modems automatically.

The GNSS collector handles fix loss independently via its failover chain and
needs no special wiring here — when the primary modem's AT port is gone it
naturally falls over to the next.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import TYPE_CHECKING

from fivegbench.config import Config, ModemConfig
from fivegbench.modem.manager import (
    ModemState,
    build_modem_info,
    connect_modem,
    find_modem_by_imei,
    get_modem_state,
)
from fivegbench.namespace import setup_namespace, teardown_namespace
from fivegbench.session import SessionManager

if TYPE_CHECKING:
    from fivegbench.bus import EventBus
    from fivegbench.collectors.rf import RFCollector

log = logging.getLogger(__name__)

POLL_INTERVAL = 5.0       # seconds between health checks
RECONNECT_HOLDDOWN = 10.0 # seconds to wait in RECONNECTING before retrying setup


class CarrierHealth(Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"


_CONNECTED_STATES = {ModemState.CONNECTED}
_DOWN_STATES = {
    ModemState.DISCONNECTED,
    ModemState.FAILED,
    ModemState.DISABLED,
}


class ModemHealthMonitor:
    """Monitors all modems for connectivity changes and drives recovery.

    Create one instance per application run, pass it the RF collectors so it
    can pause/resume them, and launch it as an asyncio task.

    Usage::

        monitor = ModemHealthMonitor(cfg, bus, session_mgr, rf_collectors)
        task = asyncio.create_task(monitor.run(), name="health_monitor")

        # In throughput/latency collectors:
        if not monitor.is_healthy(carrier):
            continue
    """

    def __init__(
        self,
        cfg: Config,
        bus: "EventBus",
        session_mgr: SessionManager,
        rf_collectors: list["RFCollector"],
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._session = session_mgr
        self._running = False

        # carrier → RFCollector (for pause/resume)
        self._rf_map: dict[str, "RFCollector"] = {
            rc.carrier: rc for rc in rf_collectors
        }

        # carrier → ModemManager index (populated at startup; re-resolved on loss)
        self._mm_index: dict[str, str] = {}

        # carrier → CarrierHealth
        self._health: dict[str, CarrierHealth] = {
            m.carrier: CarrierHealth.UNKNOWN for m in cfg.modems
        }

        # carrier → monotonic time when reconnecting state was entered
        self._reconnecting_since: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_healthy(self, carrier: str) -> bool:
        """Return True only when the carrier is fully connected and operational."""
        return self._health.get(carrier) == CarrierHealth.HEALTHY

    def health_state(self, carrier: str) -> CarrierHealth:
        return self._health.get(carrier, CarrierHealth.UNKNOWN)

    def all_health(self) -> dict[str, CarrierHealth]:
        return dict(self._health)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Poll all modems on POLL_INTERVAL until cancelled."""
        self._running = True
        log.info("ModemHealthMonitor: started (poll=%.0fs)", POLL_INTERVAL)

        # Resolve MM indices before first poll
        await self._resolve_all_mm_indices()

        try:
            while self._running:
                for modem in self._cfg.modems:
                    try:
                        await self._check_modem(modem)
                    except Exception as exc:  # noqa: BLE001
                        log.debug(
                            "ModemHealthMonitor: unexpected error checking %s: %s",
                            modem.carrier, exc,
                        )
                await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            log.debug("ModemHealthMonitor: cancelled")
            raise
        finally:
            self._running = False

    # ------------------------------------------------------------------
    # Per-modem check
    # ------------------------------------------------------------------

    async def _check_modem(self, modem: ModemConfig) -> None:
        mm_idx = await self._get_or_find_mm_index(modem)
        current = self._health[modem.carrier]

        if mm_idx is None:
            # Modem completely absent from MM — treat as disconnected
            if current in (CarrierHealth.HEALTHY, CarrierHealth.UNKNOWN):
                log.warning(
                    "ModemHealthMonitor[%s]: not found in ModemManager — disconnected",
                    modem.carrier,
                )
                await self._on_disconnect(modem)
            return

        state = await get_modem_state(mm_idx)
        log.debug("ModemHealthMonitor[%s]: MM state=%s health=%s", modem.carrier, state.value, current.value)

        if state in _CONNECTED_STATES:
            if current in (CarrierHealth.UNKNOWN, CarrierHealth.DISCONNECTED):
                # First-time baseline OR recovery from disconnect
                await self._on_reconnect(modem, mm_idx)
            elif current == CarrierHealth.RECONNECTING:
                # Namespace setup may be in progress — check if holddown elapsed
                since = self._reconnecting_since.get(modem.carrier, 0.0)
                if asyncio.get_event_loop().time() - since >= RECONNECT_HOLDDOWN:
                    await self._on_reconnect(modem, mm_idx)
            # else: already HEALTHY — nothing to do

        elif state in _DOWN_STATES:
            if current in (CarrierHealth.HEALTHY, CarrierHealth.UNKNOWN):
                await self._on_disconnect(modem)
            elif current == CarrierHealth.RECONNECTING:
                # Lost again during reconnect — back to disconnected
                log.warning(
                    "ModemHealthMonitor[%s]: lost again during reconnect (MM state=%s)",
                    modem.carrier, state.value,
                )
                self._health[modem.carrier] = CarrierHealth.DISCONNECTED

        # CONNECTING / REGISTERED: transient — let it settle, do nothing

    # ------------------------------------------------------------------
    # Disconnect
    # ------------------------------------------------------------------

    async def _on_disconnect(self, modem: ModemConfig) -> None:
        log.warning("ModemHealthMonitor[%s]: DISCONNECT", modem.carrier)
        self._health[modem.carrier] = CarrierHealth.DISCONNECTED

        # Pause RF telemetry for this carrier
        rf = self._rf_map.get(modem.carrier)
        if rf:
            rf.pause()
            log.debug("ModemHealthMonitor[%s]: RF collector paused", modem.carrier)

        # Tear down the namespace (best-effort; interface may already be gone)
        if modem.namespace:
            await teardown_namespace(modem.namespace)

        await self._publish_event("disconnect", modem, detail=f"MM state transition to disconnected")

    # ------------------------------------------------------------------
    # Reconnect / recovery
    # ------------------------------------------------------------------

    async def _on_reconnect(self, modem: ModemConfig, mm_idx: str) -> None:
        """Attempt to re-establish the namespace and resume collection."""
        previous = self._health[modem.carrier]
        self._health[modem.carrier] = CarrierHealth.RECONNECTING
        self._reconnecting_since[modem.carrier] = asyncio.get_event_loop().time()

        if previous not in (CarrierHealth.UNKNOWN,):
            # Only log/event for actual recovery (not initial startup)
            log.info("ModemHealthMonitor[%s]: attempting reconnect…", modem.carrier)

        # Ensure MM has an active connection (connect if needed)
        state = await get_modem_state(mm_idx)
        if state != ModemState.CONNECTED:
            log.info("ModemHealthMonitor[%s]: requesting MM connect (APN=%s)", modem.carrier, modem.apn)
            connected = await connect_modem(mm_idx, modem.apn)
            if not connected:
                log.error("ModemHealthMonitor[%s]: MM connect failed", modem.carrier)
                self._health[modem.carrier] = CarrierHealth.DISCONNECTED
                return

        # Fetch fresh bearer info (IP/gateway/interface can change after reconnect)
        mm_info = await build_modem_info(mm_idx, modem.imei)
        if not mm_info or not mm_info.bearer_interface:
            log.warning(
                "ModemHealthMonitor[%s]: connected but no bearer interface yet",
                modem.carrier,
            )
            self._health[modem.carrier] = CarrierHealth.DISCONNECTED
            return

        # Update runtime modem config with fresh values
        modem.net_interface = mm_info.bearer_interface

        # (Re-)establish namespace with current addressing
        ok = await setup_namespace(
            modem.namespace,
            modem.net_interface,
            ip_address=mm_info.ip_address,   # already CIDR via fixed manager.py
            gateway=mm_info.gateway,
            nameservers=self._cfg.general.dns.nameservers,
        )
        if not ok:
            log.error(
                "ModemHealthMonitor[%s]: namespace setup failed (iface=%s ip=%s gw=%s)",
                modem.carrier, modem.net_interface, mm_info.ip_address, mm_info.gateway,
            )
            self._health[modem.carrier] = CarrierHealth.DISCONNECTED
            return

        # Resume RF telemetry
        rf = self._rf_map.get(modem.carrier)
        if rf:
            rf.resume()
            log.debug("ModemHealthMonitor[%s]: RF collector resumed", modem.carrier)

        self._health[modem.carrier] = CarrierHealth.HEALTHY
        log.info("ModemHealthMonitor[%s]: HEALTHY (iface=%s)", modem.carrier, modem.net_interface)

        if previous not in (CarrierHealth.UNKNOWN,):
            await self._publish_event(
                "reconnect", modem,
                detail=f"re-established on {modem.net_interface} ip={mm_info.ip_address}",
            )

    # ------------------------------------------------------------------
    # MM index resolution
    # ------------------------------------------------------------------

    async def _resolve_all_mm_indices(self) -> None:
        """Populate _mm_index for all configured modems at startup."""
        for modem in self._cfg.modems:
            idx = await find_modem_by_imei(modem.imei)
            if idx:
                self._mm_index[modem.carrier] = idx
                log.debug("ModemHealthMonitor[%s]: MM index=%s", modem.carrier, idx)
            else:
                log.warning(
                    "ModemHealthMonitor[%s]: IMEI %s not found in ModemManager",
                    modem.carrier, modem.imei,
                )

    async def _get_or_find_mm_index(self, modem: ModemConfig) -> str | None:
        """Return cached MM index, or try to find it fresh if not known."""
        idx = self._mm_index.get(modem.carrier)
        if idx:
            return idx
        idx = await find_modem_by_imei(modem.imei)
        if idx:
            self._mm_index[modem.carrier] = idx
        return idx

    # ------------------------------------------------------------------
    # Event publishing
    # ------------------------------------------------------------------

    async def _publish_event(
        self, event_type: str, modem: ModemConfig, detail: str = ""
    ) -> None:
        session_id = self._session.session_id
        if not session_id:
            return
        from datetime import datetime
        ts = datetime.now().astimezone().isoformat(timespec="milliseconds")
        await self._bus.publish({
            "type": "modem_event",
            "timestamp": ts,
            "session_id": session_id,
            "carrier": modem.carrier,
            "modem_imei": modem.imei,
            "data": {
                "event_type": event_type,
                "modem_imei": modem.imei,
                "carrier": modem.carrier,
                "detail": detail,
            },
        })

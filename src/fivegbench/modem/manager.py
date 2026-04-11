"""ModemManager integration via mmcli CLI.

Handles:
  - Listing modems and matching by IMEI
  - Bringing up / tearing down cellular data connections
  - Querying connection state
  - Monitoring for disconnect / reconnect events

All subprocess calls use asyncio.create_subprocess_exec() and run non-blocking.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from enum import Enum

log = logging.getLogger(__name__)


class ModemState(Enum):
    UNKNOWN = "unknown"
    REGISTERED = "registered"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    DISABLED = "disabled"
    FAILED = "failed"


@dataclass
class ModemInfo:
    mm_index: str           # e.g. "0", "1", "2"
    imei: str
    state: ModemState
    primary_port: str       # e.g. "ttyUSB2"
    bearer_interface: str   # e.g. "wwan0"
    ip_address: str         # assigned IP, or ""
    gateway: str            # gateway IP, or ""


# ---------------------------------------------------------------------------
# Low-level mmcli helpers
# ---------------------------------------------------------------------------

async def _run_mmcli(*args: str, timeout: float = 15.0) -> tuple[int, str, str]:
    """Run mmcli with *args*. Returns (returncode, stdout, stderr)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "mmcli", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")
    except asyncio.TimeoutError:
        log.warning("mmcli %s timed out after %ss", " ".join(args), timeout)
        return -1, "", "timeout"
    except FileNotFoundError:
        log.error("mmcli not found. Install modemmanager.")
        return -1, "", "mmcli not found"


async def list_modem_indices() -> list[str]:
    """Return list of ModemManager modem indices (e.g. ["0", "1", "2"])."""
    rc, stdout, _ = await _run_mmcli("--list-modems")
    if rc != 0:
        return []
    return re.findall(r"/org/freedesktop/ModemManager1/Modem/(\d+)", stdout)


async def get_modem_info(index: str) -> dict:
    """Return parsed modem info dict from mmcli -m <index> --output-json."""
    rc, stdout, stderr = await _run_mmcli("-m", index, "--output-json")
    if rc != 0:
        log.debug("mmcli -m %s failed: %s", index, stderr.strip())
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        log.debug("mmcli JSON parse error for modem %s: %s", index, exc)
        return {}


async def get_bearer_info(bearer_path: str) -> dict:
    """Return bearer details dict from mmcli --bearer <path> --output-json."""
    rc, stdout, _ = await _run_mmcli("--bearer", bearer_path, "--output-json")
    if rc != 0:
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {}


# ---------------------------------------------------------------------------
# High-level operations
# ---------------------------------------------------------------------------

async def find_modem_by_imei(imei: str) -> str | None:
    """Return the ModemManager index for the modem with *imei*, or None."""
    indices = await list_modem_indices()
    for idx in indices:
        rc, stdout, _ = await _run_mmcli("-m", idx, "--output-keyvalue")
        if rc == 0 and imei in stdout:
            return idx
    return None


async def get_modem_state(mm_index: str) -> ModemState:
    """Return current ModemState for modem at *mm_index*."""
    rc, stdout, _ = await _run_mmcli("-m", mm_index, "--output-keyvalue")
    if rc != 0:
        return ModemState.UNKNOWN
    state_map = {
        "connected": ModemState.CONNECTED,
        "registered": ModemState.REGISTERED,
        "connecting": ModemState.CONNECTING,
        "disconnected": ModemState.DISCONNECTED,
        "disabled": ModemState.DISABLED,
        "failed": ModemState.FAILED,
    }
    for line in stdout.splitlines():
        if "state" in line.lower():
            for keyword, state in state_map.items():
                if keyword in line.lower():
                    return state
    return ModemState.UNKNOWN


async def connect_modem(mm_index: str, apn: str) -> bool:
    """Instruct ModemManager to connect modem *mm_index* with *apn*.

    Returns True on success.
    """
    log.info("Connecting modem %s with APN=%s", mm_index, apn)
    rc, stdout, stderr = await _run_mmcli(
        "-m", mm_index,
        "--simple-connect", f"apn={apn}",
        timeout=60.0,
    )
    if rc != 0:
        log.error("Failed to connect modem %s: %s", mm_index, stderr.strip())
        return False
    log.info("Modem %s connected.", mm_index)
    return True


async def disconnect_modem(mm_index: str) -> bool:
    """Disconnect modem *mm_index*."""
    rc, _, stderr = await _run_mmcli("-m", mm_index, "--simple-disconnect", timeout=30.0)
    if rc != 0:
        log.warning("Disconnect modem %s: %s", mm_index, stderr.strip())
        return False
    return True


async def get_bearer_interface(mm_index: str) -> tuple[str, str, str]:
    """Return (interface_name, ip_address, gateway) for the active bearer.

    Returns ("", "", "") if no active bearer is found.
    """
    rc, stdout, _ = await _run_mmcli("-m", mm_index, "--output-keyvalue")
    if rc != 0:
        return "", "", ""

    # Find bearer path
    bearer_path = None
    for line in stdout.splitlines():
        m = re.search(r"bearers\s*:\s*(/org/freedesktop/ModemManager1/Bearer/\d+)", line)
        if m:
            bearer_path = m.group(1)
            break

    if not bearer_path:
        return "", "", ""

    b = await get_bearer_info(bearer_path)
    if not b:
        return "", "", ""

    # Navigate JSON structure: bearer -> ipv4-config
    try:
        ipv4 = b.get("bearer", {}).get("ipv4-config", {})
        iface = b.get("bearer", {}).get("properties", {}).get("interface", "")
        if not iface:
            # Older mmcli versions put interface at different path
            iface = b.get("bearer", {}).get("interface", "")
        addr = ipv4.get("address", "")
        gw = ipv4.get("gateway", "")
        return iface, addr, gw
    except (AttributeError, TypeError):
        return "", "", ""


async def build_modem_info(mm_index: str, imei: str) -> ModemInfo | None:
    """Assemble a ModemInfo for modem *mm_index* / *imei*."""
    state = await get_modem_state(mm_index)
    iface, addr, gw = await get_bearer_interface(mm_index)

    # Primary port from mmcli
    rc, stdout, _ = await _run_mmcli("-m", mm_index, "--output-keyvalue")
    primary_port = ""
    if rc == 0:
        for line in stdout.splitlines():
            m = re.search(r"primary-port\s*:\s*(\S+)", line)
            if m:
                primary_port = m.group(1)
                break

    return ModemInfo(
        mm_index=mm_index,
        imei=imei,
        state=state,
        primary_port=primary_port,
        bearer_interface=iface,
        ip_address=addr,
        gateway=gw,
    )


# ---------------------------------------------------------------------------
# Connection monitor
# ---------------------------------------------------------------------------

class ModemConnectionMonitor:
    """Monitors a modem's connection state and triggers callbacks on changes.

    Usage:
        monitor = ModemConnectionMonitor(mm_index, on_connect=..., on_disconnect=...)
        task = asyncio.create_task(monitor.run())
    """

    POLL_INTERVAL = 5.0  # seconds

    def __init__(
        self,
        mm_index: str,
        imei: str,
        on_connect: "asyncio.coroutines.CoroutineType | None" = None,
        on_disconnect: "asyncio.coroutines.CoroutineType | None" = None,
    ) -> None:
        self.mm_index = mm_index
        self.imei = imei
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._last_state: ModemState | None = None
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("ModemConnectionMonitor started for MM index %s", self.mm_index)
        try:
            while self._running:
                state = await get_modem_state(self.mm_index)
                if state != self._last_state:
                    log.info(
                        "Modem %s (idx=%s) state: %s → %s",
                        self.imei, self.mm_index,
                        self._last_state.value if self._last_state else "None",
                        state.value,
                    )
                    if state == ModemState.CONNECTED and self._on_connect:
                        await self._on_connect(self.imei, self.mm_index)
                    elif state in (ModemState.DISCONNECTED, ModemState.FAILED) and self._on_disconnect:
                        await self._on_disconnect(self.imei, self.mm_index)
                    self._last_state = state
                await asyncio.sleep(self.POLL_INTERVAL)
        except asyncio.CancelledError:
            self._running = False
            raise

    def stop(self) -> None:
        self._running = False

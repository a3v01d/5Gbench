"""Modem USB discovery: enumerate ttyUSB ports, match to IMEIs, pick AT ports.

The Quectel RM520N-GL exposes several USB endpoints per modem.  ModemManager
claims one of them for its own AT command channel.  We enumerate all available
ttyUSB devices, identify which one MM is using for each modem, and return a
different port for our RF telemetry poller.

Typical RM520N-GL USB interface layout (4 interfaces per modem):
  Interface 0 — DM / diagnostic
  Interface 1 — NMEA / GPS
  Interface 2 — AT command port (ModemManager uses this)
  Interface 3 — AT command port (we use this for telemetry)
  Interface 4 — MBIM/QMI network control

This module uses /sys/bus/usb and /proc/tty/driver/usbserial to correlate
ttyUSBx devices to their parent USB device and interface number.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path

from fivegbench.modem.serial import ATSerial

log = logging.getLogger(__name__)

# USB vendor IDs for Quectel modems
QUECTEL_VID = "2c7c"

# Number of AT-capable interfaces we skip (MM uses the first, we use second)
MM_AT_INTERFACE_OFFSET = 2   # interface index MM typically claims
OUR_AT_INTERFACE_OFFSET = 3  # interface index we use


# ---------------------------------------------------------------------------
# Sysfs helpers
# ---------------------------------------------------------------------------

def _read_sysfs(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def _find_usb_tty_devices() -> list[dict]:
    """Return list of dicts describing each ttyUSBx device found in sysfs.

    Each dict: {port, usb_device_path, interface_num, vid, pid}
    """
    devices = []
    tty_base = Path("/sys/bus/usb-serial/drivers/option")
    if not tty_base.exists():
        tty_base = Path("/sys/bus/usb/drivers/option")

    for dev_link in sorted(Path("/sys/class/tty").iterdir()):
        name = dev_link.name
        if not name.startswith("ttyUSB"):
            continue
        try:
            real_path = dev_link.resolve()
        except OSError:
            continue

        # Walk up to find the usb_interface directory
        p = real_path
        interface_path = None
        for _ in range(8):
            p = p.parent
            if (p / "bInterfaceNumber").exists():
                interface_path = p
                break

        if interface_path is None:
            continue

        iface_num_str = _read_sysfs(interface_path / "bInterfaceNumber")
        usb_dev_path = interface_path.parent

        vid = _read_sysfs(usb_dev_path / "idVendor")
        pid = _read_sysfs(usb_dev_path / "idProduct")

        devices.append({
            "port": f"/dev/{name}",
            "usb_device_path": str(usb_dev_path),
            "interface_num": int(iface_num_str, 16) if iface_num_str else -1,
            "vid": vid,
            "pid": pid,
            "tty_name": name,
        })

    return sorted(devices, key=lambda d: d["tty_name"])


def _group_by_usb_device(devices: list[dict]) -> dict[str, list[dict]]:
    """Group ttyUSB entries by their parent USB device path."""
    groups: dict[str, list[dict]] = {}
    for dev in devices:
        key = dev["usb_device_path"]
        groups.setdefault(key, []).append(dev)
    return groups


# ---------------------------------------------------------------------------
# ModemManager port detection
# ---------------------------------------------------------------------------

async def _get_mm_at_ports() -> set[str]:
    """Ask ModemManager which serial ports it currently owns.

    Returns a set of /dev/ttyUSBx paths that MM is managing.
    Falls back to empty set if mmcli is not available.
    """
    mm_ports: set[str] = set()
    try:
        proc = await asyncio.create_subprocess_exec(
            "mmcli", "--list-modems",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        # stdout lines look like: /org/freedesktop/ModemManager1/Modem/0
        modem_paths = re.findall(r"/org/freedesktop/ModemManager1/Modem/(\d+)", stdout.decode())
        for idx in modem_paths:
            proc2 = await asyncio.create_subprocess_exec(
                "mmcli", f"-m", idx, "--output-keyvalue",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out2, _ = await proc2.communicate()
            for line in out2.decode().splitlines():
                m = re.search(r"primary-port\s*:\s*(\S+)", line)
                if m:
                    port = m.group(1)
                    if not port.startswith("/"):
                        port = f"/dev/{port}"
                    mm_ports.add(port)
                # Also capture all AT ports MM knows about
                m2 = re.search(r"ports\s*:\s*(.+)", line)
                if m2:
                    for token in m2.group(1).split(","):
                        token = token.strip()
                        pm = re.match(r"(ttyUSB\d+)\s*\(at\)", token)
                        if pm:
                            mm_ports.add(f"/dev/{pm.group(1)}")
    except FileNotFoundError:
        log.debug("mmcli not found — cannot auto-detect MM ports")
    except Exception as exc:  # noqa: BLE001
        log.debug("_get_mm_at_ports error: %s", exc)
    return mm_ports


# ---------------------------------------------------------------------------
# IMEI probing
# ---------------------------------------------------------------------------

async def _probe_imei(port: str) -> str | None:
    """Try to read IMEI from *port*.  Returns IMEI string or None."""
    try:
        async with ATSerial(port, timeout=4.0) as at:
            if await at.ping():
                return await at.get_imei()
    except Exception as exc:  # noqa: BLE001
        log.debug("IMEI probe failed on %s: %s", port, exc)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def discover_modems(target_imeis: list[str]) -> dict[str, dict]:
    """Discover USB ttyUSB ports for each modem IMEI in *target_imeis*.

    Returns a dict keyed by IMEI:
      {
        "<imei>": {
            "at_port": "/dev/ttyUSB3",   # port safe for us to use
            "mm_port": "/dev/ttyUSB2",   # port MM is using (do not open)
            "usb_device": "/sys/bus/usb/...",
        }
      }
    Missing IMEIs are absent from the dict.
    """
    log.info("Starting modem discovery for IMEIs: %s", target_imeis)
    tty_devices = _find_usb_tty_devices()
    mm_ports = await _get_mm_at_ports()
    log.debug("MM-owned ports: %s", mm_ports)
    log.debug("Found ttyUSB devices: %s", [d["port"] for d in tty_devices])

    # Group by USB device
    groups = _group_by_usb_device(tty_devices)

    found: dict[str, dict] = {}

    # For each USB device group (one group = one modem), probe for IMEI
    for usb_path, ifaces in groups.items():
        vid = ifaces[0].get("vid", "")
        if vid and vid.lower() != QUECTEL_VID:
            log.debug("Skipping non-Quectel device %s (vid=%s)", usb_path, vid)
            continue

        # Sort interfaces by interface number
        ifaces_sorted = sorted(ifaces, key=lambda d: d["interface_num"])

        # Identify candidate AT ports: any ttyUSB not owned by MM
        candidate_ports = [
            d["port"] for d in ifaces_sorted
            if d["port"] not in mm_ports
        ]
        mm_port_for_device = [
            d["port"] for d in ifaces_sorted
            if d["port"] in mm_ports
        ]

        if not candidate_ports:
            # Fall back: try the highest-numbered interface
            candidate_ports = [ifaces_sorted[-1]["port"]]

        log.debug(
            "USB device %s: MM ports=%s, candidate AT ports=%s",
            usb_path, mm_port_for_device, candidate_ports,
        )

        # Probe each candidate port for IMEI
        imei = None
        used_port = None
        for port in candidate_ports:
            imei = await _probe_imei(port)
            if imei:
                used_port = port
                log.info("Found modem IMEI %s on %s", imei, port)
                break

        if imei and imei in target_imeis:
            found[imei] = {
                "at_port": used_port,
                "mm_port": mm_port_for_device[0] if mm_port_for_device else None,
                "usb_device": usb_path,
                "all_ports": [d["port"] for d in ifaces_sorted],
            }

    missing = set(target_imeis) - set(found)
    if missing:
        log.warning("Could not find ttyUSB ports for IMEIs: %s", sorted(missing))

    return found


async def identify_net_interface(modem_imei: str) -> str | None:
    """Find the wwanX network interface associated with *modem_imei* via mmcli.

    Returns interface name (e.g., "wwan0") or None.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "mmcli", "--list-modems",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        modem_paths = re.findall(r"/org/freedesktop/ModemManager1/Modem/(\d+)", stdout.decode())
        for idx in modem_paths:
            proc2 = await asyncio.create_subprocess_exec(
                "mmcli", "-m", idx, "--output-keyvalue",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out2, _ = await proc2.communicate()
            text = out2.decode()
            # Check IMEI
            if modem_imei not in text:
                continue
            for line in text.splitlines():
                m = re.search(r"primary-port\s*:\s*(\S+)", line)
                if m and m.group(1).startswith("wwan"):
                    return m.group(1)
                m2 = re.search(r"ports\s*:\s*(.+)", line)
                if m2:
                    for token in m2.group(1).split(","):
                        pm = re.match(r"(wwan\d+)", token.strip())
                        if pm:
                            return pm.group(1)
    except Exception as exc:  # noqa: BLE001
        log.debug("identify_net_interface error for %s: %s", modem_imei, exc)
    return None

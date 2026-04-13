"""Modem USB discovery: enumerate ttyUSB ports, match to IMEIs, pick AT ports.

The Quectel RM520N-GL exposes several USB endpoints per modem.  ModemManager
claims one of them for its own AT command channel.  We enumerate all available
ttyUSB devices, identify which one MM is using for each modem, and return a
different port for our RF telemetry poller.

Typical RM520N-GL USB interface layout (4 interfaces per modem):
  Interface 0 — DM / diagnostic
  Interface 1 — NMEA / GPS
  Interface 2 — AT command port (ModemManager primary port)
  Interface 3 — AT command port (we use this for telemetry)
  Interface 4 — MBIM/QMI network control

Discovery strategy (in order of preference):
  1. Query ModemManager via mmcli --output-keyvalue — reads IMEI
     (equipment-identifier) and port list without touching serial ports.
  2. Fall back to sysfs enumeration + serial probing when mmcli is absent.

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
# Primary discovery path: ModemManager keyvalue query
# ---------------------------------------------------------------------------

async def _discover_via_mmcli(target_imeis: list[str]) -> dict[str, dict]:
    """Query ModemManager for IMEI → port mapping using mmcli --output-keyvalue.

    This is the preferred path — no serial probing required.  Returns a dict
    keyed by IMEI for each *target_imeis* entry found by MM.
    """
    found: dict[str, dict] = {}
    try:
        proc = await asyncio.create_subprocess_exec(
            "mmcli", "--list-modems",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        indices = re.findall(r"/org/freedesktop/ModemManager1/Modem/(\d+)", stdout.decode())
    except FileNotFoundError:
        log.debug("mmcli not found — skipping MM-based discovery")
        return found
    except Exception as exc:
        log.debug("mmcli list-modems error: %s", exc)
        return found

    for idx in indices:
        try:
            proc2 = await asyncio.create_subprocess_exec(
                "mmcli", "-m", idx, "--output-keyvalue",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out2, _ = await proc2.communicate()
            text = out2.decode()
        except Exception as exc:
            log.debug("mmcli -m %s error: %s", idx, exc)
            continue

        imei: str | None = None
        primary_port: str | None = None
        all_at_ports: list[str] = []

        for line in text.splitlines():
            # IMEI — keyvalue key is "modem.generic.equipment-identifier"
            m = re.search(r"equipment-identifier\s*:\s*(\d{15})", line)
            if m:
                imei = m.group(1)

            # Primary port — "modem.generic.primary-port : ttyUSB2"
            m = re.search(r"primary-port\s*:\s*(\S+)", line)
            if m:
                val = m.group(1)
                primary_port = val if val.startswith("/") else f"/dev/{val}"

            # Port list — keyvalue format: "ttyUSB0|ignored,ttyUSB1|gps,ttyUSB2|at,..."
            # (the table / human-readable format uses "(at)" — keyvalue uses "|at")
            m = re.search(r"\.ports\s*:\s*(.+)", line)
            if m:
                for token in m.group(1).split(","):
                    token = token.strip()
                    # Accept both keyvalue (ttyUSBx|at) and table (ttyUSBx (at)) forms
                    pm = re.match(r"(ttyUSB\d+)\s*[|(]at[)]?", token)
                    if pm:
                        port = f"/dev/{pm.group(1)}"
                        all_at_ports.append(port)

        if not imei or imei not in target_imeis:
            continue

        # Pick our AT port: first secondary AT port (not MM's primary), else primary
        our_port = next(
            (p for p in all_at_ports if p != primary_port),
            primary_port,
        )

        if our_port:
            log.info("mmcli discovery: IMEI %s → at_port=%s (mm_port=%s)",
                     imei, our_port, primary_port)
            found[imei] = {
                "at_port": our_port,
                "mm_port": primary_port,
                "usb_device": None,
                "all_ports": all_at_ports,
            }

    return found


# ---------------------------------------------------------------------------
# Fallback discovery path: sysfs enumeration + serial probing
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


async def _get_mm_primary_ports() -> set[str]:
    """Return the set of primary AT ports that ModemManager is using.

    Only the primary port is excluded from our candidate ports — secondary AT
    ports (e.g. interface 3) are left available for us to probe/use.
    """
    mm_ports: set[str] = set()
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
            for line in out2.decode().splitlines():
                m = re.search(r"primary-port\s*:\s*(\S+)", line)
                if m:
                    port = m.group(1)
                    if not port.startswith("/"):
                        port = f"/dev/{port}"
                    mm_ports.add(port)
    except FileNotFoundError:
        log.debug("mmcli not found — cannot auto-detect MM ports")
    except Exception as exc:
        log.debug("_get_mm_primary_ports error: %s", exc)
    return mm_ports


async def _probe_imei(port: str) -> str | None:
    """Try to read IMEI from *port*.  Returns IMEI string or None."""
    try:
        async with ATSerial(port, timeout=4.0) as at:
            if await at.ping():
                return await at.get_imei()
    except Exception as exc:  # noqa: BLE001
        log.debug("IMEI probe failed on %s: %s", port, exc)
    return None


async def _discover_via_sysfs(target_imeis: list[str]) -> dict[str, dict]:
    """Fallback: enumerate sysfs ttyUSB devices and probe each for IMEI."""
    log.info("Falling back to sysfs+serial discovery for IMEIs: %s", target_imeis)
    tty_devices = _find_usb_tty_devices()
    # Only exclude MM's primary port; leave secondary AT ports as candidates
    mm_ports = await _get_mm_primary_ports()
    log.debug("MM primary ports: %s", mm_ports)
    log.debug("Found ttyUSB devices: %s", [d["port"] for d in tty_devices])

    groups = _group_by_usb_device(tty_devices)
    found: dict[str, dict] = {}

    for usb_path, ifaces in groups.items():
        vid = ifaces[0].get("vid", "")
        if vid and vid.lower() != QUECTEL_VID:
            log.debug("Skipping non-Quectel device %s (vid=%s)", usb_path, vid)
            continue

        ifaces_sorted = sorted(ifaces, key=lambda d: d["interface_num"])

        # Candidate ports: everything not owned by MM as primary
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
            "usb_device": "/sys/bus/usb/...",  # may be None for mmcli path
        }
      }
    Missing IMEIs are absent from the dict.

    Strategy: try ModemManager keyvalue query first (fast, no serial probing),
    fall back to sysfs enumeration + serial probing if mmcli is unavailable or
    returns no results.
    """
    log.info("Starting modem discovery for IMEIs: %s", target_imeis)

    # Primary path: ask ModemManager (already knows the IMEI and ports)
    result = await _discover_via_mmcli(target_imeis)
    if result:
        return result

    # Fallback: enumerate sysfs and probe serial ports
    return await _discover_via_sysfs(target_imeis)


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
                m2 = re.search(r"\.ports\s*:\s*(.+)", line)
                if m2:
                    for token in m2.group(1).split(","):
                        pm = re.match(r"(wwan\d+)", token.strip())
                        if pm:
                            return pm.group(1)
    except Exception as exc:  # noqa: BLE001
        log.debug("identify_net_interface error for %s: %s", modem_imei, exc)
    return None

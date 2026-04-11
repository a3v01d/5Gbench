"""Linux network namespace management for per-carrier traffic isolation.

Each modem's network interface lives inside its own network namespace so that
iperf3, ping, and curl traffic cannot leak through the wrong interface.

Namespace naming: ns_att, ns_tmobile, ns_verizon
  Derived from carrier label (lowercase), prefixed with "ns_".

DNS: /etc/netns/<ns>/resolv.conf is written with configured public resolvers.

All operations require root or CAP_NET_ADMIN.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Sequence

log = logging.getLogger(__name__)

RESOLV_BASE = Path("/etc/netns")


# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------

async def _run(*args: str, check: bool = True, timeout: float = 10.0) -> tuple[int, str, str]:
    """Run a command. Returns (returncode, stdout, stderr)."""
    log.debug("namespace: %s", " ".join(args))
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        rc = proc.returncode
        if check and rc != 0:
            log.warning(
                "Command failed (rc=%d): %s\n  stderr: %s",
                rc, " ".join(args), stderr.decode(errors="replace").strip(),
            )
        return rc, stdout.decode(errors="replace"), stderr.decode(errors="replace")
    except asyncio.TimeoutError:
        log.error("Command timed out: %s", " ".join(args))
        return -1, "", "timeout"
    except FileNotFoundError as exc:
        log.error("Command not found: %s — %s", args[0], exc)
        return -127, "", str(exc)


async def _ip(*args: str, check: bool = True) -> tuple[int, str, str]:
    return await _run("ip", *args, check=check)


# ---------------------------------------------------------------------------
# Namespace lifecycle
# ---------------------------------------------------------------------------

async def namespace_exists(ns_name: str) -> bool:
    """Return True if the named network namespace already exists."""
    rc, stdout, _ = await _ip("netns", "list", check=False)
    return ns_name in stdout


async def create_namespace(ns_name: str) -> bool:
    """Create network namespace *ns_name*.  Idempotent."""
    if await namespace_exists(ns_name):
        log.debug("Namespace %s already exists.", ns_name)
        return True
    rc, _, stderr = await _ip("netns", "add", ns_name)
    if rc == 0:
        log.info("Created namespace: %s", ns_name)
        return True
    log.error("Failed to create namespace %s: %s", ns_name, stderr.strip())
    return False


async def delete_namespace(ns_name: str) -> bool:
    """Delete network namespace *ns_name*.  Idempotent."""
    if not await namespace_exists(ns_name):
        return True
    rc, _, _ = await _ip("netns", "delete", ns_name, check=False)
    if rc == 0:
        log.info("Deleted namespace: %s", ns_name)
        return True
    return False


# ---------------------------------------------------------------------------
# DNS configuration
# ---------------------------------------------------------------------------

def write_resolv_conf(ns_name: str, nameservers: list[str]) -> None:
    """Write /etc/netns/<ns_name>/resolv.conf with the given nameservers."""
    ns_dir = RESOLV_BASE / ns_name
    ns_dir.mkdir(parents=True, exist_ok=True)
    resolv_path = ns_dir / "resolv.conf"
    lines = [f"nameserver {ns}\n" for ns in nameservers]
    try:
        resolv_path.write_text("".join(lines))
        log.debug("Wrote %s: %s", resolv_path, nameservers)
    except OSError as exc:
        log.error("Failed to write %s: %s", resolv_path, exc)


def remove_resolv_conf(ns_name: str) -> None:
    """Remove the /etc/netns/<ns_name>/ directory on teardown."""
    ns_dir = RESOLV_BASE / ns_name
    try:
        resolv_path = ns_dir / "resolv.conf"
        if resolv_path.exists():
            resolv_path.unlink()
        if ns_dir.exists():
            ns_dir.rmdir()
    except OSError as exc:
        log.debug("remove_resolv_conf %s: %s", ns_name, exc)


# ---------------------------------------------------------------------------
# Interface migration and configuration
# ---------------------------------------------------------------------------

async def move_interface_to_ns(interface: str, ns_name: str) -> bool:
    """Move *interface* into namespace *ns_name*."""
    rc, _, stderr = await _ip("link", "set", interface, "netns", ns_name)
    if rc == 0:
        log.info("Moved %s into namespace %s", interface, ns_name)
        return True
    log.error("Failed to move %s into %s: %s", interface, ns_name, stderr.strip())
    return False


async def bring_up_interface(ns_name: str, interface: str) -> bool:
    """Bring up *interface* inside *ns_name*."""
    rc, _, stderr = await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "link", "set", interface, "up",
    )
    if rc == 0:
        log.debug("Interface %s up in %s", interface, ns_name)
        return True
    log.error("Failed to bring up %s in %s: %s", interface, ns_name, stderr.strip())
    return False


async def configure_interface_ip(ns_name: str, interface: str, ip_cidr: str) -> bool:
    """Assign *ip_cidr* (e.g. '192.168.1.100/24') to *interface* in *ns_name*."""
    # Flush any existing addresses first
    await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "addr", "flush", "dev", interface,
        check=False,
    )
    rc, _, stderr = await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "addr", "add", ip_cidr, "dev", interface,
    )
    if rc == 0:
        log.info("Assigned %s to %s in %s", ip_cidr, interface, ns_name)
        return True
    log.error("Failed to assign IP to %s in %s: %s", interface, ns_name, stderr.strip())
    return False


async def add_default_route(ns_name: str, gateway: str) -> bool:
    """Add a default route via *gateway* inside *ns_name*."""
    # Remove existing default route (ignore errors)
    await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "route", "del", "default",
        check=False,
    )
    rc, _, stderr = await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "route", "add", "default", "via", gateway,
    )
    if rc == 0:
        log.info("Default route via %s in %s", gateway, ns_name)
        return True
    log.error("Failed to add default route in %s: %s", ns_name, stderr.strip())
    return False


# ---------------------------------------------------------------------------
# Full setup / teardown
# ---------------------------------------------------------------------------

async def setup_namespace(
    ns_name: str,
    interface: str,
    ip_address: str,
    gateway: str,
    nameservers: list[str],
) -> bool:
    """Full namespace bring-up: create, DNS, move interface, configure IP + route.

    *ip_address* should be in CIDR notation (e.g. '10.1.1.100/30').
    Returns True if all steps succeeded.
    """
    log.info("Setting up namespace %s (iface=%s ip=%s gw=%s)", ns_name, interface, ip_address, gateway)

    if not await create_namespace(ns_name):
        return False

    write_resolv_conf(ns_name, nameservers)

    if not await move_interface_to_ns(interface, ns_name):
        return False

    if not await bring_up_interface(ns_name, interface):
        return False

    # Bring up loopback inside namespace
    await _run(
        "ip", "netns", "exec", ns_name,
        "ip", "link", "set", "lo", "up",
        check=False,
    )

    if ip_address:
        if not await configure_interface_ip(ns_name, interface, ip_address):
            return False

    if gateway:
        if not await add_default_route(ns_name, gateway):
            return False

    log.info("Namespace %s is up and ready.", ns_name)
    return True


async def teardown_namespace(ns_name: str) -> None:
    """Remove namespace and associated DNS config."""
    log.info("Tearing down namespace %s", ns_name)
    remove_resolv_conf(ns_name)
    await delete_namespace(ns_name)


# ---------------------------------------------------------------------------
# Exec helper for running commands inside a namespace
# ---------------------------------------------------------------------------

async def netns_exec(
    ns_name: str,
    *cmd: str,
    timeout: float = 60.0,
    capture_output: bool = True,
) -> tuple[int, str, str]:
    """Run *cmd* inside *ns_name*. Returns (rc, stdout, stderr)."""
    full_cmd = ("ip", "netns", "exec", ns_name) + cmd
    if capture_output:
        return await _run(*full_cmd, check=False, timeout=timeout)
    proc = await asyncio.create_subprocess_exec(*full_cmd)
    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        return rc, "", ""
    except asyncio.TimeoutError:
        proc.terminate()
        return -1, "", "timeout"


# ---------------------------------------------------------------------------
# Test utility
# ---------------------------------------------------------------------------

async def test_dns_resolution(ns_name: str, hostname: str = "google.com") -> bool:
    """Return True if DNS resolution works inside *ns_name*."""
    rc, stdout, _ = await netns_exec(
        ns_name, "python3", "-c",
        f"import socket; print(socket.gethostbyname('{hostname}'))",
        timeout=10.0,
    )
    return rc == 0 and stdout.strip() != ""

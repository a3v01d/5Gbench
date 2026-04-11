"""Latency test collector: ICMP ping, TCP SYN timing, and HTTP HEAD.

Latency tests run BEFORE throughput tests in each cycle to avoid measuring
latency while the link is saturated.

All three methods run against all configured targets in each test cycle.
Jitter is derived from stddev — no extra cost.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import statistics
import time
from datetime import datetime
from typing import Any

from fivegbench.bus import EventBus
from fivegbench.config import Config, LatencyConfig, ModemConfig
from fivegbench.namespace import netns_exec
from fivegbench.session import SessionManager, SessionStatus

log = logging.getLogger(__name__)

TCP_MEASUREMENT_COUNT = 10


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# ICMP ping
# ---------------------------------------------------------------------------

async def _icmp_ping(ns_name: str, target: str, count: int) -> dict[str, Any]:
    """Run ping -c <count> inside *ns_name* against *target*.

    Returns dict with min_ms, avg_ms, max_ms, stddev_ms, packet_loss_pct.
    """
    result: dict[str, Any] = {
        "method": "icmp",
        "target": target,
        "min_ms": None,
        "avg_ms": None,
        "max_ms": None,
        "stddev_ms": None,
        "packet_loss_pct": None,
        "ttfb_ms": None,
        "raw_result": {},
    }

    rc, stdout, stderr = await netns_exec(
        ns_name,
        "ping", "-c", str(count), "-W", "2", target,
        timeout=float(count * 3 + 5),
    )

    result["raw_result"] = {"stdout": stdout, "rc": rc}

    # Parse packet loss
    loss_m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*packet loss", stdout)
    if loss_m:
        result["packet_loss_pct"] = float(loss_m.group(1))

    # Parse rtt min/avg/max/mdev from line like:
    # rtt min/avg/max/mdev = 12.345/15.678/22.123/3.456 ms
    rtt_m = re.search(
        r"rtt\s+min/avg/max/(?:mdev|stddev)\s*=\s*([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)",
        stdout,
    )
    if rtt_m:
        result["min_ms"] = float(rtt_m.group(1))
        result["avg_ms"] = float(rtt_m.group(2))
        result["max_ms"] = float(rtt_m.group(3))
        result["stddev_ms"] = float(rtt_m.group(4))

    return result


# ---------------------------------------------------------------------------
# TCP SYN latency
# ---------------------------------------------------------------------------

_TCP_PYTHON_SCRIPT = """
import socket, time, sys, json, statistics

target = sys.argv[1]
port = int(sys.argv[2])
count = int(sys.argv[3])
samples = []
for _ in range(count):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    t0 = time.perf_counter()
    try:
        err = s.connect_ex((target, port))
        t1 = time.perf_counter()
        if err == 0:
            samples.append((t1 - t0) * 1000)
    except OSError:
        pass
    finally:
        s.close()

if samples:
    print(json.dumps({
        "min_ms": min(samples),
        "avg_ms": statistics.mean(samples),
        "max_ms": max(samples),
        "stddev_ms": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "count": len(samples),
    }))
else:
    print(json.dumps({"error": "no successful connections"}))
"""


async def _tcp_ping(ns_name: str, target: str, port: int, count: int) -> dict[str, Any]:
    """Measure TCP SYN latency from inside *ns_name*."""
    import json as _json
    result: dict[str, Any] = {
        "method": "tcp",
        "target": target,
        "min_ms": None,
        "avg_ms": None,
        "max_ms": None,
        "stddev_ms": None,
        "packet_loss_pct": None,
        "ttfb_ms": None,
        "raw_result": {},
    }

    rc, stdout, stderr = await netns_exec(
        ns_name,
        "python3", "-c", _TCP_PYTHON_SCRIPT,
        target, str(port), str(count),
        timeout=float(count * 3 + 10),
    )

    if rc != 0 or not stdout.strip():
        log.warning("TCP ping failed on %s to %s:%s: %s", ns_name, target, port, stderr[:100])
        return result

    try:
        data = _json.loads(stdout.strip())
        result["raw_result"] = data
        if "error" not in data:
            result["min_ms"] = data.get("min_ms")
            result["avg_ms"] = data.get("avg_ms")
            result["max_ms"] = data.get("max_ms")
            result["stddev_ms"] = data.get("stddev_ms")
    except (_json.JSONDecodeError, TypeError):
        pass

    return result


# ---------------------------------------------------------------------------
# HTTP HEAD
# ---------------------------------------------------------------------------

async def _http_head(ns_name: str, target: str) -> dict[str, Any]:
    """Send HTTP HEAD to http://<target>/ and measure timing via curl."""
    result: dict[str, Any] = {
        "method": "http_head",
        "target": target,
        "min_ms": None,
        "avg_ms": None,
        "max_ms": None,
        "stddev_ms": None,
        "packet_loss_pct": None,
        "ttfb_ms": None,
        "raw_result": {},
    }

    fmt = "%{time_connect} %{time_starttransfer} %{time_total}"
    rc, stdout, stderr = await netns_exec(
        ns_name,
        "curl", "-o", "/dev/null", "-s",
        "-w", fmt, "-I", f"http://{target}/",
        "--connect-timeout", "5",
        "--max-time", "10",
        timeout=15.0,
    )

    if rc != 0 or not stdout.strip():
        log.warning("HTTP HEAD failed on %s to %s: %s", ns_name, target, stderr[:100])
        return result

    parts = stdout.strip().split()
    if len(parts) >= 3:
        try:
            time_connect = float(parts[0]) * 1000   # to ms
            time_ttfb = float(parts[1]) * 1000
            time_total = float(parts[2]) * 1000
            result["min_ms"] = time_connect
            result["avg_ms"] = time_ttfb
            result["max_ms"] = time_total
            result["ttfb_ms"] = time_ttfb
            result["raw_result"] = {
                "time_connect_ms": time_connect,
                "time_starttransfer_ms": time_ttfb,
                "time_total_ms": time_total,
            }
        except ValueError:
            log.debug("HTTP HEAD parse error: %r", stdout)

    return result


# ---------------------------------------------------------------------------
# Collector class
# ---------------------------------------------------------------------------

class LatencyCollector:
    """Runs periodic latency tests across all modems and publishes results."""

    def __init__(
        self,
        cfg: Config,
        bus: EventBus,
        session_mgr: SessionManager,
    ) -> None:
        self._cfg = cfg
        self._lat_cfg: LatencyConfig = cfg.latency
        self._bus = bus
        self._session = session_mgr
        self._running = False
        self._trigger: asyncio.Event = asyncio.Event()

    # ------------------------------------------------------------------
    # Control
    # ------------------------------------------------------------------

    def stop(self) -> None:
        self._running = False

    def trigger_now(self) -> None:
        self._trigger.set()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self._running = True
        interval = self._cfg.polling.throughput_interval_seconds  # same interval as throughput
        log.info("LatencyCollector: started (interval=%.0fs)", interval)

        try:
            while self._running:
                try:
                    await asyncio.wait_for(self._trigger.wait(), timeout=interval)
                    self._trigger.clear()
                except asyncio.TimeoutError:
                    pass

                if self._session.status != SessionStatus.ACTIVE:
                    continue

                await self._run_all_modems()

        except asyncio.CancelledError:
            log.debug("LatencyCollector: cancelled")
            raise
        finally:
            self._running = False

    # ------------------------------------------------------------------
    # Per-modem tests
    # ------------------------------------------------------------------

    async def _run_all_modems(self) -> None:
        for modem in self._cfg.modems:
            if self._session.status != SessionStatus.ACTIVE:
                break
            await self._test_modem(modem)

    async def _test_modem(self, modem: ModemConfig) -> None:
        ns = modem.namespace
        if not ns:
            return
        session_id = self._session.session_id
        if not session_id:
            return

        log.info("LatencyCollector: testing %s", modem.carrier)

        for target in self._lat_cfg.targets:
            for method in self._lat_cfg.methods:
                ts = _now_iso()
                result = await self._run_method(method, ns, target)
                result["modem_imei"] = modem.imei
                result["carrier"] = modem.carrier
                await self._publish(session_id, ts, result)

    async def _run_method(self, method: str, ns: str, target: str) -> dict:
        if method == "icmp":
            return await _icmp_ping(ns, target, self._lat_cfg.icmp_count)
        elif method == "tcp":
            return await _tcp_ping(ns, target, self._lat_cfg.tcp_port, TCP_MEASUREMENT_COUNT)
        elif method == "http_head":
            return await _http_head(ns, target)
        else:
            log.warning("LatencyCollector: unknown method '%s'", method)
            return {"method": method, "target": target}

    async def _publish(self, session_id: str, ts: str, data: dict) -> None:
        avg = data.get("avg_ms")
        log.info(
            "Latency[%s] %s→%s: avg=%.1fms",
            data.get("carrier"), data.get("method"), data.get("target"), avg or 0,
        )
        await self._bus.publish({
            "type": "latency",
            "timestamp": ts,
            "session_id": session_id,
            "carrier": data.get("carrier"),
            "modem_imei": data.get("modem_imei"),
            "data": data,
        })

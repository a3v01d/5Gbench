"""Throughput test collector: iperf3 (primary) with HTTP fallback.

Tests run sequentially across modems (not simultaneously) to avoid shared
backhaul contention.  Order follows config modem list.

Each test run (per modem) executes:
  1. Download test
  2. Upload test

Results are published to the event bus.

If the configured iperf3 server is unreachable (connection refused or timeout),
the collector falls back to HTTP download via curl automatically.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

from fivegbench.bus import EventBus
from fivegbench.config import Config, ModemConfig, ThroughputConfig
from fivegbench.namespace import netns_exec
from fivegbench.session import SessionManager, SessionStatus

log = logging.getLogger(__name__)

IPERF3_CONNECT_TIMEOUT = 5.0   # seconds to test if server is reachable


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# iperf3
# ---------------------------------------------------------------------------

async def _iperf3_reachable(ns_name: str, server: str, port: int) -> bool:
    """TCP-connect to iperf3 server to confirm it's reachable."""
    rc, _, _ = await netns_exec(
        ns_name,
        "python3", "-c",
        (
            f"import socket,sys;"
            f"s=socket.socket();"
            f"s.settimeout({IPERF3_CONNECT_TIMEOUT});"
            f"sys.exit(0 if s.connect_ex(('{server}',{port}))==0 else 1)"
        ),
        timeout=IPERF3_CONNECT_TIMEOUT + 2,
    )
    return rc == 0


async def _run_iperf3(
    ns_name: str,
    server: str,
    port: int,
    duration: int,
    direction: str,  # "download" or "upload"
) -> dict[str, Any]:
    """Run iperf3 and return parsed result dict."""
    args = [
        "iperf3",
        "-c", server,
        "-p", str(port),
        "-t", str(duration),
        "-J",  # JSON output
    ]
    if direction == "download":
        args.append("-R")   # reverse = server sends to client

    rc, stdout, stderr = await netns_exec(
        ns_name, *args,
        timeout=float(duration + 30),
    )

    result: dict[str, Any] = {
        "method": "iperf3",
        "direction": direction,
        "server": server,
        "bandwidth_bps": None,
        "retransmits": None,
        "duration_seconds": float(duration),
        "raw_result": {},
    }

    if rc != 0 or not stdout.strip():
        log.warning("iperf3 %s failed on %s: %s", direction, ns_name, stderr[:200])
        return result

    try:
        data = json.loads(stdout)
        result["raw_result"] = data
        end = data.get("end", {})
        if direction == "download":
            stream = end.get("sum_received", {})
        else:
            stream = end.get("sum_sent", {})
        result["bandwidth_bps"] = stream.get("bits_per_second")
        result["retransmits"] = stream.get("retransmits")
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        log.warning("iperf3 JSON parse error: %s", exc)

    return result


# ---------------------------------------------------------------------------
# HTTP fallback
# ---------------------------------------------------------------------------

async def _run_http_download(ns_name: str, url: str) -> dict[str, Any]:
    """HTTP download via curl with structured timing output."""
    curl_format = '{"speed_download":%{speed_download},"time_total":%{time_total},"size_download":%{size_download}}'
    rc, stdout, stderr = await netns_exec(
        ns_name,
        "curl", "-o", "/dev/null", "-s", "-w", curl_format, url,
        timeout=120.0,
    )

    result: dict[str, Any] = {
        "method": "http",
        "direction": "download",
        "server": url,
        "bandwidth_bps": None,
        "retransmits": None,
        "duration_seconds": None,
        "raw_result": {},
    }

    if rc != 0:
        log.warning("HTTP download failed on %s: %s", ns_name, stderr[:200])
        return result

    try:
        data = json.loads(stdout.strip())
        result["raw_result"] = data
        # curl speed_download is in bytes/sec — convert to bits/sec
        bps = data.get("speed_download", 0) * 8
        result["bandwidth_bps"] = bps
        result["duration_seconds"] = data.get("time_total")
    except (json.JSONDecodeError, TypeError) as exc:
        log.warning("HTTP curl parse error: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Collector class
# ---------------------------------------------------------------------------

class ThroughputCollector:
    """Runs periodic throughput tests across all modems and publishes results."""

    def __init__(
        self,
        cfg: Config,
        bus: EventBus,
        session_mgr: SessionManager,
    ) -> None:
        self._cfg = cfg
        self._tp_cfg: ThroughputConfig = cfg.throughput
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
        """Request an immediate (on-demand) test run."""
        self._trigger.set()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self._running = True
        interval = self._cfg.polling.throughput_interval_seconds
        log.info("ThroughputCollector: started (interval=%.0fs)", interval)

        try:
            while self._running:
                # Wait for interval OR an on-demand trigger
                try:
                    await asyncio.wait_for(self._trigger.wait(), timeout=interval)
                    self._trigger.clear()
                except asyncio.TimeoutError:
                    pass

                if self._session.status != SessionStatus.ACTIVE:
                    continue

                await self._run_all_modems()

        except asyncio.CancelledError:
            log.debug("ThroughputCollector: cancelled")
            raise
        finally:
            self._running = False

    # ------------------------------------------------------------------
    # Per-modem tests
    # ------------------------------------------------------------------

    async def _run_all_modems(self) -> None:
        """Run download + upload for each modem sequentially."""
        for modem in self._cfg.modems:
            if self._session.status != SessionStatus.ACTIVE:
                break
            await self._test_modem(modem)

    async def _test_modem(self, modem: ModemConfig) -> None:
        ns = modem.namespace
        if not ns:
            log.warning("ThroughputCollector: no namespace for %s", modem.carrier)
            return

        log.info("ThroughputCollector: testing %s", modem.carrier)

        # Determine method
        use_iperf3 = (
            self._tp_cfg.method == "iperf3"
            and self._tp_cfg.iperf3_server
        )
        if use_iperf3:
            reachable = await _iperf3_reachable(
                ns, self._tp_cfg.iperf3_server, self._tp_cfg.iperf3_port
            )
            if not reachable:
                log.warning(
                    "ThroughputCollector[%s]: iperf3 server unreachable, falling back to HTTP",
                    modem.carrier,
                )
                use_iperf3 = False

        session_id = self._session.session_id
        if not session_id:
            return

        if use_iperf3:
            for direction in ("download", "upload"):
                ts = _now_iso()
                result = await _run_iperf3(
                    ns,
                    self._tp_cfg.iperf3_server,
                    self._tp_cfg.iperf3_port,
                    self._tp_cfg.iperf3_duration_seconds,
                    direction,
                )
                result["modem_imei"] = modem.imei
                result["carrier"] = modem.carrier
                await self._publish(session_id, ts, result)
        else:
            ts = _now_iso()
            result = await _run_http_download(ns, self._tp_cfg.http_fallback_url)
            result["modem_imei"] = modem.imei
            result["carrier"] = modem.carrier
            await self._publish(session_id, ts, result)

    async def _publish(self, session_id: str, ts: str, data: dict) -> None:
        mbps = (data.get("bandwidth_bps") or 0) / 1e6
        log.info(
            "Throughput[%s] %s %s: %.1f Mbps",
            data.get("carrier"), data.get("method"), data.get("direction"), mbps,
        )
        await self._bus.publish({
            "type": "throughput",
            "timestamp": ts,
            "session_id": session_id,
            "carrier": data.get("carrier"),
            "modem_imei": data.get("modem_imei"),
            "data": data,
        })

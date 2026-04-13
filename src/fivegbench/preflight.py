"""Pre-flight hardware and configuration validation.

Run with: 5gbench preflight

Checks each item and prints ✅ / ❌ with a one-line detail.
Exit code 0 if all checks pass, 1 if any fail.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import sys
from pathlib import Path
from typing import Callable

from fivegbench.config import Config

# ANSI colors (disabled if not a tty)
_COLORS = sys.stdout.isatty()
GREEN = "\033[92m" if _COLORS else ""
RED = "\033[91m" if _COLORS else ""
YELLOW = "\033[93m" if _COLORS else ""
RESET = "\033[0m" if _COLORS else ""

PASS = f"{GREEN}✅ PASS{RESET}"
FAIL = f"{RED}❌ FAIL{RESET}"
WARN = f"{YELLOW}⚠️  WARN{RESET}"


def _print_result(label: str, ok: bool | None, detail: str) -> None:
    icon = PASS if ok is True else (WARN if ok is None else FAIL)
    print(f"  {icon}  {label:<45} {detail}")


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

async def check_config(cfg: Config) -> tuple[bool, str]:
    """Config file exists, parsed, and has required fields."""
    if not cfg.modems:
        return None, "No modems defined — running in no-hardware / test mode"
    for m in cfg.modems:
        if not m.imei or not m.carrier or not m.apn:
            return False, f"Modem {m.carrier!r} missing imei/carrier/apn"
    return True, f"{len(cfg.modems)} modem(s) defined"


async def check_disk_space(cfg: Config) -> tuple[bool | None, str]:
    """Sufficient disk space in db_path (warn if < 1GB)."""
    try:
        stat = shutil.disk_usage(cfg.general.db_path)
        free_gb = stat.free / 1e9
        if free_gb < 1.0:
            return None, f"Only {free_gb:.1f} GB free in {cfg.general.db_path}"
        return True, f"{free_gb:.1f} GB free in {cfg.general.db_path}"
    except OSError as exc:
        return False, str(exc)


async def check_system_tools() -> tuple[bool, str]:
    """Required system tools (ip, mmcli, iperf3, ping, curl) are on PATH."""
    required = ["ip", "mmcli", "iperf3", "ping", "curl", "python3"]
    missing = [t for t in required if not shutil.which(t)]
    if missing:
        return False, f"Missing: {', '.join(missing)}"
    return True, "All tools found: " + ", ".join(required)


async def check_modem_detection(cfg: Config) -> tuple[bool, str]:
    """All IMEIs in config are discoverable via USB."""
    try:
        from fivegbench.modem.discovery import discover_modems
        target_imeis = [m.imei for m in cfg.modems]
        found = await discover_modems(target_imeis)
        missing = [i for i in target_imeis if i not in found]
        if missing:
            return False, f"Not found: {missing}"
        return True, f"Found {len(found)}/{len(target_imeis)} modems"
    except Exception as exc:
        return False, f"Discovery error: {exc}"


async def check_at_port_access(cfg: Config) -> tuple[bool, str]:
    """Secondary AT ports are accessible and respond to 'AT'."""
    try:
        from fivegbench.modem.discovery import discover_modems
        from fivegbench.modem.serial import ATSerial
        target_imeis = [m.imei for m in cfg.modems]
        found = await discover_modems(target_imeis)
        failures: list[str] = []
        for imei, info in found.items():
            port = info.get("at_port")
            if not port:
                failures.append(f"{imei}: no port")
                continue
            try:
                async with ATSerial(port) as at:
                    ok = await at.ping()
                    if not ok:
                        failures.append(f"{imei}:{port} no OK")
            except Exception as exc:
                failures.append(f"{imei}:{port} {exc}")
        if failures:
            return False, "; ".join(failures)
        return True, f"All {len(found)} AT ports respond OK"
    except Exception as exc:
        return False, str(exc)


async def check_imei_match(cfg: Config) -> tuple[bool, str]:
    """AT+GSN IMEI matches config for each modem."""
    try:
        from fivegbench.modem.discovery import discover_modems
        from fivegbench.modem.serial import ATSerial
        target_imeis = [m.imei for m in cfg.modems]
        found = await discover_modems(target_imeis)
        mismatches: list[str] = []
        for modem_cfg in cfg.modems:
            info = found.get(modem_cfg.imei)
            if not info:
                mismatches.append(f"{modem_cfg.carrier}: not found")
                continue
            async with ATSerial(info["at_port"]) as at:
                imei = await at.get_imei()
                if imei != modem_cfg.imei:
                    mismatches.append(f"{modem_cfg.carrier}: got {imei}")
        if mismatches:
            return False, "; ".join(mismatches)
        return True, "All IMEIs match config"
    except Exception as exc:
        return False, str(exc)


async def check_sim_present(cfg: Config) -> tuple[bool, str]:
    """SIM cards present and returning ICCID."""
    try:
        from fivegbench.modem.discovery import discover_modems
        from fivegbench.modem.serial import ATSerial
        target_imeis = [m.imei for m in cfg.modems]
        found = await discover_modems(target_imeis)
        results: list[str] = []
        failures: list[str] = []
        for modem_cfg in cfg.modems:
            info = found.get(modem_cfg.imei)
            if not info:
                failures.append(modem_cfg.carrier)
                continue
            try:
                async with ATSerial(info["at_port"]) as at:
                    iccid = await at.get_iccid()
                    results.append(f"{modem_cfg.carrier}:{iccid[-4:]}")
            except Exception as exc:
                failures.append(f"{modem_cfg.carrier}:{exc}")
        if failures:
            return False, f"SIM issues: {', '.join(failures)}"
        return True, f"ICCIDs: {', '.join(results)}"
    except Exception as exc:
        return False, str(exc)


async def check_network_registration(cfg: Config) -> tuple[bool, str]:
    """Modems are registered to their carriers via AT+COPS?."""
    try:
        from fivegbench.modem.discovery import discover_modems
        from fivegbench.modem.serial import ATSerial
        target_imeis = [m.imei for m in cfg.modems]
        found = await discover_modems(target_imeis)
        results: list[str] = []
        failures: list[str] = []
        for modem_cfg in cfg.modems:
            info = found.get(modem_cfg.imei)
            if not info:
                failures.append(modem_cfg.carrier)
                continue
            try:
                async with ATSerial(info["at_port"]) as at:
                    operator = await at.get_operator()
                    if operator:
                        results.append(f"{modem_cfg.carrier}:{operator!r}")
                    else:
                        failures.append(f"{modem_cfg.carrier}:not registered")
            except Exception as exc:
                failures.append(f"{modem_cfg.carrier}:{exc}")
        if failures:
            return False, "; ".join(failures)
        return True, "; ".join(results)
    except Exception as exc:
        return False, str(exc)


async def check_mm_connected(cfg: Config) -> tuple[bool, str]:
    """ModemManager shows connected or registered state for each modem."""
    try:
        from fivegbench.modem.manager import find_modem_by_imei, get_modem_state, ModemState
        results: list[str] = []
        warnings: list[str] = []
        failures: list[str] = []
        for modem_cfg in cfg.modems:
            idx = await find_modem_by_imei(modem_cfg.imei)
            if not idx:
                failures.append(f"{modem_cfg.carrier}: not in MM")
                continue
            state = await get_modem_state(idx)
            if state in (ModemState.CONNECTED, ModemState.REGISTERED):
                results.append(f"{modem_cfg.carrier}:{state.value}")
            elif state == ModemState.DISABLED:
                warnings.append(
                    f"{modem_cfg.carrier}:disabled (run: mmcli -m {idx} --enable)"
                )
            elif state in (ModemState.DISCONNECTED, ModemState.CONNECTING):
                warnings.append(f"{modem_cfg.carrier}:{state.value}")
            else:
                # FAILED, UNKNOWN
                failures.append(f"{modem_cfg.carrier}:{state.value}")
        if failures:
            return False, "; ".join(failures + warnings)
        if warnings:
            return None, "; ".join(warnings + results)
        return True, "; ".join(results)
    except Exception as exc:
        return False, str(exc)


async def check_namespace_ops() -> tuple[bool, str]:
    """Can create and delete a test namespace."""
    from fivegbench.namespace import create_namespace, delete_namespace
    test_ns = "ns_preflight_test"
    try:
        created = await create_namespace(test_ns)
        if not created:
            return False, "Failed to create test namespace (need root?)"
        deleted = await delete_namespace(test_ns)
        if not deleted:
            return False, "Failed to delete test namespace"
        return True, "Namespace create/delete OK"
    except Exception as exc:
        return False, str(exc)


async def check_gnss(cfg: Config) -> tuple[bool, str]:
    """Primary GNSS modem is acquiring or has a fix."""
    # Early-return paths that don't need hardware imports
    if not cfg.modems:
        return True, "No modems configured — GNSS check skipped"
    primary_carrier = cfg.gnss.primary_modem
    if not primary_carrier:
        primary_modem = cfg.modems[0]   # auto-select first
    else:
        primary_modem = next((m for m in cfg.modems if m.carrier == primary_carrier), None)
    if not primary_modem:
        return False, f"gnss.primary_modem '{primary_carrier}' not found in modems list"
    try:
        from fivegbench.modem.discovery import discover_modems
        from fivegbench.modem.serial import ATSerial, ATError
        from fivegbench.modem.parser import parse_gnss_fix
        found = await discover_modems([primary_modem.imei])
        info = found.get(primary_modem.imei)
        if not info:
            return False, "Primary GNSS modem not found"
        async with ATSerial(info["at_port"]) as at:
            await at.enable_gnss()
            await asyncio.sleep(1.0)  # brief wait for GNSS to respond
            try:
                resp = await at.get_gnss_fix()
            except ATError as exc:
                # CME ERROR 516 = "Not fixed now" — engine is active but no fix yet
                if "516" in str(exc):
                    return None, "GNSS engine active, no fix yet (CME ERROR 516) — normal if recently powered on"
                raise
            fix = parse_gnss_fix(resp)
            if fix.get("fix_type") in ("2d", "3d"):
                return True, f"Fix: {fix['fix_type']}, sats={fix.get('satellites')}"
            else:
                return None, f"No fix yet (fix_type={fix.get('fix_type')})"
    except Exception as exc:
        return False, str(exc)


async def check_iperf3_reachable(cfg: Config) -> tuple[bool, str]:
    """iperf3 server TCP-reachable from each namespace (if configured)."""
    if cfg.throughput.method != "iperf3" or not cfg.throughput.iperf3_server:
        return True, "iperf3 not configured — skipped"
    server = cfg.throughput.iperf3_server
    port = cfg.throughput.iperf3_port
    failures: list[str] = []
    successes: list[str] = []
    for modem in cfg.modems:
        ns = modem.namespace
        if not ns:
            failures.append(f"{modem.carrier}: no namespace")
            continue
        try:
            from fivegbench.namespace import netns_exec
            rc, _, _ = await netns_exec(
                ns, "python3", "-c",
                f"import socket,sys;s=socket.socket();s.settimeout(5);sys.exit(0 if s.connect_ex(('{server}',{port}))==0 else 1)",
                timeout=10.0,
            )
            if rc == 0:
                successes.append(modem.carrier)
            else:
                failures.append(f"{modem.carrier}: refused")
        except Exception as exc:
            failures.append(f"{modem.carrier}: {exc}")
    if failures:
        return False, f"Unreachable from: {', '.join(failures)}"
    return True, f"Reachable from: {', '.join(successes)}"


# ---------------------------------------------------------------------------
# Run all checks
# ---------------------------------------------------------------------------

async def run_preflight(cfg: Config, skip_hardware: bool = False) -> bool:
    """Run all preflight checks. Returns True if all pass."""
    print(f"\n5gbench preflight check\n{'─' * 60}")

    checks: list[tuple[str, bool]] = []

    async def run(label: str, coro, *args) -> bool:
        try:
            result = await coro(*args)
            ok, detail = result if isinstance(result, tuple) else (result, "")
        except Exception as exc:
            ok, detail = False, f"Exception: {exc}"
        _print_result(label, ok, detail)
        checks.append((label, ok is True or ok is None))
        return ok is not False

    await run("Config file valid", check_config, cfg)
    await run("Disk space in db_path", check_disk_space, cfg)
    await run("System tools available", check_system_tools)

    if not skip_hardware:
        await run("Modem USB detection", check_modem_detection, cfg)
        await run("AT port access", check_at_port_access, cfg)
        await run("IMEI match", check_imei_match, cfg)
        await run("SIM cards present", check_sim_present, cfg)
        await run("Network registration", check_network_registration, cfg)
        await run("ModemManager connected", check_mm_connected, cfg)
        await run("Namespace operations", check_namespace_ops)
        await run("GNSS fix status", check_gnss, cfg)
        await run("iperf3 server reachable", check_iperf3_reachable, cfg)

    passed = sum(1 for _, ok in checks if ok)
    total = len(checks)
    all_ok = all(ok for _, ok in checks)

    print(f"\n{'─' * 60}")
    print(f"  Results: {passed}/{total} checks passed")
    if all_ok:
        print(f"  {GREEN}All checks passed. Ready to run 5gbench.{RESET}\n")
    else:
        print(f"  {RED}Some checks failed. Resolve issues before running 5gbench.{RESET}\n")

    return all_ok

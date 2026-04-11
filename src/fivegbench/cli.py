"""Command-line interface for 5gbench.

Commands:
    5gbench start [--quiet] [--config PATH] [--debug]
        Start data collection.  In TUI mode, prompts for session metadata.
        In quiet mode, uses config defaults without prompting.

    5gbench stop
        Send SIGTERM to a running instance via PID file.

    5gbench preflight [--config PATH] [--no-hardware]
        Run pre-flight hardware and configuration checks.

PID file: ~/.config/5gbench/5gbench.pid
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import NoReturn

from fivegbench import __version__
from fivegbench.config import CONFIG_PATH, Config, ensure_dirs, load

log = logging.getLogger(__name__)

PID_FILE = Path("~/.config/5gbench/5gbench.pid").expanduser()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(cfg: Config, debug: bool = False, verbose: bool = False) -> None:
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    level = logging.DEBUG if debug else level_map.get(cfg.general.log_level, logging.WARNING)
    if verbose and level > logging.INFO:
        level = logging.INFO

    cfg.general.log_path.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    log_file = cfg.general.log_path / f"5gbench_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    handlers: list[logging.Handler] = [
        logging.FileHandler(log_file),
    ]
    if debug:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(level=level, format=fmt, handlers=handlers)


# ---------------------------------------------------------------------------
# PID file management
# ---------------------------------------------------------------------------

def _write_pid() -> None:
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))


def _remove_pid() -> None:
    try:
        PID_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def _read_pid() -> int | None:
    try:
        return int(PID_FILE.read_text().strip())
    except (OSError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Session metadata prompt (TUI mode)
# ---------------------------------------------------------------------------

def _prompt_session_metadata(cfg: Config) -> dict[str, str]:
    """Interactive prompts for session metadata. Returns dict of values."""
    print("\n5gbench — New Session")
    print("─" * 40)

    def _ask(label: str, default: str) -> str:
        hint = f" [{default}]" if default else ""
        try:
            val = input(f"  {label}{hint}: ").strip()
        except (EOFError, KeyboardInterrupt):
            val = ""
        return val or default

    return {
        "operator": _ask("Operator name", cfg.session_defaults.operator),
        "vehicle_id": _ask("Vehicle ID", cfg.session_defaults.vehicle_id),
        "route_description": _ask("Route description", cfg.session_defaults.route_description),
        "notes": _ask("Notes", cfg.session_defaults.notes),
    }


# ---------------------------------------------------------------------------
# Core application runner
# ---------------------------------------------------------------------------

async def _run_app(cfg: Config, quiet: bool) -> None:
    """Main async application entry point."""
    from fivegbench.bus import EventBus
    from fivegbench.db import SQLiteWriter
    from fivegbench.session import SessionManager
    from fivegbench.modem.discovery import discover_modems, identify_net_interface
    from fivegbench.modem.manager import (
        find_modem_by_imei, connect_modem, build_modem_info, ModemState,
    )
    from fivegbench.namespace import setup_namespace, teardown_namespace
    from fivegbench.collectors.rf import RFCollector
    from fivegbench.collectors.gnss import GNSSCollector
    from fivegbench.collectors.throughput import ThroughputCollector
    from fivegbench.collectors.latency import LatencyCollector

    # ── Event bus setup ────────────────────────────────────────────
    bus = EventBus()
    bus.register_consumer("sqlite")
    if not quiet and cfg.tui.enabled:
        bus.register_consumer("tui")

    # ── SQLite writer ──────────────────────────────────────────────
    sqlite_writer = SQLiteWriter(cfg.general.db_path, bus._consumers["sqlite"])

    # ── Session manager ────────────────────────────────────────────
    session_mgr = SessionManager(bus, cfg)

    # ── Gather session metadata ────────────────────────────────────
    if quiet or not cfg.tui.enabled:
        meta: dict[str, str] = {
            "operator": cfg.session_defaults.operator,
            "vehicle_id": cfg.session_defaults.vehicle_id,
            "route_description": cfg.session_defaults.route_description,
            "notes": cfg.session_defaults.notes,
        }
    else:
        meta = _prompt_session_metadata(cfg)

    # ── Modem discovery ────────────────────────────────────────────
    target_imeis = [m.imei for m in cfg.modems]
    if not quiet:
        print("Discovering modems…")
    found = await discover_modems(target_imeis)

    # Apply discovered ports to modem configs
    for modem in cfg.modems:
        info = found.get(modem.imei, {})
        modem.at_port = info.get("at_port", "")
        if not modem.at_port:
            log.warning("No AT port found for %s (%s)", modem.carrier, modem.imei)

    # ── ModemManager: connect and get network interfaces ───────────
    for modem in cfg.modems:
        mm_idx = await find_modem_by_imei(modem.imei)
        if mm_idx:
            mm_info = await build_modem_info(mm_idx, modem.imei)
            if mm_info and mm_info.state != ModemState.CONNECTED:
                await connect_modem(mm_idx, modem.apn)
                mm_info = await build_modem_info(mm_idx, modem.imei)
            if mm_info:
                modem.net_interface = mm_info.bearer_interface
                # Set up network namespace
                if modem.net_interface:
                    ok = await setup_namespace(
                        modem.namespace,
                        modem.net_interface,
                        ip_address=mm_info.ip_address,
                        gateway=mm_info.gateway,
                        nameservers=cfg.general.dns.nameservers,
                    )
                    if not ok:
                        log.error("Namespace setup failed for %s", modem.carrier)
        else:
            log.warning("ModemManager does not know modem %s", modem.imei)

    # ── Collectors ─────────────────────────────────────────────────
    rf_collectors = [
        RFCollector(modem, cfg.polling, bus, session_mgr)
        for modem in cfg.modems
        if modem.at_port
    ]
    gnss_collector = GNSSCollector(cfg, bus, session_mgr)
    throughput_collector = ThroughputCollector(cfg, bus, session_mgr)
    latency_collector = LatencyCollector(cfg, bus, session_mgr)

    # ── TUI ────────────────────────────────────────────────────────
    tui = None
    if not quiet and cfg.tui.enabled:
        from fivegbench.tui.dashboard import Dashboard
        tui = Dashboard(cfg, bus, session_mgr)

    # ── Start session ──────────────────────────────────────────────
    await session_mgr.start(**meta)

    if not quiet:
        print(f"\nSession started: {session_mgr.session_id}")
        print("Press Q to quit, T for throughput test, L for latency test\n")
    else:
        print(f"5gbench started — session {session_mgr.session_id}")

    # ── Launch all tasks ───────────────────────────────────────────
    tasks: list[asyncio.Task] = [
        asyncio.create_task(bus.dispatch(), name="bus_dispatch"),
        asyncio.create_task(sqlite_writer.run(), name="sqlite_writer"),
        asyncio.create_task(gnss_collector.run(), name="gnss"),
        asyncio.create_task(throughput_collector.run(), name="throughput"),
        asyncio.create_task(latency_collector.run(), name="latency"),
    ]
    for rf in rf_collectors:
        tasks.append(asyncio.create_task(rf.run(), name=f"rf_{rf.carrier}"))

    if tui:
        tasks.append(asyncio.create_task(tui.run(), name="tui"))

    # ── Signal handling ────────────────────────────────────────────
    stop_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("Received signal %s — shutting down", sig)
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    # ── Wait for stop ──────────────────────────────────────────────
    await stop_event.wait()

    # ── Graceful shutdown ──────────────────────────────────────────
    log.info("Shutting down…")
    await session_mgr.stop()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Teardown namespaces
    for modem in cfg.modems:
        if modem.namespace:
            await teardown_namespace(modem.namespace)

    if not quiet:
        print(f"\nSession stopped: {session_mgr.session_id}")
    else:
        print(f"5gbench stopped — session {session_mgr.session_id}")


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_start(args: argparse.Namespace) -> None:
    """Handle: 5gbench start"""
    cfg = _load_config(args)
    _setup_logging(cfg, debug=args.debug, verbose=getattr(args, "verbose", False))
    ensure_dirs(cfg)
    quiet = args.quiet or not cfg.tui.enabled

    _write_pid()
    try:
        asyncio.run(_run_app(cfg, quiet))
    except KeyboardInterrupt:
        pass
    finally:
        _remove_pid()


def cmd_stop(args: argparse.Namespace) -> None:
    """Handle: 5gbench stop — send SIGTERM to running instance."""
    pid = _read_pid()
    if pid is None:
        print("5gbench does not appear to be running (no PID file).")
        sys.exit(1)
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to 5gbench (PID {pid})")
    except ProcessLookupError:
        print(f"No process with PID {pid} — removing stale PID file.")
        _remove_pid()
        sys.exit(1)
    except PermissionError:
        print(f"Permission denied sending signal to PID {pid}.")
        sys.exit(1)


def cmd_preflight(args: argparse.Namespace) -> None:
    """Handle: 5gbench preflight"""
    cfg = _load_config(args)
    _setup_logging(cfg, debug=getattr(args, "debug", False))

    from fivegbench.preflight import run_preflight
    skip_hw = getattr(args, "no_hardware", False)
    ok = asyncio.run(run_preflight(cfg, skip_hardware=skip_hw))
    sys.exit(0 if ok else 1)


def _load_config(args: argparse.Namespace) -> Config:
    config_path = getattr(args, "config", None) or CONFIG_PATH
    try:
        return load(Path(config_path))
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except (ValueError, Exception) as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="5gbench",
        description="5G Tri-Carrier Drive-Test Benchmarking Tool",
    )
    parser.add_argument(
        "--version", action="version", version=f"5gbench {__version__}"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── start ──────────────────────────────────────────────────────
    p_start = sub.add_parser("start", help="Start data collection")
    p_start.add_argument(
        "--config", metavar="PATH",
        help=f"Config file path (default: {CONFIG_PATH})",
    )
    p_start.add_argument(
        "--quiet", "-q", action="store_true",
        help="No TUI; use config defaults without prompting (headless mode)",
    )
    p_start.add_argument(
        "--debug", action="store_true",
        help="Enable debug logging to stdout and file",
    )
    p_start.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable info-level logging",
    )
    p_start.set_defaults(func=cmd_start)

    # ── stop ───────────────────────────────────────────────────────
    p_stop = sub.add_parser("stop", help="Stop a running 5gbench instance")
    p_stop.set_defaults(func=cmd_stop)

    # ── preflight ──────────────────────────────────────────────────
    p_pf = sub.add_parser("preflight", help="Run hardware and configuration checks")
    p_pf.add_argument("--config", metavar="PATH")
    p_pf.add_argument(
        "--no-hardware", action="store_true",
        help="Skip hardware checks (config and disk only)",
    )
    p_pf.add_argument("--debug", action="store_true")
    p_pf.set_defaults(func=cmd_preflight)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

"""TUI dashboard using Rich Live display.

Shows a continuously-updated 3-column modem status panel, GNSS status bar,
and session header.  Hotkeys are handled via a background keyboard reader.

Layout:
  ┌─────────────────────────────────────────────────────────────────┐
  │ SESSION: 20260410_143022  Elapsed: 00:12:34  Operator: J.Smith  │
  │ Status: ● REC                                                   │
  ├──────────────┬──────────────┬─────────────────────────────────-─┤
  │  AT&T        │  T-Mobile    │  Verizon                          │
  │  NR5G-NSA   │  NR5G-SA    │  LTE                              │
  │  B66/n260   │  n71        │  B13                              │
  │  RSRP -85   │  RSRP -92   │  RSRP -105                        │
  │  RSRQ -10   │  RSRQ -12   │  RSRQ -14                         │
  │  SINR  18   │  SINR  12   │  SINR   5                         │
  │  DL  423 Mbps│  DL 287 Mbps│  DL  58 Mbps                      │
  │  Lat  15 ms  │  Lat  22 ms │  Lat  48 ms                       │
  ├──────────────┴──────────────┴────────────────────────────────---┤
  │ GNSS: 3D fix  Sats:12  HDOP:0.9  34.0557°N 117.8320°W  12 km/h │
  ├─────────────────────────────────────────────────────────────────┤
  │ [S]tart [P]ause [R]esume [Q]uit [T]hroughput [L]atency         │
  └─────────────────────────────────────────────────────────────────┘

Quiet mode (tui.enabled=false or --quiet): no TUI rendered.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from fivegbench.bus import EventBus
from fivegbench.config import Config
from fivegbench.session import SessionManager, SessionStatus

log = logging.getLogger(__name__)

# Signal quality color thresholds
# RSRP: good >= -80, ok >= -100, poor < -100
# RSRQ: good >= -10, ok >= -15, poor < -15
# SINR: good >= 20, ok >= 10, poor < 10

REFRESH_RATE = 2.0   # Hz


def _rsrp_style(v: float | None) -> str:
    if v is None:
        return "dim"
    if v >= -80:
        return "bold green"
    if v >= -100:
        return "yellow"
    return "bold red"


def _rsrq_style(v: float | None) -> str:
    if v is None:
        return "dim"
    if v >= -10:
        return "bold green"
    if v >= -15:
        return "yellow"
    return "bold red"


def _sinr_style(v: float | None) -> str:
    if v is None:
        return "dim"
    if v >= 20:
        return "bold green"
    if v >= 10:
        return "yellow"
    return "bold red"


def _fmt_float(v: float | None, unit: str = "", decimals: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}{unit}"


def _fmt_mbps(bps: float | None) -> str:
    if bps is None:
        return "—"
    return f"{bps / 1e6:.1f} Mbps"


def _fmt_elapsed(start: float) -> str:
    secs = int(time.monotonic() - start)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


class Dashboard:
    """Rich Live TUI dashboard.

    Consumes messages from the event bus to update per-carrier state, then
    renders on a timer.
    """

    def __init__(
        self,
        cfg: Config,
        bus: EventBus,
        session_mgr: SessionManager,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._session = session_mgr
        self._console = Console()
        self._running = False

        # Per-carrier live state
        self._rf: dict[str, dict] = {m.carrier: {} for m in cfg.modems}
        self._tp: dict[str, dict] = {m.carrier: {} for m in cfg.modems}  # last throughput
        self._lat: dict[str, dict] = {m.carrier: {} for m in cfg.modems}  # last latency
        self._gnss: dict[str, Any] = {}
        self._last_log: str = ""
        self._session_start_mono: float | None = None

        # Carrier label lookup
        self._labels: dict[str, str] = {m.carrier: m.label for m in cfg.modems}
        self._carrier_order: list[str] = [m.carrier for m in cfg.modems]

        # Hotkey callbacks (set by CLI layer) — plain callables (no args)
        self.on_start: "Any | None" = None
        self.on_pause: "Any | None" = None
        self.on_resume: "Any | None" = None
        self.on_quit: "Any | None" = None
        self.on_throughput: "Any | None" = None  # called with no args
        self.on_latency: "Any | None" = None     # called with no args

    # ------------------------------------------------------------------
    # Message consumer
    # ------------------------------------------------------------------

    async def consume(self) -> None:
        """Read from the TUI consumer queue and update local state."""
        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(
                        self._bus.get("tui"), timeout=0.2
                    )
                except asyncio.TimeoutError:
                    continue
                self._apply_message(msg)
                self._bus.task_done("tui")
        except asyncio.CancelledError:
            raise

    def _apply_message(self, msg: dict) -> None:
        t = msg.get("type")
        carrier = msg.get("carrier", "")
        data = msg.get("data", {})

        if t == "rf_telemetry" and carrier in self._rf:
            self._rf[carrier] = data
        elif t == "throughput" and carrier in self._tp:
            # Keep the last result per direction
            direction = data.get("direction", "download")
            self._tp[carrier][direction] = data
        elif t == "latency" and carrier in self._lat:
            method = data.get("method", "icmp")
            target = data.get("target", "")
            self._lat[carrier][f"{method}_{target}"] = data
        elif t == "gnss":
            self._gnss = data
        elif t == "session_event":
            ev = data.get("event")
            if ev == "start":
                self._session_start_mono = time.monotonic()
                self._last_log = f"Session started: {data.get('session_id')}"
            elif ev == "stop":
                self._last_log = "Session stopped."
        elif t == "modem_event":
            self._last_log = (
                f"[{carrier}] {data.get('event_type','')} — {data.get('detail','')}"
            )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_header(self) -> Panel:
        session = self._session.current
        session_id = session.session_id if session else "—"
        elapsed = (
            _fmt_elapsed(self._session_start_mono)
            if self._session_start_mono
            else "—"
        )
        operator = session.operator if session else ""
        status_str = {
            SessionStatus.ACTIVE: "[bold green]● REC[/]",
            SessionStatus.PAUSED: "[bold yellow]‖ PAUSED[/]",
            SessionStatus.STOPPED: "[dim]○ STOPPED[/]",
            SessionStatus.IDLE: "[dim]○ IDLE[/]",
        }.get(self._session.status, "○ IDLE")

        text = Text.assemble(
            ("SESSION: ", "bold"),
            (session_id, "cyan"),
            ("  Elapsed: ", "bold"),
            (elapsed, "cyan"),
            ("  Operator: ", "bold"),
            (operator or "—", "cyan"),
            ("  Status: ", "bold"),
        )
        return Panel(
            Text.from_markup(str(text) + status_str),
            border_style="blue",
        )

    def _render_modem_panel(self, carrier: str) -> Panel:
        label = self._labels.get(carrier, carrier)
        rf = self._rf.get(carrier, {})
        tp = self._tp.get(carrier, {})
        lat = self._lat.get(carrier, {})

        # Connection state
        tech = rf.get("access_technology") or "—"
        band = rf.get("band") or "—"
        rsrp = rf.get("rsrp")
        rsrq = rf.get("rsrq")
        sinr = rf.get("sinr")
        pci = rf.get("pci")

        # Last throughput
        dl_bps = (tp.get("download") or {}).get("bandwidth_bps")
        ul_bps = (tp.get("upload") or {}).get("bandwidth_bps")

        # Best latency (ICMP avg across targets)
        lat_vals = [
            v.get("avg_ms")
            for k, v in lat.items()
            if k.startswith("icmp_") and v.get("avg_ms") is not None
        ]
        lat_avg = sum(lat_vals) / len(lat_vals) if lat_vals else None

        table = Table.grid(padding=(0, 1))
        table.add_column(justify="right", style="dim", width=10)
        table.add_column()

        table.add_row("Tech", f"[bold]{tech}[/]")
        table.add_row("Band", band)
        if pci is not None:
            table.add_row("PCI", str(pci))
        table.add_row(
            "RSRP",
            Text(f"{_fmt_float(rsrp, ' dBm')}", style=_rsrp_style(rsrp)),
        )
        table.add_row(
            "RSRQ",
            Text(f"{_fmt_float(rsrq, ' dB')}", style=_rsrq_style(rsrq)),
        )
        table.add_row(
            "SINR",
            Text(f"{_fmt_float(sinr, ' dB')}", style=_sinr_style(sinr)),
        )
        table.add_row("DL", _fmt_mbps(dl_bps))
        table.add_row("UL", _fmt_mbps(ul_bps))
        table.add_row("Latency", f"{_fmt_float(lat_avg, ' ms')} avg")

        # Panel border color by tech
        border = {
            "NR5G-SA": "bright_green",
            "NR5G-NSA": "green",
            "LTE": "yellow",
        }.get(tech, "white")

        return Panel(table, title=f"[bold]{label}[/]", border_style=border)

    def _render_gnss_bar(self) -> Panel:
        g = self._gnss
        if not g:
            return Panel("[dim]GNSS: waiting for fix…[/]", border_style="dim")
        fix_type = g.get("fix_type", "no_fix")
        lat = g.get("latitude")
        lon = g.get("longitude")
        sats = g.get("satellites") or 0
        hdop = g.get("hdop")
        speed = g.get("speed_kmh")
        source = g.get("source_modem", "")

        fix_style = {
            "3d": "bold green",
            "2d": "yellow",
            "no_fix": "bold red",
            "interpolated": "yellow italic",
        }.get(fix_type, "white")

        coord_str = (
            f"{abs(lat):.6f}°{'N' if (lat or 0) >= 0 else 'S'} "
            f"{abs(lon):.6f}°{'E' if (lon or 0) >= 0 else 'W'}"
            if lat is not None and lon is not None
            else "—"
        )

        text = Text.assemble(
            ("GNSS [", "bold"),
            (fix_type.upper(), fix_style),
            ("]  ", "bold"),
            (f"Sats:{sats}  ", ""),
            (f"HDOP:{_fmt_float(hdop)}  ", ""),
            (coord_str + "  ", "cyan"),
            (f"{_fmt_float(speed, ' km/h')}  ", ""),
            (f"via {source}", "dim"),
        )
        return Panel(text, border_style="blue")

    def _render_footer(self) -> Panel:
        keys = (
            "[bold][S][/]tart  [bold][P][/]ause  [bold][R][/]esume  "
            "[bold][Q][/]uit  [bold][T][/]hroughput  [bold][L][/]atency"
        )
        last = f"  │  {self._last_log}" if self._last_log else ""
        return Panel(
            Text.from_markup(keys + last),
            border_style="dim",
        )

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="modems", size=12),
            Layout(name="gnss", size=3),
            Layout(name="footer", size=3),
        )
        layout["header"].update(self._render_header())

        modem_panels = [self._render_modem_panel(c) for c in self._carrier_order]
        layout["modems"].update(Columns(modem_panels, equal=True, expand=True))

        layout["gnss"].update(self._render_gnss_bar())
        layout["footer"].update(self._render_footer())
        return layout

    # ------------------------------------------------------------------
    # Keyboard input
    # ------------------------------------------------------------------

    async def _read_keys(self) -> None:
        """Non-blocking keyboard reader using stdin in raw mode."""
        import sys
        import termios
        import tty

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            loop = asyncio.get_event_loop()
            while self._running:
                try:
                    ch = await asyncio.wait_for(
                        loop.run_in_executor(None, sys.stdin.read, 1),
                        timeout=0.2,
                    )
                    await self._handle_key(ch.lower())
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    async def _handle_key(self, ch: str) -> None:
        if ch == "s":
            await self._session.start()
        elif ch == "p":
            await self._session.pause()
        elif ch == "r":
            await self._session.resume()
        elif ch == "q":
            await self._session.stop()
            self._running = False
        elif ch == "t":
            if self.on_throughput is not None:
                self.on_throughput()
                self._last_log = "On-demand throughput test triggered."
        elif ch == "l":
            if self.on_latency is not None:
                self.on_latency()
                self._last_log = "On-demand latency test triggered."

    # ------------------------------------------------------------------
    # Main render loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self._running = True
        consumer_task = asyncio.create_task(self.consume())
        key_task = asyncio.create_task(self._read_keys())

        try:
            with Live(
                self._build_layout(),
                console=self._console,
                refresh_per_second=REFRESH_RATE,
                screen=True,
            ) as live:
                while self._running:
                    live.update(self._build_layout())
                    await asyncio.sleep(1.0 / REFRESH_RATE)
        except asyncio.CancelledError:
            pass
        finally:
            consumer_task.cancel()
            key_task.cancel()
            self._running = False
            log.debug("TUI stopped.")

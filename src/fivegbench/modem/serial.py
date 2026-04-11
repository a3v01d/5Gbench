"""AT command serial interface for Quectel RM520N-GL modems.

Wraps pyserial for blocking I/O.  All public methods are async-safe —
they run the blocking pyserial calls in a thread via asyncio.to_thread().

Usage
-----
    async with ATSerial("/dev/ttyUSB2") as port:
        response = await port.command("AT+QENG=\"servingcell\"")
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import AsyncGenerator

import serial

log = logging.getLogger(__name__)

# Serial port parameters for Quectel modems
_BAUDRATE = 115200
_BYTESIZE = serial.EIGHTBITS
_PARITY = serial.PARITY_NONE
_STOPBITS = serial.STOPBITS_ONE

# AT response terminators
_OK_PATTERN = re.compile(r"^OK\s*$", re.MULTILINE)
_ERROR_PATTERN = re.compile(r"^(ERROR|\+CME ERROR:.*|\+CMS ERROR:.*)\s*$", re.MULTILINE)

DEFAULT_TIMEOUT = 5.0   # seconds
OPEN_TIMEOUT = 3.0


class ATError(Exception):
    """Raised when the modem returns an AT error response."""


class ATTimeout(Exception):
    """Raised when the modem doesn't respond within the timeout."""


class ATSerial:
    """Async-friendly AT command interface for a single serial port."""

    def __init__(self, port: str, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.port_path = port
        self.default_timeout = timeout
        self._serial: serial.Serial | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "ATSerial":
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Open / close
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Open the serial port (runs in thread)."""
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        if self._serial and self._serial.is_open:
            return
        log.debug("ATSerial: opening %s", self.port_path)
        self._serial = serial.Serial(
            port=self.port_path,
            baudrate=_BAUDRATE,
            bytesize=_BYTESIZE,
            parity=_PARITY,
            stopbits=_STOPBITS,
            timeout=OPEN_TIMEOUT,
            write_timeout=OPEN_TIMEOUT,
            exclusive=True,
        )
        # Flush any buffered data
        self._serial.reset_input_buffer()
        self._serial.reset_output_buffer()

    async def close(self) -> None:
        """Close the serial port (runs in thread)."""
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        if self._serial and self._serial.is_open:
            log.debug("ATSerial: closing %s", self.port_path)
            self._serial.close()

    @property
    def is_open(self) -> bool:
        return bool(self._serial and self._serial.is_open)

    # ------------------------------------------------------------------
    # Core command interface
    # ------------------------------------------------------------------

    async def command(
        self, cmd: str, timeout: float | None = None
    ) -> str:
        """Send *cmd* and return the full response string (without the echo).

        Raises ATError on ERROR response, ATTimeout on timeout.
        The lock ensures only one command is in flight at a time.
        """
        async with self._lock:
            return await asyncio.to_thread(
                self._command_sync, cmd, timeout or self.default_timeout
            )

    def _command_sync(self, cmd: str, timeout: float) -> str:
        if not (self._serial and self._serial.is_open):
            raise RuntimeError(f"Port {self.port_path} is not open.")

        # Send command with CRLF
        raw_cmd = (cmd.strip() + "\r\n").encode()
        log.debug("AT >> %s: %s", self.port_path, cmd.strip())
        self._serial.write(raw_cmd)
        self._serial.flush()

        # Read response until OK/ERROR or timeout
        response_lines: list[str] = []
        deadline = time.monotonic() + timeout
        buf = ""
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            self._serial.timeout = min(remaining, 0.1)
            chunk = self._serial.read(4096)
            if chunk:
                buf += chunk.decode(errors="replace")
                # Check for terminator
                if _OK_PATTERN.search(buf):
                    break
                if _ERROR_PATTERN.search(buf):
                    break
        else:
            raise ATTimeout(
                f"Timeout after {timeout}s waiting for response to: {cmd!r}"
            )

        # Strip echo (first line matching the command)
        lines = buf.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        lines = [ln for ln in lines if ln.strip()]  # remove blanks

        # Remove echo (line equal to the command, case-insensitive)
        cmd_stripped = cmd.strip()
        if lines and lines[0].strip().upper() == cmd_stripped.upper():
            lines = lines[1:]

        response = "\n".join(lines)
        log.debug("AT << %s: %s", self.port_path, response[:200])

        if _ERROR_PATTERN.search(response):
            raise ATError(f"AT error for '{cmd}': {response.strip()}")

        return response

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """Return True if the modem responds OK to 'AT'."""
        try:
            resp = await self.command("AT", timeout=3.0)
            return "OK" in resp
        except (ATError, ATTimeout, serial.SerialException):
            return False

    async def get_imei(self) -> str:
        """Return the 15-digit IMEI string via AT+GSN."""
        resp = await self.command("AT+GSN", timeout=5.0)
        for line in resp.splitlines():
            line = line.strip()
            if re.fullmatch(r"\d{15}", line):
                return line
        raise ATError(f"Could not parse IMEI from response: {resp!r}")

    async def get_iccid(self) -> str:
        """Return SIM ICCID via AT+QCCID."""
        resp = await self.command("AT+QCCID", timeout=5.0)
        for line in resp.splitlines():
            m = re.match(r"\+QCCID:\s*(\S+)", line)
            if m:
                return m.group(1).strip()
        raise ATError(f"Could not parse ICCID from response: {resp!r}")

    async def get_operator(self) -> str:
        """Return registered operator string via AT+COPS?."""
        resp = await self.command("AT+COPS?", timeout=10.0)
        for line in resp.splitlines():
            m = re.match(r'\+COPS:\s*\d+,\d+,"([^"]+)"', line)
            if m:
                return m.group(1)
        return ""

    async def enable_gnss(self) -> None:
        """Enable GNSS engine via AT+QGPS=1 (idempotent)."""
        try:
            await self.command("AT+QGPS=1", timeout=5.0)
        except ATError:
            pass  # already enabled returns +CME ERROR: 504

    async def get_gnss_fix(self) -> str:
        """Return raw AT+QGPSLOC response string."""
        return await self.command("AT+QGPSLOC=2", timeout=5.0)

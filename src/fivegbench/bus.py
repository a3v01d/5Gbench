"""Event bus: main asyncio queue, fan-out dispatcher, and consumer queue registry.

Architecture
------------
All data collectors publish structured messages to a single main queue.
A dispatcher task reads from the main queue and copies each message into
per-consumer queues.  Each consumer (SQLite writer, TUI, future API) has its
own queue and processes at its own pace without blocking producers.

Usage
-----
    bus = EventBus()
    bus.register_consumer("sqlite")
    bus.register_consumer("tui")          # optional
    asyncio.create_task(bus.dispatch())   # start dispatcher

    # Producer:
    await bus.publish({"type": "rf_telemetry", "carrier": "att", ...})

    # Consumer:
    msg = await bus.get("sqlite")
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

log = logging.getLogger(__name__)

# Type alias for bus messages
BusMessage = dict[str, Any]


class EventBus:
    """Central event bus with fan-out dispatch to registered consumers."""

    def __init__(self, maxsize: int = 2000) -> None:
        self._main: asyncio.Queue[BusMessage] = asyncio.Queue(maxsize=maxsize)
        self._consumers: dict[str, asyncio.Queue[BusMessage]] = {}
        self._running = False

    # ------------------------------------------------------------------
    # Consumer registration
    # ------------------------------------------------------------------

    def register_consumer(self, name: str, maxsize: int = 2000) -> None:
        """Register a named consumer queue.  Must be called before dispatch()."""
        if name in self._consumers:
            raise ValueError(f"Consumer '{name}' already registered.")
        self._consumers[name] = asyncio.Queue(maxsize=maxsize)
        log.debug("Bus: registered consumer '%s'", name)

    def unregister_consumer(self, name: str) -> None:
        """Remove a consumer.  The dispatcher will stop feeding it."""
        self._consumers.pop(name, None)
        log.debug("Bus: unregistered consumer '%s'", name)

    def consumers(self) -> list[str]:
        return list(self._consumers.keys())

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    async def publish(self, message: BusMessage) -> None:
        """Put a message on the main queue.  Blocks if the queue is full."""
        await self._main.put(message)

    def publish_nowait(self, message: BusMessage) -> None:
        """Non-blocking publish; drops the message if the queue is full."""
        try:
            self._main.put_nowait(message)
        except asyncio.QueueFull:
            log.warning(
                "Bus: main queue full — dropped message type=%s carrier=%s",
                message.get("type"),
                message.get("carrier"),
            )

    # ------------------------------------------------------------------
    # Consumer API
    # ------------------------------------------------------------------

    async def get(self, consumer_name: str) -> BusMessage:
        """Wait for the next message for the named consumer."""
        return await self._consumers[consumer_name].get()

    def get_nowait(self, consumer_name: str) -> BusMessage | None:
        """Non-blocking get; returns None if the consumer queue is empty."""
        try:
            return self._consumers[consumer_name].get_nowait()
        except asyncio.QueueEmpty:
            return None

    def task_done(self, consumer_name: str) -> None:
        """Mark the last message as processed (for join() support)."""
        self._consumers[consumer_name].task_done()

    # ------------------------------------------------------------------
    # Dispatcher task
    # ------------------------------------------------------------------

    async def dispatch(self) -> None:
        """Fan-out loop: read from main queue and copy to each consumer queue.

        Run as an asyncio task: asyncio.create_task(bus.dispatch())
        Runs indefinitely until cancelled.
        """
        self._running = True
        log.debug("Bus: dispatcher started with consumers: %s", list(self._consumers))
        try:
            while True:
                message = await self._main.get()
                for name, queue in list(self._consumers.items()):
                    try:
                        queue.put_nowait(message)
                    except asyncio.QueueFull:
                        log.warning(
                            "Bus: consumer '%s' queue full — dropped message type=%s",
                            name,
                            message.get("type"),
                        )
                self._main.task_done()
        except asyncio.CancelledError:
            self._running = False
            log.debug("Bus: dispatcher cancelled.")
            raise

    @property
    def running(self) -> bool:
        return self._running

    def main_queue_size(self) -> int:
        return self._main.qsize()

    def consumer_queue_size(self, name: str) -> int:
        return self._consumers[name].qsize()

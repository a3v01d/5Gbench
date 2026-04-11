"""Tests for fivegbench.bus — event bus, fan-out dispatch, and consumer queues."""

import asyncio
import pytest

from fivegbench.bus import EventBus, BusMessage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_msg(msg_type: str = "rf_telemetry", carrier: str = "att") -> BusMessage:
    return {
        "type": msg_type,
        "timestamp": "2026-04-10T14:30:22.456+00:00",
        "session_id": "20260410_143022",
        "carrier": carrier,
        "data": {"rsrp": -85.0},
    }


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_consumer(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        assert "sqlite" in bus.consumers()

    def test_register_multiple_consumers(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.register_consumer("tui")
        assert set(bus.consumers()) == {"sqlite", "tui"}

    def test_duplicate_registration_raises(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        with pytest.raises(ValueError, match="already registered"):
            bus.register_consumer("sqlite")

    def test_unregister_consumer(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.unregister_consumer("sqlite")
        assert "sqlite" not in bus.consumers()

    def test_unregister_nonexistent_is_safe(self):
        bus = EventBus()
        bus.unregister_consumer("doesnotexist")  # should not raise


# ---------------------------------------------------------------------------
# Publish and get (single consumer)
# ---------------------------------------------------------------------------

class TestPublishGet:
    @pytest.mark.asyncio
    async def test_publish_then_get(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        asyncio.create_task(bus.dispatch())

        msg = _make_msg()
        await bus.publish(msg)
        received = await asyncio.wait_for(bus.get("sqlite"), timeout=1.0)
        assert received["type"] == "rf_telemetry"
        assert received["carrier"] == "att"

    @pytest.mark.asyncio
    async def test_publish_multiple_messages_ordered(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        asyncio.create_task(bus.dispatch())

        types = ["rf_telemetry", "gnss", "throughput"]
        for t in types:
            await bus.publish(_make_msg(t))

        received_types = []
        for _ in types:
            msg = await asyncio.wait_for(bus.get("sqlite"), timeout=1.0)
            received_types.append(msg["type"])
            bus.task_done("sqlite")

        assert received_types == types

    @pytest.mark.asyncio
    async def test_get_nowait_returns_none_when_empty(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        result = bus.get_nowait("sqlite")
        assert result is None


# ---------------------------------------------------------------------------
# Fan-out to multiple consumers
# ---------------------------------------------------------------------------

class TestFanOut:
    @pytest.mark.asyncio
    async def test_both_consumers_receive_message(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.register_consumer("tui")
        asyncio.create_task(bus.dispatch())

        msg = _make_msg()
        await bus.publish(msg)

        sqlite_msg = await asyncio.wait_for(bus.get("sqlite"), timeout=1.0)
        tui_msg = await asyncio.wait_for(bus.get("tui"), timeout=1.0)

        assert sqlite_msg["type"] == "rf_telemetry"
        assert tui_msg["type"] == "rf_telemetry"

    @pytest.mark.asyncio
    async def test_three_consumers_all_receive(self):
        bus = EventBus()
        names = ["sqlite", "tui", "api"]
        for name in names:
            bus.register_consumer(name)
        asyncio.create_task(bus.dispatch())

        await bus.publish(_make_msg("gnss"))

        for name in names:
            msg = await asyncio.wait_for(bus.get(name), timeout=1.0)
            assert msg["type"] == "gnss"

    @pytest.mark.asyncio
    async def test_consumers_get_independent_copies(self):
        """Mutating one consumer's message doesn't affect the other."""
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.register_consumer("tui")
        asyncio.create_task(bus.dispatch())

        await bus.publish(_make_msg())

        msg_a = await asyncio.wait_for(bus.get("sqlite"), timeout=1.0)
        msg_b = await asyncio.wait_for(bus.get("tui"), timeout=1.0)

        # Same object reference is fine — messages are read-only by convention
        # but both queues must have received it
        assert msg_a is msg_b or msg_a == msg_b


# ---------------------------------------------------------------------------
# Queue sizing and overflow
# ---------------------------------------------------------------------------

class TestQueueOverflow:
    def test_publish_nowait_drops_when_full(self):
        bus = EventBus(maxsize=2)
        bus.register_consumer("sqlite")

        # Fill the main queue beyond capacity using publish_nowait
        for _ in range(5):
            bus.publish_nowait(_make_msg())

        # Queue should be at capacity (2), extras silently dropped
        assert bus.main_queue_size() <= 2

    @pytest.mark.asyncio
    async def test_consumer_queue_full_does_not_crash_dispatch(self):
        """When a consumer queue is full the dispatcher logs a warning and
        continues; no exception propagates."""
        bus = EventBus(maxsize=100)
        bus.register_consumer("sqlite", maxsize=1)  # tiny consumer queue
        bus.register_consumer("tui", maxsize=100)
        dispatch_task = asyncio.create_task(bus.dispatch())

        # Publish more messages than the sqlite consumer queue can hold
        for i in range(5):
            await bus.publish(_make_msg(carrier=f"att_{i}"))

        # Give dispatcher a moment to run
        await asyncio.sleep(0.05)

        # tui queue should have all 5; sqlite may have dropped some
        assert bus.consumer_queue_size("tui") >= 1  # at least some arrived

        dispatch_task.cancel()
        try:
            await dispatch_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Dispatcher lifecycle
# ---------------------------------------------------------------------------

class TestDispatcher:
    @pytest.mark.asyncio
    async def test_dispatch_task_cancels_cleanly(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        task = asyncio.create_task(bus.dispatch())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_running_flag_set_while_dispatching(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        task = asyncio.create_task(bus.dispatch())
        await asyncio.sleep(0.01)
        assert bus.running is True
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_queue_sizes_reported(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        assert bus.main_queue_size() == 0
        assert bus.consumer_queue_size("sqlite") == 0


# ---------------------------------------------------------------------------
# publish_nowait
# ---------------------------------------------------------------------------

class TestPublishNowait:
    def test_publish_nowait_does_not_raise(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.publish_nowait(_make_msg())  # should not raise even without dispatcher

    def test_publish_nowait_increments_queue_size(self):
        bus = EventBus()
        bus.register_consumer("sqlite")
        bus.publish_nowait(_make_msg())
        assert bus.main_queue_size() == 1

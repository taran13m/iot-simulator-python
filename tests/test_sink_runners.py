"""Tests for SinkRunner - buffer, drain loop, retries, backpressure."""

from __future__ import annotations

import asyncio

import pytest

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink, SinkRunner

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _make_records(n: int = 3) -> list[SensorRecord]:
    return [
        SensorRecord(
            sensor_name=f"sensor_{i}",
            industry="test",
            value=float(i * 10),
            unit="kW",
            sensor_type="power",
            timestamp=1_700_000_000.0 + i,
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
        )
        for i in range(n)
    ]


class _DummySink(Sink):
    """In-memory sink for testing SinkRunner."""

    def __init__(self, *, fail_count: int = 0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.written: list[list[SensorRecord]] = []
        self.connected = False
        self.closed = False
        self.flushed = False
        self._fail_count = fail_count
        self._call_count = 0

    async def connect(self) -> None:
        self.connected = True

    async def write(self, records: list[SensorRecord]) -> None:
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise RuntimeError("Simulated write failure")
        self.written.append(list(records))

    async def flush(self) -> None:
        self.flushed = True

    async def close(self) -> None:
        self.closed = True


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestSinkRunnerBasic:
    """Basic start/enqueue/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_connects_sink(self) -> None:
        sink = _DummySink(rate_hz=10.0)
        runner = SinkRunner(sink)
        await runner.start()
        assert sink.connected
        await runner.stop()
        assert sink.closed

    @pytest.mark.asyncio
    async def test_enqueue_and_drain(self) -> None:
        sink = _DummySink(rate_hz=50.0, batch_size=10)
        runner = SinkRunner(sink)
        await runner.start()
        await runner.enqueue(_make_records(5))
        # Wait for drain loop to process
        await asyncio.sleep(0.15)
        await runner.stop()
        # Records should have been written
        total = sum(len(batch) for batch in sink.written)
        assert total == 5

    @pytest.mark.asyncio
    async def test_stop_drains_remaining(self) -> None:
        sink = _DummySink(rate_hz=0.1, batch_size=100)  # Very slow rate
        runner = SinkRunner(sink)
        await runner.start()
        await runner.enqueue(_make_records(10))
        # Don't wait for drain - stop immediately should flush remaining
        await runner.stop()
        total = sum(len(batch) for batch in sink.written)
        assert total == 10
        assert sink.flushed
        assert sink.closed


class TestSinkRunnerBackpressure:
    """Backpressure policies."""

    @pytest.mark.asyncio
    async def test_drop_oldest_policy(self) -> None:
        sink = _DummySink(rate_hz=0.1, max_buffer_size=5, backpressure="drop_oldest")
        runner = SinkRunner(sink)
        await runner.start()
        # Enqueue more than max_buffer_size
        await runner.enqueue(_make_records(10))
        # Buffer should be capped at 5 (oldest dropped by deque maxlen)
        assert len(runner._buffer) <= 5
        await runner.stop()

    @pytest.mark.asyncio
    async def test_drop_newest_policy(self) -> None:
        sink = _DummySink(rate_hz=0.1, max_buffer_size=5, backpressure="drop_newest")
        runner = SinkRunner(sink)
        await runner.start()
        # Fill buffer exactly
        await runner.enqueue(_make_records(5))
        # New records should be dropped
        await runner.enqueue(_make_records(5))
        # Buffer should not exceed max
        assert len(runner._buffer) <= 5
        await runner.stop()


class TestSinkRunnerRetries:
    """Write retry behavior."""

    @pytest.mark.asyncio
    async def test_retry_on_failure(self) -> None:
        """Sink that fails once then succeeds should still deliver."""
        sink = _DummySink(rate_hz=50.0, batch_size=10, fail_count=1, retry_delay_s=0.01)
        runner = SinkRunner(sink)
        await runner.start()
        await runner.enqueue(_make_records(3))
        await asyncio.sleep(0.2)
        await runner.stop()
        # Should have succeeded on retry
        total = sum(len(batch) for batch in sink.written)
        assert total == 3

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_drops_records(self) -> None:
        """Sink that always fails should drop records after retry_count."""
        sink = _DummySink(
            rate_hz=50.0,
            batch_size=10,
            fail_count=999,  # Always fail
            retry_count=2,
            retry_delay_s=0.01,
        )
        runner = SinkRunner(sink)
        await runner.start()
        await runner.enqueue(_make_records(3))
        await asyncio.sleep(0.3)
        await runner.stop()
        # Records should be dropped (none written successfully)
        assert len(sink.written) == 0


class TestSinkRunnerNoRate:
    """SinkRunner with no rate_hz (fast path)."""

    @pytest.mark.asyncio
    async def test_no_rate_hz_drains_quickly(self) -> None:
        sink = _DummySink(batch_size=10)  # rate_hz=None by default
        runner = SinkRunner(sink)
        await runner.start()
        await runner.enqueue(_make_records(5))
        await asyncio.sleep(0.15)
        await runner.stop()
        total = sum(len(batch) for batch in sink.written)
        assert total == 5

    @pytest.mark.asyncio
    async def test_enqueue_after_stop_is_ignored(self) -> None:
        sink = _DummySink(rate_hz=10.0)
        runner = SinkRunner(sink)
        await runner.start()
        await runner.stop()
        # Enqueue after stop should be silently ignored
        await runner.enqueue(_make_records(3))
        assert len(runner._buffer) == 0

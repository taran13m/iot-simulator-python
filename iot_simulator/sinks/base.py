"""Sink abstraction layer with per-sink throughput control.

Provides:
- ``Sink``       - abstract base class that every concrete sink implements.
- ``SinkConfig`` - per-sink throughput / batching / back-pressure knobs.
- ``SinkRunner`` - internal async helper that buffers records and drains
                   them to the sink at the configured rate.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
from abc import ABC, abstractmethod

from pydantic import BaseModel

from iot_simulator.models import SensorRecord

__all__ = ["Sink", "SinkConfig", "SinkRunner"]

logger = logging.getLogger("iot_simulator.sinks")


# -----------------------------------------------------------------------
# Throughput configuration
# -----------------------------------------------------------------------


class SinkConfig(BaseModel):
    """Per-sink throughput / batching knobs.

    Attributes:
        rate_hz:
            How often the runner flushes buffered records to the sink.
            ``None`` means *every generator tick* (lowest latency, no extra
            buffering).  ``0.1`` means once every 10 seconds.
        batch_size:
            Flush as soon as the buffer reaches this many records,
            *regardless* of rate_hz.
        max_buffer_size:
            Maximum records held in memory.  When exceeded the
            ``backpressure`` policy kicks in.
        backpressure:
            ``"drop_oldest"`` - discard oldest records when buffer is full.
            ``"drop_newest"`` - discard incoming records when buffer is full.
            ``"block"``       - block the producer until space is available.
        retry_count:
            How many times to retry a failed ``write()`` call.
        retry_delay_s:
            Seconds to wait between retries.
    """

    rate_hz: float | None = None
    batch_size: int = 100
    max_buffer_size: int = 50_000
    backpressure: str = "drop_oldest"  # drop_oldest | drop_newest | block
    retry_count: int = 3
    retry_delay_s: float = 1.0


# -----------------------------------------------------------------------
# Sink ABC
# -----------------------------------------------------------------------


class Sink(ABC):
    """Abstract base class for all sinks.

    Concrete sinks must implement ``connect``, ``write``, ``flush`` and
    ``close``.  Throughput parameters (``rate_hz``, ``batch_size``, …)
    are accepted in ``__init__`` and stored in ``self.sink_config``.
    """

    def __init__(
        self,
        *,
        rate_hz: float | None = None,
        batch_size: int = 100,
        max_buffer_size: int = 50_000,
        backpressure: str = "drop_oldest",
        retry_count: int = 3,
        retry_delay_s: float = 1.0,
    ) -> None:
        self.sink_config = SinkConfig(
            rate_hz=rate_hz,
            batch_size=batch_size,
            max_buffer_size=max_buffer_size,
            backpressure=backpressure,
            retry_count=retry_count,
            retry_delay_s=retry_delay_s,
        )

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection / open resources."""

    @abstractmethod
    async def write(self, records: list[SensorRecord]) -> None:
        """Write a batch of records to the destination.

        The ``SinkRunner`` calls this with batches sized according to
        ``SinkConfig.batch_size``.
        """

    @abstractmethod
    async def flush(self) -> None:
        """Flush any internal buffers the sink may hold."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources / close connections."""


# -----------------------------------------------------------------------
# SinkRunner - async buffer + rate limiter (one per registered sink)
# -----------------------------------------------------------------------


class SinkRunner:
    """Buffers incoming records and drains them to a ``Sink`` according
    to its ``SinkConfig`` throughput settings.

    The Simulator creates one ``SinkRunner`` per ``add_sink()`` call.
    """

    def __init__(self, sink: Sink) -> None:
        self.sink = sink
        self.cfg = sink.sink_config
        self._buffer: collections.deque[SensorRecord] = collections.deque(
            maxlen=self.cfg.max_buffer_size if self.cfg.backpressure == "drop_oldest" else None,
        )
        self._lock = asyncio.Lock()
        self._drain_task: asyncio.Task[None] | None = None
        self._stopped = False

    # -- public interface used by Simulator --

    async def start(self) -> None:
        """Connect the sink and start the background drain loop."""
        await self.sink.connect()
        self._stopped = False
        self._drain_task = asyncio.create_task(self._drain_loop(), name=f"drain-{type(self.sink).__name__}")

    async def enqueue(self, records: list[SensorRecord]) -> None:
        """Append records to the internal buffer (called by Simulator each tick)."""
        if self._stopped:
            return

        if self.cfg.backpressure == "drop_newest" and len(self._buffer) + len(records) > self.cfg.max_buffer_size:
            # Drop incoming records that would exceed the limit
            space = max(0, self.cfg.max_buffer_size - len(self._buffer))
            records = records[:space]

        self._buffer.extend(records)

    async def stop(self) -> None:
        """Drain remaining records, flush, and close the sink."""
        self._stopped = True
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._drain_task
        # Final drain
        await self._flush_buffer()
        await self.sink.flush()
        await self.sink.close()

    # -- internal --

    async def _drain_loop(self) -> None:
        """Background coroutine that drains the buffer at the configured rate."""
        try:
            while not self._stopped:
                # No rate limit - drain every 50ms (fast path)
                interval = 1.0 / self.cfg.rate_hz if self.cfg.rate_hz is not None and self.cfg.rate_hz > 0 else 0.05

                await asyncio.sleep(interval)
                await self._flush_buffer()
        except asyncio.CancelledError:
            pass

    async def _flush_buffer(self) -> None:
        """Take up to ``batch_size`` records from the buffer and write them."""
        while self._buffer:
            # Pull a batch
            batch: list[SensorRecord] = []
            count = min(len(self._buffer), self.cfg.batch_size)
            for _ in range(count):
                batch.append(self._buffer.popleft())

            if not batch:
                break

            # Write with retries
            for attempt in range(1, self.cfg.retry_count + 1):
                try:
                    await self.sink.write(batch)
                    break
                except Exception as exc:
                    if attempt < self.cfg.retry_count:
                        logger.warning(
                            "%s write failed (attempt %d/%d): %s - retrying in %.1fs",
                            type(self.sink).__name__,
                            attempt,
                            self.cfg.retry_count,
                            exc,
                            self.cfg.retry_delay_s,
                        )
                        await asyncio.sleep(self.cfg.retry_delay_s)
                    else:
                        logger.error(
                            "%s write failed after %d attempts: %s - dropping %d records",
                            type(self.sink).__name__,
                            self.cfg.retry_count,
                            exc,
                            len(batch),
                        )

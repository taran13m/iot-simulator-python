"""Callback sink – delegates writes to a user-provided Python callable.

This allows users to hook any custom logic into the pipeline without
having to subclass :class:`Sink`::

    sim.add_sink(lambda records: print(len(records)))
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Any

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["CallbackSink"]


class CallbackSink(Sink):
    """Wraps a user-supplied function as a sink.

    The callable receives a ``list[SensorRecord]`` on each flush.  It can
    be a regular function, a coroutine function, or a lambda.

    Parameters:
        callback: ``(records: list[SensorRecord]) -> None`` or async variant.
        rate_hz: Throughput – how often to flush batches.
        **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        callback: Callable[[list[SensorRecord]], Any],
        *,
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs,
    ) -> None:
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._callback = callback
        self._is_async = inspect.iscoroutinefunction(callback)

    async def connect(self) -> None:
        """No-op."""

    async def write(self, records: list[SensorRecord]) -> None:
        if self._is_async:
            await self._callback(records)
        else:
            # Run sync callback in executor to avoid blocking the event loop
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._callback, records)

    async def flush(self) -> None:
        """No-op."""

    async def close(self) -> None:
        """No-op."""

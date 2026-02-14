"""Console sink - prints sensor records to stdout.

Useful for debugging, demos, and verifying the pipeline is working.
"""

from __future__ import annotations

import sys
from typing import IO

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["ConsoleSink"]


class ConsoleSink(Sink):
    """Writes sensor records to the console (stdout by default).

    Parameters:
        fmt: Output format - ``"text"`` (human-readable) or ``"json"``
             (one JSON object per record).
        stream: Writable file-like object (defaults to ``sys.stdout``).
        rate_hz: Throughput - how often to flush batches.
        **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        fmt: str = "text",
        stream: IO[str] | None = None,
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs,
    ) -> None:
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._fmt = fmt
        self._stream = stream or sys.stdout

    async def connect(self) -> None:
        """No-op - stdout is always available."""

    async def write(self, records: list[SensorRecord]) -> None:
        if self._fmt == "json":
            for rec in records:
                self._stream.write(rec.to_json() + "\n")
        else:
            for rec in records:
                self._stream.write(
                    f"[{rec.industry}/{rec.sensor_name}] "
                    f"{rec.value:>12.3f} {rec.unit:<8s} "
                    f"(type={rec.sensor_type}"
                    f"{', FAULT' if rec.fault_active else ''})\n"
                )
        self._stream.flush()

    async def flush(self) -> None:
        self._stream.flush()

    async def close(self) -> None:
        """No-op - we do not own stdout."""

"""Delta Lake sink - writes sensor records as Delta Lake tables.

Requires the ``delta`` extra::

    pip install iot-data-simulator[delta]
"""

from __future__ import annotations

import logging

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["DeltaSink"]

logger = logging.getLogger("iot_simulator.sinks.delta")

try:
    import pyarrow as pa
    from deltalake import write_deltalake

    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False


# Pre-defined Arrow schema for sensor records
_ARROW_SCHEMA = None


def _get_arrow_schema():
    global _ARROW_SCHEMA
    if _ARROW_SCHEMA is None:
        import pyarrow as pa

        _ARROW_SCHEMA = pa.schema(
            [
                ("timestamp", pa.float64()),
                ("industry", pa.string()),
                ("sensor_name", pa.string()),
                ("sensor_type", pa.string()),
                ("value", pa.float64()),
                ("unit", pa.string()),
                ("min_value", pa.float64()),
                ("max_value", pa.float64()),
                ("nominal_value", pa.float64()),
                ("fault_active", pa.bool_()),
            ]
        )
    return _ARROW_SCHEMA


class DeltaSink(Sink):
    """Write sensor records to a Delta Lake table.

    Parameters:
        table_path:
            Path to the Delta table directory (local filesystem or
            cloud storage URI).
        mode:
            Write mode - ``"append"`` (default) or ``"overwrite"``.
        partition_by:
            Optional list of columns to partition by, e.g.
            ``["industry"]``.
        storage_options:
            Cloud storage credentials, e.g.
            ``{"AWS_ACCESS_KEY_ID": "…", "AWS_SECRET_ACCESS_KEY": "…"}``.
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        table_path: str = "/tmp/delta/sensors",
        mode: str = "append",
        partition_by: list[str] | None = None,
        storage_options: dict[str, str] | None = None,
        rate_hz: float | None = None,
        batch_size: int = 1000,
        **kwargs,
    ) -> None:
        if not DELTA_AVAILABLE:
            raise ImportError(
                "deltalake and pyarrow are required for DeltaSink.  Install with: pip install iot-data-simulator[delta]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._table_path = table_path
        self._mode = mode
        self._partition_by = partition_by
        self._storage_options = storage_options or {}

    async def connect(self) -> None:
        logger.info(
            "DeltaSink ready - table_path=%s, mode=%s",
            self._table_path,
            self._mode,
        )

    async def write(self, records: list[SensorRecord]) -> None:
        rows = [
            {
                "timestamp": rec.timestamp,
                "industry": rec.industry,
                "sensor_name": rec.sensor_name,
                "sensor_type": rec.sensor_type,
                "value": rec.value,
                "unit": rec.unit,
                "min_value": rec.min_value,
                "max_value": rec.max_value,
                "nominal_value": rec.nominal_value,
                "fault_active": rec.fault_active,
            }
            for rec in records
        ]

        table = pa.Table.from_pylist(rows, schema=_get_arrow_schema())
        write_deltalake(
            self._table_path,
            table,
            mode=self._mode,
            partition_by=self._partition_by,
            storage_options=self._storage_options or None,
        )
        logger.debug("Wrote %d records to Delta table %s", len(records), self._table_path)

    async def flush(self) -> None:
        """No-op - each write() already commits."""

    async def close(self) -> None:
        logger.info("DeltaSink closed (table_path=%s)", self._table_path)

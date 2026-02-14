"""Database sink - inserts sensor records into a SQL database via SQLAlchemy async.

Requires the ``database`` extra::

    pip install iot-data-simulator[database]

Supports any database that SQLAlchemy supports (PostgreSQL, SQLite, MySQL, …).
For PostgreSQL use ``asyncpg``; for SQLite use ``aiosqlite``.
"""

from __future__ import annotations

import logging

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["DatabaseSink"]

logger = logging.getLogger("iot_simulator.sinks.database")

try:
    from sqlalchemy import Boolean, Column, Float, MetaData, String, Table
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


class DatabaseSink(Sink):
    """Insert sensor records into a SQL table.

    The table is created automatically on ``connect()`` if it does not
    exist.

    Parameters:
        connection_string:
            SQLAlchemy async connection URL, e.g.
            ``"sqlite+aiosqlite:///sensors.db"`` or
            ``"postgresql+asyncpg://user:pass@localhost/iot"``.
        table: Table name (default ``"sensor_data"``).
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        connection_string: str = "sqlite+aiosqlite:///sensor_data.db",
        table: str = "sensor_data",
        rate_hz: float | None = None,
        batch_size: int = 200,
        **kwargs,
    ) -> None:
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "sqlalchemy is required for DatabaseSink.  Install with: pip install iot-data-simulator[database]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._url = connection_string
        self._table_name = table
        self._engine: AsyncEngine | None = None
        self._table: Table | None = None
        self._metadata = MetaData()

    async def connect(self) -> None:
        self._engine = create_async_engine(self._url, echo=False)

        # Define table schema
        self._table = Table(
            self._table_name,
            self._metadata,
            Column("timestamp", Float, nullable=False),
            Column("industry", String(64), nullable=False),
            Column("sensor_name", String(128), nullable=False),
            Column("sensor_type", String(32)),
            Column("value", Float, nullable=False),
            Column("unit", String(16)),
            Column("min_value", Float),
            Column("max_value", Float),
            Column("nominal_value", Float),
            Column("fault_active", Boolean, default=False),
        )

        # Create table if needed
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)

        logger.info("DatabaseSink connected to %s (table=%s)", self._url, self._table_name)

    async def write(self, records: list[SensorRecord]) -> None:
        if self._engine is None or self._table is None:
            raise RuntimeError("DatabaseSink is not connected")

        rows = []
        for rec in records:
            rows.append(
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
            )

        async with self._engine.begin() as conn:
            await conn.execute(self._table.insert(), rows)

    async def flush(self) -> None:
        """No-op - each write() is already committed."""

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            logger.info("DatabaseSink closed")

"""Tests for DatabaseSink - mocked SQLAlchemy dependency."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from iot_simulator.models import SensorRecord

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


# -----------------------------------------------------------------------
# Mock setup
# -----------------------------------------------------------------------


def _make_mock_sqlalchemy():
    """Create mock sqlalchemy modules."""
    # Core SA types
    mock_sa = ModuleType("sqlalchemy")
    mock_sa.Boolean = MagicMock(name="Boolean")
    mock_sa.Column = MagicMock(name="Column")
    mock_sa.Float = MagicMock(name="Float")
    mock_sa.MetaData = MagicMock(name="MetaData")
    mock_sa.String = MagicMock(name="String")
    mock_sa.Table = MagicMock(name="Table")

    # Async engine
    mock_ext = ModuleType("sqlalchemy.ext")
    mock_ext_asyncio = ModuleType("sqlalchemy.ext.asyncio")

    mock_engine = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.run_sync = AsyncMock()
    mock_conn.execute = AsyncMock()

    # Context manager for engine.begin()
    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    mock_engine.begin = MagicMock(return_value=mock_cm)
    mock_engine.dispose = AsyncMock()

    mock_create_engine = MagicMock(return_value=mock_engine)
    mock_ext_asyncio.create_async_engine = mock_create_engine
    mock_ext_asyncio.AsyncEngine = MagicMock()

    return (
        {
            "sqlalchemy": mock_sa,
            "sqlalchemy.ext": mock_ext,
            "sqlalchemy.ext.asyncio": mock_ext_asyncio,
        },
        mock_engine,
        mock_conn,
        mock_create_engine,
    )


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestDatabaseSink:
    """DatabaseSink with mocked SQLAlchemy."""

    def _import_database_sink(self, mock_modules):
        with patch.dict(sys.modules, mock_modules):
            if "iot_simulator.sinks.database" in sys.modules:
                del sys.modules["iot_simulator.sinks.database"]
            from iot_simulator.sinks.database import DatabaseSink

            return DatabaseSink

    @pytest.mark.asyncio
    async def test_connect_creates_engine_and_table(self) -> None:
        mock_modules, _mock_engine, mock_conn, mock_create = _make_mock_sqlalchemy()
        DatabaseSink = self._import_database_sink(mock_modules)

        sink = DatabaseSink(
            connection_string="sqlite+aiosqlite:///test.db",
            table="test_data",
        )
        await sink.connect()

        mock_create.assert_called_once()
        mock_conn.run_sync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_inserts_records(self) -> None:
        mock_modules, _mock_engine, mock_conn, _ = _make_mock_sqlalchemy()
        DatabaseSink = self._import_database_sink(mock_modules)

        sink = DatabaseSink(connection_string="sqlite+aiosqlite:///test.db")
        await sink.connect()

        records = _make_records(3)
        await sink.write(records)

        mock_conn.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_modules, _, _, _ = _make_mock_sqlalchemy()
        DatabaseSink = self._import_database_sink(mock_modules)

        sink = DatabaseSink(connection_string="sqlite+aiosqlite:///test.db")
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_modules, _, _, _ = _make_mock_sqlalchemy()
        DatabaseSink = self._import_database_sink(mock_modules)

        sink = DatabaseSink(connection_string="sqlite+aiosqlite:///test.db")
        await sink.connect()
        await sink.flush()  # Should not raise

    @pytest.mark.asyncio
    async def test_close_disposes_engine(self) -> None:
        mock_modules, mock_engine, _, _ = _make_mock_sqlalchemy()
        DatabaseSink = self._import_database_sink(mock_modules)

        sink = DatabaseSink(connection_string="sqlite+aiosqlite:///test.db")
        await sink.connect()
        await sink.close()

        mock_engine.dispose.assert_awaited_once()

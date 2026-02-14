"""Tests for DeltaSink - mocked deltalake/pyarrow dependency."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

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


def _make_mock_deltalake_pyarrow():
    """Create mock deltalake and pyarrow modules."""
    # pyarrow mock
    mock_pa = MagicMock()
    mock_pa.float64 = MagicMock(return_value="float64")
    mock_pa.string = MagicMock(return_value="string")
    mock_pa.bool_ = MagicMock(return_value="bool")
    mock_pa.schema = MagicMock(return_value=MagicMock())
    mock_table = MagicMock()
    mock_pa.Table.from_pylist = MagicMock(return_value=mock_table)

    # deltalake mock
    mock_deltalake = ModuleType("deltalake")
    mock_write_deltalake = MagicMock()
    mock_deltalake.write_deltalake = mock_write_deltalake

    return (
        {
            "pyarrow": mock_pa,
            "deltalake": mock_deltalake,
        },
        mock_write_deltalake,
        mock_pa,
    )


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestDeltaSink:
    """DeltaSink with mocked deltalake."""

    def _import_delta_sink(self, mock_modules):
        with patch.dict(sys.modules, mock_modules):
            if "iot_simulator.sinks.delta" in sys.modules:
                del sys.modules["iot_simulator.sinks.delta"]
            from iot_simulator.sinks.delta import DeltaSink

            return DeltaSink

    @pytest.mark.asyncio
    async def test_connect_logs_ready(self) -> None:
        mock_modules, _, _ = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        sink = DeltaSink(table_path="/tmp/test_delta")
        await sink.connect()  # Should not raise

    @pytest.mark.asyncio
    async def test_write_calls_write_deltalake(self) -> None:
        mock_modules, mock_write, mock_pa = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        sink = DeltaSink(table_path="/tmp/test_delta", mode="append")
        await sink.connect()
        await sink.write(_make_records(3))

        mock_pa.Table.from_pylist.assert_called_once()
        mock_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_with_partition_by(self) -> None:
        mock_modules, mock_write, _ = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        sink = DeltaSink(
            table_path="/tmp/test_delta",
            partition_by=["industry"],
        )
        await sink.connect()
        await sink.write(_make_records(2))

        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["partition_by"] == ["industry"]

    @pytest.mark.asyncio
    async def test_write_with_storage_options(self) -> None:
        mock_modules, mock_write, _ = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        opts = {"AWS_ACCESS_KEY_ID": "xxx"}
        sink = DeltaSink(table_path="/tmp/test_delta", storage_options=opts)
        await sink.connect()
        await sink.write(_make_records(1))

        call_kwargs = mock_write.call_args[1]
        assert call_kwargs["storage_options"] == opts

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_modules, _, _ = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        sink = DeltaSink(table_path="/tmp/test_delta")
        await sink.connect()
        await sink.flush()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        mock_modules, _, _ = _make_mock_deltalake_pyarrow()
        DeltaSink = self._import_delta_sink(mock_modules)

        sink = DeltaSink(table_path="/tmp/test_delta")
        await sink.connect()
        await sink.close()

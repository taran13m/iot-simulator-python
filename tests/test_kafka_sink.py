"""Tests for KafkaSink - mocked aiokafka dependency."""

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


def _make_mock_aiokafka():
    """Create mock aiokafka module and producer."""
    mock_producer_class = MagicMock()
    mock_producer_instance = AsyncMock()
    mock_producer_instance.start = AsyncMock()
    mock_producer_instance.send = AsyncMock()
    mock_producer_instance.flush = AsyncMock()
    mock_producer_instance.stop = AsyncMock()
    mock_producer_class.return_value = mock_producer_instance

    mock_module = ModuleType("aiokafka")
    mock_module.AIOKafkaProducer = mock_producer_class

    return mock_module, mock_producer_class, mock_producer_instance


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestKafkaSink:
    """KafkaSink with mocked aiokafka."""

    def _import_kafka_sink(self, mock_module):
        """Force re-import of kafka module with mock in place."""
        with patch.dict(sys.modules, {"aiokafka": mock_module}):
            # Remove cached module to force re-import
            if "iot_simulator.sinks.kafka" in sys.modules:
                del sys.modules["iot_simulator.sinks.kafka"]
            from iot_simulator.sinks.kafka import KafkaSink

            return KafkaSink

    @pytest.mark.asyncio
    async def test_connect_and_write(self) -> None:
        mock_module, mock_cls, mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            rate_hz=1.0,
        )
        await sink.connect()
        mock_cls.assert_called_once()
        mock_inst.start.assert_awaited_once()

        records = _make_records(3)
        await sink.write(records)
        assert mock_inst.send.await_count == 3

    @pytest.mark.asyncio
    async def test_write_with_key_field(self) -> None:
        mock_module, _mock_cls, mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            key_field="sensor_name",
        )
        await sink.connect()
        records = _make_records(1)
        await sink.write(records)

        call_kwargs = mock_inst.send.call_args
        assert call_kwargs[1]["key"] == b"sensor_0"

    @pytest.mark.asyncio
    async def test_write_no_key_field(self) -> None:
        mock_module, _mock_cls, mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            key_field=None,
        )
        await sink.connect()
        await sink.write(_make_records(1))

        call_kwargs = mock_inst.send.call_args
        assert call_kwargs[1]["key"] is None

    @pytest.mark.asyncio
    async def test_flush(self) -> None:
        mock_module, _mock_cls, mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(bootstrap_servers="localhost:9092", topic="test-topic")
        await sink.connect()
        await sink.flush()
        mock_inst.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        mock_module, _mock_cls, mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(bootstrap_servers="localhost:9092", topic="test-topic")
        await sink.connect()
        await sink.close()
        mock_inst.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_module, _, _ = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(bootstrap_servers="localhost:9092", topic="test-topic")
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_security_config(self) -> None:
        mock_module, mock_cls, _mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
            acks=-1,
        )
        await sink.connect()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["security_protocol"] == "SASL_SSL"
        assert call_kwargs["sasl_mechanism"] == "PLAIN"
        assert call_kwargs["sasl_plain_username"] == "user"
        assert call_kwargs["sasl_plain_password"] == "pass"
        assert call_kwargs["acks"] == "all"

    @pytest.mark.asyncio
    async def test_extra_producer_config(self) -> None:
        mock_module, mock_cls, _mock_inst = _make_mock_aiokafka()
        KafkaSink = self._import_kafka_sink(mock_module)

        sink = KafkaSink(
            bootstrap_servers="localhost:9092",
            topic="test",
            extra_producer_config={"linger_ms": 100},
        )
        await sink.connect()
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs["linger_ms"] == 100

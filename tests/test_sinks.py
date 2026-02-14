"""Tests for built-in sinks – ConsoleSink, CallbackSink, FileSink."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import SinkConfig
from iot_simulator.sinks.callback import CallbackSink
from iot_simulator.sinks.console import ConsoleSink
from iot_simulator.sinks.file import FileSink


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _make_records(n: int = 3) -> list[SensorRecord]:
    """Create *n* sample SensorRecord instances."""
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
# SinkConfig
# -----------------------------------------------------------------------


class TestSinkConfig:
    """SinkConfig Pydantic model."""

    def test_defaults(self) -> None:
        cfg = SinkConfig()
        assert cfg.rate_hz is None
        assert cfg.batch_size == 100
        assert cfg.max_buffer_size == 50_000
        assert cfg.backpressure == "drop_oldest"
        assert cfg.retry_count == 3

    def test_custom_values(self) -> None:
        cfg = SinkConfig(rate_hz=2.0, batch_size=50, backpressure="drop_newest")
        assert cfg.rate_hz == 2.0
        assert cfg.batch_size == 50
        assert cfg.backpressure == "drop_newest"


# -----------------------------------------------------------------------
# ConsoleSink
# -----------------------------------------------------------------------


class TestConsoleSink:
    """ConsoleSink text and JSON output."""

    @pytest.mark.asyncio
    async def test_text_format(self) -> None:
        buf = io.StringIO()
        sink = ConsoleSink(fmt="text", stream=buf, rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(2))
        await sink.flush()
        await sink.close()
        output = buf.getvalue()
        assert "sensor_0" in output
        assert "sensor_1" in output

    @pytest.mark.asyncio
    async def test_json_format(self) -> None:
        buf = io.StringIO()
        sink = ConsoleSink(fmt="json", stream=buf, rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(1))
        await sink.close()
        output = buf.getvalue().strip()
        parsed = json.loads(output)
        assert parsed["sensor_name"] == "sensor_0"


# -----------------------------------------------------------------------
# CallbackSink
# -----------------------------------------------------------------------


class TestCallbackSink:
    """CallbackSink with sync and async callbacks."""

    @pytest.mark.asyncio
    async def test_sync_callback_receives_records(self) -> None:
        received: list[list[SensorRecord]] = []
        sink = CallbackSink(lambda recs: received.append(recs), rate_hz=1.0)
        await sink.connect()
        records = _make_records(3)
        await sink.write(records)
        await sink.flush()
        await sink.close()
        assert len(received) == 1
        assert received[0] == records

    @pytest.mark.asyncio
    async def test_async_callback(self) -> None:
        received: list[list[SensorRecord]] = []

        async def async_cb(recs: list[SensorRecord]) -> None:
            received.append(recs)

        sink = CallbackSink(async_cb, rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(2))
        await sink.close()
        assert len(received) == 1


# -----------------------------------------------------------------------
# FileSink
# -----------------------------------------------------------------------


class TestFileSink:
    """FileSink CSV and JSON output."""

    @pytest.mark.asyncio
    async def test_csv_output(self, tmp_path: Path) -> None:
        sink = FileSink(path=str(tmp_path), format="csv", rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(5))
        await sink.flush()
        await sink.close()

        csv_files = list(tmp_path.glob("*.csv"))
        assert len(csv_files) == 1
        content = csv_files[0].read_text()
        assert "sensor_0" in content
        # Header + 5 data rows
        lines = [ln for ln in content.strip().splitlines() if ln]
        assert len(lines) == 6

    @pytest.mark.asyncio
    async def test_json_output(self, tmp_path: Path) -> None:
        sink = FileSink(path=str(tmp_path), format="json", rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(3))
        await sink.flush()
        await sink.close()

        jsonl_files = list(tmp_path.glob("*.jsonl"))
        assert len(jsonl_files) == 1
        lines = jsonl_files[0].read_text().strip().splitlines()
        assert len(lines) == 3
        parsed = json.loads(lines[0])
        assert "sensor_name" in parsed

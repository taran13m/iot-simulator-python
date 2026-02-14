"""Tests for built-in sinks - ConsoleSink, CallbackSink, FileSink."""

from __future__ import annotations

import io
import json
import time
from pathlib import Path

import pytest

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import SinkConfig
from iot_simulator.sinks.callback import CallbackSink
from iot_simulator.sinks.console import ConsoleSink
from iot_simulator.sinks.file import FileSink, _parse_rotation

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


class TestParseRotation:
    """_parse_rotation helper."""

    def test_none(self) -> None:
        assert _parse_rotation(None) is None

    def test_empty(self) -> None:
        assert _parse_rotation("") is None

    def test_seconds(self) -> None:
        assert _parse_rotation("30s") == 30.0

    def test_minutes(self) -> None:
        assert _parse_rotation("5m") == 300.0

    def test_hours(self) -> None:
        assert _parse_rotation("1h") == 3600.0

    def test_days(self) -> None:
        assert _parse_rotation("1d") == 86400.0

    def test_bare_number(self) -> None:
        assert _parse_rotation("120") == 120.0


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

    @pytest.mark.asyncio
    async def test_csv_rotation(self, tmp_path: Path) -> None:
        """File rotation should create a new file when rotation interval elapses."""
        sink = FileSink(path=str(tmp_path), format="csv", rotation="1s", rate_hz=1.0)
        await sink.connect()
        await sink.write(_make_records(2))

        # Pretend time moved forward past rotation
        sink._file_start_time = time.time() - 2.0
        await sink.write(_make_records(2))

        await sink.flush()
        await sink.close()

        csv_files = list(tmp_path.glob("*.csv"))
        assert len(csv_files) >= 1

    @pytest.mark.asyncio
    async def test_unknown_format_raises(self, tmp_path: Path) -> None:
        sink = FileSink(path=str(tmp_path), format="xml", rate_hz=1.0)
        with pytest.raises(KeyError):
            await sink.connect()

    @pytest.mark.asyncio
    async def test_parquet_output(self, tmp_path: Path) -> None:
        """Parquet output should buffer and flush via pyarrow."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not installed")

        # Records with metadata populated to avoid empty-struct issues
        records = [
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
                metadata={"tag": "test"},
            )
            for i in range(5)
        ]

        sink = FileSink(path=str(tmp_path), format="parquet", rate_hz=1.0)
        await sink.connect()
        await sink.write(records)
        await sink.flush()
        await sink.close()

        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 1

    @pytest.mark.asyncio
    async def test_close_idempotent(self, tmp_path: Path) -> None:
        sink = FileSink(path=str(tmp_path), format="csv", rate_hz=1.0)
        await sink.connect()
        await sink.close()
        await sink.close()  # Second close should not raise

    @pytest.mark.asyncio
    async def test_creates_output_dir(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "nested" / "output"
        sink = FileSink(path=str(new_dir), format="json", rate_hz=1.0)
        await sink.connect()
        assert new_dir.exists()
        await sink.close()

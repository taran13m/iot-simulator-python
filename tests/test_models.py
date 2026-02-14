"""Tests for iot_simulator.models – SensorRecord creation and serialisation."""

from __future__ import annotations

import json
import time

import pytest

from iot_simulator.models import SensorRecord


# -----------------------------------------------------------------------
# Construction
# -----------------------------------------------------------------------


class TestSensorRecordCreation:
    """SensorRecord instantiation and field defaults."""

    def test_minimal_construction(self) -> None:
        rec = SensorRecord(
            sensor_name="s1",
            industry="mining",
            value=42.5,
            unit="kW",
            sensor_type="power",
            timestamp=1_000_000.0,
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
        )
        assert rec.sensor_name == "s1"
        assert rec.industry == "mining"
        assert rec.value == 42.5
        assert rec.fault_active is False
        assert rec.metadata == {}

    def test_fault_active_override(self) -> None:
        rec = SensorRecord(
            sensor_name="s1",
            industry="mining",
            value=99.0,
            unit="kW",
            sensor_type="power",
            timestamp=1.0,
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
            fault_active=True,
        )
        assert rec.fault_active is True

    def test_metadata_dict(self) -> None:
        rec = SensorRecord(
            sensor_name="s1",
            industry="mining",
            value=1.0,
            unit="kW",
            sensor_type="power",
            timestamp=1.0,
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
            metadata={"location": "pit_a"},
        )
        assert rec.metadata == {"location": "pit_a"}


# -----------------------------------------------------------------------
# Serialisation round-trip
# -----------------------------------------------------------------------


class TestSensorRecordSerialisation:
    """to_dict / to_json / from_dict round-trip."""

    @pytest.fixture()
    def sample_record(self) -> SensorRecord:
        return SensorRecord(
            sensor_name="temp_1",
            industry="utilities",
            value=72.3,
            unit="°C",
            sensor_type="temperature",
            timestamp=1_700_000_000.0,
            min_value=0.0,
            max_value=200.0,
            nominal_value=70.0,
            fault_active=False,
            metadata={"zone": "north"},
        )

    def test_to_dict_returns_plain_dict(self, sample_record: SensorRecord) -> None:
        d = sample_record.to_dict()
        assert isinstance(d, dict)
        assert d["sensor_name"] == "temp_1"
        assert d["metadata"] == {"zone": "north"}

    def test_to_json_returns_valid_json(self, sample_record: SensorRecord) -> None:
        j = sample_record.to_json()
        parsed = json.loads(j)
        assert parsed["value"] == 72.3

    def test_from_dict_roundtrip(self, sample_record: SensorRecord) -> None:
        d = sample_record.to_dict()
        restored = SensorRecord.from_dict(d)
        assert restored == sample_record

    def test_from_dict_with_extra_keys_ignored(self) -> None:
        """from_dict should reject unknown keys (Pydantic strict)."""
        data = {
            "sensor_name": "s",
            "industry": "m",
            "value": 1.0,
            "unit": "u",
            "sensor_type": "t",
            "timestamp": 1.0,
            "min_value": 0.0,
            "max_value": 1.0,
            "nominal_value": 0.5,
        }
        rec = SensorRecord.from_dict(data)
        assert rec.sensor_name == "s"


# -----------------------------------------------------------------------
# Convenience helpers
# -----------------------------------------------------------------------


class TestSensorRecordHelpers:
    """Static helper methods."""

    def test_now_returns_recent_epoch(self) -> None:
        ts = SensorRecord.now()
        assert isinstance(ts, float)
        assert abs(ts - time.time()) < 2.0

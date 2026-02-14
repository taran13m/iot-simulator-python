"""Tests for iot_simulator.generator – DataGenerator init, tick, and from_csv."""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import pytest

from iot_simulator.generator import DataGenerator
from iot_simulator.models import SensorRecord
from iot_simulator.sensor_models import SensorConfig, SensorType


# -----------------------------------------------------------------------
# Basic construction and tick
# -----------------------------------------------------------------------


class TestDataGeneratorInit:
    """DataGenerator construction with built-in and custom sensors."""

    def test_init_with_builtin_industry(self) -> None:
        gen = DataGenerator(industries=["mining"])
        assert gen.sensor_count > 0
        assert "mining" in gen.industries

    def test_init_with_multiple_industries(self) -> None:
        gen = DataGenerator(industries=["mining", "utilities"])
        assert "mining" in gen.industries
        assert "utilities" in gen.industries
        assert gen.sensor_count > 0

    def test_init_with_custom_sensors(self) -> None:
        custom = [
            SensorConfig(
                name="custom_temp",
                sensor_type=SensorType.TEMPERATURE,
                unit="°C",
                min_value=0,
                max_value=100,
                nominal_value=50,
            ),
        ]
        gen = DataGenerator(custom_sensors=custom, custom_industry="lab")
        assert gen.sensor_count == 1
        assert "lab" in gen.industries

    def test_init_with_unknown_industry_skipped(self) -> None:
        gen = DataGenerator(industries=["nonexistent_xyz"])
        assert gen.sensor_count == 0

    def test_empty_init(self) -> None:
        gen = DataGenerator()
        assert gen.sensor_count == 0


class TestDataGeneratorTick:
    """DataGenerator.tick() produces SensorRecord batches."""

    def test_tick_returns_records(self) -> None:
        gen = DataGenerator(industries=["mining"])
        records = gen.tick()
        assert isinstance(records, list)
        assert len(records) == gen.sensor_count
        assert all(isinstance(r, SensorRecord) for r in records)

    def test_tick_record_fields(self) -> None:
        gen = DataGenerator(industries=["mining"])
        records = gen.tick()
        rec = records[0]
        assert rec.industry == "mining"
        assert rec.sensor_name != ""
        assert isinstance(rec.value, float)
        assert isinstance(rec.timestamp, float)

    def test_tick_empty_generator(self) -> None:
        gen = DataGenerator()
        records = gen.tick()
        assert records == []


# -----------------------------------------------------------------------
# from_csv alternative constructor
# -----------------------------------------------------------------------


class TestDataGeneratorFromCSV:
    """DataGenerator.from_csv() loads sensors from a CSV file."""

    def test_from_csv_loads_sensors(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "sensors.csv"
        with csv_file.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "sensor_type", "unit", "min_value", "max_value", "nominal_value"])
            writer.writerow(["temp_1", "temperature", "°C", "0", "100", "50"])
            writer.writerow(["press_1", "pressure", "bar", "0", "10", "5"])

        gen = DataGenerator.from_csv(csv_file, industry="test_lab")
        assert gen.sensor_count == 2
        assert "test_lab" in gen.industries

        records = gen.tick()
        assert len(records) == 2

    def test_from_csv_with_optional_columns(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "sensors.csv"
        with csv_file.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "name", "sensor_type", "unit", "min_value", "max_value",
                "nominal_value", "noise_std", "cyclic",
            ])
            writer.writerow(["motor_speed", "speed", "RPM", "0", "3000", "1500", "10", "true"])

        gen = DataGenerator.from_csv(csv_file)
        assert gen.sensor_count == 1

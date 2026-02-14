"""Tests for pyspark_datasource - mocked PySpark dependency."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

# -----------------------------------------------------------------------
# Mock PySpark setup
# -----------------------------------------------------------------------


def _make_mock_pyspark():
    """Create a full mock PySpark module hierarchy."""
    mock_pyspark = ModuleType("pyspark")
    mock_pyspark_sql = ModuleType("pyspark.sql")
    mock_pyspark_sql_datasource = ModuleType("pyspark.sql.datasource")
    mock_pyspark_sql_types = ModuleType("pyspark.sql.types")

    # DataSource ABCs
    mock_pyspark_sql_datasource.DataSource = type(
        "DataSource",
        (),
        {
            "options": {},
        },
    )
    mock_pyspark_sql_datasource.DataSourceReader = type("DataSourceReader", (), {})
    mock_pyspark_sql_datasource.DataSourceStreamReader = type("DataSourceStreamReader", (), {})
    mock_pyspark_sql_datasource.InputPartition = type("InputPartition", (), {})
    mock_pyspark_sql_types.StructType = MagicMock()

    return {
        "pyspark": mock_pyspark,
        "pyspark.sql": mock_pyspark_sql,
        "pyspark.sql.datasource": mock_pyspark_sql_datasource,
        "pyspark.sql.types": mock_pyspark_sql_types,
    }


def _import_module(mock_modules):
    """Import pyspark_datasource with mocked PySpark."""
    with patch.dict(sys.modules, mock_modules):
        if "iot_simulator.pyspark_datasource" in sys.modules:
            del sys.modules["iot_simulator.pyspark_datasource"]
        import iot_simulator.pyspark_datasource as mod

        return mod


# -----------------------------------------------------------------------
# Helper parsing tests
# -----------------------------------------------------------------------


class TestParseIndustries:
    """_parse_industries helper."""

    def test_default(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_industries({})
        assert result == ["mining"]

    def test_single_industry(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_industries({"industries": "utilities"})
        assert result == ["utilities"]

    def test_multiple_industries(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_industries({"industries": "mining, utilities, energy"})
        assert result == ["mining", "utilities", "energy"]

    def test_strips_whitespace(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_industries({"industries": " mining , utilities "})
        assert result == ["mining", "utilities"]


class TestParseSensorOverrides:
    """_parse_sensor_overrides helper."""

    def test_empty_options(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_sensor_overrides({})
        assert result == {}

    def test_noise_std(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_sensor_overrides({"noiseStd": "0.5"})
        assert result == {"noise_std": 0.5}

    def test_cyclic_true(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_sensor_overrides({"cyclic": "true"})
        assert result == {"cyclic": True}

    def test_cyclic_false(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_sensor_overrides({"cyclic": "false"})
        assert result == {"cyclic": False}

    def test_multiple_overrides(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        result = mod._parse_sensor_overrides(
            {
                "noiseStd": "1.0",
                "driftRate": "0.01",
                "anomalyProbability": "0.05",
            }
        )
        assert result == {
            "noise_std": 1.0,
            "drift_rate": 0.01,
            "anomaly_probability": 0.05,
        }


# -----------------------------------------------------------------------
# DataSource class tests
# -----------------------------------------------------------------------


class TestIoTSimulatorDataSource:
    """IoTSimulatorDataSource class."""

    def test_name(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        assert mod.IoTSimulatorDataSource.name() == "iot_simulator"

    def test_schema(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        ds = mod.IoTSimulatorDataSource.__new__(mod.IoTSimulatorDataSource)
        schema = ds.schema()
        assert "sensor_name" in schema
        assert "value double" in schema

    def test_reader(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        ds = mod.IoTSimulatorDataSource.__new__(mod.IoTSimulatorDataSource)
        ds.options = {"industries": "mining"}
        reader = ds.reader(MagicMock())
        assert isinstance(reader, mod.IoTSimulatorBatchReader)

    def test_stream_reader(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        ds = mod.IoTSimulatorDataSource.__new__(mod.IoTSimulatorDataSource)
        ds.options = {"industries": "mining"}
        reader = ds.streamReader(MagicMock())
        assert isinstance(reader, mod.IoTSimulatorStreamReader)


# -----------------------------------------------------------------------
# Batch reader tests
# -----------------------------------------------------------------------


class TestBatchReader:
    """IoTSimulatorBatchReader."""

    def test_default_num_ticks(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorBatchReader(MagicMock(), {"industries": "mining"})
        assert reader.num_ticks == 10

    def test_custom_num_ticks(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorBatchReader(MagicMock(), {"industries": "mining", "numRows": "5"})
        assert reader.num_ticks == 5

    def test_read_produces_rows(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorBatchReader(MagicMock(), {"industries": "mining", "numRows": "2"})
        rows = list(reader.read(None))
        assert len(rows) > 0
        # Each row should be a tuple
        assert all(isinstance(r, tuple) for r in rows)
        # Each row should have 11 fields (matching _SCHEMA)
        assert all(len(r) == 11 for r in rows)

    def test_read_with_overrides(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorBatchReader(
            MagicMock(),
            {"industries": "mining", "numRows": "1", "noiseStd": "0.0"},
        )
        rows = list(reader.read(None))
        assert len(rows) > 0

    def test_read_with_padding(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorBatchReader(
            MagicMock(),
            {"industries": "mining", "numRows": "1", "metadataPaddingBytes": "100"},
        )
        rows = list(reader.read(None))
        assert len(rows) > 0
        # Last field is metadata JSON - should contain padding
        import json

        metadata = json.loads(rows[0][-1])
        assert "padding" in metadata
        assert len(metadata["padding"]) == 100


# -----------------------------------------------------------------------
# Stream reader tests
# -----------------------------------------------------------------------


class TestStreamReader:
    """IoTSimulatorStreamReader."""

    def test_initial_offset(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorStreamReader(MagicMock(), {"industries": "mining"})
        assert reader.initialOffset() == {"offset": 0}

    def test_latest_offset_advances(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorStreamReader(MagicMock(), {"industries": "mining", "rowsPerBatch": "3"})
        off1 = reader.latestOffset()
        assert off1 == {"offset": 3}
        off2 = reader.latestOffset()
        assert off2 == {"offset": 6}

    def test_partitions(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorStreamReader(MagicMock(), {"industries": "mining"})
        parts = reader.partitions({"offset": 0}, {"offset": 5})
        assert len(parts) == 1
        assert parts[0].start == 0
        assert parts[0].end == 5

    def test_commit_is_noop(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorStreamReader(MagicMock(), {"industries": "mining"})
        reader.commit({"offset": 10})  # Should not raise

    def test_read_produces_rows(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        reader = mod.IoTSimulatorStreamReader(MagicMock(), {"industries": "mining", "rowsPerBatch": "2"})
        partition = mod._RangePartition(0, 2)
        rows = list(reader.read(partition))
        assert len(rows) > 0
        assert all(isinstance(r, tuple) for r in rows)


class TestBuildGenerator:
    """_build_generator helper."""

    def test_unknown_industry_skipped(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        simulators, _padding = mod._build_generator({"industries": "nonexistent_xyz"})
        assert len(simulators) == 0

    def test_valid_industry(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        simulators, _padding = mod._build_generator({"industries": "mining"})
        assert len(simulators) > 0

    def test_padding_bytes(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        _, padding = mod._build_generator({"metadataPaddingBytes": "256"})
        assert padding == 256


class TestTickToRows:
    """_tick_to_rows helper."""

    def test_produces_expected_fields(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        simulators, _ = mod._build_generator({"industries": "mining"})
        rows = mod._tick_to_rows(simulators, 0)
        assert len(rows) > 0
        # Each row has 11 fields
        for row in rows:
            assert len(row) == 11

    def test_with_padding(self) -> None:
        mod = _import_module(_make_mock_pyspark())
        simulators, _ = mod._build_generator({"industries": "mining"})
        rows = mod._tick_to_rows(simulators, 50)
        import json

        for row in rows:
            meta = json.loads(row[-1])
            assert "padding" in meta

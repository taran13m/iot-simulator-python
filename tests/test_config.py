"""Tests for iot_simulator.config – YAML config loading and parsing."""

from __future__ import annotations

from pathlib import Path

import pytest

from iot_simulator.config import SimulatorYAMLConfig, load_yaml_config


# -----------------------------------------------------------------------
# SimulatorYAMLConfig model
# -----------------------------------------------------------------------


class TestSimulatorYAMLConfig:
    """SimulatorYAMLConfig defaults and construction."""

    def test_defaults(self) -> None:
        cfg = SimulatorYAMLConfig()
        assert cfg.industries == []
        assert cfg.update_rate_hz == 2.0
        assert cfg.anomaly_probability is None
        assert cfg.custom_industry == "custom"
        assert cfg.custom_sensors == []
        assert cfg.sink_configs == []
        assert cfg.duration_s is None
        assert cfg.log_level == "INFO"

    def test_custom_values(self) -> None:
        cfg = SimulatorYAMLConfig(
            industries=["mining", "utilities"],
            update_rate_hz=5.0,
            log_level="DEBUG",
            duration_s=60.0,
        )
        assert cfg.industries == ["mining", "utilities"]
        assert cfg.update_rate_hz == 5.0
        assert cfg.log_level == "DEBUG"
        assert cfg.duration_s == 60.0


# -----------------------------------------------------------------------
# load_yaml_config
# -----------------------------------------------------------------------


class TestLoadYAMLConfig:
    """load_yaml_config() parsing from temporary YAML files."""

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_yaml_config(tmp_path / "nonexistent.yaml")

    def test_minimal_config(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "sim.yaml"
        cfg_file.write_text("""\
simulator:
  industries: [mining]
  update_rate_hz: 1.0
""")
        cfg = load_yaml_config(cfg_file)
        assert cfg.industries == ["mining"]
        assert cfg.update_rate_hz == 1.0

    def test_full_config(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "sim.yaml"
        cfg_file.write_text("""\
simulator:
  industries: [mining, utilities]
  update_rate_hz: 4.0
  duration_s: 30
  log_level: DEBUG

custom_sensors:
  industry: smart_office
  sensors:
    - name: room_temp
      sensor_type: temperature
      unit: "°C"
      min_value: 15
      max_value: 35
      nominal_value: 22

sinks:
  - type: console
    rate_hz: 1.0
  - type: file
    path: ./output
    format: csv
""")
        cfg = load_yaml_config(cfg_file)
        assert cfg.industries == ["mining", "utilities"]
        assert cfg.update_rate_hz == 4.0
        assert cfg.duration_s == 30.0
        assert cfg.log_level == "DEBUG"
        assert cfg.custom_industry == "smart_office"
        assert len(cfg.custom_sensors) == 1
        assert cfg.custom_sensors[0].name == "room_temp"
        assert len(cfg.sink_configs) == 2

    def test_empty_file_returns_defaults(self, tmp_path: Path) -> None:
        cfg_file = tmp_path / "empty.yaml"
        cfg_file.write_text("")
        cfg = load_yaml_config(cfg_file)
        assert cfg.industries == []
        assert cfg.sink_configs == []

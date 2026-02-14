"""Tests for iot_simulator.sensor_models – SensorConfig, SensorSimulator, industry helpers."""

from __future__ import annotations

import pytest

from iot_simulator.sensor_models import (
    INDUSTRY_SENSORS,
    IndustryType,
    SensorConfig,
    SensorSimulator,
    SensorType,
    get_all_sensors,
    get_industry_sensors,
)


# -----------------------------------------------------------------------
# SensorConfig (Pydantic model)
# -----------------------------------------------------------------------


class TestSensorConfig:
    """SensorConfig construction, defaults, and immutability."""

    def test_keyword_construction(self) -> None:
        cfg = SensorConfig(
            name="temp_1",
            sensor_type=SensorType.TEMPERATURE,
            unit="°C",
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
        )
        assert cfg.name == "temp_1"
        assert cfg.sensor_type == SensorType.TEMPERATURE
        assert cfg.noise_std == pytest.approx(0.01)
        assert cfg.cyclic is False

    def test_positional_construction(self) -> None:
        """Positional args must work for backward compat with INDUSTRY_SENSORS."""
        cfg = SensorConfig("s1", SensorType.PRESSURE, "bar", 0, 10, 5, 0.05)
        assert cfg.name == "s1"
        assert cfg.noise_std == pytest.approx(0.05)

    def test_positional_with_keyword_mix(self) -> None:
        cfg = SensorConfig(
            "s1", SensorType.SPEED, "RPM", 0, 3000, 1500, 10,
            cyclic=True, cycle_period_seconds=120,
        )
        assert cfg.cyclic is True
        assert cfg.cycle_period_seconds == 120.0

    def test_frozen_model(self) -> None:
        from pydantic import ValidationError

        cfg = SensorConfig(name="s", sensor_type=SensorType.FLOW, unit="LPM",
                           min_value=0, max_value=100, nominal_value=50)
        with pytest.raises(ValidationError, match="frozen"):
            cfg.name = "changed"  # type: ignore[misc]


# -----------------------------------------------------------------------
# SensorSimulator
# -----------------------------------------------------------------------


class TestSensorSimulator:
    """SensorSimulator update behaviour."""

    @pytest.fixture()
    def sim(self) -> SensorSimulator:
        cfg = SensorConfig(
            name="test",
            sensor_type=SensorType.TEMPERATURE,
            unit="°C",
            min_value=10.0,
            max_value=90.0,
            nominal_value=50.0,
            noise_std=0.5,
        )
        return SensorSimulator(cfg)

    def test_initial_value_is_nominal(self, sim: SensorSimulator) -> None:
        assert sim.current_value == sim.config.nominal_value

    def test_update_returns_float(self, sim: SensorSimulator) -> None:
        val = sim.update()
        assert isinstance(val, float)

    def test_update_stays_in_range(self, sim: SensorSimulator) -> None:
        for _ in range(200):
            val = sim.update()
            assert sim.config.min_value <= val <= sim.config.max_value

    def test_get_value_matches_last_update(self, sim: SensorSimulator) -> None:
        val = sim.update()
        assert sim.get_value() == val

    def test_inject_fault(self, sim: SensorSimulator) -> None:
        sim.inject_fault(duration_seconds=5.0)
        assert sim.fault_active is True


# -----------------------------------------------------------------------
# Industry helpers
# -----------------------------------------------------------------------


class TestIndustryHelpers:
    """get_industry_sensors and get_all_sensors."""

    def test_get_industry_sensors_returns_list(self) -> None:
        sims = get_industry_sensors(IndustryType.MINING)
        assert isinstance(sims, list)
        assert len(sims) > 0
        assert all(isinstance(s, SensorSimulator) for s in sims)

    def test_all_industries_have_sensors(self) -> None:
        for industry in IndustryType:
            sims = get_industry_sensors(industry)
            assert len(sims) > 0, f"{industry.value} has no sensors"

    def test_get_all_sensors_covers_all_industries(self) -> None:
        all_sensors = get_all_sensors()
        assert set(all_sensors.keys()) == set(IndustryType)

    def test_industry_sensors_dict_matches_enum(self) -> None:
        assert set(INDUSTRY_SENSORS.keys()) == set(IndustryType)

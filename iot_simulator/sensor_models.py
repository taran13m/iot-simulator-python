"""Realistic sensor models for industrial OT environments.

Defines the core types (``SensorConfig``, ``SensorSimulator``, ``SensorType``,
``IndustryType``) and re-exports the ``INDUSTRY_SENSORS`` catalog so that
callers can continue importing everything from this single module.

Sensor data for the 16 built-in industries lives in ``_sensor_catalog.py``.
"""

from __future__ import annotations

import math
import random
import time
try:
    from enum import StrEnum
except ImportError:  # Python < 3.11
    from enum import Enum

    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """Minimal backport of ``enum.StrEnum`` for Python 3.10."""

from pydantic import BaseModel

__all__ = [
    "INDUSTRY_SENSORS",
    "IndustryType",
    "SensorConfig",
    "SensorSimulator",
    "SensorType",
    "get_all_sensors",
    "get_industry_sensors",
]


class IndustryType(StrEnum):
    """Industrial sector types."""

    MINING = "mining"
    UTILITIES = "utilities"
    MANUFACTURING = "manufacturing"
    OIL_GAS = "oil_gas"
    AEROSPACE = "aerospace"
    SPACE = "space"
    WATER_WASTEWATER = "water_wastewater"
    ELECTRIC_POWER = "electric_power"
    AUTOMOTIVE = "automotive"
    CHEMICAL = "chemical"
    FOOD_BEVERAGE = "food_beverage"
    PHARMACEUTICAL = "pharmaceutical"
    DATA_CENTER = "data_center"
    SMART_BUILDING = "smart_building"
    AGRICULTURE = "agriculture"
    RENEWABLE_ENERGY = "renewable_energy"


class SensorType(StrEnum):
    """Types of industrial sensors."""

    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    FLOW = "flow"
    VIBRATION = "vibration"
    LEVEL = "level"
    SPEED = "speed"
    POWER = "power"
    CURRENT = "current"
    VOLTAGE = "voltage"
    HUMIDITY = "humidity"
    PH = "ph"
    CONDUCTIVITY = "conductivity"
    POSITION = "position"
    STATUS = "status"


class SensorConfig(BaseModel):
    """Configuration for a single sensor."""

    model_config = {"frozen": True}

    name: str
    sensor_type: SensorType
    unit: str
    min_value: float
    max_value: float
    nominal_value: float
    noise_std: float = 0.01
    drift_rate: float = 0.0001
    anomaly_probability: float = 0.001
    anomaly_magnitude: float = 2.0
    update_frequency_hz: float = 1.0
    cyclic: bool = False
    cycle_period_seconds: float = 60.0
    cycle_amplitude: float = 0.1


class SensorSimulator:
    """Simulates a single sensor with realistic behavior."""

    def __init__(self, config: SensorConfig):
        self.config = config
        self.current_value = config.nominal_value
        self.drift_accumulator = 0.0
        self.cycle_offset = random.uniform(0, 2 * math.pi)
        self.last_update = time.time()
        self.fault_active = False
        self.fault_end_time = 0.0

    def update(self) -> float:
        """Update sensor value with realistic variations."""
        now = time.time()
        dt = now - self.last_update
        self.last_update = now

        value = self.config.nominal_value

        # Cyclic variation (rotating equipment, solar cycles, etc.)
        if self.config.cyclic:
            cycle_phase = (2 * math.pi * now / self.config.cycle_period_seconds) + self.cycle_offset
            cycle_variation = math.sin(cycle_phase) * self.config.cycle_amplitude * self.config.nominal_value
            value += cycle_variation

        # Slow drift
        self.drift_accumulator += self.config.drift_rate * dt * random.choice([-1, 1])
        self.drift_accumulator = max(-0.05, min(0.05, self.drift_accumulator))
        value += value * self.drift_accumulator

        # Random noise
        noise = random.gauss(0, self.config.noise_std * abs(self.config.nominal_value))
        value += noise

        # Anomaly injection
        if not self.fault_active and random.random() < self.config.anomaly_probability:
            self.fault_active = True
            self.fault_end_time = now + random.uniform(5, 30)

        if self.fault_active:
            if now < self.fault_end_time:
                anomaly_type = random.choice(["spike", "drift", "oscillation"])
                if anomaly_type == "spike":
                    value *= self.config.anomaly_magnitude
                elif anomaly_type == "drift":
                    value += (self.config.max_value - self.config.min_value) * 0.3
                elif anomaly_type == "oscillation":
                    value += math.sin(now * 10) * (self.config.max_value - self.config.min_value) * 0.2
            else:
                self.fault_active = False

        # Clamp to physical limits
        value = max(self.config.min_value, min(self.config.max_value, value))

        self.current_value = value
        return value

    def get_value(self) -> float:
        """Get current sensor value."""
        return self.current_value

    def inject_fault(self, duration_seconds: float = 10.0):
        """Manually inject a fault condition."""
        self.fault_active = True
        self.fault_end_time = time.time() + duration_seconds


# ---------------------------------------------------------------------------
# Sensor catalog (379 sensors across 16 industries)
# ---------------------------------------------------------------------------

from iot_simulator._sensor_catalog import INDUSTRY_SENSORS  # noqa: E402


def get_industry_sensors(industry: IndustryType) -> list[SensorSimulator]:
    """Get configured simulators for an industry."""
    configs = INDUSTRY_SENSORS.get(industry, [])
    return [SensorSimulator(config) for config in configs]


def get_all_sensors() -> dict[IndustryType, list[SensorSimulator]]:
    """Get all configured simulators for all industries."""
    return {industry: get_industry_sensors(industry) for industry in IndustryType}

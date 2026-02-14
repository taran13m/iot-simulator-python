"""Common data models for the IoT Data Simulator library.

Defines the SensorRecord — the universal record format that all sinks receive.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, Field

__all__ = ["SensorRecord"]


class SensorRecord(BaseModel):
    """A single sensor reading produced by the data generator.

    Every sink receives batches of ``SensorRecord`` objects.  The record
    carries the sensor value together with enough context (industry, unit,
    range, fault status) for sinks to persist or forward without needing
    access to the original sensor configuration.

    Attributes:
        sensor_name: Sensor identifier, e.g. ``"crusher_1_motor_power"``.
        industry: Grouping label, e.g. ``"mining"`` or a custom label.
        value: Current reading produced by the simulator.
        unit: Engineering unit string, e.g. ``"kW"``, ``"°C"``.
        sensor_type: Category string, e.g. ``"power"``, ``"temperature"``.
        timestamp: Unix epoch seconds (float) when the value was generated.
        min_value: Physical minimum of the sensor range.
        max_value: Physical maximum of the sensor range.
        nominal_value: Expected steady-state value.
        fault_active: ``True`` when the simulator has injected a fault.
        metadata: Optional dict for user-provided tags (location, asset id, …).
    """

    sensor_name: str
    industry: str
    value: float
    unit: str
    sensor_type: str
    timestamp: float
    min_value: float
    max_value: float
    nominal_value: float
    fault_active: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation (JSON-safe types)."""
        return self.model_dump()

    def to_json(self) -> str:
        """Return a compact JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SensorRecord:
        """Construct a ``SensorRecord`` from a plain dict."""
        return cls.model_validate(data)

    # ------------------------------------------------------------------
    # Convenience factories
    # ------------------------------------------------------------------

    @staticmethod
    def now() -> float:
        """Return current epoch timestamp (seconds, float)."""
        return time.time()

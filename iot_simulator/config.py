"""Configuration loader for the new sink-aware YAML format.

Parses YAML files with the following top-level sections::

    simulator:        # generator settings (industries, update_rate_hz, …)
    custom_sensors:   # user-defined sensor definitions
    sinks:            # list of sink configs with throughput params

Example:

.. code-block:: yaml

    simulator:
      industries: [mining, utilities]
      update_rate_hz: 2.0

    custom_sensors:
      industry: smart_office
      sensors:
        - name: room_temperature
          sensor_type: temperature
          unit: "°C"
          min_value: 15.0
          max_value: 35.0
          nominal_value: 22.0

    sinks:
      - type: console
        rate_hz: 0.5
      - type: kafka
        bootstrap_servers: localhost:9092
        topic: iot-data
        batch_size: 50
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from iot_simulator.sensor_models import SensorConfig as SensorCfg
from iot_simulator.sensor_models import SensorType

__all__ = ["SimulatorYAMLConfig", "load_yaml_config"]

logger = logging.getLogger("iot_simulator.config")


class SimulatorYAMLConfig(BaseModel):
    """Parsed representation of the full YAML configuration.

    Attributes:
        industries: Built-in industry names to activate.
        update_rate_hz: Generator tick rate.
        anomaly_probability: Global anomaly probability override (optional).
        custom_industry: Grouping label for custom sensors.
        custom_sensors: List of :class:`SensorConfig` dicts ready for the generator.
        sink_configs: Raw dicts passed to the sink factory.
        duration_s: Optional run duration (seconds).
        log_level: Logging level string.
    """

    industries: list[str] = Field(default_factory=list)
    update_rate_hz: float = 2.0
    anomaly_probability: float | None = None
    custom_industry: str = "custom"
    custom_sensors: list[SensorCfg] = Field(default_factory=list)
    sink_configs: list[dict[str, Any]] = Field(default_factory=list)
    duration_s: float | None = None
    log_level: str = "INFO"


def load_yaml_config(path: str | Path) -> SimulatorYAMLConfig:
    """Load and validate a YAML configuration file.

    Returns a :class:`SimulatorYAMLConfig` ready to be passed to
    :class:`Simulator`.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}

    # --- simulator section ---
    sim_section = raw.get("simulator", {})
    industries = sim_section.get("industries", [])
    update_rate_hz = float(sim_section.get("update_rate_hz", 2.0))
    anomaly_prob = sim_section.get("anomaly_probability")
    duration_s = sim_section.get("duration_s")
    log_level = sim_section.get("log_level", "INFO")

    # --- custom_sensors section ---
    cs_section = raw.get("custom_sensors", {})
    custom_industry = cs_section.get("industry", "custom")
    custom_sensors = _parse_custom_sensors(cs_section.get("sensors", []))

    # --- sinks section ---
    sink_configs = raw.get("sinks", [])

    config = SimulatorYAMLConfig(
        industries=industries,
        update_rate_hz=update_rate_hz,
        anomaly_probability=float(anomaly_prob) if anomaly_prob is not None else None,
        custom_industry=custom_industry,
        custom_sensors=custom_sensors,
        sink_configs=sink_configs,
        duration_s=float(duration_s) if duration_s is not None else None,
        log_level=log_level,
    )

    logger.info(
        "Loaded config: %d industries, %d custom sensors, %d sinks",
        len(config.industries),
        len(config.custom_sensors),
        len(config.sink_configs),
    )
    return config


def _parse_custom_sensors(sensor_dicts: list[dict[str, Any]]) -> list[SensorCfg]:
    """Convert raw YAML sensor dicts into ``SensorConfig`` model instances."""
    sensors: list[SensorCfg] = []
    for d in sensor_dicts:
        d = dict(d)  # copy
        # Convert sensor_type string to enum
        raw_type = d.pop("sensor_type", "temperature")
        try:
            sensor_type = SensorType(raw_type.lower().strip())
        except ValueError:
            logger.warning("Unknown sensor_type '%s' - falling back to 'temperature'", raw_type)
            sensor_type = SensorType.TEMPERATURE

        name = d.pop("name")
        unit = d.pop("unit", "")
        min_val = float(d.pop("min_value", 0))
        max_val = float(d.pop("max_value", 100))
        nominal = float(d.pop("nominal_value", 50))

        # Remaining keys map to optional SensorConfig fields
        optional: dict[str, Any] = {}
        for key in (
            "noise_std",
            "drift_rate",
            "anomaly_probability",
            "anomaly_magnitude",
            "update_frequency_hz",
            "cycle_period_seconds",
            "cycle_amplitude",
        ):
            if key in d:
                optional[key] = float(d.pop(key))
        if "cyclic" in d:
            val = d.pop("cyclic")
            optional["cyclic"] = val if isinstance(val, bool) else str(val).lower() in ("true", "1", "yes")

        sensors.append(
            SensorCfg(
                name=name,
                sensor_type=sensor_type,
                unit=unit,
                min_value=min_val,
                max_value=max_val,
                nominal_value=nominal,
                **optional,
            )
        )
    return sensors

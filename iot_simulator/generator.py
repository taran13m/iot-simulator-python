"""Data generator – produces SensorRecord batches each tick.

Wraps the existing ``sensor_models.py`` engine and supports both
built-in industry sensor sets **and** user-defined custom sensors.
"""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Any

from iot_simulator.models import SensorRecord
from iot_simulator.sensor_models import (
    IndustryType,
    SensorConfig,
    SensorSimulator,
    SensorType,
    get_industry_sensors,
)

__all__ = ["DataGenerator"]

logger = logging.getLogger("iot_simulator.generator")


class DataGenerator:
    """Creates and manages ``SensorSimulator`` instances, producing
    :class:`SensorRecord` batches on every call to :meth:`tick`.

    Parameters:
        industries:
            List of built-in industry names (e.g. ``["mining", "utilities"]``).
            Each resolves to the pre-configured sensors in
            ``sensor_models.INDUSTRY_SENSORS``.
        custom_sensors:
            A list of user-defined :class:`SensorConfig` objects.
        custom_industry:
            Grouping label applied to ``custom_sensors`` records.
        update_rate_hz:
            Target generation rate (informational – the actual timing
            is driven by :class:`Simulator`).
    """

    def __init__(
        self,
        industries: list[str] | None = None,
        custom_sensors: list[SensorConfig] | None = None,
        custom_industry: str = "custom",
        update_rate_hz: float = 2.0,
    ) -> None:
        self.update_rate_hz = update_rate_hz
        # { "mining/crusher_1_motor_power": (industry_label, SensorSimulator) }
        self._simulators: dict[str, tuple[str, SensorSimulator]] = {}

        # Register built-in industry sensors
        if industries:
            for name in industries:
                self._add_industry(name)

        # Register custom user-defined sensors
        if custom_sensors:
            self.add_sensors(custom_sensors, industry=custom_industry)

        logger.info(
            "DataGenerator initialised with %d sensors (industries=%s, custom=%d)",
            self.sensor_count,
            industries or [],
            len(custom_sensors) if custom_sensors else 0,
        )

    # ------------------------------------------------------------------
    # Public helpers to add sensors after construction
    # ------------------------------------------------------------------

    def add_sensors(self, sensors: list[SensorConfig], industry: str = "custom") -> None:
        """Register additional sensors under the given industry label."""
        for cfg in sensors:
            sim = SensorSimulator(cfg)
            key = f"{industry}/{cfg.name}"
            self._simulators[key] = (industry, sim)
        logger.info("Added %d custom sensors under industry '%s'", len(sensors), industry)

    # ------------------------------------------------------------------
    # Tick – produces one batch of SensorRecords
    # ------------------------------------------------------------------

    def tick(self) -> list[SensorRecord]:
        """Update every simulator and return a list of fresh records."""
        now = time.time()
        records: list[SensorRecord] = []
        for _key, (industry, sim) in self._simulators.items():
            value = sim.update()
            cfg = sim.config
            records.append(
                SensorRecord(
                    sensor_name=cfg.name,
                    industry=industry,
                    value=value,
                    unit=cfg.unit,
                    sensor_type=cfg.sensor_type.value if isinstance(cfg.sensor_type, SensorType) else str(cfg.sensor_type),
                    timestamp=now,
                    min_value=cfg.min_value,
                    max_value=cfg.max_value,
                    nominal_value=cfg.nominal_value,
                    fault_active=sim.fault_active,
                )
            )
        return records

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sensor_count(self) -> int:
        return len(self._simulators)

    @property
    def industries(self) -> list[str]:
        """Return deduplicated list of industry labels."""
        seen: dict[str, None] = {}
        for industry, _sim in self._simulators.values():
            seen.setdefault(industry)
        return list(seen)

    # ------------------------------------------------------------------
    # Alternative constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_csv(
        cls,
        path: str | Path,
        industry: str = "custom",
        update_rate_hz: float = 2.0,
    ) -> DataGenerator:
        """Load sensor definitions from a CSV file.

        Expected CSV columns (header row required):
            ``name, sensor_type, unit, min_value, max_value, nominal_value``

        Optional columns (use defaults if absent):
            ``noise_std, drift_rate, anomaly_probability, anomaly_magnitude,
            cyclic, cycle_period_seconds, cycle_amplitude``

        Example CSV::

            name,sensor_type,unit,min_value,max_value,nominal_value,noise_std,cyclic
            room_temp,temperature,°C,15,35,22,0.3,false
            co2_level,level,ppm,400,2000,600,15,false
        """
        path = Path(path)
        sensors: list[SensorConfig] = []

        with path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Required fields
                name = row["name"].strip()
                sensor_type = SensorType(row["sensor_type"].strip().lower())
                unit = row["unit"].strip()
                min_val = float(row["min_value"])
                max_val = float(row["max_value"])
                nominal = float(row["nominal_value"])

                # Optional fields with defaults
                kwargs: dict[str, Any] = {}
                if "noise_std" in row and row["noise_std"]:
                    kwargs["noise_std"] = float(row["noise_std"])
                if "drift_rate" in row and row["drift_rate"]:
                    kwargs["drift_rate"] = float(row["drift_rate"])
                if "anomaly_probability" in row and row["anomaly_probability"]:
                    kwargs["anomaly_probability"] = float(row["anomaly_probability"])
                if "anomaly_magnitude" in row and row["anomaly_magnitude"]:
                    kwargs["anomaly_magnitude"] = float(row["anomaly_magnitude"])
                if "cyclic" in row and row["cyclic"]:
                    kwargs["cyclic"] = row["cyclic"].strip().lower() in ("true", "1", "yes")
                if "cycle_period_seconds" in row and row["cycle_period_seconds"]:
                    kwargs["cycle_period_seconds"] = float(row["cycle_period_seconds"])
                if "cycle_amplitude" in row and row["cycle_amplitude"]:
                    kwargs["cycle_amplitude"] = float(row["cycle_amplitude"])

                sensors.append(
                    SensorConfig(name, sensor_type, unit, min_val, max_val, nominal, **kwargs)
                )

        gen = cls(update_rate_hz=update_rate_hz)
        gen.add_sensors(sensors, industry=industry)
        return gen

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _add_industry(self, name: str) -> None:
        """Register all sensors for a built-in industry."""
        try:
            industry_enum = IndustryType(name)
        except ValueError:
            logger.warning("Unknown built-in industry '%s' – skipping", name)
            return

        simulators = get_industry_sensors(industry_enum)
        for sim in simulators:
            key = f"{name}/{sim.config.name}"
            self._simulators[key] = (name, sim)

        logger.info("Loaded %d sensors for built-in industry '%s'", len(simulators), name)

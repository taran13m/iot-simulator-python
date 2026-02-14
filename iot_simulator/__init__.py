"""IoT Data Simulator - generate realistic industrial sensor data and push
it to pluggable sinks with per-sink throughput control.

Quick start::

    from iot_simulator import Simulator, SensorConfig, SensorType
    from iot_simulator.sinks import ConsoleSink

    sim = Simulator(industries=["mining", "utilities"], update_rate_hz=2.0)
    sim.add_sink(ConsoleSink(rate_hz=0.5))
    sim.run(duration_s=10)
"""

from __future__ import annotations

from iot_simulator.generator import DataGenerator
from iot_simulator.models import SensorRecord
from iot_simulator.sensor_models import IndustryType, SensorConfig, SensorType
from iot_simulator.simulator import Simulator

__all__ = [
    "DataGenerator",
    "IndustryType",
    "SensorConfig",
    "SensorRecord",
    "SensorType",
    "Simulator",
]

__version__ = "0.1.0"

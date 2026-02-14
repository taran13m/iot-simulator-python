"""Tests for iot_simulator.simulator - Simulator wiring, add_sink, run."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from iot_simulator.models import SensorRecord
from iot_simulator.sensor_models import SensorConfig, SensorType
from iot_simulator.simulator import Simulator
from iot_simulator.sinks.callback import CallbackSink
from iot_simulator.sinks.console import ConsoleSink

# -----------------------------------------------------------------------
# Construction
# -----------------------------------------------------------------------


class TestSimulatorConstruction:
    """Simulator initialisation and sensor wiring."""

    def test_init_with_industry(self) -> None:
        sim = Simulator(industries=["mining"])
        assert sim.sensor_count > 0

    def test_init_with_custom_sensors(self) -> None:
        custom = [
            SensorConfig(
                name="s1",
                sensor_type=SensorType.TEMPERATURE,
                unit="°C",
                min_value=0,
                max_value=100,
                nominal_value=50,
            ),
        ]
        sim = Simulator(custom_sensors=custom, custom_industry="lab")
        assert sim.sensor_count == 1

    def test_add_sensors_after_init(self) -> None:
        sim = Simulator(industries=["mining"])
        initial = sim.sensor_count
        sim.add_sensors(
            [
                SensorConfig(
                    name="extra",
                    sensor_type=SensorType.PRESSURE,
                    unit="bar",
                    min_value=0,
                    max_value=10,
                    nominal_value=5,
                )
            ],
            industry="extra",
        )
        assert sim.sensor_count == initial + 1


# -----------------------------------------------------------------------
# Sink registration
# -----------------------------------------------------------------------


class TestSimulatorSinks:
    """add_sink with Sink instances and bare callables."""

    def test_add_sink_with_sink_instance(self) -> None:
        sim = Simulator(industries=["mining"])
        sim.add_sink(ConsoleSink(rate_hz=1.0))
        assert len(sim._runners) == 1

    def test_add_sink_with_callable(self) -> None:
        received: list[list[SensorRecord]] = []
        sim = Simulator(industries=["mining"])
        sim.add_sink(lambda recs: received.append(recs))
        assert len(sim._runners) == 1

    def test_add_multiple_sinks(self) -> None:
        sim = Simulator(industries=["mining"])
        sim.add_sink(ConsoleSink(rate_hz=1.0))
        sim.add_sink(CallbackSink(lambda r: None, rate_hz=1.0))
        assert len(sim._runners) == 2


# -----------------------------------------------------------------------
# Run (short duration)
# -----------------------------------------------------------------------


class TestSimulatorRun:
    """Simulator.run / run_async with a short duration."""

    def test_run_with_callback_collects_records(self) -> None:
        received: list[list[SensorRecord]] = []

        sim = Simulator(industries=["mining"], update_rate_hz=10.0)
        sim.add_sink(CallbackSink(lambda recs: received.append(recs), rate_hz=5.0))
        sim.run(duration_s=0.5)

        assert len(received) > 0
        assert all(isinstance(r, SensorRecord) for batch in received for r in batch)

    @pytest.mark.asyncio
    async def test_run_async(self) -> None:
        received: list[list[SensorRecord]] = []

        sim = Simulator(industries=["mining"], update_rate_hz=10.0)
        sim.add_sink(CallbackSink(lambda recs: received.append(recs), rate_hz=5.0))
        await sim.run_async(duration_s=0.5)

        assert len(received) > 0

    def test_run_no_sinks_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        sim = Simulator(industries=["mining"])
        sim.run(duration_s=0.1)
        assert "No sinks registered" in caplog.text

    def test_run_with_multiple_sinks(self) -> None:
        received_1: list[list[SensorRecord]] = []
        received_2: list[list[SensorRecord]] = []

        sim = Simulator(industries=["mining"], update_rate_hz=10.0)
        sim.add_sink(CallbackSink(lambda recs: received_1.append(recs), rate_hz=5.0))
        sim.add_sink(CallbackSink(lambda recs: received_2.append(recs), rate_hz=5.0))
        sim.run(duration_s=0.5)

        assert len(received_1) > 0
        assert len(received_2) > 0


# -----------------------------------------------------------------------
# Sink throughput overrides
# -----------------------------------------------------------------------


class TestSinkOverrides:
    """Override rate_hz and batch_size via add_sink."""

    def test_override_rate_on_sink(self) -> None:
        buf = io.StringIO()
        sink = ConsoleSink(fmt="text", stream=buf, rate_hz=1.0)
        sim = Simulator(industries=["mining"])
        sim.add_sink(sink, rate_hz=5.0)
        assert sim._runners[0].cfg.rate_hz == 5.0

    def test_override_batch_size_on_sink(self) -> None:
        buf = io.StringIO()
        sink = ConsoleSink(fmt="text", stream=buf, rate_hz=1.0)
        sim = Simulator(industries=["mining"])
        sim.add_sink(sink, batch_size=50)
        assert sim._runners[0].cfg.batch_size == 50

    def test_callable_with_rate_and_batch(self) -> None:
        sim = Simulator(industries=["mining"])
        sim.add_sink(lambda recs: None, rate_hz=2.0, batch_size=25)
        assert sim._runners[0].cfg.rate_hz == 2.0
        assert sim._runners[0].cfg.batch_size == 25


# -----------------------------------------------------------------------
# from_csv
# -----------------------------------------------------------------------


class TestSimulatorFromCSV:
    """Simulator.from_csv class method."""

    def test_from_csv(self, tmp_path: Path) -> None:
        csv_content = "name,sensor_type,unit,min_value,max_value,nominal_value\n"
        csv_content += "temp_1,temperature,C,0,100,50\n"
        csv_content += "press_1,pressure,bar,0,10,5\n"

        csv_file = tmp_path / "sensors.csv"
        csv_file.write_text(csv_content)

        sim = Simulator.from_csv(str(csv_file), industry="test_lab")
        assert sim.sensor_count == 2

#!/usr/bin/env python3
"""ConsoleSink examples -- 4 cases demonstrating different output formats,
frequencies, and filtering strategies.

Directly runnable (no external services required).

Usage::

    python examples/sinks/console_sink_example.py           # Case 1 (default)
    python examples/sinks/console_sink_example.py --case 2   # JSON with metadata
    python examples/sinks/console_sink_example.py --case 3   # High-frequency firehose
    python examples/sinks/console_sink_example.py --case 4   # Fault-only filtered output
"""

from __future__ import annotations

import argparse

# ---------------------------------------------------------------------------
# Case 1: Text format -- low frequency, human-readable
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Human-readable text output at a comfortable reading pace.

    Knobs demonstrated:
      - fmt="text"          -> one-line-per-record text format
      - update_rate_hz=2.0  -> 2 batches/sec from the generator
      - rate_hz=0.5         -> flush to console every 2 seconds
      - batch_size=50       -> at most 50 records per flush
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks import ConsoleSink

    print("=== Case 1: Text format (low frequency) ===\n")

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)
    sim.add_sink(ConsoleSink(fmt="text", rate_hz=0.5, batch_size=50))
    sim.run(duration_s=6)


# ---------------------------------------------------------------------------
# Case 2: JSON format with pre-populated metadata
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """JSON output with custom sensors carrying metadata tags.

    Knobs demonstrated:
      - fmt="json"          -> one JSON object per record
      - custom_sensors      -> small payload (2 sensors)
      - metadata dict       -> extra fields (location, asset_id) in each record
      - rate_hz=1.0         -> flush once per second
    """
    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks import ConsoleSink

    print("=== Case 2: JSON format with metadata ===\n")

    sim = Simulator(
        custom_sensors=[
            SensorConfig("room_temp", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("room_humidity", SensorType.HUMIDITY, "%RH", 30, 70, 50),
        ],
        custom_industry="smart_office",
        update_rate_hz=2.0,
    )

    # Inject metadata into every record before it reaches the console
    def inject_metadata(records):
        for r in records:
            r.metadata["location"] = "building-a"
            r.metadata["asset_id"] = "HVAC-001"
            r.metadata["floor"] = 3

    # First sink: inject metadata via callback (runs before console sees data)
    sim.add_sink(inject_metadata, rate_hz=1.0, batch_size=20)

    # Second sink: print JSON to console
    sim.add_sink(ConsoleSink(fmt="json", rate_hz=1.0, batch_size=20))

    sim.run(duration_s=6)


# ---------------------------------------------------------------------------
# Case 3: High-frequency firehose
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Stress-test stdout with a fast generator and no rate limiting.

    Knobs demonstrated:
      - update_rate_hz=10.0  -> 10 batches/sec (17 sensors * 10 = 170 records/sec)
      - rate_hz=None          -> flush every generator tick (lowest latency)
      - batch_size=200        -> large batch to reduce flush overhead
      - industries=["utilities"]  -> 17 sensors per tick
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks import ConsoleSink

    print("=== Case 3: High-frequency firehose ===\n")

    sim = Simulator(industries=["utilities"], update_rate_hz=10.0)

    # rate_hz=None means "flush every tick" (no extra buffering)
    sim.add_sink(ConsoleSink(fmt="text", rate_hz=None, batch_size=200))

    sim.run(duration_s=3)


# ---------------------------------------------------------------------------
# Case 4: Fault-only filtered output
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Print only records where a fault/anomaly is active.

    Uses a CallbackSink to filter records before printing. This pattern is
    useful for monitoring and alerting pipelines.

    Knobs demonstrated:
      - anomaly_probability  -> set high (0.3) on custom sensors to trigger faults
      - CallbackSink filter  -> only forward fault records to stdout
      - update_rate_hz=5.0   -> faster generation to catch more anomalies
    """
    from iot_simulator import SensorConfig, SensorType, Simulator

    print("=== Case 4: Fault-only filtered output ===\n")
    print("(Only printing records with fault_active=True)\n")

    # Sensors with very high anomaly probability for demo purposes
    sim = Simulator(
        custom_sensors=[
            SensorConfig(
                "pump_vibration",
                SensorType.VIBRATION,
                "mm/s",
                0.5,
                15.0,
                2.5,
                noise_std=0.3,
                anomaly_probability=0.3,  # 30% chance of fault
                anomaly_magnitude=3.0,
            ),
            SensorConfig(
                "motor_temp",
                SensorType.TEMPERATURE,
                "°C",
                40,
                95,
                65,
                noise_std=2.0,
                anomaly_probability=0.3,
                anomaly_magnitude=2.0,
            ),
        ],
        custom_industry="demo",
        update_rate_hz=5.0,
    )

    # Filter: only print fault records
    def fault_filter(records):
        faults = [r for r in records if r.fault_active]
        if faults:
            for r in faults:
                print(
                    f"  FAULT [{r.industry}/{r.sensor_name}] "
                    f"value={r.value:.3f} {r.unit} "
                    f"(nominal={r.nominal_value:.1f}, max={r.max_value:.1f})"
                )

    sim.add_sink(fault_filter, rate_hz=2.0, batch_size=50)
    sim.run(duration_s=8)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="ConsoleSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

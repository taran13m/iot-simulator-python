#!/usr/bin/env python3
"""CallbackSink examples -- 5 cases demonstrating lambda shortcuts, async
callbacks, aggregation, metadata padding, and record transformation.

Directly runnable (no external services required).

Usage::

    python examples/sinks/callback_sink_example.py           # Case 1 (default)
    python examples/sinks/callback_sink_example.py --case 2   # Async callback
    python examples/sinks/callback_sink_example.py --case 3   # Aggregation analytics
    python examples/sinks/callback_sink_example.py --case 4   # Metadata padding (load test)
    python examples/sinks/callback_sink_example.py --case 5   # Unit transformation
"""

from __future__ import annotations

import argparse

# ---------------------------------------------------------------------------
# Case 1: Lambda shorthand -- simplest possible sink
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """The absolute simplest sink: a lambda that prints batch sizes.

    Knobs demonstrated:
      - lambda as sink   -> no class needed, just a callable
      - rate_hz=1.0      -> flush once per second
      - batch_size=50    -> up to 50 records per call
    """
    from iot_simulator import Simulator

    print("=== Case 1: Lambda shorthand ===\n")

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    # Simplest possible sink -- just a lambda
    sim.add_sink(
        lambda records: print(f"  Received {len(records)} records"),
        rate_hz=1.0,
        batch_size=50,
    )

    sim.run(duration_s=6)


# ---------------------------------------------------------------------------
# Case 2: Async callback -- auto-detected by CallbackSink
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Async callback that simulates I/O-bound processing.

    CallbackSink auto-detects async functions via inspect.iscoroutinefunction
    and awaits them directly instead of using run_in_executor.

    Knobs demonstrated:
      - async def callback  -> awaited natively in the event loop
      - rate_hz=2.0         -> 2 flushes per second
      - batch_size=30       -> moderate batch size
    """
    import asyncio

    from iot_simulator import Simulator
    from iot_simulator.sinks.callback import CallbackSink

    print("=== Case 2: Async callback ===\n")

    async def async_processor(records):
        """Simulate async I/O (e.g., writing to an async database)."""
        # Simulate 10ms of async I/O
        await asyncio.sleep(0.01)
        sensors = {r.sensor_name for r in records}
        print(f"  [async] Processed {len(records)} records from {len(sensors)} sensors")

    sim = Simulator(industries=["manufacturing"], update_rate_hz=2.0)
    sim.add_sink(CallbackSink(async_processor, rate_hz=2.0, batch_size=30))
    sim.run(duration_s=6)


# ---------------------------------------------------------------------------
# Case 3: Aggregation callback -- running analytics
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Compute running min/max/average across batches.

    Demonstrates using a callback for real-time analytics on the stream.

    Knobs demonstrated:
      - Stateful callback  -> accumulates stats across multiple flushes
      - rate_hz=1.0        -> aggregate once per second
      - batch_size=100     -> larger batch for meaningful stats
    """
    from iot_simulator import Simulator

    print("=== Case 3: Aggregation callback ===\n")

    # Mutable state for running statistics
    stats = {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf")}

    def aggregate(records):
        for r in records:
            stats["count"] += 1
            stats["sum"] += r.value
            stats["min"] = min(stats["min"], r.value)
            stats["max"] = max(stats["max"], r.value)

        avg = stats["sum"] / stats["count"] if stats["count"] else 0
        print(
            f"  [stats] total={stats['count']:>6d}  "
            f"avg={avg:>12.3f}  "
            f"min={stats['min']:>12.3f}  "
            f"max={stats['max']:>12.3f}"
        )

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)
    sim.add_sink(aggregate, rate_hz=1.0, batch_size=100)
    sim.run(duration_s=8)

    print(f"\n  Final: {stats['count']} records processed")


# ---------------------------------------------------------------------------
# Case 4: Metadata padding for load testing
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Inflate each record with ~1KB of padding in the metadata dict.

    Useful for stress-testing downstream systems that need to handle
    large payloads. Each record's serialised size jumps from ~300 bytes
    to ~1.3KB.

    Knobs demonstrated:
      - metadata["padding"]  -> 1KB string per record
      - update_rate_hz=5.0   -> high generation rate
      - batch_size=200       -> large batches
      - 4 industries         -> many sensors (60+) for high throughput
    """

    from iot_simulator import Simulator

    print("=== Case 4: Metadata padding for load testing ===\n")

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing", "oil_gas"],
        update_rate_hz=5.0,
    )

    total_bytes = 0

    def pad_and_measure(records):
        nonlocal total_bytes
        for r in records:
            # Inflate payload with ~1KB of padding
            r.metadata["padding"] = "x" * 1024
            r.metadata["device_id"] = "load-test-001"

        # Measure approximate serialised size
        batch_size = sum(len(r.to_json()) for r in records)
        total_bytes += batch_size
        print(
            f"  Batch: {len(records):>4d} records, "
            f"{batch_size / 1024:.1f} KB  "
            f"(cumulative: {total_bytes / 1024:.0f} KB)"
        )

    sim.add_sink(pad_and_measure, rate_hz=2.0, batch_size=200)
    sim.run(duration_s=6)

    print(f"\n  Total data generated: {total_bytes / 1024:.0f} KB")


# ---------------------------------------------------------------------------
# Case 5: Record transformation -- unit conversion
# ---------------------------------------------------------------------------


def run_case_5() -> None:
    """Transform records (Celsius -> Fahrenheit) and round values.

    Demonstrates using a callback as a processing/transformation step.

    Knobs demonstrated:
      - Value mutation    -> convert units in-place
      - custom_sensors    -> temperature sensors for conversion demo
      - rate_hz=1.0       -> moderate pace for readability
    """
    from iot_simulator import SensorConfig, SensorType, Simulator

    print("=== Case 5: Record transformation (C -> F) ===\n")

    sim = Simulator(
        custom_sensors=[
            SensorConfig("indoor_temp", SensorType.TEMPERATURE, "°C", 15, 35, 22, 0.5),
            SensorConfig("outdoor_temp", SensorType.TEMPERATURE, "°C", -10, 45, 18, 1.0),
            SensorConfig("server_room_temp", SensorType.TEMPERATURE, "°C", 18, 28, 22, 0.3),
        ],
        custom_industry="building",
        update_rate_hz=2.0,
    )

    def celsius_to_fahrenheit(records):
        for r in records:
            original_c = r.value
            # Convert value
            r.value = round(r.value * 9 / 5 + 32, 2)
            r.unit = "°F"
            # Convert range bounds too
            r.min_value = round(r.min_value * 9 / 5 + 32, 2)
            r.max_value = round(r.max_value * 9 / 5 + 32, 2)
            r.nominal_value = round(r.nominal_value * 9 / 5 + 32, 2)

            print(
                f"  {r.sensor_name:<20s}  "
                f"{original_c:>6.1f}°C -> {r.value:>6.1f}°F  "
                f"(range: {r.min_value:.0f}-{r.max_value:.0f}°F)"
            )

    sim.add_sink(celsius_to_fahrenheit, rate_hz=1.0, batch_size=20)
    sim.run(duration_s=6)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="CallbackSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4, 5], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {
        1: run_case_1,
        2: run_case_2,
        3: run_case_3,
        4: run_case_4,
        5: run_case_5,
    }
    cases[args.case]()


if __name__ == "__main__":
    main()

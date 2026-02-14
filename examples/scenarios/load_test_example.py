#!/usr/bin/env python3
"""Load test example -- stress-test the simulator with all industries, high
generation rates, metadata padding, and different backpressure policies.

Directly runnable (no external services required). Uses CallbackSink to
measure throughput.

Usage::

    python examples/scenarios/load_test_example.py           # Case 1 (default)
    python examples/scenarios/load_test_example.py --case 2   # drop_newest policy
    python examples/scenarios/load_test_example.py --case 3   # block policy
"""

from __future__ import annotations

import argparse
import time


def _run_load_test(
    backpressure: str,
    max_buffer_size: int,
    update_rate_hz: float = 50.0,
    duration_s: float = 10.0,
    pad_size: int = 1024,
) -> None:
    """Run a load test with the given backpressure configuration.

    Parameters:
        backpressure: "drop_oldest", "drop_newest", or "block"
        max_buffer_size: Maximum records held in the sink buffer
        update_rate_hz: How fast the generator produces data
        duration_s: Run duration
        pad_size: Bytes of padding to add to each record's metadata
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks.callback import CallbackSink

    # All 16 built-in industries
    all_industries = [
        "mining", "utilities", "manufacturing", "oil_gas",
        "aerospace", "space", "water_wastewater", "electric_power",
        "automotive", "chemical", "food_beverage", "pharmaceutical",
        "data_center", "smart_building", "agriculture", "renewable_energy",
    ]

    sim = Simulator(
        industries=all_industries,
        update_rate_hz=update_rate_hz,
    )

    print(f"  Active sensors:    {sim.sensor_count}")
    print(f"  Generator rate:    {update_rate_hz} Hz")
    print(f"  Expected output:   ~{sim.sensor_count * update_rate_hz:.0f} records/sec")
    print(f"  Metadata padding:  {pad_size} bytes/record")
    print(f"  Backpressure:      {backpressure}")
    print(f"  Buffer limit:      {max_buffer_size}")
    print(f"  Duration:          {duration_s}s\n")

    # Throughput measurement state
    stats = {
        "total_records": 0,
        "total_bytes": 0,
        "batches": 0,
        "dropped_estimate": 0,
        "start_time": None,
    }

    def measure_throughput(records):
        if stats["start_time"] is None:
            stats["start_time"] = time.time()

        # Add metadata padding to inflate payload
        for r in records:
            r.metadata["padding"] = "x" * pad_size
            r.metadata["load_test"] = True

        batch_bytes = sum(len(r.to_json()) for r in records)
        stats["total_records"] += len(records)
        stats["total_bytes"] += batch_bytes
        stats["batches"] += 1

        elapsed = time.time() - stats["start_time"]
        if elapsed > 0:
            rps = stats["total_records"] / elapsed
            mbps = stats["total_bytes"] / elapsed / 1024 / 1024
            print(
                f"  [{elapsed:>5.1f}s] "
                f"batch #{stats['batches']:>4d}  "
                f"{len(records):>4d} records  "
                f"cumulative: {stats['total_records']:>7d} records  "
                f"{rps:>8.0f} rec/s  "
                f"{mbps:>5.2f} MB/s"
            )

    sim.add_sink(CallbackSink(
        measure_throughput,
        rate_hz=10.0,                     # 10 flushes per second
        batch_size=500,                   # 500 records per flush
        max_buffer_size=max_buffer_size,
        backpressure=backpressure,
    ))

    sim.run(duration_s=duration_s)

    # Final summary
    elapsed = time.time() - (stats["start_time"] or time.time())
    print(f"\n  === Load Test Results ===")
    print(f"  Total records:     {stats['total_records']:,}")
    print(f"  Total data:        {stats['total_bytes'] / 1024 / 1024:.2f} MB")
    print(f"  Avg throughput:    {stats['total_records'] / max(elapsed, 0.001):,.0f} records/sec")
    print(f"  Avg bandwidth:     {stats['total_bytes'] / max(elapsed, 0.001) / 1024 / 1024:.2f} MB/sec")
    print(f"  Batches written:   {stats['batches']}")


# ---------------------------------------------------------------------------
# Case 1: drop_oldest (default backpressure)
# ---------------------------------------------------------------------------

def run_case_1() -> None:
    """Load test with drop_oldest backpressure.

    When the buffer fills up, the oldest records are silently discarded
    to make room for new ones. This ensures the sink always has the
    freshest data.

    Knobs demonstrated:
      - backpressure="drop_oldest"
      - max_buffer_size=5000
      - All 16 industries (~300+ sensors)
      - update_rate_hz=50 (high generation rate)
      - metadata padding (~1KB per record)
    """
    print("=== Case 1: Load test -- drop_oldest backpressure ===\n")
    _run_load_test(
        backpressure="drop_oldest",
        max_buffer_size=5000,
        update_rate_hz=50.0,
        duration_s=10.0,
    )


# ---------------------------------------------------------------------------
# Case 2: drop_newest
# ---------------------------------------------------------------------------

def run_case_2() -> None:
    """Load test with drop_newest backpressure.

    When the buffer fills up, new incoming records are dropped. This
    preserves the oldest data (useful when historical completeness
    matters more than freshness).

    Knobs demonstrated:
      - backpressure="drop_newest"
      - max_buffer_size=2000 (intentionally small to trigger drops)
    """
    print("=== Case 2: Load test -- drop_newest backpressure ===\n")
    _run_load_test(
        backpressure="drop_newest",
        max_buffer_size=2000,    # Small buffer to trigger drops
        update_rate_hz=50.0,
        duration_s=10.0,
    )


# ---------------------------------------------------------------------------
# Case 3: block
# ---------------------------------------------------------------------------

def run_case_3() -> None:
    """Load test with blocking backpressure.

    When the buffer fills up, the generator blocks until space is
    available. This guarantees no data loss but may slow down the
    generator.

    Knobs demonstrated:
      - backpressure="block"
      - max_buffer_size=3000
      - Lower update_rate_hz=20 (to avoid excessive blocking)
    """
    print("=== Case 3: Load test -- block backpressure ===\n")
    _run_load_test(
        backpressure="block",
        max_buffer_size=3000,
        update_rate_hz=20.0,     # Lower rate to avoid excessive blocking
        duration_s=10.0,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Load test examples")
    parser.add_argument("--case", type=int, default=1, choices=[1, 2, 3],
                        help="Which backpressure policy to test (default: 1)")
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3}
    cases[args.case]()


if __name__ == "__main__":
    main()

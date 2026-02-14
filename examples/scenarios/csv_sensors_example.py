#!/usr/bin/env python3
"""CSV sensor definitions example -- load custom sensors from a CSV file
using Simulator.from_csv() instead of defining them programmatically.

This is useful when sensor definitions are maintained in spreadsheets or
exported from asset management systems.

Directly runnable (no external services required).

Usage::

    python examples/scenarios/csv_sensors_example.py
"""

from __future__ import annotations

from pathlib import Path


def main() -> None:
    from iot_simulator import Simulator
    from iot_simulator.sinks import ConsoleSink

    print("=== CSV Sensor Definitions Example ===\n")

    # Resolve the CSV file relative to this script
    csv_path = Path(__file__).parent.parent / "configs" / "custom_sensors.csv"

    if not csv_path.exists():
        print(f"  CSV file not found: {csv_path}")
        return

    # Show the CSV contents
    print(f"  Loading sensors from: {csv_path}\n")
    lines = csv_path.read_text().strip().split("\n")
    print(f"  CSV header: {lines[0]}")
    print(f"  Sensor rows: {len(lines) - 1}\n")
    for line in lines[1:]:
        name = line.split(",")[0]
        stype = line.split(",")[1]
        unit = line.split(",")[2]
        print(f"    {name:<24s} type={stype:<14s} unit={unit}")

    print()

    # --- Build Simulator from CSV ---
    sim = Simulator.from_csv(
        csv_path,
        industry="smart_office",
        update_rate_hz=2.0,
    )

    print(f"  Loaded {sim.sensor_count} sensors as 'smart_office' industry\n")

    # Add a console sink for visibility
    sim.add_sink(
        ConsoleSink(
            fmt="text",
            rate_hz=1.0,
            batch_size=50,
        )
    )

    sim.run(duration_s=10)


if __name__ == "__main__":
    main()

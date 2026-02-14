#!/usr/bin/env python3
"""Multi-sink fan-out example -- one Simulator writing to Console, File,
and Database simultaneously, each with independent throughput settings.

Demonstrates that each sink consumes data at its own pace: the Console
flushes frequently for real-time visibility, the File sink batches for
efficiency, and the Database flushes at a moderate rate.

Requires the database extra for the DatabaseSink::

    pip install iot-data-simulator[database]

Usage::

    python examples/scenarios/multi_sink_fanout_example.py
"""

from __future__ import annotations

import os
import shutil


def main() -> None:
    from iot_simulator import Simulator
    from iot_simulator.sinks import ConsoleSink
    from iot_simulator.sinks.file import FileSink

    print("=== Multi-Sink Fan-Out Example ===\n")

    output_dir = "./output/multi_sink_fanout"
    db_path = "output/multi_sink_fanout.db"

    # Clean up from previous runs
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    if os.path.exists(db_path):
        os.remove(db_path)
    os.makedirs(output_dir, exist_ok=True)

    sim = Simulator(
        industries=["mining", "utilities"],
        update_rate_hz=2.0,
    )

    print(f"  Active sensors: {sim.sensor_count}")
    print(f"  Generator rate: 2.0 Hz\n")

    # --- Sink 1: Console (fast, for real-time visibility) ---
    sim.add_sink(ConsoleSink(
        fmt="text",
        rate_hz=2.0,        # Flush 2 times per second
        batch_size=20,       # Small batch for readability
    ))
    print("  Sink 1: Console  (rate_hz=2.0, batch_size=20)")

    # --- Sink 2: CSV File (batched, for offline analysis) ---
    sim.add_sink(FileSink(
        path=output_dir,
        format="csv",
        rate_hz=0.5,         # Flush every 2 seconds
        batch_size=500,      # Large batch for efficient writes
    ))
    print("  Sink 2: File/CSV (rate_hz=0.5, batch_size=500)")

    # --- Sink 3: SQLite Database (moderate throughput) ---
    try:
        from iot_simulator.sinks.database import DatabaseSink

        sim.add_sink(DatabaseSink(
            connection_string=f"sqlite+aiosqlite:///{db_path}",
            table="sensor_data",
            rate_hz=1.0,         # Flush once per second
            batch_size=200,      # Moderate batch
        ))
        print("  Sink 3: Database (rate_hz=1.0, batch_size=200)")
        db_enabled = True
    except ImportError:
        print("  Sink 3: Database (SKIPPED -- pip install iot-data-simulator[database])")
        db_enabled = False

    print(f"\n  Running for 10 seconds...\n")
    sim.run(duration_s=10)

    # --- Summary ---
    print("\n  === Summary ===\n")

    # File output
    if os.path.exists(output_dir):
        files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
        total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in files)
        print(f"  File sink:     {len(files)} file(s), {total_size / 1024:.1f} KB")

    # Database output
    if db_enabled and os.path.exists(db_path):
        import asyncio

        async def count_rows():
            import aiosqlite
            async with aiosqlite.connect(db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM sensor_data") as cur:
                    count = (await cur.fetchone())[0]
                    print(f"  Database sink: {count} rows in {db_path}")

        asyncio.run(count_rows())


if __name__ == "__main__":
    main()

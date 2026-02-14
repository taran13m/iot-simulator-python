#!/usr/bin/env python3
"""DatabaseSink examples -- 4 cases demonstrating SQLite storage, custom table
names, backpressure/retry configuration, and post-run data verification.

Directly runnable using SQLite (no external database required).

Requires the database extra::

    pip install iot-data-simulator[database]

Usage::

    python examples/sinks/database_sink_example.py           # Case 1 (default)
    python examples/sinks/database_sink_example.py --case 2   # Custom table name
    python examples/sinks/database_sink_example.py --case 3   # Backpressure and retry
    python examples/sinks/database_sink_example.py --case 4   # Query after run
"""

from __future__ import annotations

import argparse
import os


def _check_database():
    try:
        from iot_simulator.sinks.database import DatabaseSink  # noqa: F401
        return True
    except ImportError:
        print("DatabaseSink requires sqlalchemy + aiosqlite. Install with:")
        print("  pip install iot-data-simulator[database]")
        return False


def _clean_db(path: str) -> None:
    """Remove SQLite database file from previous runs."""
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------------------
# Case 1: SQLite basic
# ---------------------------------------------------------------------------

def run_case_1() -> None:
    """Basic SQLite storage with moderate throughput.

    Knobs demonstrated:
      - connection_string  -> SQLite via aiosqlite (zero-config database)
      - rate_hz=1.0        -> flush once per second
      - batch_size=200     -> 200 records per INSERT batch
    """
    if not _check_database():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.database import DatabaseSink

    print("=== Case 1: SQLite basic ===\n")

    db_path = "output/example_sensors.db"
    os.makedirs("output", exist_ok=True)
    _clean_db(db_path)

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(DatabaseSink(
        connection_string=f"sqlite+aiosqlite:///{db_path}",
        table="sensor_data",
        rate_hz=1.0,
        batch_size=200,
    ))

    sim.run(duration_s=8)
    print(f"\n  Data written to: {db_path}")


# ---------------------------------------------------------------------------
# Case 2: Custom table name
# ---------------------------------------------------------------------------

def run_case_2() -> None:
    """Write to a named table for multi-table setups.

    Useful when different industries or data sources should go to
    separate tables in the same database.

    Knobs demonstrated:
      - table="mining_sensors"  -> custom table name
      - industries=["mining"]   -> single industry for focused storage
      - batch_size=100          -> moderate batch size
    """
    if not _check_database():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.database import DatabaseSink

    print("=== Case 2: Custom table name ===\n")

    db_path = "output/example_multi_table.db"
    os.makedirs("output", exist_ok=True)
    _clean_db(db_path)

    conn_str = f"sqlite+aiosqlite:///{db_path}"
    sim = Simulator(industries=["mining", "utilities"], update_rate_hz=2.0)

    # Each industry could write to its own table
    sim.add_sink(DatabaseSink(
        connection_string=conn_str,
        table="mining_sensors",    # Custom table name
        rate_hz=1.0,
        batch_size=100,
    ))

    sim.run(duration_s=6)
    print(f"\n  Data written to: {db_path} (table: mining_sensors)")


# ---------------------------------------------------------------------------
# Case 3: Backpressure and retry
# ---------------------------------------------------------------------------

def run_case_3() -> None:
    """Configure backpressure and retry policies for resilient storage.

    Knobs demonstrated:
      - backpressure="drop_newest" -> drop incoming records when buffer is full
      - max_buffer_size=1000       -> cap memory at 1000 records
      - retry_count=5              -> retry failed writes up to 5 times
      - retry_delay_s=2.0          -> wait 2 seconds between retries
      - update_rate_hz=5.0         -> fast generation to stress the buffer
    """
    if not _check_database():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.database import DatabaseSink

    print("=== Case 3: Backpressure and retry ===\n")

    db_path = "output/example_backpressure.db"
    os.makedirs("output", exist_ok=True)
    _clean_db(db_path)

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing"],
        update_rate_hz=5.0,  # Fast generation to stress the buffer
    )

    sim.add_sink(DatabaseSink(
        connection_string=f"sqlite+aiosqlite:///{db_path}",
        table="sensor_data",
        rate_hz=0.5,                # Slow flush rate (every 2 seconds)
        batch_size=200,
        max_buffer_size=1000,       # Cap buffer at 1000 records
        backpressure="drop_newest", # Drop new records when buffer is full
        retry_count=5,              # Retry failed writes up to 5 times
        retry_delay_s=2.0,          # Wait 2 seconds between retries
    ))

    print(f"  Active sensors: {sim.sensor_count}")
    print(f"  Generation rate: ~{sim.sensor_count * 5} records/sec")
    print(f"  Buffer limit: 1000 records (drop_newest policy)")
    print(f"  Flush rate: 0.5 Hz (every 2 seconds)\n")

    sim.run(duration_s=10)
    print(f"\n  Data written to: {db_path}")


# ---------------------------------------------------------------------------
# Case 4: Query after run
# ---------------------------------------------------------------------------

def run_case_4() -> None:
    """Write data and then query it back to verify integrity.

    Demonstrates the full round-trip: generate -> store -> query.

    Knobs demonstrated:
      - Verification via raw SQL queries after sim.run()
      - SELECT COUNT(*), AVG(value), sensor types
    """
    if not _check_database():
        return

    import asyncio

    from iot_simulator import Simulator
    from iot_simulator.sinks.database import DatabaseSink

    print("=== Case 4: Query after run ===\n")

    db_path = "output/example_query.db"
    os.makedirs("output", exist_ok=True)
    _clean_db(db_path)

    conn_str = f"sqlite+aiosqlite:///{db_path}"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)
    sim.add_sink(DatabaseSink(
        connection_string=conn_str,
        table="sensor_data",
        rate_hz=1.0,
        batch_size=200,
    ))

    sim.run(duration_s=8)

    # Now query the data back
    print("\n  Querying stored data...\n")

    async def query_results():
        import aiosqlite
        async with aiosqlite.connect(db_path) as db:
            # Total record count
            async with db.execute("SELECT COUNT(*) FROM sensor_data") as cursor:
                count = (await cursor.fetchone())[0]
                print(f"    Total records:    {count}")

            # Average value
            async with db.execute("SELECT AVG(value) FROM sensor_data") as cursor:
                avg = (await cursor.fetchone())[0]
                print(f"    Average value:    {avg:.3f}")

            # Min/Max value
            async with db.execute("SELECT MIN(value), MAX(value) FROM sensor_data") as cursor:
                row = await cursor.fetchone()
                print(f"    Value range:      {row[0]:.3f} to {row[1]:.3f}")

            # Records per sensor type
            async with db.execute(
                "SELECT sensor_type, COUNT(*) as cnt "
                "FROM sensor_data GROUP BY sensor_type ORDER BY cnt DESC"
            ) as cursor:
                print(f"\n    Records by sensor type:")
                async for row in cursor:
                    print(f"      {row[0]:<16s} {row[1]:>6d}")

            # Fault count
            async with db.execute(
                "SELECT COUNT(*) FROM sensor_data WHERE fault_active = 1"
            ) as cursor:
                faults = (await cursor.fetchone())[0]
                print(f"\n    Fault records:    {faults}")

    asyncio.run(query_results())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DatabaseSink examples")
    parser.add_argument("--case", type=int, default=1, choices=[1, 2, 3, 4],
                        help="Which example case to run (default: 1)")
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""DeltaSink examples -- 4 cases demonstrating append/overwrite modes,
single-column and multi-column partitioning.

Requires the delta extra::

    pip install iot-data-simulator[delta]

Usage::

    python examples/sinks/delta_sink_example.py           # Case 1 (default)
    python examples/sinks/delta_sink_example.py --case 2   # Overwrite mode
    python examples/sinks/delta_sink_example.py --case 3   # Partition by industry
    python examples/sinks/delta_sink_example.py --case 4   # Multi-column partition
"""

from __future__ import annotations

import argparse
import os
import shutil


def _check_delta():
    try:
        from iot_simulator.sinks.delta import DeltaSink  # noqa: F401
        return True
    except ImportError:
        print("DeltaSink requires deltalake + pyarrow. Install with:")
        print("  pip install iot-data-simulator[delta]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Append mode
# ---------------------------------------------------------------------------

def run_case_1() -> None:
    """Basic Delta table writes in append mode.

    Each write() adds new rows to the Delta table. Running the example
    multiple times accumulates data.

    Knobs demonstrated:
      - mode="append"       -> add rows (default)
      - table_path          -> local filesystem Delta table
      - batch_size=500      -> buffer before writing to Delta
      - rate_hz=1.0         -> write once per second
    """
    if not _check_delta():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.delta import DeltaSink

    print("=== Case 1: Append mode ===\n")

    table_path = "./output/delta_example_append"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(DeltaSink(
        table_path=table_path,
        mode="append",
        rate_hz=1.0,
        batch_size=500,
    ))

    sim.run(duration_s=8)

    # Show table info
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        print(f"\n  Delta table: {table_path}")
        print(f"  Version: {dt.version()}")
        print(f"  Files: {len(dt.files())}")
        df = dt.to_pandas()
        print(f"  Total rows: {len(df)}")
    except Exception as e:
        print(f"\n  Table info: {e}")


# ---------------------------------------------------------------------------
# Case 2: Overwrite mode
# ---------------------------------------------------------------------------

def run_case_2() -> None:
    """Full-refresh mode: each write overwrites the entire table.

    Useful for snapshot tables that should always contain only the
    latest data window.

    Knobs demonstrated:
      - mode="overwrite"   -> replace all data on each write
      - batch_size=1000    -> large batch = one big snapshot
      - rate_hz=0.2        -> overwrite every 5 seconds
    """
    if not _check_delta():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.delta import DeltaSink

    print("=== Case 2: Overwrite mode ===\n")

    table_path = "./output/delta_example_overwrite"
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(DeltaSink(
        table_path=table_path,
        mode="overwrite",    # Replace the table on each write
        rate_hz=0.2,         # Overwrite every 5 seconds
        batch_size=1000,
    ))

    sim.run(duration_s=12)

    # Show that only the last batch of data remains
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        df = dt.to_pandas()
        print(f"\n  Delta table: {table_path}")
        print(f"  Version: {dt.version()} (overwrites increment version)")
        print(f"  Rows (only last batch): {len(df)}")
    except Exception as e:
        print(f"\n  Table info: {e}")


# ---------------------------------------------------------------------------
# Case 3: Partition by industry
# ---------------------------------------------------------------------------

def run_case_3() -> None:
    """Partition the Delta table by industry column.

    Creates separate data directories per industry, enabling efficient
    partition pruning in downstream queries.

    Knobs demonstrated:
      - partition_by=["industry"]  -> one partition per industry
      - 2 industries               -> creates 2 partition directories
      - batch_size=500
    """
    if not _check_delta():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.delta import DeltaSink

    print("=== Case 3: Partition by industry ===\n")

    table_path = "./output/delta_example_partitioned"
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    sim = Simulator(
        industries=["mining", "utilities"],
        update_rate_hz=2.0,
    )

    sim.add_sink(DeltaSink(
        table_path=table_path,
        mode="append",
        partition_by=["industry"],  # Partition by industry column
        rate_hz=1.0,
        batch_size=500,
    ))

    sim.run(duration_s=8)

    # Show partition structure
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        df = dt.to_pandas()
        print(f"\n  Delta table: {table_path}")
        print(f"  Total rows: {len(df)}")
        print(f"  Partitions (by industry):")
        for industry, group in df.groupby("industry"):
            print(f"    {industry}: {len(group)} rows")
    except Exception as e:
        print(f"\n  Table info: {e}")


# ---------------------------------------------------------------------------
# Case 4: Multi-column partition
# ---------------------------------------------------------------------------

def run_case_4() -> None:
    """Partition by both industry and sensor_type for fine-grained layout.

    Creates a hierarchical partition structure:
    industry=mining/sensor_type=temperature/...

    Knobs demonstrated:
      - partition_by=["industry", "sensor_type"]  -> two-level partitioning
      - 3 industries    -> many partition combinations
      - batch_size=1000 -> large batches for efficient Parquet files
      - rate_hz=0.5     -> write every 2 seconds
    """
    if not _check_delta():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.delta import DeltaSink

    print("=== Case 4: Multi-column partition ===\n")

    table_path = "./output/delta_example_multi_partition"
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing"],
        update_rate_hz=2.0,
    )

    sim.add_sink(DeltaSink(
        table_path=table_path,
        mode="append",
        partition_by=["industry", "sensor_type"],  # Two-level partitioning
        rate_hz=0.5,
        batch_size=1000,
    ))

    print(f"  Active sensors: {sim.sensor_count}")
    sim.run(duration_s=10)

    # Show partition structure
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        df = dt.to_pandas()
        print(f"\n  Delta table: {table_path}")
        print(f"  Total rows: {len(df)}")
        print(f"  Partition combinations:")
        combos = df.groupby(["industry", "sensor_type"]).size()
        for (ind, stype), count in combos.items():
            print(f"    {ind}/{stype}: {count} rows")
    except Exception as e:
        print(f"\n  Table info: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DeltaSink examples")
    parser.add_argument("--case", type=int, default=1, choices=[1, 2, 3, 4],
                        help="Which example case to run (default: 1)")
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

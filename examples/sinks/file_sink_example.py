#!/usr/bin/env python3
"""FileSink examples -- 4 cases demonstrating CSV/JSON/Parquet output,
file rotation, large multi-industry batches, and Parquet read-back.

Cases 1-3 work with CSV/JSON (no extras). Case 1 and Case 4 need the
``file`` extra for Parquet support::

    pip install iot-data-simulator[file]

Usage::

    python examples/sinks/file_sink_example.py           # Case 1 (default)
    python examples/sinks/file_sink_example.py --case 2   # Rotation intervals
    python examples/sinks/file_sink_example.py --case 3   # Large multi-industry batch
    python examples/sinks/file_sink_example.py --case 4   # Parquet read-back
"""

from __future__ import annotations

import argparse
import os
import shutil

# ---------------------------------------------------------------------------
# Case 1: All three formats side-by-side
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Write the same data to CSV, JSON, and Parquet simultaneously.

    Three FileSink instances attached to the same Simulator, each writing
    to a separate subdirectory.

    Knobs demonstrated:
      - format="csv" / "json" / "parquet" -> three output formats
      - path=... -> each sink writes to its own directory
      - rate_hz=1.0, batch_size=100 -> same throughput for all
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks.file import FileSink

    print("=== Case 1: All three formats side-by-side ===\n")

    output_base = "./output/file_example_case1"
    # Clean up from previous runs
    if os.path.exists(output_base):
        shutil.rmtree(output_base)

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    # CSV sink
    sim.add_sink(
        FileSink(
            path=f"{output_base}/csv",
            format="csv",
            rate_hz=1.0,
            batch_size=100,
        )
    )

    # JSON Lines sink
    sim.add_sink(
        FileSink(
            path=f"{output_base}/json",
            format="json",
            rate_hz=1.0,
            batch_size=100,
        )
    )

    # Parquet sink (requires pyarrow)
    try:
        sim.add_sink(
            FileSink(
                path=f"{output_base}/parquet",
                format="parquet",
                rate_hz=1.0,
                batch_size=100,
            )
        )
        print("  Parquet sink enabled (pyarrow found)")
    except ImportError:
        print("  Parquet sink skipped (install with: pip install iot-data-simulator[file])")

    sim.run(duration_s=6)

    # Show what was written
    print(f"\n  Output written to: {output_base}/")
    for fmt_dir in ["csv", "json", "parquet"]:
        path = os.path.join(output_base, fmt_dir)
        if os.path.exists(path):
            files = os.listdir(path)
            total_size = sum(os.path.getsize(os.path.join(path, f)) for f in files)
            print(f"    {fmt_dir}/: {len(files)} file(s), {total_size / 1024:.1f} KB total")


# ---------------------------------------------------------------------------
# Case 2: Rotation intervals
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Demonstrate file rotation producing multiple files over time.

    Knobs demonstrated:
      - rotation="30s"    -> rotate to a new file every 30 seconds
      - duration_s=90     -> run long enough to see 3 file rotations
      - format="csv"      -> easy to inspect rotated files
      - batch_size=500    -> large batches for fast file growth
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks.file import FileSink

    print("=== Case 2: Rotation intervals (30s rotation, 90s run) ===\n")
    print("  This will create ~3 rotated CSV files.\n")

    output_dir = "./output/file_example_case2"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    sim = Simulator(industries=["mining", "utilities"], update_rate_hz=2.0)

    sim.add_sink(
        FileSink(
            path=output_dir,
            format="csv",
            rotation="30s",  # New file every 30 seconds
            rate_hz=1.0,
            batch_size=500,
        )
    )

    sim.run(duration_s=90)

    # Show rotated files
    if os.path.exists(output_dir):
        files = sorted(os.listdir(output_dir))
        print(f"\n  Rotated files in {output_dir}/:")
        for f in files:
            fpath = os.path.join(output_dir, f)
            size = os.path.getsize(fpath)
            print(f"    {f}  ({size / 1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Case 3: Large multi-industry batch
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Generate a large dataset from 4 industries simultaneously.

    Knobs demonstrated:
      - 4 industries       -> 60+ sensors producing data every tick
      - update_rate_hz=5.0 -> 5 ticks/sec * 60+ sensors = 300+ records/sec
      - batch_size=1000    -> large batches for efficient file writes
      - format="json"      -> JSON Lines for easy downstream processing
    """
    from iot_simulator import Simulator
    from iot_simulator.sinks.file import FileSink

    print("=== Case 3: Large multi-industry batch ===\n")

    output_dir = "./output/file_example_case3"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing", "oil_gas"],
        update_rate_hz=5.0,
    )

    print(f"  Active sensors: {sim.sensor_count}")
    print(f"  Expected throughput: ~{sim.sensor_count * 5} records/sec\n")

    sim.add_sink(
        FileSink(
            path=output_dir,
            format="json",
            rate_hz=2.0,
            batch_size=1000,
        )
    )

    sim.run(duration_s=10)

    # Report file size
    if os.path.exists(output_dir):
        files = os.listdir(output_dir)
        total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in files)
        print(f"\n  Output: {len(files)} file(s), {total_size / 1024:.1f} KB total")


# ---------------------------------------------------------------------------
# Case 4: Parquet read-back
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Write Parquet, then read it back and print a summary.

    Requires::

        pip install iot-data-simulator[file]

    Knobs demonstrated:
      - format="parquet"   -> columnar storage, efficient for analytics
      - batch_size=500     -> buffer records before flushing to Parquet
      - Read-back with pyarrow to verify data integrity
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("This case requires pyarrow. Install with:")
        print("  pip install iot-data-simulator[file]")
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.file import FileSink

    print("=== Case 4: Parquet read-back ===\n")

    output_dir = "./output/file_example_case4"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(
        FileSink(
            path=output_dir,
            format="parquet",
            rate_hz=1.0,
            batch_size=500,
        )
    )

    sim.run(duration_s=8)

    # Read back the Parquet file
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    if not parquet_files:
        print("  No Parquet files found.")
        return

    filepath = os.path.join(output_dir, parquet_files[0])
    table = pq.read_table(filepath)
    df = table.to_pandas()

    print(f"\n  Read back: {filepath}")
    print(f"  Rows:    {len(df)}")
    print(f"  Columns: {list(df.columns)}")
    print("\n  Summary statistics:")
    print(f"    Industries:   {sorted(df['industry'].unique())}")
    print(f"    Sensors:      {df['sensor_name'].nunique()}")
    print(f"    Avg value:    {df['value'].mean():.3f}")
    print(f"    Fault count:  {df['fault_active'].sum()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="FileSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

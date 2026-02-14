#!/usr/bin/env python3
"""PySpark custom DataSource examples -- 5 cases demonstrating the IoT
simulator as a batch and streaming source for Spark Structured Streaming.

**Must be run inside a Spark environment** (Databricks notebook, spark-submit,
PySpark shell).  Not runnable as a standalone Python script.

Usage (Databricks notebook -- paste each case into a cell)::

    # Case 1: Batch with defaults
    # Case 2: Batch with multiple industries + sensor behavior knobs
    # Case 3: Batch load-test with metadata padding
    # Case 4: Streaming with micro-batch trigger
    # Case 5: Streaming with Databricks Real-Time Mode trigger

For spark-submit::

    spark-submit examples/scenarios/spark_datasource_example.py --case 1
"""

from __future__ import annotations

import argparse

# ---------------------------------------------------------------------------
# Case 1: Batch -- defaults (mining industry, 10 ticks)
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Simplest batch read: mining industry sensors, 10 ticks.

    Knobs demonstrated:
      - Default industries="mining"  -> 16 mining sensors per tick
      - Default numRows="10"         -> 10 ticks → 160 total rows
    """
    from pyspark.sql import SparkSession

    from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

    print("=== Case 1: Batch read with defaults ===\n")

    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(IoTSimulatorDataSource)

    df = spark.read.format("iot_simulator").load()
    df.show(20, truncate=False)
    print(f"Total rows: {df.count()}")


# ---------------------------------------------------------------------------
# Case 2: Batch -- multiple industries + sensor behavior knobs
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Batch read with multiple industries and sensor behavior overrides.

    Knobs demonstrated:
      - industries="mining,utilities,oil_gas"  -> ~57 sensors per tick
      - numRows="5"                            -> 5 ticks
      - anomalyProbability="0.05"              -> 5% anomaly rate (50x default)
      - noiseStd="0.1"                         -> higher noise across all sensors
      - metadataPaddingBytes="1024"            -> ~1KB padding per record
    """
    from pyspark.sql import SparkSession

    from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

    print("=== Case 2: Batch read with knobs ===\n")

    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(IoTSimulatorDataSource)

    df = (
        spark.read.format("iot_simulator")
        .option("industries", "mining,utilities,oil_gas")
        .option("numRows", "5")
        .option("anomalyProbability", "0.05")
        .option("noiseStd", "0.1")
        .option("metadataPaddingBytes", "1024")
        .load()
    )

    df.show(20, truncate=False)

    # Show some analytics
    print(f"Total rows: {df.count()}")
    print(f"Distinct industries: {df.select('industry').distinct().count()}")
    print(f"Distinct sensors: {df.select('sensor_name').distinct().count()}")
    print(f"Faults detected: {df.filter('fault_active = true').count()}")


# ---------------------------------------------------------------------------
# Case 3: Batch -- load test (many industries, large padding)
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """High-throughput batch read for stress-testing downstream systems.

    Knobs demonstrated:
      - industries (all 16)              -> ~300+ sensors per tick
      - numRows="20"                     -> 20 ticks → ~6000+ rows
      - metadataPaddingBytes="4096"      -> ~4KB padding per record
      - anomalyProbability="0.0"         -> no anomalies (pure throughput test)

    This produces a large DataFrame useful for benchmarking writes to
    Delta Lake, Kafka, databases, etc.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, length

    from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

    print("=== Case 3: Batch load test ===\n")

    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(IoTSimulatorDataSource)

    all_industries = (
        "mining,utilities,manufacturing,oil_gas,aerospace,space,"
        "water_wastewater,electric_power,automotive,chemical,"
        "food_beverage,pharmaceutical,data_center,smart_building,"
        "agriculture,renewable_energy"
    )

    df = (
        spark.read.format("iot_simulator")
        .option("industries", all_industries)
        .option("numRows", "20")
        .option("metadataPaddingBytes", "4096")
        .option("anomalyProbability", "0.0")
        .load()
    )

    print(f"Total rows: {df.count()}")
    print(f"Distinct industries: {df.select('industry').distinct().count()}")
    print(f"Distinct sensors: {df.select('sensor_name').distinct().count()}")

    # Approximate payload size
    avg_metadata_len = df.select(length(col("metadata")).alias("len")).groupBy().avg("len").collect()[0][0]
    print(f"Avg metadata field length: {avg_metadata_len:.0f} chars")


# ---------------------------------------------------------------------------
# Case 4: Streaming -- micro-batch (processingTime trigger)
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Standard Structured Streaming with processingTime trigger.

    Knobs demonstrated:
      - rowsPerBatch="2"                      -> 2 ticks per micro-batch
      - industries="manufacturing"            -> 18 sensors per tick
      - trigger(processingTime="5 seconds")   -> micro-batch every 5s
      - driftRate="0.01"                      -> exaggerated drift for visibility

    The stream writes to the console sink and runs for ~30 seconds.
    """
    from pyspark.sql import SparkSession

    from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

    print("=== Case 4: Streaming (micro-batch) ===\n")

    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(IoTSimulatorDataSource)

    df = (
        spark.readStream.format("iot_simulator")
        .option("industries", "manufacturing")
        .option("rowsPerBatch", "2")
        .option("driftRate", "0.01")
        .load()
    )

    query = df.writeStream.format("console").option("truncate", "false").trigger(processingTime="5 seconds").start()

    # Run for 30 seconds then stop
    query.awaitTermination(timeout=30)
    query.stop()
    print("\nStreaming query stopped.")


# ---------------------------------------------------------------------------
# Case 5: Streaming -- Databricks Real-Time Mode
# ---------------------------------------------------------------------------


def run_case_5() -> None:
    """Structured Streaming with Databricks Real-Time Mode trigger.

    **Databricks-only** — requires:
      - Databricks Runtime 16.4 LTS or later
      - Dedicated (single-user) cluster with Photon disabled
      - Spark config: spark.databricks.streaming.realTimeMode.enabled = true
      - Autoscaling disabled, no spot instances

    Real-Time Mode achieves ultra-low latency (~5ms end-to-end) by
    executing long-running batches where data is processed as it arrives.

    Knobs demonstrated:
      - trigger(realTime="5 minutes")     -> real-time mode with 5-min batches
      - outputMode("update")              -> required for real-time mode
      - format("noop")                    -> no-op sink (measures throughput)
      - rowsPerBatch="1"                  -> 1 tick per micro-batch (low latency)
      - industries="mining"               -> 16 sensors per tick
      - anomalyProbability="0.02"         -> moderate anomaly rate

    Task slot sizing:
      - Single-stage stateless pipeline (this example)
      - If maxPartitions = N, you need at least N task slots
      - See https://docs.databricks.com/aws/en/structured-streaming/real-time

    This case will raise an error on open-source Spark (realTime trigger
    is Databricks-only).

    References:
      - https://docs.databricks.com/aws/en/structured-streaming/real-time
      - https://docs.databricks.com/aws/en/pyspark/datasources
    """
    from pyspark.sql import SparkSession

    from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

    print("=== Case 5: Streaming (Databricks Real-Time Mode) ===\n")
    print("NOTE: This case requires Databricks Runtime 16.4 LTS+ with")
    print("      spark.databricks.streaming.realTimeMode.enabled = true\n")

    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(IoTSimulatorDataSource)

    df = (
        spark.readStream.format("iot_simulator")
        .option("industries", "mining")
        .option("rowsPerBatch", "1")
        .option("anomalyProbability", "0.02")
        .load()
    )

    # Real-Time Mode: noop sink, update output mode, realTime trigger.
    # Checkpoint location is required for production; for demo we use /tmp.
    query = (
        df.writeStream.format("noop")
        .outputMode("update")
        .option("checkpointLocation", "/tmp/iot_simulator_rt_checkpoint")
        .trigger(realTime="5 minutes")
        .start()
    )

    # Run for 60 seconds then stop
    query.awaitTermination(timeout=60)
    query.stop()
    print("\nReal-time streaming query stopped.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PySpark IoT Simulator DataSource examples",
        epilog="Must be run inside a Spark environment (Databricks, spark-submit).",
    )
    parser.add_argument(
        "--case",
        type=int,
        default=1,
        choices=[1, 2, 3, 4, 5],
        help="Which example case to run (default: 1)",
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

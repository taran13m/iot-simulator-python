#!/usr/bin/env python3
"""KafkaSink examples -- 4 cases demonstrating compression, key partitioning,
SASL authentication, and extra producer configuration.

Requires a running Kafka broker and the kafka extra::

    pip install iot-data-simulator[kafka]

Usage::

    python examples/sinks/kafka_sink_example.py           # Case 1 (default)
    python examples/sinks/kafka_sink_example.py --case 2   # Key partitioning strategies
    python examples/sinks/kafka_sink_example.py --case 3   # SASL authentication
    python examples/sinks/kafka_sink_example.py --case 4   # Extra producer config
"""

from __future__ import annotations

import argparse


def _check_kafka():
    try:
        from iot_simulator.sinks.kafka import KafkaSink  # noqa: F401

        return True
    except ImportError:
        print("KafkaSink requires aiokafka. Install with:")
        print("  pip install iot-data-simulator[kafka]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Compression variants
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Compare different compression algorithms for Kafka messages.

    Knobs demonstrated:
      - compression="snappy"  -> fast, moderate ratio (default)
      - compression="gzip"    -> slower, better ratio
      - compression="lz4"     -> fastest, good ratio
      - compression="zstd"    -> best ratio, moderate speed
    """
    if not _check_kafka():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.kafka import KafkaSink

    print("=== Case 1: Compression variants ===\n")

    # NOTE: Change bootstrap_servers to match your Kafka broker
    BOOTSTRAP = "localhost:9092"
    TOPIC_PREFIX = "iot-compression-test"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    # Snappy -- fast compression, moderate ratio (recommended default)
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic=f"{TOPIC_PREFIX}-snappy",
            compression="snappy",
            rate_hz=2.0,
            batch_size=50,
        )
    )

    # Gzip -- slower but better compression ratio for bandwidth-constrained links
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic=f"{TOPIC_PREFIX}-gzip",
            compression="gzip",
            rate_hz=2.0,
            batch_size=50,
        )
    )

    # LZ4 -- fastest compression, good for high-throughput pipelines
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic=f"{TOPIC_PREFIX}-lz4",
            compression="lz4",
            rate_hz=2.0,
            batch_size=50,
        )
    )

    print(f"  Publishing to {TOPIC_PREFIX}-{{snappy,gzip,lz4}} on {BOOTSTRAP}")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 2: Key partitioning strategies
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Demonstrate different Kafka message key strategies.

    The key_field determines how records are partitioned across topic
    partitions.

    Knobs demonstrated:
      - key_field="sensor_name"  -> one partition per sensor (fine-grained)
      - key_field="industry"     -> one partition per industry (coarse)
      - key_field=None           -> round-robin (maximum parallelism)
    """
    if not _check_kafka():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.kafka import KafkaSink

    print("=== Case 2: Key partitioning strategies ===\n")

    BOOTSTRAP = "localhost:9092"

    sim = Simulator(
        industries=["mining", "utilities"],
        update_rate_hz=2.0,
    )

    # Key by sensor_name: records for the same sensor always go to the same partition
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic="iot-by-sensor",
            key_field="sensor_name",
            compression="snappy",
            rate_hz=2.0,
            batch_size=100,
        )
    )

    # Key by industry: all mining records go to one partition, utilities to another
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic="iot-by-industry",
            key_field="industry",
            compression="snappy",
            rate_hz=2.0,
            batch_size=100,
        )
    )

    # No key: round-robin across all partitions (best throughput distribution)
    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic="iot-round-robin",
            key_field=None,
            compression="snappy",
            rate_hz=2.0,
            batch_size=100,
        )
    )

    print("  Publishing to: iot-by-sensor, iot-by-industry, iot-round-robin")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 3: SASL authentication
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Connect to a Kafka cluster with SASL/SSL authentication.

    Knobs demonstrated:
      - security_protocol="SASL_SSL"   -> encrypted + authenticated
      - sasl_mechanism="PLAIN"         -> username/password auth
      - sasl_username / sasl_password  -> credentials

    NOTE: Replace placeholder values with your actual broker details.
    """
    if not _check_kafka():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.kafka import KafkaSink

    print("=== Case 3: SASL authentication ===\n")

    # -- Replace these with your actual credentials --
    BOOTSTRAP = "kafka-broker.example.com:9093"
    USERNAME = "iot-producer"
    PASSWORD = "your-secret-password"
    TOPIC = "iot-secure-data"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("temp_sensor", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("pressure_sensor", SensorType.PRESSURE, "bar", 2, 8, 5),
        ],
        custom_industry="demo",
        update_rate_hz=1.0,
    )

    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic=TOPIC,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username=USERNAME,
            sasl_password=PASSWORD,
            compression="snappy",
            acks="all",  # Wait for all replicas to acknowledge
            rate_hz=1.0,
            batch_size=20,
        )
    )

    print(f"  Connecting to {BOOTSTRAP} with SASL_SSL/PLAIN")
    print(f"  Topic: {TOPIC}")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 4: Extra producer config -- high-throughput tuning
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Fine-tune the Kafka producer for maximum throughput.

    Knobs demonstrated:
      - extra_producer_config   -> linger_ms, max_batch_size for batching
      - update_rate_hz=10.0     -> high generation rate
      - rate_hz=5.0             -> 5 flushes/sec to Kafka
      - batch_size=200          -> 200 records per flush = 1000 records/sec
      - acks=1                  -> leader-only ack for lower latency
    """
    if not _check_kafka():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.kafka import KafkaSink

    print("=== Case 4: Extra producer config (high throughput) ===\n")

    BOOTSTRAP = "localhost:9092"

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing"],
        update_rate_hz=10.0,
    )

    sim.add_sink(
        KafkaSink(
            bootstrap_servers=BOOTSTRAP,
            topic="iot-high-throughput",
            compression="lz4",  # Fastest compression for throughput
            key_field="industry",
            acks=1,  # Leader-only ack (lower latency)
            extra_producer_config={
                "linger_ms": 50,  # Wait up to 50ms to batch messages
                "max_batch_size": 32768,  # 32KB max batch to broker
            },
            rate_hz=5.0,
            batch_size=200,
        )
    )

    print(f"  Active sensors: {sim.sensor_count}")
    print(f"  Target: ~{sim.sensor_count * 10} records/sec to Kafka")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="KafkaSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

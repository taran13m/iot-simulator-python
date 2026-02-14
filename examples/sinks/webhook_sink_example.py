#!/usr/bin/env python3
"""WebhookSink examples -- 4 cases demonstrating authentication headers,
high-frequency small batches, low-frequency large batches, and retry behaviour.

Requires the webhook extra and a running HTTP endpoint::

    pip install iot-data-simulator[webhook]

Usage::

    python examples/sinks/webhook_sink_example.py           # Case 1 (default)
    python examples/sinks/webhook_sink_example.py --case 2   # High-frequency small batch
    python examples/sinks/webhook_sink_example.py --case 3   # Low-frequency large batch
    python examples/sinks/webhook_sink_example.py --case 4   # Retry on failure
"""

from __future__ import annotations

import argparse


def _check_webhook():
    try:
        from iot_simulator.sinks.webhook import WebhookSink  # noqa: F401

        return True
    except ImportError:
        print("WebhookSink requires httpx. Install with:")
        print("  pip install iot-data-simulator[webhook]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Bearer token auth
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """POST sensor data with Bearer token authentication.

    Knobs demonstrated:
      - url              -> target HTTP endpoint
      - headers          -> Authorization: Bearer header
      - timeout_s=30.0   -> per-request timeout
      - rate_hz=1.0      -> one POST per second
      - batch_size=50    -> 50 records per POST body

    NOTE: Replace the URL and token with your actual endpoint.
    """
    if not _check_webhook():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.webhook import WebhookSink

    print("=== Case 1: Bearer token auth ===\n")

    # -- Replace with your actual endpoint and token --
    URL = "https://httpbin.org/post"
    TOKEN = "your-api-token-here"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(
        WebhookSink(
            url=URL,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "X-Source": "iot-simulator",
            },
            timeout_s=30.0,
            rate_hz=1.0,
            batch_size=50,
        )
    )

    print(f"  POSTing to: {URL}")
    print("  Auth: Bearer token")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 2: High-frequency small batch
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Near-real-time delivery with small, frequent POSTs.

    Suitable for low-latency alerting or live dashboards that need
    fresh data every 100ms.

    Knobs demonstrated:
      - rate_hz=10       -> 10 POSTs per second
      - batch_size=5     -> only 5 records per POST
      - custom_sensors   -> small payload (2 sensors)
      - update_rate_hz=5.0 -> fast generation
    """
    if not _check_webhook():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.webhook import WebhookSink

    print("=== Case 2: High-frequency small batch ===\n")

    URL = "https://httpbin.org/post"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("temp_sensor", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("humidity_sensor", SensorType.HUMIDITY, "%RH", 30, 70, 50),
        ],
        custom_industry="realtime",
        update_rate_hz=5.0,
    )

    sim.add_sink(
        WebhookSink(
            url=URL,
            timeout_s=5.0,  # Short timeout for real-time delivery
            rate_hz=10,  # 10 POSTs per second
            batch_size=5,  # Tiny batches for low latency
        )
    )

    print(f"  POSTing to: {URL}")
    print("  Delivery: 10 Hz, 5 records/batch (near-real-time)")
    sim.run(duration_s=8)


# ---------------------------------------------------------------------------
# Case 3: Low-frequency large batch
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Bulk ingestion with infrequent, large POSTs.

    Suitable for batch ETL endpoints that prefer fewer, bigger payloads.

    Knobs demonstrated:
      - rate_hz=0.1        -> one POST every 10 seconds
      - batch_size=500     -> large payload per POST
      - 3 industries       -> 50+ sensors for high data volume
      - update_rate_hz=5.0 -> fast generation to fill buffer
    """
    if not _check_webhook():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.webhook import WebhookSink

    print("=== Case 3: Low-frequency large batch ===\n")

    URL = "https://httpbin.org/post"

    sim = Simulator(
        industries=["mining", "utilities", "manufacturing"],
        update_rate_hz=5.0,
    )

    sim.add_sink(
        WebhookSink(
            url=URL,
            timeout_s=60.0,  # Long timeout for large payloads
            rate_hz=0.1,  # One POST every 10 seconds
            batch_size=500,  # Large batch per POST
        )
    )

    print(f"  POSTing to: {URL}")
    print(f"  Active sensors: {sim.sensor_count}")
    print("  Delivery: 0.1 Hz, 500 records/batch (bulk)")
    sim.run(duration_s=25)


# ---------------------------------------------------------------------------
# Case 4: Retry on failure
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Demonstrate retry behaviour when the endpoint is unreachable.

    Points at a non-existent URL to trigger connection errors. The
    SinkRunner retry logic will attempt re-delivery before dropping
    the batch.

    Knobs demonstrated:
      - url (bad)           -> triggers connection errors
      - retry_count=3       -> retry each failed write 3 times
      - retry_delay_s=2.0   -> wait 2 seconds between retries
      - batch_size=20       -> small batches to see retries faster
    """
    if not _check_webhook():
        return

    import logging

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.webhook import WebhookSink

    print("=== Case 4: Retry on failure ===\n")

    # Enable debug logging to see retry attempts
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-30s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    # This URL will fail -- that's intentional
    BAD_URL = "http://localhost:59999/nonexistent"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("test_sensor", SensorType.TEMPERATURE, "°C", 10, 40, 25),
        ],
        custom_industry="retry-demo",
        update_rate_hz=1.0,
    )

    sim.add_sink(
        WebhookSink(
            url=BAD_URL,
            timeout_s=2.0,  # Short timeout to fail fast
            rate_hz=0.5,
            batch_size=20,
            retry_count=3,  # Retry 3 times before dropping
            retry_delay_s=2.0,  # Wait 2 seconds between retries
        )
    )

    print(f"  POSTing to (will fail): {BAD_URL}")
    print("  Retry policy: 3 attempts, 2s delay\n")
    sim.run(duration_s=12)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="WebhookSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

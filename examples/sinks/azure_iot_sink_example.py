#!/usr/bin/env python3
"""AzureIoTSink examples -- 3 cases demonstrating minimal payload,
metadata-enriched messages, and throttled delivery for IoT Hub.

Requires the cloud extra and an Azure IoT Hub device connection string::

    pip install iot-data-simulator[cloud]

Usage::

    python examples/sinks/azure_iot_sink_example.py           # Case 1 (default)
    python examples/sinks/azure_iot_sink_example.py --case 2   # Metadata-enriched
    python examples/sinks/azure_iot_sink_example.py --case 3   # Throttled delivery
"""

from __future__ import annotations

import argparse


def _check_azure():
    try:
        from iot_simulator.sinks.cloud_iot import AzureIoTSink  # noqa: F401

        return True
    except ImportError:
        print("AzureIoTSink requires azure-iot-device. Install with:")
        print("  pip install iot-data-simulator[cloud]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Minimal 2-sensor payload
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Send a tiny payload from 2 custom sensors to Azure IoT Hub.

    Azure IoT Hub has a 256 KB message limit and per-device throttling.
    Using few sensors with small batch sizes keeps messages well within
    those limits.

    Knobs demonstrated:
      - custom_sensors (2)   -> small payload per tick
      - batch_size=10        -> small batch to stay within IoT Hub limits
      - rate_hz=1.0          -> moderate delivery rate
      - connection_string    -> Azure IoT Hub device connection string

    NOTE: Replace the connection string with your actual device credentials.
    """
    if not _check_azure():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.cloud_iot import AzureIoTSink

    print("=== Case 1: Minimal 2-sensor payload ===\n")

    # -- Replace with your actual connection string --
    CONN_STR = (
        "HostName=your-hub.azure-devices.net;DeviceId=iot-simulator-device;SharedAccessKey=YOUR_SHARED_ACCESS_KEY_HERE"
    )

    sim = Simulator(
        custom_sensors=[
            SensorConfig("temp_sensor", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("pressure_sensor", SensorType.PRESSURE, "bar", 2, 8, 5),
        ],
        custom_industry="iot_edge",
        update_rate_hz=1.0,
    )

    sim.add_sink(
        AzureIoTSink(
            connection_string=CONN_STR,
            content_type="application/json",
            rate_hz=1.0,
            batch_size=10,
        )
    )

    print(f"  Sensors: {sim.sensor_count}")
    print("  Delivery: 1 Hz, 10 records/batch")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 2: Metadata-enriched messages
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Enrich messages with device metadata before sending to IoT Hub.

    Azure IoT Hub supports custom message properties. This example adds
    location, floor, and device identity to each message via the metadata
    dict and a pre-processing callback.

    Knobs demonstrated:
      - metadata dict       -> location, floor, device_id enrichment
      - CallbackSink        -> pre-processing step before Azure delivery
      - custom_sensors      -> building sensors (temp, humidity, CO2)
    """
    if not _check_azure():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.cloud_iot import AzureIoTSink

    print("=== Case 2: Metadata-enriched messages ===\n")

    CONN_STR = (
        "HostName=your-hub.azure-devices.net;DeviceId=building-a-edge;SharedAccessKey=YOUR_SHARED_ACCESS_KEY_HERE"
    )

    sim = Simulator(
        custom_sensors=[
            SensorConfig("zone_1_temp", SensorType.TEMPERATURE, "°C", 18, 26, 21, 0.3),
            SensorConfig("zone_1_humidity", SensorType.HUMIDITY, "%RH", 30, 60, 45, 2.0),
            SensorConfig("zone_1_co2", SensorType.LEVEL, "ppm", 400, 1000, 600, 20),
        ],
        custom_industry="smart_building",
        update_rate_hz=1.0,
    )

    # Pre-process: inject device metadata into every record
    def enrich_metadata(records):
        for r in records:
            r.metadata["location"] = "building-a"
            r.metadata["floor"] = 3
            r.metadata["device_id"] = "edge-gateway-001"
            r.metadata["firmware"] = "2.4.1"

    sim.add_sink(enrich_metadata, rate_hz=1.0, batch_size=20)

    sim.add_sink(
        AzureIoTSink(
            connection_string=CONN_STR,
            content_type="application/json",
            rate_hz=1.0,
            batch_size=20,
        )
    )

    print(f"  Sensors: {sim.sensor_count}")
    print("  Metadata: location, floor, device_id, firmware")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 3: Throttled delivery
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Respect Azure IoT Hub throttling tiers with slow delivery.

    Free/S1 tier IoT Hubs have strict message-per-second limits.
    This example uses a very low rate to stay within those limits.

    Knobs demonstrated:
      - rate_hz=0.2       -> one flush every 5 seconds
      - batch_size=10     -> small batch per flush
      - update_rate_hz=1.0 -> moderate generation
      - max_buffer_size=500 -> cap memory for constrained edge devices
    """
    if not _check_azure():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.cloud_iot import AzureIoTSink

    print("=== Case 3: Throttled delivery ===\n")

    CONN_STR = (
        "HostName=your-hub.azure-devices.net;DeviceId=iot-simulator-device;SharedAccessKey=YOUR_SHARED_ACCESS_KEY_HERE"
    )

    sim = Simulator(
        custom_sensors=[
            SensorConfig("ambient_temp", SensorType.TEMPERATURE, "°C", -10, 50, 22),
            SensorConfig("ambient_humidity", SensorType.HUMIDITY, "%RH", 10, 90, 50),
        ],
        custom_industry="field_device",
        update_rate_hz=1.0,
    )

    sim.add_sink(
        AzureIoTSink(
            connection_string=CONN_STR,
            rate_hz=0.2,  # One flush every 5 seconds
            batch_size=10,  # Small batch per flush
            max_buffer_size=500,  # Memory cap for edge devices
            backpressure="drop_oldest",
        )
    )

    print("  Delivery: 0.2 Hz (every 5 seconds), 10 records/batch")
    print("  Buffer limit: 500 records (drop_oldest)")
    sim.run(duration_s=30)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="AzureIoTSink examples")
    parser.add_argument("--case", type=int, default=1, choices=[1, 2, 3], help="Which example case to run (default: 1)")
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3}
    cases[args.case]()


if __name__ == "__main__":
    main()

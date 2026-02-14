#!/usr/bin/env python3
"""AWSIoTSink examples -- 3 cases demonstrating per-device topics,
metadata device tagging, and session token credentials.

Requires the cloud extra and an AWS IoT Core endpoint::

    pip install iot-data-simulator[cloud]

Usage::

    python examples/sinks/aws_iot_sink_example.py           # Case 1 (default)
    python examples/sinks/aws_iot_sink_example.py --case 2   # Metadata device tagging
    python examples/sinks/aws_iot_sink_example.py --case 3   # Session token credentials
"""

from __future__ import annotations

import argparse


def _check_aws():
    try:
        from iot_simulator.sinks.cloud_iot import AWSIoTSink  # noqa: F401
        return True
    except ImportError:
        print("AWSIoTSink requires boto3. Install with:")
        print("  pip install iot-data-simulator[cloud]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Per-device topic
# ---------------------------------------------------------------------------

def run_case_1() -> None:
    """Publish to a device-specific MQTT topic on AWS IoT Core.

    AWS IoT Core uses MQTT topic hierarchies for device routing.
    This example uses a per-device topic path, enabling IoT Rules
    to route data based on the device ID.

    Knobs demonstrated:
      - topic="iot/devices/device-001/telemetry"  -> device-specific routing
      - endpoint                -> AWS IoT Core custom endpoint
      - region="us-east-1"     -> AWS region
      - custom_sensors (2)     -> small payload per message
      - batch_size=10          -> small batches for MQTT

    NOTE: Replace endpoint and region with your actual AWS IoT Core details.
    """
    if not _check_aws():
        return

    from iot_simulator import Simulator, SensorConfig, SensorType
    from iot_simulator.sinks.cloud_iot import AWSIoTSink

    print("=== Case 1: Per-device topic ===\n")

    # -- Replace with your actual endpoint --
    ENDPOINT = "abc123-ats.iot.us-east-1.amazonaws.com"
    DEVICE_ID = "device-001"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("temp_sensor", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("humidity_sensor", SensorType.HUMIDITY, "%RH", 30, 70, 50),
        ],
        custom_industry="field_device",
        update_rate_hz=1.0,
    )

    sim.add_sink(AWSIoTSink(
        endpoint=ENDPOINT,
        topic=f"iot/devices/{DEVICE_ID}/telemetry",
        region="us-east-1",
        rate_hz=1.0,
        batch_size=10,
    ))

    print(f"  Endpoint: {ENDPOINT}")
    print(f"  Topic: iot/devices/{DEVICE_ID}/telemetry")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 2: Metadata device tagging
# ---------------------------------------------------------------------------

def run_case_2() -> None:
    """Enrich records with device identity and location metadata.

    This is critical for multi-device fleets: each record carries the
    originating device's identity in the metadata dict, which flows
    through to the MQTT message payload.

    Knobs demonstrated:
      - metadata dict          -> device_id, firmware_version, site, GPS
      - CallbackSink           -> pre-processing enrichment
      - topic="iot/fleet/telemetry"  -> shared fleet topic
      - custom_sensors         -> 3 environmental sensors
    """
    if not _check_aws():
        return

    from iot_simulator import Simulator, SensorConfig, SensorType
    from iot_simulator.sinks.cloud_iot import AWSIoTSink

    print("=== Case 2: Metadata device tagging ===\n")

    ENDPOINT = "abc123-ats.iot.us-east-1.amazonaws.com"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("env_temp", SensorType.TEMPERATURE, "°C", -20, 50, 18, 1.0),
            SensorConfig("env_humidity", SensorType.HUMIDITY, "%RH", 10, 95, 55, 3.0),
            SensorConfig("env_pressure", SensorType.PRESSURE, "hPa", 950, 1050, 1013, 1.0),
        ],
        custom_industry="weather_station",
        update_rate_hz=1.0,
    )

    # Enrich every record with device identity metadata
    def tag_device(records):
        for r in records:
            r.metadata["device_id"] = "weather-station-042"
            r.metadata["firmware_version"] = "3.2.1"
            r.metadata["site"] = "factory-north"
            r.metadata["gps_lat"] = 47.6062
            r.metadata["gps_lon"] = -122.3321

    sim.add_sink(tag_device, rate_hz=1.0, batch_size=20)

    sim.add_sink(AWSIoTSink(
        endpoint=ENDPOINT,
        topic="iot/fleet/telemetry",    # Shared fleet topic
        region="us-east-1",
        rate_hz=1.0,
        batch_size=20,
    ))

    print(f"  Topic: iot/fleet/telemetry")
    print(f"  Device metadata: device_id, firmware, site, GPS")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 3: Session token credentials
# ---------------------------------------------------------------------------

def run_case_3() -> None:
    """Use temporary AWS credentials (e.g., from STS AssumeRole).

    Useful for CI/CD pipelines or applications that obtain temporary
    credentials from AWS STS rather than using long-lived access keys.

    Knobs demonstrated:
      - aws_access_key_id      -> explicit temporary key
      - aws_secret_access_key  -> explicit temporary secret
      - aws_session_token      -> STS session token
      - region="eu-west-1"     -> non-default region

    NOTE: Replace with actual temporary credentials from STS.
    """
    if not _check_aws():
        return

    from iot_simulator import Simulator, SensorConfig, SensorType
    from iot_simulator.sinks.cloud_iot import AWSIoTSink

    print("=== Case 3: Session token credentials ===\n")

    # -- Replace with actual temporary credentials from STS --
    ENDPOINT = "xyz789-ats.iot.eu-west-1.amazonaws.com"
    ACCESS_KEY = "ASIA..."
    SECRET_KEY = "your-temporary-secret-key"
    SESSION_TOKEN = "FwoGZX..."

    sim = Simulator(
        custom_sensors=[
            SensorConfig("machine_vibration", SensorType.VIBRATION, "mm/s", 0.5, 15, 3),
        ],
        custom_industry="factory",
        update_rate_hz=1.0,
    )

    sim.add_sink(AWSIoTSink(
        endpoint=ENDPOINT,
        topic="iot/eu-factory/telemetry",
        region="eu-west-1",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        rate_hz=0.5,
        batch_size=10,
    ))

    print(f"  Endpoint: {ENDPOINT}")
    print(f"  Region: eu-west-1")
    print(f"  Auth: temporary STS credentials")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="AWSIoTSink examples")
    parser.add_argument("--case", type=int, default=1, choices=[1, 2, 3],
                        help="Which example case to run (default: 1)")
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3}
    cases[args.case]()


if __name__ == "__main__":
    main()

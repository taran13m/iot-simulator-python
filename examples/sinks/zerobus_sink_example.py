#!/usr/bin/env python3
"""ZerobusSink examples -- 4 cases demonstrating named Databricks profiles,
explicit credentials, environment variable resolution, and proto vs JSON.

Requires the zerobus extra and Databricks workspace access::

    pip install iot-data-simulator[zerobus]

Usage::

    python examples/sinks/zerobus_sink_example.py           # Case 1 (default)
    python examples/sinks/zerobus_sink_example.py --case 2   # Explicit credentials
    python examples/sinks/zerobus_sink_example.py --case 3   # Environment variable resolution
    python examples/sinks/zerobus_sink_example.py --case 4   # Proto vs JSON record type
"""

from __future__ import annotations

import argparse


def _check_zerobus():
    try:
        from iot_simulator.sinks.zerobus import ZerobusSink  # noqa: F401

        return True
    except ImportError:
        print("ZerobusSink requires databricks-zerobus-ingest-sdk. Install with:")
        print("  pip install iot-data-simulator[zerobus]")
        return False


# ---------------------------------------------------------------------------
# Case 1: Named Databricks profile
# ---------------------------------------------------------------------------


def run_case_1() -> None:
    """Use a named profile from ~/.databrickscfg for authentication.

    The Databricks SDK resolves credentials from the profile file
    automatically. This is the simplest approach for interactive use.

    ~/.databrickscfg example::

        [staging]
        host = https://my-workspace.cloud.databricks.com
        client_id = xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        client_secret = your-secret-here

    Knobs demonstrated:
      - databricks_profile="staging"  -> read from ~/.databrickscfg
      - server_endpoint               -> Zerobus server endpoint
      - table_name                    -> Unity Catalog table
      - record_type="json"            -> JSON mode (no schema generation needed)
      - rate_hz=1.0, batch_size=100
    """
    if not _check_zerobus():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.zerobus import ZerobusSink

    print("=== Case 1: Named Databricks profile ===\n")

    # -- Replace with your actual endpoints --
    SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    TABLE_NAME = "catalog.schema.sensor_data"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    sim.add_sink(
        ZerobusSink(
            server_endpoint=SERVER_ENDPOINT,
            table_name=TABLE_NAME,
            databricks_profile="staging",  # Reads from ~/.databrickscfg [staging]
            record_type="json",
            rate_hz=1.0,
            batch_size=100,
        )
    )

    print(f"  Endpoint: {SERVER_ENDPOINT}")
    print(f"  Table: {TABLE_NAME}")
    print("  Auth: ~/.databrickscfg [staging]")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 2: Explicit credentials
# ---------------------------------------------------------------------------


def run_case_2() -> None:
    """Pass all three credentials explicitly (no profile/env lookup).

    Useful for environments where you cannot use the Databricks config
    file (e.g., containerised deployments, CI/CD).

    Knobs demonstrated:
      - workspace_url    -> explicit workspace URL
      - client_id        -> service principal application ID
      - client_secret    -> service principal secret
      - No profile fallback (all three provided)

    NOTE: Replace with your actual credentials.
    """
    if not _check_zerobus():
        return

    from iot_simulator import SensorConfig, SensorType, Simulator
    from iot_simulator.sinks.zerobus import ZerobusSink

    print("=== Case 2: Explicit credentials ===\n")

    SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    TABLE_NAME = "catalog.schema.sensor_data"

    # -- Replace with your actual credentials --
    WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
    CLIENT_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    CLIENT_SECRET = "your-client-secret-here"

    sim = Simulator(
        custom_sensors=[
            SensorConfig("temp_1", SensorType.TEMPERATURE, "°C", 15, 35, 22),
            SensorConfig("pressure_1", SensorType.PRESSURE, "bar", 2, 8, 5),
            SensorConfig("flow_1", SensorType.FLOW, "LPM", 100, 500, 300),
        ],
        custom_industry="demo",
        update_rate_hz=2.0,
    )

    sim.add_sink(
        ZerobusSink(
            server_endpoint=SERVER_ENDPOINT,
            table_name=TABLE_NAME,
            workspace_url=WORKSPACE_URL,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            record_type="json",
            rate_hz=1.0,
            batch_size=50,
        )
    )

    print(f"  Workspace: {WORKSPACE_URL}")
    print("  Auth: explicit client_id + client_secret")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 3: Environment variable resolution
# ---------------------------------------------------------------------------


def run_case_3() -> None:
    """Rely on environment variables for credential resolution.

    Set these environment variables before running::

        export DATABRICKS_HOST="https://my-workspace.cloud.databricks.com"
        export DATABRICKS_CLIENT_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        export DATABRICKS_CLIENT_SECRET="your-secret"

    The Databricks SDK resolves credentials automatically from env vars
    when no explicit values or profile are provided.

    Knobs demonstrated:
      - No workspace_url, client_id, client_secret args
      - No databricks_profile arg
      - Resolution falls through to DATABRICKS_* env vars
    """
    if not _check_zerobus():
        return

    import os

    from iot_simulator import Simulator
    from iot_simulator.sinks.zerobus import ZerobusSink

    print("=== Case 3: Environment variable resolution ===\n")

    # Check that env vars are set
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print("  The following environment variables must be set:")
        for v in missing:
            print(f'    export {v}="..."')
        print("\n  Skipping this example.")
        return

    SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    TABLE_NAME = "catalog.schema.sensor_data"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    # No explicit credentials -- SDK resolves from DATABRICKS_* env vars
    sim.add_sink(
        ZerobusSink(
            server_endpoint=SERVER_ENDPOINT,
            table_name=TABLE_NAME,
            record_type="json",
            rate_hz=1.0,
            batch_size=100,
        )
    )

    print("  Auth: DATABRICKS_* environment variables")
    print(f"  DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST', '(not set)')}")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Case 4: Proto vs JSON record type
# ---------------------------------------------------------------------------


def run_case_4() -> None:
    """Compare JSON and Proto record types side-by-side.

    - JSON mode: no schema generation required, recommended for most cases.
    - Proto mode: requires a Protocol Buffers schema, more efficient encoding.

    Knobs demonstrated:
      - record_type="json"   -> JSON mode (simpler)
      - record_type="proto"  -> Protocol Buffers mode (more efficient)
      - Two ZerobusSink instances on the same Simulator
    """
    if not _check_zerobus():
        return

    from iot_simulator import Simulator
    from iot_simulator.sinks.zerobus import ZerobusSink

    print("=== Case 4: Proto vs JSON record type ===\n")

    SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"

    sim = Simulator(industries=["mining"], update_rate_hz=2.0)

    # JSON mode sink -- writes to one table
    sim.add_sink(
        ZerobusSink(
            server_endpoint=SERVER_ENDPOINT,
            table_name="catalog.schema.sensor_data_json",
            databricks_profile="staging",
            record_type="json",  # JSON mode -- no schema needed
            rate_hz=1.0,
            batch_size=100,
        )
    )

    # Proto mode sink -- writes to another table
    sim.add_sink(
        ZerobusSink(
            server_endpoint=SERVER_ENDPOINT,
            table_name="catalog.schema.sensor_data_proto",
            databricks_profile="staging",
            record_type="proto",  # Proto mode -- more efficient encoding
            rate_hz=1.0,
            batch_size=100,
        )
    )

    print("  JSON table:  catalog.schema.sensor_data_json")
    print("  Proto table: catalog.schema.sensor_data_proto")
    sim.run(duration_s=10)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="ZerobusSink examples")
    parser.add_argument(
        "--case", type=int, default=1, choices=[1, 2, 3, 4], help="Which example case to run (default: 1)"
    )
    args = parser.parse_args()

    cases = {1: run_case_1, 2: run_case_2, 3: run_case_3, 4: run_case_4}
    cases[args.case]()


if __name__ == "__main__":
    main()

# Databricks notebook source
# MAGIC %md
# MAGIC # ZeroBus Ingest -- IoT Simulator
# MAGIC
# MAGIC End-to-end notebook that:
# MAGIC 1. Installs the IoT simulator package
# MAGIC 2. Configures credentials and simulator knobs via **widgets**
# MAGIC 3. Creates the target Delta table using the **auto-detected schema**
# MAGIC 4. Grants permissions to the service principal
# MAGIC 5. Streams simulated sensor data via ZeroBus with **live metrics**
# MAGIC 6. Queries the table to verify data landed
# MAGIC
# MAGIC All configuration is driven by the widget bar at the top of the notebook.
# MAGIC Change industries, record type, duration, etc. without editing any code.

# COMMAND ----------

# Install the IoT simulator with the zerobus extra
%pip install iot-data-simulator[zerobus]
dbutils.library.restartPython()

# COMMAND ----------

# ------------------------------------------------------------------
# Widgets -- configure everything from the widget bar
# ------------------------------------------------------------------

# Credentials
dbutils.widgets.text("server_endpoint",
                     "1234567890123456.zerobus.us-west-2.cloud.databricks.com",
                     "ZeroBus Server Endpoint")
dbutils.widgets.text("table_name",
                     "catalog.schema.sensor_data",
                     "Target Table (catalog.schema.table)")
dbutils.widgets.text("secret_scope",
                     "iot-simulator",
                     "Secret Scope")

# Simulator knobs
dbutils.widgets.text("industries",
                     "mining",
                     "Industries (comma-separated, or 'custom')")
dbutils.widgets.text("custom_sensors_json",
                     '[{"name":"temp_1","sensor_type":"temperature","unit":"C","min_value":15,"max_value":35,"nominal_value":22},'
                     '{"name":"pressure_1","sensor_type":"pressure","unit":"bar","min_value":2,"max_value":8,"nominal_value":5},'
                     '{"name":"flow_1","sensor_type":"flow","unit":"LPM","min_value":100,"max_value":500,"nominal_value":300}]',
                     "Custom Sensors JSON (used when industries='custom')")
dbutils.widgets.dropdown("record_type", "json", ["json", "proto"], "Record Type")
dbutils.widgets.text("duration_s", "30", "Duration (seconds)")
dbutils.widgets.text("rate_hz", "1.0", "Sink Rate (Hz)")
dbutils.widgets.text("batch_size", "100", "Batch Size")

print("Widgets created. Configure values in the widget bar above.")

# COMMAND ----------

# ------------------------------------------------------------------
# Read widget values and resolve credentials
# ------------------------------------------------------------------

SERVER_ENDPOINT = dbutils.widgets.get("server_endpoint")
TABLE_NAME      = dbutils.widgets.get("table_name")
SECRET_SCOPE    = dbutils.widgets.get("secret_scope")
INDUSTRIES_STR  = dbutils.widgets.get("industries")
RECORD_TYPE     = dbutils.widgets.get("record_type")
DURATION_S      = int(dbutils.widgets.get("duration_s"))
RATE_HZ         = float(dbutils.widgets.get("rate_hz"))
BATCH_SIZE      = int(dbutils.widgets.get("batch_size"))

# Workspace URL from notebook context
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
if not WORKSPACE_URL.startswith("https://"):
    WORKSPACE_URL = f"https://{WORKSPACE_URL}"

# Credentials from secret scope
CLIENT_ID     = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-secret")

print(f"Workspace   : {WORKSPACE_URL}")
print(f"Endpoint    : {SERVER_ENDPOINT}")
print(f"Table       : {TABLE_NAME}")
print(f"Industries  : {INDUSTRIES_STR}")
print(f"Record type : {RECORD_TYPE}")
print(f"Duration    : {DURATION_S}s")
print(f"Rate        : {RATE_HZ} Hz")
print(f"Batch size  : {BATCH_SIZE}")
print(f"Auth        : dbutils.secrets (scope={SECRET_SCOPE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the target table
# MAGIC
# MAGIC The schema is imported directly from the package (`_SCHEMA`).  It is
# MAGIC always in sync with `SensorRecord` and works for **every** sensor
# MAGIC configuration -- built-in industries, custom sensors, load tests,
# MAGIC JSON, and Proto all produce the same 11-column rows.

# COMMAND ----------

from iot_simulator.pyspark_datasource import _SCHEMA

print(f"Schema (from iot_simulator.pyspark_datasource._SCHEMA):\n  {_SCHEMA}\n")

create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({_SCHEMA}) USING DELTA"
print(create_sql)
spark.sql(create_sql)
print(f"\nTable {TABLE_NAME} is ready.")

# COMMAND ----------

# Grant Unity Catalog permissions to the service principal so ZeroBus can
# write to the table.  These are idempotent -- safe to re-run.

catalog, schema_name, _table_short = TABLE_NAME.split(".")

grants = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{CLIENT_ID}`",
    f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema_name}` TO `{CLIENT_ID}`",
    f"GRANT SELECT, MODIFY ON TABLE {TABLE_NAME} TO `{CLIENT_ID}`",
]

for stmt in grants:
    print(f"Running: {stmt}")
    spark.sql(stmt)

print("\nAll grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics-enabled ZeroBus sink
# MAGIC
# MAGIC `MetricsZerobusSink` wraps the standard `ZerobusSink` and prints a
# MAGIC one-line progress summary after every batch: record count, latency,
# MAGIC throughput, and error count.

# COMMAND ----------

import time
from typing import Any

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.zerobus import ZerobusSink


class MetricsZerobusSink(ZerobusSink):
    """ZerobusSink subclass that prints live metrics after every write."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._total_records = 0
        self._total_batches = 0
        self._total_errors = 0
        self._total_latency = 0.0
        self._start_time: float | None = None

    async def connect(self) -> None:
        await super().connect()
        self._start_time = time.perf_counter()
        print(
            f"\n{'='*70}\n"
            f"  Connected to ZeroBus\n"
            f"  Endpoint : {self._server_endpoint}\n"
            f"  Table    : {self._table_name}\n"
            f"{'='*70}\n"
        )

    async def write(self, records: list[SensorRecord]) -> None:
        t0 = time.perf_counter()
        try:
            await super().write(records)
        except Exception:
            self._total_errors += 1
            raise
        finally:
            elapsed = time.perf_counter() - t0
            self._total_batches += 1
            self._total_records += len(records)
            self._total_latency += elapsed
            avg_latency = self._total_latency / self._total_batches
            wall = time.perf_counter() - (self._start_time or t0)
            throughput = self._total_records / wall if wall > 0 else 0

            print(
                f"[batch {self._total_batches:>4d}]  "
                f"{self._total_records:>7,d} records  |  "
                f"batch {len(records):>4d}  |  "
                f"latency {elapsed:.3f}s  |  "
                f"avg {avg_latency:.3f}s  |  "
                f"throughput {throughput:,.0f} rec/s  |  "
                f"errors {self._total_errors}"
            )

    async def close(self) -> None:
        wall = time.perf_counter() - (self._start_time or time.perf_counter())
        print(
            f"\n{'='*70}\n"
            f"  Final Summary\n"
            f"  Records sent : {self._total_records:,d}\n"
            f"  Batches      : {self._total_batches:,d}\n"
            f"  Errors       : {self._total_errors:,d}\n"
            f"  Wall time    : {wall:.1f}s\n"
            f"  Avg latency  : {self._total_latency / max(self._total_batches, 1):.3f}s\n"
            f"  Throughput   : {self._total_records / max(wall, 0.001):,.0f} rec/s\n"
            f"{'='*70}\n"
        )
        await super().close()

# COMMAND ----------

# ------------------------------------------------------------------
# Build the Simulator from widget values and run
# ------------------------------------------------------------------

import json as _json

from iot_simulator import Simulator

if INDUSTRIES_STR.strip().lower() == "custom":
    from iot_simulator import SensorConfig, SensorType

    raw_sensors = _json.loads(dbutils.widgets.get("custom_sensors_json"))
    custom_sensors = [SensorConfig(**s) for s in raw_sensors]
    sim = Simulator(
        custom_sensors=custom_sensors,
        custom_industry="custom",
        update_rate_hz=2.0,
    )
    print(f"Simulator: {len(custom_sensors)} custom sensors")
else:
    industries = [s.strip() for s in INDUSTRIES_STR.split(",") if s.strip()]
    sim = Simulator(industries=industries, update_rate_hz=2.0)
    print(f"Simulator: industries={industries}  ({sim.sensor_count} sensors)")

sim.add_sink(
    MetricsZerobusSink(
        server_endpoint=SERVER_ENDPOINT,
        table_name=TABLE_NAME,
        workspace_url=WORKSPACE_URL,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        record_type=RECORD_TYPE,
        rate_hz=RATE_HZ,
        batch_size=BATCH_SIZE,
    )
)

print(f"\nStarting simulation for {DURATION_S}s (record_type={RECORD_TYPE}) ...\n")
sim.run(duration_s=DURATION_S)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify ingested data

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp DESC LIMIT 20"))

# COMMAND ----------

df_stats = spark.sql(f"""
    SELECT
        COUNT(*)                    AS total_rows,
        COUNT(DISTINCT sensor_name) AS distinct_sensors,
        COUNT(DISTINCT industry)    AS distinct_industries,
        MIN(timestamp)              AS earliest_ts,
        MAX(timestamp)              AS latest_ts,
        SUM(CASE WHEN fault_active THEN 1 ELSE 0 END) AS fault_count
    FROM {TABLE_NAME}
""")
display(df_stats)

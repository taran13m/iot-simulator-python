# Databricks notebook source
# MAGIC %md
# MAGIC # ZeroBus Ingest -- IoT Simulator
# MAGIC
# MAGIC End-to-end notebook that:
# MAGIC 1. Installs the IoT simulator package
# MAGIC 2. Configures credentials (secret scope **or** plain variables)
# MAGIC 3. Creates the target Delta table with the correct schema
# MAGIC 4. Grants permissions to the service principal
# MAGIC 5. Streams simulated sensor data via ZeroBus with **live metrics**
# MAGIC 6. Queries the table to verify data landed

# COMMAND ----------

# Install the IoT simulator with the zerobus extra
%pip install iot-data-simulator[zerobus]
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Run **either** the next cell (dbutils.secrets) **or** the one after it (plain variables).
# MAGIC Do not run both.

# COMMAND ----------

# --- Option A: Credentials from a Databricks secret scope (recommended) ---

SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"  # replace
TABLE_NAME      = "catalog.schema.sensor_data"                               # replace
SECRET_SCOPE    = "iot-simulator"                                            # replace

WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
if not WORKSPACE_URL.startswith("https://"):
    WORKSPACE_URL = f"https://{WORKSPACE_URL}"

CLIENT_ID     = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-secret")

print(f"Workspace : {WORKSPACE_URL}")
print(f"Endpoint  : {SERVER_ENDPOINT}")
print(f"Table     : {TABLE_NAME}")
print(f"Auth      : dbutils.secrets (scope={SECRET_SCOPE})")

# COMMAND ----------

# --- Option B: Plain variables (skip the cell above and uncomment below) ---

# SERVER_ENDPOINT = "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
# TABLE_NAME      = "catalog.schema.sensor_data"
# WORKSPACE_URL   = "https://my-workspace.cloud.databricks.com"
# CLIENT_ID       = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# CLIENT_SECRET   = "your-client-secret-here"
#
# print(f"Workspace : {WORKSPACE_URL}")
# print(f"Endpoint  : {SERVER_ENDPOINT}")
# print(f"Table     : {TABLE_NAME}")
# print(f"Auth      : plain variables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the target table
# MAGIC
# MAGIC The schema is derived from `SensorRecord.model_fields` and the DDL
# MAGIC string used by the PySpark DataSource.  Running this cell is
# MAGIC idempotent (`CREATE TABLE IF NOT EXISTS`).

# COMMAND ----------

from iot_simulator.models import SensorRecord

# Show the SensorRecord fields for reference
print("SensorRecord fields:")
for name, field_info in SensorRecord.model_fields.items():
    print(f"  {name:20s}  {field_info.annotation.__name__ if hasattr(field_info.annotation, '__name__') else str(field_info.annotation)}")

# Build and execute the CREATE TABLE statement
catalog, schema_name, table_short = TABLE_NAME.split(".")

create_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    sensor_name    STRING    COMMENT 'Sensor identifier, e.g. crusher_1_motor_power',
    industry       STRING    COMMENT 'Grouping label, e.g. mining',
    value          DOUBLE    COMMENT 'Current sensor reading',
    unit           STRING    COMMENT 'Engineering unit, e.g. kW or degC',
    sensor_type    STRING    COMMENT 'Sensor category, e.g. power, temperature',
    timestamp      DOUBLE    COMMENT 'Unix epoch seconds when the value was generated',
    min_value      DOUBLE    COMMENT 'Physical minimum of the sensor range',
    max_value      DOUBLE    COMMENT 'Physical maximum of the sensor range',
    nominal_value  DOUBLE    COMMENT 'Expected steady-state value',
    fault_active   BOOLEAN   COMMENT 'True when the simulator injected a fault',
    metadata       STRING    COMMENT 'JSON-encoded dict of optional tags'
)
USING DELTA
COMMENT 'IoT simulator sensor data ingested via ZeroBus'
"""

print(f"\n{create_sql}")
spark.sql(create_sql)
print(f"Table {TABLE_NAME} is ready.")

# COMMAND ----------

# Grant Unity Catalog permissions to the service principal so ZeroBus can
# write to the table.  These are idempotent -- safe to re-run.

catalog, schema_name, table_short = TABLE_NAME.split(".")

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

from iot_simulator import Simulator

DURATION_S = 30  # seconds to run the simulation

sim = Simulator(industries=["mining"], update_rate_hz=2.0)

sim.add_sink(
    MetricsZerobusSink(
        server_endpoint=SERVER_ENDPOINT,
        table_name=TABLE_NAME,
        workspace_url=WORKSPACE_URL,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        record_type="json",
        rate_hz=1.0,
        batch_size=100,
    )
)

print(f"Starting simulation for {DURATION_S}s ...\n")
sim.run(duration_s=DURATION_S)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify ingested data

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp DESC LIMIT 20"))

# COMMAND ----------

df_stats = spark.sql(f"""
    SELECT
        COUNT(*)                   AS total_rows,
        COUNT(DISTINCT sensor_name) AS distinct_sensors,
        COUNT(DISTINCT industry)   AS distinct_industries,
        MIN(timestamp)             AS earliest_ts,
        MAX(timestamp)             AS latest_ts,
        SUM(CASE WHEN fault_active THEN 1 ELSE 0 END) AS fault_count
    FROM {TABLE_NAME}
""")
display(df_stats)

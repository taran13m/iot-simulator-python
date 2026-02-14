# IoT Data Simulator

**`iot-data-simulator`** -- Generate realistic industrial IoT sensor data and push it to pluggable sinks with per-sink throughput control.

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)
![Version: 0.1.0](https://img.shields.io/badge/version-0.1.0-orange)

---

## Features

- **379 built-in sensors** across 16 industrial sectors -- mining, aerospace, pharmaceutical, automotive, and more -- each with realistic noise, drift, cyclic patterns, and anomaly injection
- **Custom sensor definitions** via Python API, YAML config, or CSV import
- **10 sink types** out of the box: Console, Callback, Kafka, File (CSV / JSON / Parquet), Database (PostgreSQL / SQLite), Webhook (HTTP POST), Delta Lake, Azure IoT Hub, AWS IoT Core, Databricks Zerobus Ingest
- **Per-sink throughput control** -- each sink independently controls its flush rate, batch size, buffer limits, and backpressure policy
- **Two usage modes**: programmatic Python API and config-driven YAML
- **CLI with subcommands**: `run`, `list-industries`, `list-sinks`, `list-sensors`, `init-config` -- discover everything from the terminal
- **Inline CLI sinks**: use `--sink console`, `--sink file`, or both directly from the command line without writing YAML
- **Extensible**: subclass `Sink` to write your own, register it for YAML use

---

## Installation

**Core** (console + callback sinks, no external dependencies beyond PyYAML):

```bash
pip install iot-data-simulator
```

**With optional sink extras:**

| Extra | Installs | Use case |
|-------|----------|----------|
| `[kafka]` | `aiokafka` | Publish to Apache Kafka |
| `[file]` | `pyarrow` | Write Parquet files |
| `[database]` | `sqlalchemy`, `aiosqlite` | Insert into PostgreSQL / SQLite |
| `[webhook]` | `httpx` | POST to HTTP endpoints |
| `[delta]` | `deltalake` | Write Delta Lake tables |
| `[cloud]` | `azure-iot-device`, `boto3` | Azure IoT Hub / AWS IoT Core |
| `[zerobus]` | `databricks-zerobus-ingest-sdk`, `databricks-sdk` | Databricks Zerobus Ingest |
| `[protocols]` | `asyncua`, `aiomqtt`, `pymodbus` | OPC-UA / MQTT / Modbus servers |
| `[all]` | Everything above | Full install |

```bash
# Install with specific extras
pip install iot-data-simulator[kafka,database]

# Install everything
pip install iot-data-simulator[all]
```

**From a Git URL** (internal / private distribution):

```bash
pip install git+https://your-git-server.com/org/Iot-Simulator.git
```

**Development mode:**

```bash
git clone <repo-url>
cd Iot-Simulator
pip install -e ".[all]"
```

---

## Quick Start

**Python (5 lines):**

```python
from iot_simulator import Simulator
from iot_simulator.sinks import ConsoleSink

sim = Simulator(industries=["mining"], update_rate_hz=2.0)
sim.add_sink(ConsoleSink(rate_hz=0.5))
sim.run(duration_s=10)
```

**CLI:**

```bash
# Run with console output (default)
iot-simulator run --industries mining utilities --rate 2 --duration 10

# Run with both console and file sinks
iot-simulator run -i mining -s console -s file -o ./data --output-format csv -d 30

# Discover available industries and sensors
iot-simulator list-industries
iot-simulator list-sensors mining

# Generate a starter YAML config
iot-simulator init-config --output simulator.yaml
```

Output from `run`:

```
[mining/crusher_1_motor_power]      450.123 kW       (type=power)
[mining/crusher_1_bearing_temp]       76.891 °C       (type=temperature)
[mining/conveyor_belt_1_speed]        1.782 m/s      (type=speed)
[utilities/turbine_1_power_output]       49.234 MW       (type=power)
...
```

---

## Programmatic API

### Built-in Industries

The library ships with 16 industrial sectors containing 379 pre-configured sensors:

| Industry | Sensors | Industry | Sensors |
|----------|---------|----------|---------|
| `mining` | 17 | `utilities` | 18 |
| `manufacturing` | 19 | `oil_gas` | 26 |
| `aerospace` | 25 | `space` | 20 |
| `water_wastewater` | 22 | `electric_power` | 24 |
| `automotive` | 28 | `chemical` | 26 |
| `food_beverage` | 25 | `pharmaceutical` | 30 |
| `data_center` | 22 | `smart_building` | 25 |
| `agriculture` | 20 | `renewable_energy` | 32 |

```python
from iot_simulator import Simulator
from iot_simulator.sinks import ConsoleSink

# Activate one or more industries
sim = Simulator(industries=["mining", "utilities", "aerospace"], update_rate_hz=2.0)
sim.add_sink(ConsoleSink(rate_hz=1.0))
sim.run(duration_s=30)
```

### Custom Sensors

Define your own sensors using `SensorConfig`:

```python
from iot_simulator import Simulator, SensorConfig, SensorType
from iot_simulator.sinks import ConsoleSink

my_sensors = [
    SensorConfig(
        name="room_temperature",
        sensor_type=SensorType.TEMPERATURE,
        unit="°C",
        min_value=15.0,
        max_value=35.0,
        nominal_value=22.0,
        noise_std=0.3,
        cyclic=True,
        cycle_period_seconds=3600,   # daily HVAC cycle
    ),
    SensorConfig(
        name="co2_level",
        sensor_type=SensorType.LEVEL,
        unit="ppm",
        min_value=400,
        max_value=2000,
        nominal_value=600,
        noise_std=15.0,
        anomaly_probability=0.005,
    ),
    SensorConfig(
        name="door_open_count",
        sensor_type=SensorType.POSITION,
        unit="count",
        min_value=0,
        max_value=1000,
        nominal_value=50,
        noise_std=0,
    ),
]

sim = Simulator(
    custom_sensors=my_sensors,
    custom_industry="smart_office",
    update_rate_hz=1.0,
)
sim.add_sink(ConsoleSink(rate_hz=1.0))
sim.run(duration_s=10)
```

### Combining Built-in and Custom Sensors

```python
sim = Simulator(
    industries=["mining"],
    custom_sensors=my_sensors,
    custom_industry="smart_office",
    update_rate_hz=2.0,
)
```

### Adding Sensors After Initialization

```python
sim = Simulator(industries=["mining"], update_rate_hz=2.0)
sim.add_sensors(my_sensors, industry="smart_office")
print(f"Total sensors: {sim.sensor_count}")  # mining + custom
```

### Loading Sensors from CSV

Create a CSV file with sensor definitions (useful for bulk import from spreadsheets):

```csv
name,sensor_type,unit,min_value,max_value,nominal_value,noise_std,cyclic,cycle_period_seconds
boiler_temp,temperature,°C,80,200,150,2.0,true,30
steam_pressure,pressure,bar,5,25,15,0.5,false,
feed_water_flow,flow,L/m,100,500,300,10.0,false,
```

```python
sim = Simulator.from_csv("my_sensors.csv", industry="power_plant")
sim.add_sink(ConsoleSink(rate_hz=1.0))
sim.run(duration_s=10)
```

Required CSV columns: `name`, `sensor_type`, `unit`, `min_value`, `max_value`, `nominal_value`.
Optional columns: `noise_std`, `drift_rate`, `anomaly_probability`, `anomaly_magnitude`, `cyclic`, `cycle_period_seconds`, `cycle_amplitude`.

### Lambda / Callback Sink

Pass any callable (function, lambda, or async function) directly to `add_sink`:

```python
# Lambda
sim.add_sink(lambda records: print(f"Got {len(records)} records"))

# Collect records for analysis
all_records = []
sim.add_sink(lambda records: all_records.extend(records))
sim.run(duration_s=5)

# Analyze
from collections import Counter
sensors = Counter(r.sensor_name for r in all_records)
print(sensors.most_common(5))
```

### Multiple Sinks at Different Rates

```python
from iot_simulator import Simulator
from iot_simulator.sinks import ConsoleSink
from iot_simulator.sinks.file import FileSink
from iot_simulator.sinks.kafka import KafkaSink

sim = Simulator(industries=["mining"], update_rate_hz=5.0)

# Fast: Kafka gets every record
sim.add_sink(KafkaSink(
    bootstrap_servers="localhost:9092",
    topic="iot-data",
    rate_hz=5.0,
    batch_size=50,
))

# Medium: File writes every 10 seconds in large batches
sim.add_sink(FileSink(
    path="./output",
    format="parquet",
    rotation="1h",
    rate_hz=0.1,
    batch_size=500,
))

# Slow: Console prints every 2 seconds
sim.add_sink(ConsoleSink(rate_hz=0.5))

sim.run()  # runs until Ctrl-C
```

### Async Usage

Use `run_async()` inside an existing event loop:

```python
import asyncio
from iot_simulator import Simulator
from iot_simulator.sinks import ConsoleSink

async def main():
    sim = Simulator(industries=["mining"], update_rate_hz=2.0)
    sim.add_sink(ConsoleSink(rate_hz=1.0))
    await sim.run_async(duration_s=10)

asyncio.run(main())
```

---

## Sink Reference

Every sink accepts the common throughput parameters (`rate_hz`, `batch_size`, `max_buffer_size`, `backpressure`, `retry_count`, `retry_delay_s`) in addition to its own configuration.

### ConsoleSink

Prints sensor records to stdout. No extra dependencies.

```python
from iot_simulator.sinks import ConsoleSink

# Human-readable text format (default)
sim.add_sink(ConsoleSink(fmt="text", rate_hz=1.0))

# One JSON object per record
sim.add_sink(ConsoleSink(fmt="json", rate_hz=0.5))

# Write to a custom stream
import io
buffer = io.StringIO()
sim.add_sink(ConsoleSink(stream=buffer))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fmt` | `str` | `"text"` | Output format: `"text"` or `"json"` |
| `stream` | file-like | `sys.stdout` | Writable output target |

### KafkaSink

Publishes JSON messages to Apache Kafka. Requires `pip install iot-data-simulator[kafka]`.

```python
from iot_simulator.sinks.kafka import KafkaSink

sim.add_sink(KafkaSink(
    bootstrap_servers="localhost:9092",
    topic="iot-sensor-data",
    compression="snappy",      # snappy, gzip, lz4, zstd, or None
    key_field="sensor_name",   # message key: "sensor_name", "industry", or None
    acks="all",
    rate_hz=2.0,
    batch_size=50,
))

# With SASL authentication
sim.add_sink(KafkaSink(
    bootstrap_servers="broker.example.com:9093",
    topic="iot-data",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="user",
    sasl_password="secret",
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bootstrap_servers` | `str` | `"localhost:9092"` | Comma-separated broker addresses |
| `topic` | `str` | `"iot-sensor-data"` | Target Kafka topic |
| `compression` | `str \| None` | `"snappy"` | Compression codec |
| `key_field` | `str \| None` | `"sensor_name"` | Record field used as message key |
| `acks` | `str \| int` | `"all"` | Producer acknowledgement level |
| `security_protocol` | `str` | `"PLAINTEXT"` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | `str \| None` | `None` | `PLAIN`, `SCRAM-SHA-256`, etc. |
| `sasl_username` | `str \| None` | `None` | SASL username |
| `sasl_password` | `str \| None` | `None` | SASL password |

### FileSink

Writes records to CSV, JSON Lines, or Parquet files with optional time-based rotation. Parquet requires `pip install iot-data-simulator[file]`.

```python
from iot_simulator.sinks.file import FileSink

# CSV output
sim.add_sink(FileSink(path="./output", format="csv", rate_hz=1.0))

# JSON Lines output
sim.add_sink(FileSink(path="./output", format="json", rate_hz=1.0))

# Parquet with hourly rotation
sim.add_sink(FileSink(
    path="./output",
    format="parquet",
    rotation="1h",         # rotate every hour (also: 30s, 5m, 1d)
    rate_hz=0.1,
    batch_size=500,
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | `"./output"` | Output directory (created automatically) |
| `format` | `str` | `"csv"` | File format: `"csv"`, `"json"`, or `"parquet"` |
| `rotation` | `str \| None` | `None` | Time-based rotation: `"30s"`, `"5m"`, `"1h"`, `"1d"` |

### DatabaseSink

Inserts records into a SQL database via SQLAlchemy async. Requires `pip install iot-data-simulator[database]`.

The table is created automatically on connect if it does not exist.

```python
from iot_simulator.sinks.database import DatabaseSink

# SQLite (great for local testing)
sim.add_sink(DatabaseSink(
    connection_string="sqlite+aiosqlite:///sensor_data.db",
    table="sensor_readings",
    rate_hz=1.0,
    batch_size=200,
))

# PostgreSQL
sim.add_sink(DatabaseSink(
    connection_string="postgresql+asyncpg://user:pass@localhost:5432/iot",
    table="sensor_data",
    rate_hz=2.0,
    batch_size=500,
    max_buffer_size=10000,
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection_string` | `str` | `"sqlite+aiosqlite:///sensor_data.db"` | SQLAlchemy async URL |
| `table` | `str` | `"sensor_data"` | Target table name |

### WebhookSink

POSTs JSON arrays of records to an HTTP endpoint. Requires `pip install iot-data-simulator[webhook]`.

```python
from iot_simulator.sinks.webhook import WebhookSink

sim.add_sink(WebhookSink(
    url="https://example.com/api/ingest",
    headers={"Authorization": "Bearer my-token"},
    timeout_s=30.0,
    rate_hz=0.5,
    batch_size=100,
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | *(required)* | Target endpoint URL |
| `headers` | `dict \| None` | `None` | Extra HTTP headers |
| `timeout_s` | `float` | `30.0` | Per-request timeout in seconds |

### DeltaSink

Writes records to a Delta Lake table. Requires `pip install iot-data-simulator[delta]`.

```python
from iot_simulator.sinks.delta import DeltaSink

# Local Delta table
sim.add_sink(DeltaSink(
    table_path="/tmp/delta/sensors",
    mode="append",
    partition_by=["industry"],
    rate_hz=0.2,
    batch_size=1000,
))

# S3-backed Delta table
sim.add_sink(DeltaSink(
    table_path="s3://my-bucket/delta/sensors",
    storage_options={
        "AWS_ACCESS_KEY_ID": "...",
        "AWS_SECRET_ACCESS_KEY": "...",
    },
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_path` | `str` | `"/tmp/delta/sensors"` | Delta table directory (local or cloud URI) |
| `mode` | `str` | `"append"` | `"append"` or `"overwrite"` |
| `partition_by` | `list[str] \| None` | `None` | Columns to partition by |
| `storage_options` | `dict \| None` | `None` | Cloud storage credentials |

### AzureIoTSink

Sends device-to-cloud messages to Azure IoT Hub. Requires `pip install iot-data-simulator[cloud]`.

```python
from iot_simulator.sinks.cloud_iot import AzureIoTSink

sim.add_sink(AzureIoTSink(
    connection_string="HostName=hub.azure-devices.net;DeviceId=sim;SharedAccessKey=...",
    rate_hz=1.0,
    batch_size=50,
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection_string` | `str` | *(required)* | Device connection string from Azure Portal |
| `content_type` | `str` | `"application/json"` | MIME type for messages |

### AWSIoTSink

Publishes to AWS IoT Core via the `iot-data` publish API. Requires `pip install iot-data-simulator[cloud]`.

```python
from iot_simulator.sinks.cloud_iot import AWSIoTSink

sim.add_sink(AWSIoTSink(
    endpoint="abc123-ats.iot.us-east-1.amazonaws.com",
    topic="iot/sensor-data",
    region="us-east-1",
    rate_hz=1.0,
    batch_size=50,
))
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpoint` | `str` | *(required)* | AWS IoT Core endpoint |
| `topic` | `str` | `"iot/sensor-data"` | MQTT topic |
| `region` | `str` | `"us-east-1"` | AWS region |
| `aws_access_key_id` | `str \| None` | `None` | Explicit credentials (optional) |
| `aws_secret_access_key` | `str \| None` | `None` | Explicit credentials (optional) |

### ZerobusSink

Streams sensor records to a Databricks Unity Catalog table via [Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest). Requires `pip install iot-data-simulator[zerobus]`.

Credentials are resolved through **Databricks unified authentication** -- you do not need to put secrets in config files. The resolution order is: explicit args → environment variables (`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_HOST`) → `~/.databrickscfg` profile.

```python
from iot_simulator.sinks.zerobus import ZerobusSink

# Option 1: Credentials from ~/.databrickscfg profile (recommended)
sim.add_sink(ZerobusSink(
    server_endpoint="1234567890123456.zerobus.us-west-2.cloud.databricks.com",
    table_name="catalog.schema.sensor_data",
    databricks_profile="DEFAULT",    # reads ~/.databrickscfg [DEFAULT]
    rate_hz=1.0,
    batch_size=100,
))

# Option 2: Explicit credentials (for testing / CI)
sim.add_sink(ZerobusSink(
    server_endpoint="1234567890123456.zerobus.us-west-2.cloud.databricks.com",
    workspace_url="https://dbc-a1b2c3d4-e5f6.cloud.databricks.com",
    table_name="catalog.schema.sensor_data",
    client_id="your-client-id",
    client_secret="your-client-secret",
    record_type="json",
    rate_hz=1.0,
    batch_size=100,
))
```

**Setting up `~/.databrickscfg`:**

```ini
[DEFAULT]
host = https://dbc-a1b2c3d4-e5f6.cloud.databricks.com
client_id = 52393ed8-ea22-4830-a6ef-6b6545e6be5f
client_secret = your-service-principal-secret
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `server_endpoint` | `str` | *(required)* | Zerobus server endpoint |
| `table_name` | `str` | *(required)* | Fully-qualified Unity Catalog table (`catalog.schema.table`) |
| `databricks_profile` | `str \| None` | `None` | Named profile in `~/.databrickscfg` |
| `workspace_url` | `str \| None` | `None` | Databricks workspace URL (resolved from profile/env if omitted) |
| `client_id` | `str \| None` | `None` | Service principal client ID (resolved from profile/env if omitted) |
| `client_secret` | `str \| None` | `None` | Service principal secret (resolved from profile/env if omitted) |
| `record_type` | `str` | `"json"` | `"json"` or `"proto"` |

### CallbackSink

Wraps any Python callable (sync or async) as a sink:

```python
from iot_simulator.sinks import CallbackSink

# Synchronous callback
def my_handler(records):
    for rec in records:
        print(f"{rec.sensor_name}: {rec.value}")

sim.add_sink(CallbackSink(my_handler, rate_hz=1.0))

# Async callback
async def async_handler(records):
    await some_async_operation(records)

sim.add_sink(CallbackSink(async_handler, rate_hz=2.0))
```

> **Shortcut:** You can pass a callable directly to `sim.add_sink()` and it will be wrapped in a `CallbackSink` automatically.

---

## Throughput Control

Each sink has independent throughput parameters. The generator runs at a fixed `update_rate_hz` and fans out records to each sink's internal buffer. Each sink drains its buffer at its own pace.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rate_hz` | `float \| None` | `None` | Flush frequency. `None` = every generator tick (lowest latency). `0.1` = once every 10 seconds. |
| `batch_size` | `int` | `100` | Flush when the buffer reaches this many records, regardless of `rate_hz`. |
| `max_buffer_size` | `int` | `50,000` | Maximum records held in memory before backpressure kicks in. |
| `backpressure` | `str` | `"drop_oldest"` | Policy when buffer is full: `"drop_oldest"`, `"drop_newest"`, or `"block"`. |
| `retry_count` | `int` | `3` | Number of retries on a failed `write()`. |
| `retry_delay_s` | `float` | `1.0` | Seconds between retries. |

### Flush Logic

A sink flushes when **either** condition is met (whichever comes first):

- The time since last flush exceeds `1 / rate_hz` seconds
- The buffer contains `batch_size` or more records

### Example: Fast and Slow Sinks from the Same Generator

```python
from iot_simulator import Simulator
from iot_simulator.sinks import ConsoleSink
from iot_simulator.sinks.kafka import KafkaSink
from iot_simulator.sinks.file import FileSink

sim = Simulator(industries=["mining"], update_rate_hz=5.0)

# Kafka: flushes every 0.5s or when 50 records buffered
sim.add_sink(KafkaSink(
    bootstrap_servers="localhost:9092",
    topic="iot-data",
    rate_hz=2.0,
    batch_size=50,
))

# File: flushes every 10s or when 500 records buffered
sim.add_sink(FileSink(
    path="./output",
    format="parquet",
    rate_hz=0.1,
    batch_size=500,
))

# Console: flushes every 2s
sim.add_sink(ConsoleSink(rate_hz=0.5))

sim.run()
```

All three sinks receive the same data, but the Kafka sink writes in near-real-time while the file sink accumulates large batches.

---

## Config-Driven (YAML) Mode

Instead of writing Python code, you can define everything in a YAML file:

```yaml
# simulator.yaml

simulator:
  industries: [mining, utilities]
  update_rate_hz: 2.0
  duration_s: 60                    # optional: auto-stop after 60s
  log_level: INFO

# Optional: user-defined custom sensors
custom_sensors:
  industry: smart_office
  sensors:
    - name: room_temperature
      sensor_type: temperature
      unit: "°C"
      min_value: 15.0
      max_value: 35.0
      nominal_value: 22.0
      noise_std: 0.3
      cyclic: true
      cycle_period_seconds: 3600

    - name: co2_level
      sensor_type: level
      unit: ppm
      min_value: 400
      max_value: 2000
      nominal_value: 600
      noise_std: 15.0
      anomaly_probability: 0.005

# Sinks with per-sink throughput control
sinks:
  - type: console
    fmt: text
    rate_hz: 0.5

  - type: kafka
    bootstrap_servers: localhost:9092
    topic: iot-data
    compression: snappy
    rate_hz: 2.0
    batch_size: 50

  - type: file
    path: ./output
    format: parquet
    rotation: 1h
    rate_hz: 0.1
    batch_size: 500

  - type: database
    connection_string: postgresql+asyncpg://user:pass@localhost/iot
    table: sensor_data
    rate_hz: 1.0
    batch_size: 200
    max_buffer_size: 10000

  - type: webhook
    url: https://example.com/ingest
    rate_hz: 0.5
    batch_size: 100
    headers:
      Authorization: Bearer my-token

  - type: delta
    table_path: /tmp/delta/sensors
    rate_hz: 0.2
    batch_size: 1000

  - type: zerobus
    server_endpoint: "1234567890123456.zerobus.us-west-2.cloud.databricks.com"
    table_name: catalog.schema.sensor_data
    databricks_profile: DEFAULT        # reads ~/.databrickscfg (no secrets in YAML)
    record_type: json
    rate_hz: 1.0
    batch_size: 100
```

**Run it:**

```bash
# Using the installed entry point
iot-simulator --config simulator.yaml

# Or via Python module
python -m iot_simulator --config simulator.yaml

# Override duration from CLI
iot-simulator --config simulator.yaml --duration 120
```

---

## SensorRecord Reference

Every sink receives batches of `SensorRecord` objects. This is the universal data format flowing through the pipeline.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `sensor_name` | `str` | Sensor identifier, e.g. `"crusher_1_motor_power"` |
| `industry` | `str` | Grouping label, e.g. `"mining"` or `"smart_office"` |
| `value` | `float` | Current sensor reading |
| `unit` | `str` | Engineering unit, e.g. `"kW"`, `"°C"`, `"bar"` |
| `sensor_type` | `str` | Category: `"power"`, `"temperature"`, `"pressure"`, etc. |
| `timestamp` | `float` | Unix epoch seconds when the value was generated |
| `min_value` | `float` | Physical minimum of the sensor range |
| `max_value` | `float` | Physical maximum of the sensor range |
| `nominal_value` | `float` | Expected steady-state value |
| `fault_active` | `bool` | `True` when the simulator has injected a fault |
| `metadata` | `dict` | Optional user-provided tags (location, asset ID, etc.) |

### Serialization

```python
record = all_records[0]

# To dict (JSON-safe)
d = record.to_dict()
# {'sensor_name': 'crusher_1_motor_power', 'industry': 'mining', 'value': 450.12, ...}

# To compact JSON string
j = record.to_json()
# '{"sensor_name":"crusher_1_motor_power","industry":"mining","value":450.12,...}'

# Reconstruct from dict
restored = SensorRecord.from_dict(d)
assert restored == record
```

---

## SensorConfig Reference

`SensorConfig` defines everything about a sensor. Used for both built-in and custom sensors.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | *(required)* | Sensor identifier |
| `sensor_type` | `SensorType` | *(required)* | Category enum (see below) |
| `unit` | `str` | *(required)* | Engineering unit string |
| `min_value` | `float` | *(required)* | Physical minimum |
| `max_value` | `float` | *(required)* | Physical maximum |
| `nominal_value` | `float` | *(required)* | Expected steady-state value |
| `noise_std` | `float` | `0.01` | Standard deviation for Gaussian noise |
| `drift_rate` | `float` | `0.0001` | Slow drift per update cycle |
| `anomaly_probability` | `float` | `0.001` | Probability (0-1) of anomaly per tick |
| `anomaly_magnitude` | `float` | `2.0` | Multiplier for anomaly spikes |
| `update_frequency_hz` | `float` | `1.0` | Nominal updates per second |
| `cyclic` | `bool` | `False` | Enable sinusoidal cyclic pattern |
| `cycle_period_seconds` | `float` | `60.0` | Period of the cyclic pattern |
| `cycle_amplitude` | `float` | `0.1` | Amplitude as fraction of nominal value |

### SensorType Enum

All 14 supported sensor types:

| Value | Description | Value | Description |
|-------|-------------|-------|-------------|
| `temperature` | Temperature sensors | `pressure` | Pressure sensors |
| `flow` | Flow rate sensors | `vibration` | Vibration sensors |
| `level` | Level / fill sensors | `speed` | Rotational / linear speed |
| `power` | Power consumption | `current` | Electrical current |
| `voltage` | Electrical voltage | `humidity` | Humidity sensors |
| `ph` | pH level sensors | `conductivity` | Conductivity sensors |
| `position` | Position / displacement | `status` | Binary / status sensors |

---

## Writing a Custom Sink

Subclass `Sink` and implement four async methods:

```python
from iot_simulator.sinks.base import Sink
from iot_simulator.models import SensorRecord


class StatsSink(Sink):
    """Tracks min/max/avg per sensor."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.stats = {}

    async def connect(self):
        print("StatsSink ready")

    async def write(self, records: list[SensorRecord]):
        for rec in records:
            self.stats.setdefault(rec.sensor_name, []).append(rec.value)

    async def flush(self):
        pass  # nothing to flush

    async def close(self):
        for name, values in sorted(self.stats.items()):
            avg = sum(values) / len(values)
            print(f"{name}: min={min(values):.1f} max={max(values):.1f} avg={avg:.1f}")


# Use it
sim.add_sink(StatsSink(rate_hz=1.0, batch_size=200))
```

### Registering for YAML Config

To use your custom sink in YAML configurations, register it with the factory:

```python
from iot_simulator.sinks.factory import register_sink

register_sink("stats", "mypackage.sinks", "StatsSink")
```

Then in YAML:

```yaml
sinks:
  - type: stats
    rate_hz: 1.0
    batch_size: 200
```

---

## Architecture

```mermaid
flowchart TD
    subgraph input [Sensor Definitions]
        BuiltIn["Built-in Industries\n16 industries, 379 sensors"]
        Custom["Custom Sensors\nSensorConfig list / CSV"]
    end

    subgraph core [iot_simulator core]
        SensorModels["sensor_models.py\nSensorConfig + SensorSimulator"]
        Generator["generator.py\nDataGenerator"]
        Record["models.py\nSensorRecord"]
    end

    subgraph dispatch [Throughput Control Layer]
        RunnerA["SinkRunner A\nrate_hz=2.0, batch=50"]
        RunnerB["SinkRunner B\nrate_hz=0.1, batch=500"]
        RunnerC["SinkRunner C\nrate_hz=0.5, batch=100"]
    end

    subgraph sinks [iot_simulator.sinks]
        Console["ConsoleSink"]
        Kafka["KafkaSink"]
        File["FileSink"]
        DB["DatabaseSink"]
        Webhook["WebhookSink"]
        Delta["DeltaSink"]
        Cloud["AzureIoT / AWSIoT"]
        Zerobus["ZerobusSink"]
        Callback["CallbackSink"]
    end

    BuiltIn --> SensorModels
    Custom --> SensorModels
    SensorModels --> Generator
    Generator -->|"tick -> list of SensorRecord"| Record
    Record --> RunnerA
    Record --> RunnerB
    Record --> RunnerC
    RunnerA --> Kafka
    RunnerB --> File
    RunnerC --> Console
```

**Data flow:** The `DataGenerator` calls `tick()` at `update_rate_hz` to produce a batch of `SensorRecord` objects. The `Simulator` fans out each batch to every registered `SinkRunner`. Each runner independently buffers records and drains them to its sink at the configured `rate_hz` and `batch_size`.

---

## CLI Reference

The CLI is available as `iot-simulator` (installed entry point) or `python -m iot_simulator`.

```
iot-simulator <command> [OPTIONS]
```

### Commands

| Command | Description |
|---------|-------------|
| `run` | Run the simulator (generate data and send to sinks) |
| `list-industries` | List all 16 built-in industries with sensor counts |
| `list-sinks` | List all available sink types with install instructions |
| `list-sensors <industry>` | List every sensor in an industry (name, type, unit, range) |
| `init-config` | Generate a sample YAML configuration file |

### `run` flags

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--config` | `-c` | `str` | `None` | Path to YAML config file (when set, `--sink`/`--output-*` flags are ignored) |
| `--industries` | `-i` | `str...` | `["mining"]` | Built-in industry names |
| `--rate` | | `float` | `2.0` | Generator tick rate in Hz |
| `--duration` | `-d` | `float` | `None` | Run duration in seconds (`None` = run until Ctrl-C) |
| `--log-level` | | `str` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--sink` | `-s` | `str` | `console` | Sink to enable: `console` or `file` (repeatable: `-s console -s file`) |
| `--format` | | `str` | `text` | Console output format: `text` or `json` |
| `--output-dir` | `-o` | `str` | `./output` | Directory for file sink output |
| `--output-format` | | `str` | `csv` | File format: `csv`, `json`, or `parquet` |
| `--rotation` | | `str` | `None` | File rotation interval: `30s`, `5m`, `1h`, `1d` |

### `init-config` flags

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--output` | `-o` | `str` | stdout | Write config to file instead of printing |

### Examples

```bash
# Quick demo -- console output, mining sensors, 10 seconds
iot-simulator run --industries mining --duration 10

# Multiple industries, JSON output
iot-simulator run -i mining utilities aerospace --format json -d 30

# Write CSV files to disk (no console output)
iot-simulator run -i mining --sink file -o ./data --output-format csv -d 60

# Console + Parquet files simultaneously, with hourly rotation
iot-simulator run -i mining -s console -s file -o ./data --output-format parquet --rotation 1h

# Config-driven with duration override
iot-simulator run --config simulator.yaml --duration 120 --log-level DEBUG

# Discover what's available
iot-simulator list-industries
iot-simulator list-sensors mining
iot-simulator list-sinks

# Generate a starter config and run it
iot-simulator init-config --output my_config.yaml
iot-simulator run --config my_config.yaml
```

### Discovery commands output

**`list-industries`:**

```
Industry                  Sensors
----------------------------------
mining                         17
utilities                      18
manufacturing                  19
...
renewable_energy               32
----------------------------------
TOTAL                         379
```

**`list-sensors mining`:**

```
Sensors for 'mining' (17 sensors):

Name                                 Type           Unit         Min        Max    Nominal
------------------------------------------------------------------------------------------
conveyor_belt_1_speed                speed          m/s         0.50       3.00       1.80
crusher_1_motor_power                power          kW        200.00     800.00     450.00
crusher_1_bearing_temp               temperature    °C         35.00     105.00      70.00
...
```

**`list-sinks`:**

```
Sink Type      Class                Install Extra
--------------------------------------------------------------
console        ConsoleSink          (built-in)
callback       CallbackSink         (built-in)
kafka          KafkaSink            pip install iot-data-simulator[kafka]
file           FileSink             pip install iot-data-simulator[file]
database       DatabaseSink         pip install iot-data-simulator[database]
webhook        WebhookSink          pip install iot-data-simulator[webhook]
delta          DeltaSink            pip install iot-data-simulator[delta]
azure_iot      AzureIoTSink         pip install iot-data-simulator[cloud]
aws_iot        AWSIoTSink           pip install iot-data-simulator[cloud]
zerobus        ZerobusSink          pip install iot-data-simulator[zerobus]
```

> **Backward compatibility:** Running without a subcommand (e.g. `iot-simulator --industries mining --duration 10`) is still supported -- it is treated as an implicit `run`.

---

## Running Tests

```bash
# Install in development mode with all extras
pip install -e ".[all]"

# Run the test suite
pytest tests/iot_simulator/ -v
```

The test suite covers:
- `SensorRecord` creation, serialization, and round-trip
- `DataGenerator` with built-in industries, custom sensors, and CSV import
- `ConsoleSink`, `CallbackSink`, `FileSink` (CSV + JSON)
- `SinkRunner` buffering and batch-size enforcement
- `SinkConfig` defaults and custom values
- `ZerobusSink` construction, credential resolution via `~/.databrickscfg`, connect, write, flush, close, and factory registration (all with mocked SDK)
- `Simulator` lifecycle, custom sensors, and multi-sink fan-out
- YAML config parsing (minimal, full, custom sensors)
- Sink factory creation, unknown type errors, and custom registration

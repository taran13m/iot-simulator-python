# IoT Data Simulator -- Examples

Runnable examples demonstrating every sink type, payload configuration knob,
and throughput control option available in the `iot-data-simulator` library.

## Directory Structure

```
examples/
├── README.md
├── sinks/                          Per-sink examples (one file per sink type)
│   ├── console_sink_example.py         4 cases
│   ├── callback_sink_example.py        5 cases
│   ├── file_sink_example.py            4 cases
│   ├── kafka_sink_example.py           4 cases
│   ├── database_sink_example.py        4 cases
│   ├── webhook_sink_example.py         4 cases
│   ├── delta_sink_example.py           4 cases
│   ├── azure_iot_sink_example.py       3 cases
│   ├── aws_iot_sink_example.py         3 cases
│   └── zerobus_sink_example.py         4 cases
├── scenarios/                      Composite / cross-cutting examples
│   ├── multi_sink_fanout_example.py    Console + File + DB fan-out
│   ├── load_test_example.py            Backpressure stress test (3 cases)
│   ├── yaml_config_example.py          Config-driven via YAML
│   ├── csv_sensors_example.py          Sensor defs from CSV
│   └── spark_datasource_example.py     PySpark DataSource (5 cases)
└── configs/                        Sample configuration files
    ├── simulator_config.yaml           YAML config for yaml_config_example
    └── custom_sensors.csv              CSV sensors for csv_sensors_example
```

## Quick Start

```bash
# Install the library (core only -- Console, Callback, File sinks)
pip install -e .

# Run the simplest example
python examples/sinks/console_sink_example.py

# Run a specific case within an example
python examples/sinks/console_sink_example.py --case 2
```

## Examples Overview

### sinks/ -- Per-Sink Examples

| File | Sink | Cases | Runnable | Extra Install |
|---|---|---|---|---|
| `console_sink_example.py` | ConsoleSink | 4 | Yes | -- |
| `callback_sink_example.py` | CallbackSink | 5 | Yes | -- |
| `file_sink_example.py` | FileSink | 4 | Yes (Parquet needs `[file]`) | `pip install iot-data-simulator[file]` |
| `kafka_sink_example.py` | KafkaSink | 4 | Needs Kafka broker | `pip install iot-data-simulator[kafka]` |
| `database_sink_example.py` | DatabaseSink | 4 | Yes (SQLite) | `pip install iot-data-simulator[database]` |
| `webhook_sink_example.py` | WebhookSink | 4 | Needs HTTP endpoint | `pip install iot-data-simulator[webhook]` |
| `delta_sink_example.py` | DeltaSink | 4 | Yes (local FS) | `pip install iot-data-simulator[delta]` |
| `azure_iot_sink_example.py` | AzureIoTSink | 3 | Needs Azure IoT Hub | `pip install iot-data-simulator[cloud]` |
| `aws_iot_sink_example.py` | AWSIoTSink | 3 | Needs AWS IoT Core | `pip install iot-data-simulator[cloud]` |
| `zerobus_sink_example.py` | ZerobusSink | 4 | Needs Databricks | `pip install iot-data-simulator[zerobus]` |

### scenarios/ -- Composite Examples

| File | Description | Runnable | Extra Install |
|---|---|---|---|
| `multi_sink_fanout_example.py` | Console + File + Database on one Simulator | Yes | `pip install iot-data-simulator[database]` |
| `load_test_example.py` | All 16 industries, backpressure stress test | Yes | -- |
| `yaml_config_example.py` | Config-driven approach via YAML | Yes | -- |
| `csv_sensors_example.py` | Load sensor definitions from CSV | Yes | -- |
| `spark_datasource_example.py` | PySpark custom DataSource: batch, streaming, Real-Time Mode (5 cases) | Needs Spark runtime | PySpark pre-installed in runtime; Case 5 requires Databricks 16.4 LTS+ |

### configs/ -- Sample Data Files

| File | Used By |
|---|---|
| `simulator_config.yaml` | `scenarios/yaml_config_example.py` |
| `custom_sensors.csv` | `scenarios/csv_sensors_example.py` |

## Install All Extras

```bash
pip install -e ".[all]"
```

## Running Examples

Every per-sink example supports a `--case N` argument to select a specific
scenario. Omitting `--case` runs case 1 (the simplest).

```bash
# Console sink -- JSON format with metadata
python examples/sinks/console_sink_example.py --case 2

# File sink -- all three formats side-by-side
python examples/sinks/file_sink_example.py --case 1

# Database sink -- query data after run
python examples/sinks/database_sink_example.py --case 4

# Load test -- backpressure stress test
python examples/scenarios/load_test_example.py --case 2

# YAML-driven configuration
python examples/scenarios/yaml_config_example.py

# Spark DataSource -- batch read (run via spark-submit or Databricks notebook)
spark-submit examples/scenarios/spark_datasource_example.py --case 1

# Spark DataSource -- streaming micro-batch
spark-submit examples/scenarios/spark_datasource_example.py --case 4
```

## Payload Configuration Knobs

These examples collectively demonstrate every knob discussed in the library:

| Knob | Where | What it Controls |
|---|---|---|
| `update_rate_hz` | `Simulator(...)` | Records generated per second |
| `rate_hz` | `Sink(...)` / `add_sink(...)` | Writes per second to a sink |
| `batch_size` | `Sink(...)` / `add_sink(...)` | Records per write call |
| `industries` | `Simulator(...)` | Number of sensors (payload breadth) |
| `custom_sensors` | `Simulator(...)` | Fine-grained sensor control |
| `metadata` | Per-record dict | Extra payload data / padding |
| `max_buffer_size` | `Sink(...)` | Memory cap for buffered records |
| `backpressure` | `Sink(...)` | `drop_oldest` / `drop_newest` / `block` |
| `retry_count` | `Sink(...)` | Retries on failed writes |
| `retry_delay_s` | `Sink(...)` | Delay between retries |

### PySpark DataSource Options

When using the IoT simulator as a PySpark custom data source, the following
`.option()` keys are available (all values are strings):

| Option | Description | Default |
|---|---|---|
| `industries` | Comma-separated industry names | `"mining"` |
| `numRows` | Generator ticks for batch reads | `"10"` |
| `rowsPerBatch` | Ticks per streaming micro-batch | `"2"` |
| `noiseStd` | Gaussian noise std-dev override | per-sensor |
| `driftRate` | Slow drift per update override | per-sensor |
| `anomalyProbability` | Anomaly probability override (0.0–1.0) | per-sensor |
| `anomalyMagnitude` | Anomaly multiplier override | per-sensor |
| `cyclic` | Force cyclic patterns (`"true"` / `"false"`) | per-sensor |
| `cyclePeriodSeconds` | Cyclic period override (seconds) | per-sensor |
| `cycleAmplitude` | Cyclic amplitude override (fraction of nominal) | per-sensor |
| `metadataPaddingBytes` | Inject N bytes of padding per record | `"0"` |

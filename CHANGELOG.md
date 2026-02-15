# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-02-15

### Added

- **379 built-in sensors** across 16 industrial sectors (mining, aerospace, pharmaceutical, automotive, and more) with realistic noise, drift, cyclic patterns, and anomaly injection.
- **Custom sensor definitions** via Python API, YAML config, or CSV import.
- **10 sink types**: Console, Callback, Kafka, File (CSV/JSON/Parquet), Database (PostgreSQL/SQLite), Webhook (HTTP POST), Delta Lake, Azure IoT Hub, AWS IoT Core, Databricks Zerobus Ingest.
- **Per-sink throughput control** with independent flush rate, batch size, buffer limits, and backpressure policy.
- **Two usage modes**: programmatic Python API and config-driven YAML.
- **CLI with subcommands**: `run`, `list-industries`, `list-sinks`, `list-sensors`, `init-config`.
- **Inline CLI sinks**: `--sink console`, `--sink file` directly from the command line.
- **PySpark DataSource V2** integration for Spark-native streaming.
- **Extensible sink architecture**: subclass `Sink` to write your own, register it for YAML use.

[Unreleased]: https://github.com/taran13m/iot-simulator-python/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/taran13m/iot-simulator-python/releases/tag/v0.1.0

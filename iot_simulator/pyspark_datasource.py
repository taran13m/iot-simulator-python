"""PySpark custom data source for the IoT Data Simulator.

Exposes the :class:`DataGenerator` as a PySpark DataSource, supporting both
batch reads and Spark Structured Streaming (including Databricks Real-Time
Mode).

**Requires PySpark 3.5+ (Databricks Runtime 15.4 LTS+).**  PySpark is *not*
declared as a pip dependency — it is expected to be present in the Spark
runtime (Databricks notebooks, ``spark-submit``, etc.).

Batch usage::

    spark.dataSource.register(IoTSimulatorDataSource)
    spark.read.format("iot_simulator").option("industries", "mining,utilities").load().show()

Streaming (micro-batch) usage::

    spark.dataSource.register(IoTSimulatorDataSource)
    spark.readStream.format("iot_simulator") \\
        .option("industries", "mining") \\
        .option("rowsPerBatch", "2") \\
        .load() \\
        .writeStream.format("console") \\
        .trigger(processingTime="5 seconds") \\
        .start()

Streaming (Databricks Real-Time Mode) usage::

    spark.dataSource.register(IoTSimulatorDataSource)
    spark.readStream.format("iot_simulator").load() \\
        .writeStream.format("noop") \\
        .outputMode("update") \\
        .trigger(realTime="5 minutes") \\
        .start()

References:
    - https://docs.databricks.com/aws/en/pyspark/datasources
    - https://docs.databricks.com/aws/en/structured-streaming/real-time
"""

from __future__ import annotations

import json
from typing import Any, Iterator, Tuple

try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        DataSourceStreamReader,
        InputPartition,
    )
    from pyspark.sql.types import StructType
except ImportError:
    raise ImportError(
        "pyspark is required to use IoTSimulatorDataSource. "
        "Run this module inside a Spark environment "
        "(Databricks, spark-submit, etc.)."
    )

__all__ = [
    "IoTSimulatorDataSource",
    "IoTSimulatorBatchReader",
    "IoTSimulatorStreamReader",
]

# -----------------------------------------------------------------------
# Schema (matches SensorRecord in iot_simulator/models.py)
# -----------------------------------------------------------------------

_SCHEMA = (
    "sensor_name string, "
    "industry string, "
    "value double, "
    "unit string, "
    "sensor_type string, "
    "timestamp double, "
    "min_value double, "
    "max_value double, "
    "nominal_value double, "
    "fault_active boolean, "
    "metadata string"
)

# -----------------------------------------------------------------------
# Helpers – option parsing & DataGenerator construction
# -----------------------------------------------------------------------

# Sensor-behavior option names mapped to their SensorConfig field names.
_SENSOR_OVERRIDE_KEYS: dict[str, str] = {
    "noiseStd": "noise_std",
    "driftRate": "drift_rate",
    "anomalyProbability": "anomaly_probability",
    "anomalyMagnitude": "anomaly_magnitude",
    "cyclic": "cyclic",
    "cyclePeriodSeconds": "cycle_period_seconds",
    "cycleAmplitude": "cycle_amplitude",
}


def _parse_industries(options: dict[str, str]) -> list[str]:
    """Return a list of industry name strings from the ``industries`` option."""
    raw = options.get("industries", "mining")
    return [s.strip() for s in raw.split(",") if s.strip()]


def _parse_sensor_overrides(options: dict[str, str]) -> dict[str, Any]:
    """Extract sensor-behavior overrides from the options dict.

    Returns a dict keyed by *SensorConfig field name* (snake_case) with
    correctly typed values — ready to pass to ``SensorConfig.model_copy``.
    """
    overrides: dict[str, Any] = {}
    for opt_key, cfg_field in _SENSOR_OVERRIDE_KEYS.items():
        if opt_key in options:
            raw = options[opt_key]
            if cfg_field == "cyclic":
                overrides[cfg_field] = raw.strip().lower() in ("true", "1", "yes")
            else:
                overrides[cfg_field] = float(raw)
    return overrides


def _build_generator(options: dict[str, str]):
    """Construct a ``DataGenerator`` from data-source options.

    This function is intentionally called inside ``read()`` — *not* in a
    reader's ``__init__`` — because PySpark serializes reader instances to
    workers and ``DataGenerator`` contains non-serializable state.
    """
    # Import here so the module-level import of pyspark doesn't pull in
    # the full iot_simulator tree (which may fail on the driver if the
    # package is only installed on workers via spark.jars.packages, etc.).
    from iot_simulator.sensor_models import (
        INDUSTRY_SENSORS,
        IndustryType,
        SensorConfig,
        SensorSimulator,
    )

    industries = _parse_industries(options)
    overrides = _parse_sensor_overrides(options)
    padding_bytes = int(options.get("metadataPaddingBytes", "0"))

    # Build simulators, optionally applying global overrides.
    simulators: dict[str, tuple[str, SensorSimulator]] = {}
    for name in industries:
        try:
            industry_enum = IndustryType(name)
        except ValueError:
            continue
        configs = INDUSTRY_SENSORS.get(industry_enum, [])
        for cfg in configs:
            if overrides:
                cfg = cfg.model_copy(update=overrides)
            sim = SensorSimulator(cfg)
            key = f"{name}/{cfg.name}"
            simulators[key] = (name, sim)

    return simulators, padding_bytes


def _tick_to_rows(
    simulators: dict[str, tuple[str, Any]],
    padding_bytes: int,
) -> list[tuple]:
    """Run one tick across all simulators and return a list of Row tuples."""
    import time as _time
    from iot_simulator.sensor_models import SensorType

    now = _time.time()
    rows: list[tuple] = []
    padding = "x" * padding_bytes if padding_bytes > 0 else ""

    for _key, (industry, sim) in simulators.items():
        value = sim.update()
        cfg = sim.config
        metadata: dict[str, Any] = {}
        if padding:
            metadata["padding"] = padding

        rows.append((
            cfg.name,                                                          # sensor_name
            industry,                                                          # industry
            float(value),                                                      # value
            cfg.unit,                                                          # unit
            cfg.sensor_type.value if isinstance(cfg.sensor_type, SensorType)
            else str(cfg.sensor_type),                                         # sensor_type
            float(now),                                                        # timestamp
            float(cfg.min_value),                                              # min_value
            float(cfg.max_value),                                              # max_value
            float(cfg.nominal_value),                                          # nominal_value
            bool(sim.fault_active),                                            # fault_active
            json.dumps(metadata),                                              # metadata (JSON)
        ))
    return rows


# -----------------------------------------------------------------------
# DataSource
# -----------------------------------------------------------------------

class IoTSimulatorDataSource(DataSource):
    """PySpark custom data source backed by the IoT Data Simulator.

    Register once, then use via ``spark.read`` / ``spark.readStream``::

        spark.dataSource.register(IoTSimulatorDataSource)
        df = spark.read.format("iot_simulator").load()

    Supported options (all strings):

    +-----------------------+---------------------------------------------+------------+
    | Option                | Description                                 | Default    |
    +=======================+=============================================+============+
    | industries            | Comma-separated industry names              | ``mining`` |
    +-----------------------+---------------------------------------------+------------+
    | numRows               | Generator ticks for batch reads             | ``10``     |
    +-----------------------+---------------------------------------------+------------+
    | rowsPerBatch          | Ticks per streaming micro-batch             | ``2``      |
    +-----------------------+---------------------------------------------+------------+
    | noiseStd              | Gaussian noise std-dev override             | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | driftRate             | Slow drift per update override              | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | anomalyProbability    | Anomaly probability override (0.0–1.0)     | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | anomalyMagnitude      | Anomaly multiplier override                 | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | cyclic                | Force cyclic patterns (true/false)          | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | cyclePeriodSeconds    | Cyclic period override (seconds)            | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | cycleAmplitude        | Cyclic amplitude override (fraction)        | per-sensor |
    +-----------------------+---------------------------------------------+------------+
    | metadataPaddingBytes  | Inject N bytes of padding per record        | ``0``      |
    +-----------------------+---------------------------------------------+------------+
    """

    @classmethod
    def name(cls) -> str:
        return "iot_simulator"

    def schema(self) -> str:
        return _SCHEMA

    def reader(self, schema: StructType):
        return IoTSimulatorBatchReader(schema, self.options)

    def streamReader(self, schema: StructType):
        return IoTSimulatorStreamReader(schema, self.options)


# -----------------------------------------------------------------------
# Batch reader
# -----------------------------------------------------------------------

class IoTSimulatorBatchReader(DataSourceReader):
    """Reads a fixed number of ticks from the IoT simulator (batch mode).

    The number of ticks is controlled by the ``numRows`` option.  Each tick
    produces one record per active sensor, so the total row count is
    ``numRows × number_of_sensors``.
    """

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = options
        self.num_ticks = int(options.get("numRows", "10"))

    def read(self, partition) -> Iterator[Tuple]:
        """Yield rows for the batch read.

        ``DataGenerator`` is constructed here (not in ``__init__``) because
        PySpark serializes reader instances to workers and the generator
        contains non-serializable state.
        """
        simulators, padding_bytes = _build_generator(self.options)

        for _ in range(self.num_ticks):
            for row in _tick_to_rows(simulators, padding_bytes):
                yield row


# -----------------------------------------------------------------------
# Streaming reader
# -----------------------------------------------------------------------

class _RangePartition(InputPartition):
    """Describes a contiguous range of ticks for a single partition."""

    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end


class IoTSimulatorStreamReader(DataSourceStreamReader):
    """Streaming reader that produces IoT sensor data in micro-batches.

    Each micro-batch advances an internal offset by ``rowsPerBatch`` ticks.
    The reader is trigger-agnostic — it works with ``processingTime``,
    ``availableNow``, and Databricks ``realTime`` triggers.
    """

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = options
        self.rows_per_batch = int(options.get("rowsPerBatch", "2"))
        self.current = 0

    def initialOffset(self) -> dict:
        """Return the initial start offset of the reader."""
        return {"offset": 0}

    def latestOffset(self) -> dict:
        """Return the latest offset for the next micro-batch."""
        self.current += self.rows_per_batch
        return {"offset": self.current}

    def partitions(self, start: dict, end: dict) -> list[InputPartition]:
        """Plan partitions for the current micro-batch."""
        return [_RangePartition(start["offset"], end["offset"])]

    def commit(self, end: dict) -> None:
        """Called when Spark has finished processing data up to *end*."""
        pass

    def read(self, partition) -> Iterator[Tuple]:
        """Generate rows for a single partition (range of ticks).

        ``DataGenerator`` is constructed here (not in ``__init__``) because
        PySpark serializes reader instances to workers and the generator
        contains non-serializable state.
        """
        simulators, padding_bytes = _build_generator(self.options)

        num_ticks = partition.end - partition.start
        for _ in range(num_ticks):
            for row in _tick_to_rows(simulators, padding_bytes):
                yield row

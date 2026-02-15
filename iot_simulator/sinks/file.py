"""File sink - writes sensor records to CSV, JSON, or Parquet files
with optional time-based rotation.

Parquet support requires the ``file`` extra::

    pip install iot-data-simulator[file]
"""

from __future__ import annotations

import csv
import io
import logging
import time
from pathlib import Path
from typing import Any, ClassVar

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["FileSink"]

logger = logging.getLogger("iot_simulator.sinks.file")


def _parse_rotation(rotation: str | None) -> float | None:
    """Convert a human-readable rotation interval to seconds.

    Accepted formats: ``"30s"``, ``"5m"``, ``"1h"``, ``"1d"``.
    Returns ``None`` if rotation is disabled.
    """
    if not rotation:
        return None
    rotation = rotation.strip().lower()
    if rotation.endswith("s"):
        return float(rotation[:-1])
    if rotation.endswith("m"):
        return float(rotation[:-1]) * 60
    if rotation.endswith("h"):
        return float(rotation[:-1]) * 3600
    if rotation.endswith("d"):
        return float(rotation[:-1]) * 86400
    return float(rotation)  # assume seconds


class FileSink(Sink):
    """Write sensor records to local files (CSV, JSON, or Parquet).

    Parameters:
        path: Output directory (created automatically).
        format: ``"csv"``, ``"json"``, or ``"parquet"``.
        rotation: Rotate to a new file periodically - e.g. ``"1h"``,
                  ``"30m"``, ``"60s"``.  ``None`` means single file.
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        path: str = "./output",
        format: str = "csv",
        rotation: str | None = None,
        rate_hz: float | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> None:
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._dir = Path(path)
        self._format = format.lower()
        self._rotation_s = _parse_rotation(rotation)
        self._current_file: io.TextIOWrapper[Any] | io.BufferedWriter | None = None
        self._csv_writer: csv.DictWriter[str] | None = None
        self._file_start_time: float = 0.0
        self._record_buffer: list[dict[str, Any]] = []  # for parquet batching

    async def connect(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        self._open_new_file()
        logger.info("FileSink writing %s to %s", self._format, self._dir)

    async def write(self, records: list[SensorRecord]) -> None:
        # Check rotation
        if self._rotation_s and (time.time() - self._file_start_time >= self._rotation_s):
            self._close_current_file()
            self._open_new_file()

        if self._format == "csv":
            self._write_csv(records)
        elif self._format == "json":
            self._write_json(records)
        elif self._format == "parquet":
            self._write_parquet(records)
        else:
            raise ValueError(f"Unknown file format: {self._format}")

    async def flush(self) -> None:
        if self._format == "parquet" and self._record_buffer:
            self._flush_parquet()
        if self._current_file and not self._current_file.closed:
            self._current_file.flush()

    async def close(self) -> None:
        await self.flush()
        self._close_current_file()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _timestamp_suffix(self) -> str:
        return time.strftime("%Y%m%d_%H%M%S", time.localtime())

    def _open_new_file(self) -> None:
        ext = {"csv": "csv", "json": "jsonl", "parquet": "parquet"}[self._format]
        name = f"sensor_data_{self._timestamp_suffix()}.{ext}"
        filepath = self._dir / name

        if self._format == "parquet":
            # Parquet is binary - we buffer dicts and flush via pyarrow
            self._parquet_path = filepath
        elif self._format == "json":
            self._current_file = open(filepath, "w", encoding="utf-8")  # noqa: SIM115
        else:
            self._current_file = open(filepath, "w", newline="", encoding="utf-8")  # noqa: SIM115
            self._csv_writer = None  # will init on first write

        self._file_start_time = time.time()
        logger.debug("Opened file: %s", filepath)

    def _close_current_file(self) -> None:
        if self._format == "parquet" and self._record_buffer:
            self._flush_parquet()
        if self._current_file and not self._current_file.closed:
            self._current_file.close()
        self._current_file = None
        self._csv_writer = None

    # --- CSV ---

    _CSV_FIELDS: ClassVar[list[str]] = [
        "timestamp",
        "industry",
        "sensor_name",
        "sensor_type",
        "value",
        "unit",
        "min_value",
        "max_value",
        "nominal_value",
        "fault_active",
    ]

    def _write_csv(self, records: list[SensorRecord]) -> None:
        if self._current_file is None or isinstance(self._current_file, io.BufferedWriter):
            return
        if self._csv_writer is None:
            self._csv_writer = csv.DictWriter(
                self._current_file,
                fieldnames=self._CSV_FIELDS,
                extrasaction="ignore",
            )
            self._csv_writer.writeheader()
        for rec in records:
            self._csv_writer.writerow(rec.to_dict())
        self._current_file.flush()

    # --- JSON Lines ---

    def _write_json(self, records: list[SensorRecord]) -> None:
        if self._current_file is None or isinstance(self._current_file, io.BufferedWriter):
            return
        for rec in records:
            self._current_file.write(rec.to_json() + "\n")
        self._current_file.flush()

    # --- Parquet ---

    def _write_parquet(self, records: list[SensorRecord]) -> None:
        for rec in records:
            self._record_buffer.append(rec.to_dict())

    def _flush_parquet(self) -> None:
        if not self._record_buffer:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as err:
            raise ImportError(
                "pyarrow is required for Parquet output.  Install with: pip install iot-data-simulator[file]"
            ) from err

        table = pa.Table.from_pylist(self._record_buffer)

        filepath = getattr(self, "_parquet_path", self._dir / "sensor_data.parquet")
        if filepath.exists():
            existing = pq.read_table(str(filepath))
            table = pa.concat_tables([existing, table])

        pq.write_table(table, str(filepath))
        logger.debug("Flushed %d records to %s", len(self._record_buffer), filepath)
        self._record_buffer.clear()

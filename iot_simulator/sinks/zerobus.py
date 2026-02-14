"""Zerobus Ingest sink - streams sensor records to Databricks via Zerobus.

Requires the ``zerobus`` extra::

    pip install iot-data-simulator[zerobus]

Uses the Databricks Zerobus Ingest Python SDK in JSON mode so that
sensor records (already JSON-serialisable via ``SensorRecord.to_dict()``)
can be ingested without generating a Protocol Buffers schema.

Credentials are resolved using Databricks unified authentication:
  1. Explicit constructor args (``client_id``, ``client_secret``)
  2. Environment variables (``DATABRICKS_CLIENT_ID``, ``DATABRICKS_CLIENT_SECRET``)
  3. ``~/.databrickscfg`` profile (``DEFAULT`` or named via ``databricks_profile``)

See: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["ZerobusSink"]

logger = logging.getLogger("iot_simulator.sinks.zerobus")

try:
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    ZEROBUS_AVAILABLE = True
except ImportError:
    ZEROBUS_AVAILABLE = False
    ZerobusSdk = None  # type: ignore[assignment,misc]
    RecordType = None  # type: ignore[assignment,misc]
    StreamConfigurationOptions = None  # type: ignore[assignment,misc]
    TableProperties = None  # type: ignore[assignment,misc]


def _resolve_credentials(
    *,
    workspace_url: str | None,
    client_id: str | None,
    client_secret: str | None,
    databricks_profile: str | None,
) -> tuple[str, str, str]:
    """Resolve workspace URL, client ID and secret using Databricks unified auth.

    Tries, in order:
      1. Values passed explicitly (non-None args).
      2. Environment variables (``DATABRICKS_HOST``, ``DATABRICKS_CLIENT_ID``, …).
      3. ``~/.databrickscfg`` profile.

    Returns:
        ``(workspace_url, client_id, client_secret)``

    Raises:
        ValueError: If any required credential cannot be resolved.
    """
    try:
        from databricks.sdk.config import Config
    except ImportError as err:
        raise ImportError(
            "databricks-sdk is required for credential resolution.  "
            "Install with: pip install iot-data-simulator[zerobus]"
        ) from err

    cfg_kwargs: dict[str, Any] = {}
    if workspace_url:
        cfg_kwargs["host"] = workspace_url
    if client_id:
        cfg_kwargs["client_id"] = client_id
    if client_secret:
        cfg_kwargs["client_secret"] = client_secret
    if databricks_profile:
        cfg_kwargs["profile"] = databricks_profile

    cfg = Config(**cfg_kwargs)

    resolved_host = workspace_url or cfg.host
    resolved_id = client_id or cfg.client_id
    resolved_secret = client_secret or cfg.client_secret

    missing: list[str] = []
    if not resolved_host:
        missing.append("workspace_url (or DATABRICKS_HOST / host in ~/.databrickscfg)")
    if not resolved_id:
        missing.append("client_id (or DATABRICKS_CLIENT_ID / client_id in ~/.databrickscfg)")
    if not resolved_secret:
        missing.append("client_secret (or DATABRICKS_CLIENT_SECRET / client_secret in ~/.databrickscfg)")

    if missing:
        profile_hint = f" [profile={databricks_profile}]" if databricks_profile else ""
        raise ValueError(
            f"Could not resolve Zerobus credentials{profile_hint}.  "
            f"Missing: {', '.join(missing)}.  "
            "Provide them explicitly, via environment variables, or in ~/.databrickscfg."
        )

    return resolved_host, resolved_id, resolved_secret  # type: ignore[return-value]


class ZerobusSink(Sink):
    """Stream sensor records to a Databricks Unity Catalog table via Zerobus Ingest.

    Credentials are resolved through Databricks unified authentication --
    you do **not** need to put secrets in config files.  The resolution
    order is: explicit args -> env vars -> ``~/.databrickscfg`` profile.

    Parameters:
        server_endpoint:
            Zerobus server endpoint, e.g.
            ``"1234567890123456.zerobus.us-west-2.cloud.databricks.com"``.
        table_name:
            Fully-qualified Unity Catalog table, e.g.
            ``"catalog.schema.sensor_data"``.
        workspace_url:
            Databricks workspace URL.  Optional -- resolved from
            ``DATABRICKS_HOST`` or ``~/.databrickscfg`` if omitted.
        client_id:
            Service principal application (client) ID.  Optional --
            resolved from ``DATABRICKS_CLIENT_ID`` or ``~/.databrickscfg``.
        client_secret:
            Service principal secret.  Optional --
            resolved from ``DATABRICKS_CLIENT_SECRET`` or ``~/.databrickscfg``.
        databricks_profile:
            Named profile in ``~/.databrickscfg`` to read credentials from
            (e.g. ``"DEFAULT"``, ``"staging"``).  When omitted the SDK
            default lookup order applies.
        record_type:
            ``"json"`` (default) or ``"proto"``.  JSON mode requires no
            schema generation and is recommended for most use cases.
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        server_endpoint: str,
        table_name: str,
        workspace_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        databricks_profile: str | None = None,
        record_type: str = "json",
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs: Any,
    ) -> None:
        if not ZEROBUS_AVAILABLE:
            raise ImportError(
                "databricks-zerobus-ingest-sdk is required for ZerobusSink.  "
                "Install with: pip install iot-data-simulator[zerobus]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._server_endpoint = server_endpoint
        self._table_name = table_name
        self._workspace_url = workspace_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._databricks_profile = databricks_profile

        # Map user-facing string to SDK enum
        _type_map = {"json": RecordType.JSON, "proto": RecordType.PROTO}
        rt = record_type.lower().strip()
        if rt not in _type_map:
            raise ValueError(f"Invalid record_type '{record_type}'. Must be 'json' or 'proto'.")
        self._record_type: RecordType = _type_map[rt]

        self._sdk: ZerobusSdk | None = None
        self._stream: Any = None  # ZerobusStream (typed as Any for guarded import)

    # ------------------------------------------------------------------
    # Sink interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Resolve credentials and open a Zerobus ingest stream."""
        loop = asyncio.get_running_loop()

        def _create_stream() -> Any:
            # Resolve credentials via Databricks unified auth
            resolved_host, resolved_id, resolved_secret = _resolve_credentials(
                workspace_url=self._workspace_url,
                client_id=self._client_id,
                client_secret=self._client_secret,
                databricks_profile=self._databricks_profile,
            )

            sdk = ZerobusSdk(self._server_endpoint, resolved_host)
            table_props = TableProperties(self._table_name)
            options = StreamConfigurationOptions(record_type=self._record_type)
            stream = sdk.create_stream(
                resolved_id,
                resolved_secret,
                table_props,
                options,
            )
            return sdk, stream

        self._sdk, self._stream = await loop.run_in_executor(None, _create_stream)
        logger.info(
            "ZerobusSink connected - endpoint=%s table=%s",
            self._server_endpoint,
            self._table_name,
        )

    async def write(self, records: list[SensorRecord]) -> None:
        """Ingest a batch of sensor records and wait for acknowledgments."""
        if self._stream is None:
            raise RuntimeError("ZerobusSink is not connected")

        loop = asyncio.get_running_loop()

        def _ingest_batch() -> None:
            acks = []
            for rec in records:
                record_dict = rec.to_dict()
                ack = self._stream.ingest_record(record_dict)
                acks.append(ack)
            # Wait for all acknowledgments to ensure durability
            for ack in acks:
                ack.wait_for_ack()

        await loop.run_in_executor(None, _ingest_batch)
        logger.debug(
            "Ingested %d records to Zerobus table %s",
            len(records),
            self._table_name,
        )

    async def flush(self) -> None:
        """No-op - each write() waits for acknowledgments."""

    async def close(self) -> None:
        """Close the Zerobus ingest stream."""
        if self._stream is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._stream.close)
            self._stream = None
        self._sdk = None
        logger.info("ZerobusSink closed (table=%s)", self._table_name)

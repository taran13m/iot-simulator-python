"""Webhook sink - POSTs JSON batches of sensor records to an HTTP endpoint.

Requires the ``webhook`` extra::

    pip install iot-data-simulator[webhook]
"""

from __future__ import annotations

import json
import logging

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["WebhookSink"]

logger = logging.getLogger("iot_simulator.sinks.webhook")

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class WebhookSink(Sink):
    """POST sensor records as JSON to an HTTP endpoint.

    Each ``write()`` sends a single JSON array of record dicts.

    Parameters:
        url: Target endpoint (must accept ``POST``).
        headers: Extra HTTP headers (e.g. ``{"Authorization": "Bearer …"}``).
        timeout_s: Per-request timeout in seconds.
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        timeout_s: float = 30.0,
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs,
    ) -> None:
        if not HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for WebhookSink.  Install with: pip install iot-data-simulator[webhook]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._url = url
        self._headers = {"Content-Type": "application/json", **(headers or {})}
        self._timeout = timeout_s
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            headers=self._headers,
        )
        logger.info("WebhookSink ready - target: %s", self._url)

    async def write(self, records: list[SensorRecord]) -> None:
        if self._client is None:
            raise RuntimeError("WebhookSink is not connected")

        payload = json.dumps([rec.to_dict() for rec in records])
        resp = await self._client.post(self._url, content=payload)
        resp.raise_for_status()

        logger.debug("POST %s - %d records - HTTP %d", self._url, len(records), resp.status_code)

    async def flush(self) -> None:
        """No-op - writes are already synchronous POSTs."""

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("WebhookSink closed")

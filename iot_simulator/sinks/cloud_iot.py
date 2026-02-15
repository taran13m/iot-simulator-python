"""Cloud IoT sinks - Azure IoT Hub and AWS IoT Core.

Requires the ``cloud`` extra::

    pip install iot-data-simulator[cloud]
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["AWSIoTSink", "AzureIoTSink"]

logger = logging.getLogger("iot_simulator.sinks.cloud_iot")

# ---------------------------------------------------------------------------
# Azure IoT Hub
# ---------------------------------------------------------------------------

try:
    from azure.iot.device import Message as AzureMessage
    from azure.iot.device.aio import IoTHubDeviceClient

    AZURE_IOT_AVAILABLE = True
except ImportError:
    AZURE_IOT_AVAILABLE = False


class AzureIoTSink(Sink):
    """Send sensor records as device-to-cloud messages to Azure IoT Hub.

    Parameters:
        connection_string:
            Device connection string from Azure Portal, e.g.
            ``"HostName=hub.azure-devices.net;DeviceId=sim;SharedAccessKey=…"``.
        content_type: MIME type for messages (default JSON).
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        connection_string: str,
        content_type: str = "application/json",
        rate_hz: float | None = None,
        batch_size: int = 50,
        **kwargs: Any,
    ) -> None:
        if not AZURE_IOT_AVAILABLE:
            raise ImportError(
                "azure-iot-device is required for AzureIoTSink.  Install with: pip install iot-data-simulator[cloud]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._conn_str = connection_string
        self._content_type = content_type
        self._client: IoTHubDeviceClient | None = None

    async def connect(self) -> None:
        self._client = IoTHubDeviceClient.create_from_connection_string(self._conn_str)
        await self._client.connect()
        logger.info("AzureIoTSink connected to IoT Hub")

    async def write(self, records: list[SensorRecord]) -> None:
        if self._client is None:
            raise RuntimeError("AzureIoTSink is not connected")

        for rec in records:
            msg = AzureMessage(rec.to_json())
            msg.content_type = self._content_type
            msg.content_encoding = "utf-8"
            msg.custom_properties["industry"] = rec.industry
            msg.custom_properties["sensor_type"] = rec.sensor_type
            await self._client.send_message(msg)

    async def flush(self) -> None:
        """No-op - messages are sent immediately."""

    async def close(self) -> None:
        if self._client:
            await self._client.shutdown()
            self._client = None
            logger.info("AzureIoTSink disconnected")


# ---------------------------------------------------------------------------
# AWS IoT Core
# ---------------------------------------------------------------------------

try:
    import boto3

    AWS_IOT_AVAILABLE = True
except ImportError:
    AWS_IOT_AVAILABLE = False


class AWSIoTSink(Sink):
    """Publish sensor records to AWS IoT Core via the MQTT data plane
    (``iot-data`` / ``publish`` API).

    Parameters:
        endpoint: AWS IoT Core endpoint
                  (e.g. ``"abc123-ats.iot.us-east-1.amazonaws.com"``).
        topic: MQTT topic to publish to.
        region: AWS region.
        aws_access_key_id / aws_secret_access_key / aws_session_token:
            Explicit credentials (optional - falls back to the default
            credential chain).
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        topic: str = "iot/sensor-data",
        region: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        rate_hz: float | None = None,
        batch_size: int = 50,
        **kwargs: Any,
    ) -> None:
        if not AWS_IOT_AVAILABLE:
            raise ImportError("boto3 is required for AWSIoTSink.  Install with: pip install iot-data-simulator[cloud]")
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._endpoint = endpoint
        self._topic = topic

        session_kwargs: dict[str, Any] = {"region_name": region}
        if aws_access_key_id:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            session_kwargs["aws_session_token"] = aws_session_token

        self._session_kwargs = session_kwargs
        self._client = None

    async def connect(self) -> None:
        session = boto3.Session(**self._session_kwargs)
        self._client = session.client(
            "iot-data",
            endpoint_url=f"https://{self._endpoint}",
        )
        logger.info("AWSIoTSink connected - endpoint=%s topic=%s", self._endpoint, self._topic)

    async def write(self, records: list[SensorRecord]) -> None:
        if self._client is None:
            raise RuntimeError("AWSIoTSink is not connected")

        loop = asyncio.get_running_loop()
        for rec in records:
            payload = rec.to_json().encode()
            await loop.run_in_executor(
                None,
                lambda p=payload: self._client.publish(
                    topic=self._topic,
                    qos=1,
                    payload=p,
                ),
            )

    async def flush(self) -> None:
        """No-op."""

    async def close(self) -> None:
        self._client = None
        logger.info("AWSIoTSink closed")

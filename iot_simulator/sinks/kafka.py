"""Kafka sink - publishes sensor records to Apache Kafka.

Requires the ``kafka`` extra::

    pip install iot-data-simulator[kafka]
"""

from __future__ import annotations

import logging
from typing import Any

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["KafkaSink"]

logger = logging.getLogger("iot_simulator.sinks.kafka")

try:
    from aiokafka import AIOKafkaProducer

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaSink(Sink):
    """Publish sensor records as JSON messages to a Kafka topic.

    Parameters:
        bootstrap_servers: Comma-separated broker addresses.
        topic: Kafka topic name.
        compression: ``"snappy"``, ``"gzip"``, ``"lz4"``, ``"zstd"``, or ``None``.
        key_field: Record field used as the Kafka message key
                   (``"sensor_name"``, ``"industry"``, or ``None``).
        acks: ``0``, ``1``, or ``"all"`` (``-1``).
        security_protocol: ``"PLAINTEXT"``, ``"SSL"``, ``"SASL_PLAINTEXT"``,
                           ``"SASL_SSL"``.
        sasl_mechanism: ``"PLAIN"``, ``"SCRAM-SHA-256"``, etc.
        sasl_username / sasl_password: SASL credentials.
        extra_producer_config: Additional kwargs forwarded to
                               ``AIOKafkaProducer``.
        rate_hz / batch_size / **kwargs: Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "iot-sensor-data",
        compression: str | None = "snappy",
        key_field: str | None = "sensor_name",
        acks: str | int = "all",
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        extra_producer_config: dict[str, Any] | None = None,
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs: Any,
    ) -> None:
        if not KAFKA_AVAILABLE:
            raise ImportError(
                "aiokafka is required for KafkaSink.  Install with: pip install iot-data-simulator[kafka]"
            )
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._compression = compression
        self._key_field = key_field

        self._producer_config: dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "compression_type": compression,
            "acks": "all" if acks == -1 else str(acks),
        }
        if security_protocol != "PLAINTEXT":
            self._producer_config["security_protocol"] = security_protocol
        if sasl_mechanism:
            self._producer_config["sasl_mechanism"] = sasl_mechanism
        if sasl_username:
            self._producer_config["sasl_plain_username"] = sasl_username
        if sasl_password:
            self._producer_config["sasl_plain_password"] = sasl_password
        if extra_producer_config:
            self._producer_config.update(extra_producer_config)

        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        logger.info("Connecting to Kafka at %s ...", self._bootstrap_servers)
        self._producer = AIOKafkaProducer(**self._producer_config)
        await self._producer.start()
        logger.info("Connected to Kafka - publishing to topic '%s'", self._topic)

    async def write(self, records: list[SensorRecord]) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaSink is not connected")

        for rec in records:
            key = None
            if self._key_field:
                key = getattr(rec, self._key_field, "").encode()
            value = rec.to_json().encode()
            await self._producer.send(self._topic, value=value, key=key)

    async def flush(self) -> None:
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer closed")

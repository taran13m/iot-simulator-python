"""Sink factory – creates sink instances from configuration dicts.

Used by the config-driven (YAML) mode to instantiate sinks declaratively::

    sinks:
      - type: console
        rate_hz: 0.5
      - type: kafka
        bootstrap_servers: localhost:9092
        topic: iot-data
        batch_size: 50
"""

from __future__ import annotations

import logging
from typing import Any

from iot_simulator.sinks.base import Sink

__all__ = ["create_sink", "register_sink"]

logger = logging.getLogger("iot_simulator.sinks.factory")

# Throughput keys that should be extracted before passing to the sink constructor
_THROUGHPUT_KEYS = {
    "rate_hz", "batch_size", "max_buffer_size",
    "backpressure", "retry_count", "retry_delay_s",
}

# Registry of type names → (module_path, class_name)
_SINK_REGISTRY: dict[str, tuple[str, str]] = {
    "console": ("iot_simulator.sinks.console", "ConsoleSink"),
    "callback": ("iot_simulator.sinks.callback", "CallbackSink"),
    "kafka": ("iot_simulator.sinks.kafka", "KafkaSink"),
    "file": ("iot_simulator.sinks.file", "FileSink"),
    "database": ("iot_simulator.sinks.database", "DatabaseSink"),
    "webhook": ("iot_simulator.sinks.webhook", "WebhookSink"),
    "delta": ("iot_simulator.sinks.delta", "DeltaSink"),
    "azure_iot": ("iot_simulator.sinks.cloud_iot", "AzureIoTSink"),
    "aws_iot": ("iot_simulator.sinks.cloud_iot", "AWSIoTSink"),
    "zerobus": ("iot_simulator.sinks.zerobus", "ZerobusSink"),
}


def create_sink(config: dict[str, Any]) -> Sink:
    """Create a sink instance from a configuration dict.

    The dict must contain a ``"type"`` key matching a registered sink
    name.  All other keys are forwarded as keyword arguments to the
    sink constructor.

    Example::

        sink = create_sink({
            "type": "kafka",
            "bootstrap_servers": "localhost:9092",
            "topic": "iot-data",
            "rate_hz": 2.0,
            "batch_size": 50,
        })

    Returns:
        A fully-constructed :class:`Sink` instance (not yet connected).
    """
    config = dict(config)  # shallow copy
    sink_type = config.pop("type", None)

    if sink_type is None:
        raise ValueError("Sink config must include a 'type' key")

    sink_type = sink_type.lower().strip()

    if sink_type not in _SINK_REGISTRY:
        raise ValueError(
            f"Unknown sink type '{sink_type}'.  "
            f"Available: {sorted(_SINK_REGISTRY)}"
        )

    module_path, class_name = _SINK_REGISTRY[sink_type]

    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    logger.debug("Creating %s with config: %s", class_name, config)
    return cls(**config)


def register_sink(name: str, module_path: str, class_name: str) -> None:
    """Register a custom sink type for config-driven instantiation.

    Example::

        from iot_simulator.sinks.factory import register_sink
        register_sink("my_sink", "mypackage.sinks", "MySink")

    Then in YAML::

        sinks:
          - type: my_sink
            custom_param: value
    """
    _SINK_REGISTRY[name.lower().strip()] = (module_path, class_name)

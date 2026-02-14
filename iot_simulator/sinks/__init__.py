"""Pluggable sinks for the IoT Data Simulator.

Import any sink you need directly from this package::

    from iot_simulator.sinks import ConsoleSink, KafkaSink, FileSink
"""

from __future__ import annotations

import importlib
from typing import Any

from iot_simulator.sinks.base import Sink, SinkConfig, SinkRunner
from iot_simulator.sinks.callback import CallbackSink
from iot_simulator.sinks.console import ConsoleSink

# Lazy-loaded sinks (require optional extras)
# Import them directly when needed:
#   from iot_simulator.sinks.kafka import KafkaSink
#   from iot_simulator.sinks.file import FileSink
#   from iot_simulator.sinks.database import DatabaseSink
#   from iot_simulator.sinks.webhook import WebhookSink
#   from iot_simulator.sinks.delta import DeltaSink
#   from iot_simulator.sinks.cloud_iot import AzureIoTSink, AWSIoTSink
#   from iot_simulator.sinks.zerobus import ZerobusSink

__all__ = [
    "CallbackSink",
    "ConsoleSink",
    "Sink",
    "SinkConfig",
    "SinkRunner",
]


def __getattr__(name: str) -> Any:
    """Lazy-import sinks that require optional dependencies."""
    _lazy = {
        "KafkaSink": "iot_simulator.sinks.kafka",
        "FileSink": "iot_simulator.sinks.file",
        "DatabaseSink": "iot_simulator.sinks.database",
        "WebhookSink": "iot_simulator.sinks.webhook",
        "DeltaSink": "iot_simulator.sinks.delta",
        "AzureIoTSink": "iot_simulator.sinks.cloud_iot",
        "AWSIoTSink": "iot_simulator.sinks.cloud_iot",
        "ZerobusSink": "iot_simulator.sinks.zerobus",
    }
    if name in _lazy:
        mod = importlib.import_module(_lazy[name])
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

"""Tests for iot_simulator.sinks.factory - create_sink and register_sink."""

from __future__ import annotations

import pytest

from iot_simulator.sinks.base import Sink
from iot_simulator.sinks.console import ConsoleSink
from iot_simulator.sinks.factory import _SINK_REGISTRY, create_sink, register_sink

# -----------------------------------------------------------------------
# create_sink
# -----------------------------------------------------------------------


class TestCreateSink:
    """create_sink() creates typed sink instances from config dicts."""

    def test_create_console_sink(self) -> None:
        sink = create_sink({"type": "console", "fmt": "text", "rate_hz": 1.0})
        assert isinstance(sink, ConsoleSink)

    def test_create_console_json(self) -> None:
        sink = create_sink({"type": "console", "fmt": "json", "rate_hz": 0.5})
        assert isinstance(sink, Sink)

    def test_missing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="type"):
            create_sink({"rate_hz": 1.0})

    def test_unknown_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown sink type"):
            create_sink({"type": "nonexistent_sink_xyz"})

    def test_case_insensitive_type(self) -> None:
        sink = create_sink({"type": "Console", "rate_hz": 1.0})
        assert isinstance(sink, ConsoleSink)


# -----------------------------------------------------------------------
# register_sink
# -----------------------------------------------------------------------


class TestRegisterSink:
    """register_sink() extends the factory registry."""

    def test_register_and_lookup(self) -> None:
        register_sink("test_sink_abc", "iot_simulator.sinks.console", "ConsoleSink")
        assert "test_sink_abc" in _SINK_REGISTRY

        sink = create_sink({"type": "test_sink_abc", "rate_hz": 1.0})
        assert isinstance(sink, ConsoleSink)

        # Cleanup
        del _SINK_REGISTRY["test_sink_abc"]

    def test_register_normalises_name(self) -> None:
        register_sink("  My_Sink  ", "iot_simulator.sinks.console", "ConsoleSink")
        assert "my_sink" in _SINK_REGISTRY
        del _SINK_REGISTRY["my_sink"]

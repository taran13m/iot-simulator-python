"""Tests for iot_simulator.sinks.__init__ - lazy loading of optional sinks."""

from __future__ import annotations

import pytest

import iot_simulator.sinks as sinks_pkg


class TestSinksPackage:
    """Tests for sinks package __all__ and lazy imports."""

    def test_direct_exports(self) -> None:
        """Built-in sinks should be directly importable."""
        assert hasattr(sinks_pkg, "Sink")
        assert hasattr(sinks_pkg, "SinkConfig")
        assert hasattr(sinks_pkg, "SinkRunner")
        assert hasattr(sinks_pkg, "ConsoleSink")
        assert hasattr(sinks_pkg, "CallbackSink")

    def test_all_contains_expected_names(self) -> None:
        assert "Sink" in sinks_pkg.__all__
        assert "SinkConfig" in sinks_pkg.__all__
        assert "SinkRunner" in sinks_pkg.__all__
        assert "ConsoleSink" in sinks_pkg.__all__
        assert "CallbackSink" in sinks_pkg.__all__

    def test_lazy_import_file_sink(self) -> None:
        """FileSink should be lazily importable."""
        cls = sinks_pkg.FileSink
        from iot_simulator.sinks.file import FileSink

        assert cls is FileSink

    def test_lazy_import_unknown_raises(self) -> None:
        """Accessing a non-existent attribute should raise AttributeError."""
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = sinks_pkg.NonExistentSink  # type: ignore[attr-defined]

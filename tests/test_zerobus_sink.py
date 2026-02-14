"""Tests for ZerobusSink - mocked databricks-zerobus-ingest-sdk dependency."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from iot_simulator.models import SensorRecord

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------


def _make_records(n: int = 3) -> list[SensorRecord]:
    return [
        SensorRecord(
            sensor_name=f"sensor_{i}",
            industry="test",
            value=float(i * 10),
            unit="kW",
            sensor_type="power",
            timestamp=1_700_000_000.0 + i,
            min_value=0.0,
            max_value=100.0,
            nominal_value=50.0,
        )
        for i in range(n)
    ]


# -----------------------------------------------------------------------
# Mock setup
# -----------------------------------------------------------------------


def _make_mock_zerobus():
    """Create mock zerobus modules."""
    # RecordType enum mock
    mock_record_type = MagicMock()
    mock_record_type.JSON = "JSON"
    mock_record_type.PROTO = "PROTO"

    mock_table_props = MagicMock()
    mock_stream_config = MagicMock()

    # Stream mock
    mock_stream = MagicMock()
    mock_ack = MagicMock()
    mock_ack.wait_for_ack = MagicMock()
    mock_stream.ingest_record = MagicMock(return_value=mock_ack)
    mock_stream.close = MagicMock()

    # SDK mock
    mock_sdk_inst = MagicMock()
    mock_sdk_inst.create_stream = MagicMock(return_value=mock_stream)
    mock_sdk_cls = MagicMock(return_value=mock_sdk_inst)

    # Module hierarchy
    mock_zerobus = ModuleType("zerobus")
    mock_zerobus_sdk = ModuleType("zerobus.sdk")
    mock_zerobus_sdk_shared = ModuleType("zerobus.sdk.shared")
    mock_zerobus_sdk_shared.RecordType = mock_record_type
    mock_zerobus_sdk_shared.StreamConfigurationOptions = mock_stream_config
    mock_zerobus_sdk_shared.TableProperties = mock_table_props
    mock_zerobus_sdk_sync = ModuleType("zerobus.sdk.sync")
    mock_zerobus_sdk_sync.ZerobusSdk = mock_sdk_cls

    modules = {
        "zerobus": mock_zerobus,
        "zerobus.sdk": mock_zerobus_sdk,
        "zerobus.sdk.shared": mock_zerobus_sdk_shared,
        "zerobus.sdk.sync": mock_zerobus_sdk_sync,
    }

    return modules, mock_sdk_cls, mock_sdk_inst, mock_stream


def _make_mock_db_config(host="https://ws.cloud.databricks.com", cid="test-cid", secret="test-secret"):
    """Create a mock databricks Config class."""
    mock_config_cls = MagicMock()
    mock_config_inst = MagicMock()
    mock_config_inst.host = host
    mock_config_inst.client_id = cid
    mock_config_inst.client_secret = secret
    mock_config_cls.return_value = mock_config_inst
    return mock_config_cls, mock_config_inst


def _import_zerobus(mock_modules):
    """Force re-import of zerobus module with mocks."""
    with patch.dict(sys.modules, mock_modules):
        if "iot_simulator.sinks.zerobus" in sys.modules:
            del sys.modules["iot_simulator.sinks.zerobus"]
        import iot_simulator.sinks.zerobus as mod

        return mod


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestZerobusSink:
    """ZerobusSink with mocked zerobus SDK."""

    @pytest.mark.asyncio
    async def test_connect_resolves_credentials(self) -> None:
        mock_modules, mock_sdk_cls, _, _ = _make_mock_zerobus()
        _mock_config_cls, _ = _make_mock_db_config()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint.zerobus.cloud.databricks.com",
            table_name="catalog.schema.table",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="csecret",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "csecret")):
            await sink.connect()

        mock_sdk_cls.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_ingests_records(self) -> None:
        mock_modules, _, _, mock_stream = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "cs")):
            await sink.connect()

        await sink.write(_make_records(3))
        assert mock_stream.ingest_record.call_count == 3

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
        )
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "cs")):
            await sink.connect()

        await sink.flush()

    @pytest.mark.asyncio
    async def test_close_closes_stream(self) -> None:
        mock_modules, _, _, mock_stream = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "cs")):
            await sink.connect()

        await sink.close()
        mock_stream.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_without_connect(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
        )
        await sink.close()  # Should not raise

    def test_invalid_record_type(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        with pytest.raises(ValueError, match="Invalid record_type"):
            mod.ZerobusSink(
                server_endpoint="endpoint",
                table_name="cat.sch.tbl",
                workspace_url="https://ws",
                client_id="cid",
                client_secret="cs",
                record_type="invalid",
            )

    @pytest.mark.asyncio
    async def test_proto_record_type(self) -> None:
        mock_modules, _mock_sdk_cls, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            workspace_url="https://ws",
            client_id="cid",
            client_secret="cs",
            record_type="proto",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "cs")):
            await sink.connect()

    @pytest.mark.asyncio
    async def test_credential_resolution_via_profile(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        _mock_config_cls, _ = _make_mock_db_config()
        mod = _import_zerobus(mock_modules)

        sink = mod.ZerobusSink(
            server_endpoint="endpoint",
            table_name="cat.sch.tbl",
            databricks_profile="staging",
        )

        with patch.object(mod, "_resolve_credentials", return_value=("https://ws", "cid", "cs")) as mock_resolve:
            await sink.connect()
            call_kwargs = mock_resolve.call_args[1]
            assert call_kwargs["databricks_profile"] == "staging"


class TestResolveCredentials:
    """Tests for _resolve_credentials helper."""

    def test_explicit_credentials(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mock_config_cls, _ = _make_mock_db_config()
        mod = _import_zerobus(mock_modules)

        with (
            patch("iot_simulator.sinks.zerobus.importlib.import_module")
            if False
            else patch.dict(sys.modules, {"databricks.sdk.config": MagicMock(Config=mock_config_cls)})
        ):
            # Patch the Config import inside _resolve_credentials
            pass

        # Simpler approach: patch at function level
        with patch.dict(
            sys.modules,
            {
                "databricks": MagicMock(),
                "databricks.sdk": MagicMock(),
                "databricks.sdk.config": MagicMock(Config=mock_config_cls),
            },
        ):
            host, cid, secret = mod._resolve_credentials(
                workspace_url="https://explicit",
                client_id="explicit-id",
                client_secret="explicit-secret",
                databricks_profile=None,
            )
        assert host == "https://explicit"
        assert cid == "explicit-id"
        assert secret == "explicit-secret"

    def test_missing_credentials_raises(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mock_config_cls, _mock_config_inst = _make_mock_db_config(
            host=None,
            cid=None,
            secret=None,
        )
        mod = _import_zerobus(mock_modules)

        with (
            patch.dict(
                sys.modules,
                {
                    "databricks": MagicMock(),
                    "databricks.sdk": MagicMock(),
                    "databricks.sdk.config": MagicMock(Config=mock_config_cls),
                },
            ),
            pytest.raises(ValueError, match="Could not resolve"),
        ):
            mod._resolve_credentials(
                workspace_url=None,
                client_id=None,
                client_secret=None,
                databricks_profile=None,
            )

    def test_missing_credentials_with_profile_hint(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mock_config_cls, _ = _make_mock_db_config(host=None, cid=None, secret=None)
        mod = _import_zerobus(mock_modules)

        with (
            patch.dict(
                sys.modules,
                {
                    "databricks": MagicMock(),
                    "databricks.sdk": MagicMock(),
                    "databricks.sdk.config": MagicMock(Config=mock_config_cls),
                },
            ),
            pytest.raises(ValueError, match="profile=staging"),
        ):
            mod._resolve_credentials(
                workspace_url=None,
                client_id=None,
                client_secret=None,
                databricks_profile="staging",
            )

    def test_databricks_sdk_not_installed(self) -> None:
        mock_modules, _, _, _ = _make_mock_zerobus()
        mod = _import_zerobus(mock_modules)

        # Make importing databricks.sdk.config fail
        def _failing_import(name, *args, **kwargs):
            if "databricks" in name:
                raise ImportError("no databricks")
            return original_import(name, *args, **kwargs)

        import builtins

        original_import = builtins.__import__

        with (
            patch.dict(
                sys.modules,
                {
                    "databricks": None,
                    "databricks.sdk": None,
                    "databricks.sdk.config": None,
                },
            ),
            pytest.raises(ImportError, match="databricks-sdk"),
        ):
            mod._resolve_credentials(
                workspace_url=None,
                client_id=None,
                client_secret=None,
                databricks_profile=None,
            )

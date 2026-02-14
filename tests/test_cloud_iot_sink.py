"""Tests for AzureIoTSink and AWSIoTSink - mocked cloud dependencies."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

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
# Azure IoT mock setup
# -----------------------------------------------------------------------


def _make_mock_azure():
    """Create mock azure.iot.device modules."""
    mock_message_cls = MagicMock()
    mock_message_inst = MagicMock()
    mock_message_inst.custom_properties = {}
    mock_message_cls.return_value = mock_message_inst

    mock_client_cls = MagicMock()
    mock_client_inst = AsyncMock()
    mock_client_inst.connect = AsyncMock()
    mock_client_inst.send_message = AsyncMock()
    mock_client_inst.shutdown = AsyncMock()
    mock_client_cls.create_from_connection_string = MagicMock(return_value=mock_client_inst)

    # Module hierarchy
    mock_azure = ModuleType("azure")
    mock_azure_iot = ModuleType("azure.iot")
    mock_azure_iot_device = ModuleType("azure.iot.device")
    mock_azure_iot_device.Message = mock_message_cls
    mock_azure_iot_device_aio = ModuleType("azure.iot.device.aio")
    mock_azure_iot_device_aio.IoTHubDeviceClient = mock_client_cls

    return (
        {
            "azure": mock_azure,
            "azure.iot": mock_azure_iot,
            "azure.iot.device": mock_azure_iot_device,
            "azure.iot.device.aio": mock_azure_iot_device_aio,
        },
        mock_client_cls,
        mock_client_inst,
        mock_message_cls,
    )


# -----------------------------------------------------------------------
# AWS IoT mock setup
# -----------------------------------------------------------------------


def _make_mock_boto3():
    """Create mock boto3 module."""
    mock_iot_client = MagicMock()
    mock_iot_client.publish = MagicMock()

    mock_session = MagicMock()
    mock_session.client = MagicMock(return_value=mock_iot_client)

    mock_boto3 = ModuleType("boto3")
    mock_boto3.Session = MagicMock(return_value=mock_session)

    return mock_boto3, mock_session, mock_iot_client


# -----------------------------------------------------------------------
# Azure IoT Tests
# -----------------------------------------------------------------------


class TestAzureIoTSink:
    """AzureIoTSink with mocked azure-iot-device."""

    def _import_azure_sink(self, mock_modules):
        with patch.dict(sys.modules, mock_modules):
            if "iot_simulator.sinks.cloud_iot" in sys.modules:
                del sys.modules["iot_simulator.sinks.cloud_iot"]
            from iot_simulator.sinks.cloud_iot import AzureIoTSink

            return AzureIoTSink

    @pytest.mark.asyncio
    async def test_connect(self) -> None:
        mock_modules, _mock_cls, mock_inst, _ = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub.azure-devices.net;DeviceId=sim;SharedAccessKey=key")
        await sink.connect()
        mock_inst.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_sends_messages(self) -> None:
        mock_modules, _, mock_inst, _mock_msg = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub;DeviceId=d;SharedAccessKey=k")
        await sink.connect()
        await sink.write(_make_records(2))

        assert mock_inst.send_message.await_count == 2

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_modules, _, _, _ = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub;DeviceId=d;SharedAccessKey=k")
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_modules, _, _, _ = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub;DeviceId=d;SharedAccessKey=k")
        await sink.connect()
        await sink.flush()

    @pytest.mark.asyncio
    async def test_close_shuts_down_client(self) -> None:
        mock_modules, _, mock_inst, _ = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub;DeviceId=d;SharedAccessKey=k")
        await sink.connect()
        await sink.close()
        mock_inst.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_without_connect(self) -> None:
        mock_modules, _, _, _ = _make_mock_azure()
        AzureIoTSink = self._import_azure_sink(mock_modules)

        sink = AzureIoTSink(connection_string="HostName=hub;DeviceId=d;SharedAccessKey=k")
        await sink.close()  # Should not raise


# -----------------------------------------------------------------------
# AWS IoT Tests
# -----------------------------------------------------------------------


class TestAWSIoTSink:
    """AWSIoTSink with mocked boto3."""

    def _import_aws_sink(self, mock_boto3):
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            if "iot_simulator.sinks.cloud_iot" in sys.modules:
                del sys.modules["iot_simulator.sinks.cloud_iot"]
            from iot_simulator.sinks.cloud_iot import AWSIoTSink

            return AWSIoTSink

    @pytest.mark.asyncio
    async def test_connect(self) -> None:
        mock_boto3, mock_session, _ = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(endpoint="abc.iot.us-east-1.amazonaws.com", topic="test/topic")
        await sink.connect()
        mock_session.client.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_publishes_records(self) -> None:
        mock_boto3, _, mock_iot = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(endpoint="abc.iot.us-east-1.amazonaws.com")
        await sink.connect()
        await sink.write(_make_records(2))

        assert mock_iot.publish.call_count == 2

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_boto3, _, _ = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(endpoint="abc.iot.us-east-1.amazonaws.com")
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_explicit_credentials(self) -> None:
        mock_boto3, _, _ = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(
            endpoint="abc.iot.us-east-1.amazonaws.com",
            region="eu-west-1",
            aws_access_key_id="AKID",
            aws_secret_access_key="SECRET",
            aws_session_token="TOKEN",
        )
        await sink.connect()
        call_kwargs = mock_boto3.Session.call_args[1]
        assert call_kwargs["region_name"] == "eu-west-1"
        assert call_kwargs["aws_access_key_id"] == "AKID"
        assert call_kwargs["aws_secret_access_key"] == "SECRET"
        assert call_kwargs["aws_session_token"] == "TOKEN"

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_boto3, _, _ = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(endpoint="abc.iot.us-east-1.amazonaws.com")
        await sink.connect()
        await sink.flush()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        mock_boto3, _, _ = _make_mock_boto3()
        AWSIoTSink = self._import_aws_sink(mock_boto3)

        sink = AWSIoTSink(endpoint="abc.iot.us-east-1.amazonaws.com")
        await sink.connect()
        await sink.close()

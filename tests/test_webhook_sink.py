"""Tests for WebhookSink - mocked httpx dependency."""

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
# Mock setup
# -----------------------------------------------------------------------


def _make_mock_httpx():
    """Create mock httpx module."""
    mock_httpx = ModuleType("httpx")

    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.aclose = AsyncMock()

    mock_async_client_cls = MagicMock(return_value=mock_client)
    mock_httpx.AsyncClient = mock_async_client_cls
    mock_httpx.Timeout = MagicMock()

    return mock_httpx, mock_client, mock_response


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------


class TestWebhookSink:
    """WebhookSink with mocked httpx."""

    def _import_webhook_sink(self, mock_module):
        with patch.dict(sys.modules, {"httpx": mock_module}):
            if "iot_simulator.sinks.webhook" in sys.modules:
                del sys.modules["iot_simulator.sinks.webhook"]
            from iot_simulator.sinks.webhook import WebhookSink

            return WebhookSink

    @pytest.mark.asyncio
    async def test_connect_creates_client(self) -> None:
        mock_httpx, _mock_client, _ = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(url="https://example.com/ingest")
        await sink.connect()
        mock_httpx.AsyncClient.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_posts_records(self) -> None:
        mock_httpx, mock_client, mock_resp = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(url="https://example.com/ingest")
        await sink.connect()
        await sink.write(_make_records(3))

        mock_client.post.assert_awaited_once()
        mock_resp.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_without_connect_raises(self) -> None:
        mock_httpx, _, _ = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(url="https://example.com/ingest")
        with pytest.raises(RuntimeError, match="not connected"):
            await sink.write(_make_records(1))

    @pytest.mark.asyncio
    async def test_custom_headers(self) -> None:
        mock_httpx, _mock_client, _ = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(
            url="https://example.com/ingest",
            headers={"Authorization": "Bearer token123"},
            timeout_s=10.0,
        )
        await sink.connect()
        call_kwargs = mock_httpx.AsyncClient.call_args[1]
        assert "Authorization" in call_kwargs["headers"]
        assert call_kwargs["headers"]["Authorization"] == "Bearer token123"

    @pytest.mark.asyncio
    async def test_flush_is_noop(self) -> None:
        mock_httpx, _, _ = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(url="https://example.com/ingest")
        await sink.connect()
        await sink.flush()

    @pytest.mark.asyncio
    async def test_close(self) -> None:
        mock_httpx, mock_client, _ = _make_mock_httpx()
        WebhookSink = self._import_webhook_sink(mock_httpx)

        sink = WebhookSink(url="https://example.com/ingest")
        await sink.connect()
        await sink.close()
        mock_client.aclose.assert_awaited_once()

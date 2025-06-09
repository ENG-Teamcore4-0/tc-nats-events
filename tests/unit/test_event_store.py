"""
Unit Tests for Event Store
==========================

Test the NATSEventStore class.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import nats
import pytest
from nats.js.errors import BadRequestError, NotFoundError

from tc_nats_events.core.event_store import NATSEventStore
from tc_nats_events.models.event import Event, EventMetadata
from tc_nats_events.utils.config import NATSConfig
from tc_nats_events.utils.exceptions import (
    ConnectionError,
    EventStoreError,
    PublishError,
    StreamConfigError,
)


class TestNATSEventStore:
    """Test NATSEventStore class."""

    @pytest.fixture
    def event_store(self, nats_config):
        """Create an event store instance."""
        return NATSEventStore(nats_config)

    @pytest.mark.asyncio
    async def test_connect_success(self, event_store, mock_nats_client, mock_jetstream):
        """Test successful connection to NATS."""
        with patch("nats.connect", return_value=mock_nats_client):
            mock_nats_client.jetstream.return_value = mock_jetstream

            await event_store.connect()

            assert event_store.is_connected
            assert event_store._nc == mock_nats_client
            assert event_store._js == mock_jetstream

    @pytest.mark.asyncio
    async def test_connect_already_connected(self, event_store, mock_nats_client):
        """Test connecting when already connected."""
        event_store._is_connected = True
        event_store._nc = mock_nats_client

        with patch("nats.connect") as mock_connect:
            await event_store.connect()

            # Should not attempt to connect again
            mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_failure(self, event_store):
        """Test connection failure."""
        with patch("nats.connect", side_effect=Exception("Connection failed")):
            with pytest.raises(ConnectionError, match="NATS connection failed"):
                await event_store.connect()

    @pytest.mark.asyncio
    async def test_ensure_stream_exists_creates_new(self, event_store, mock_jetstream):
        """Test creating a new stream when it doesn't exist."""
        event_store._js = mock_jetstream

        # Stream doesn't exist
        mock_jetstream.stream_info.side_effect = NotFoundError()
        mock_jetstream.add_stream = AsyncMock()

        await event_store._ensure_stream_exists()

        # Should create stream
        mock_jetstream.add_stream.assert_called_once()
        call_args = mock_jetstream.add_stream.call_args[0][0]

        assert call_args.name == event_store.stream_name
        assert call_args.subjects == [f"{event_store.subject_prefix}.*"]

    @pytest.mark.asyncio
    async def test_ensure_stream_exists_updates_subjects(
        self, event_store, mock_jetstream
    ):
        """Test updating stream when subjects have changed."""
        event_store._js = mock_jetstream

        # Stream exists with different subjects
        stream_info = MagicMock()
        stream_info.config.subjects = ["old.events.*"]
        mock_jetstream.stream_info.return_value = stream_info
        mock_jetstream.update_stream = AsyncMock()

        await event_store._ensure_stream_exists()

        # Should update stream
        mock_jetstream.update_stream.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_event_success(
        self, event_store, mock_jetstream, sample_event
    ):
        """Test successful event publishing."""
        event_store._js = mock_jetstream
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Mock publish response
        ack = MagicMock()
        ack.seq = 42
        mock_jetstream.publish = AsyncMock(return_value=ack)

        sequence = await event_store.publish_event(sample_event)

        assert sequence == 42
        mock_jetstream.publish.assert_called_once()

        # Check publish arguments
        call_args = mock_jetstream.publish.call_args
        assert (
            call_args[1]["subject"]
            == f"{event_store.subject_prefix}.{sample_event.event_type}"
        )
        assert call_args[1]["payload"] == sample_event.to_json()

    @pytest.mark.asyncio
    async def test_publish_event_not_connected(self, event_store, sample_event):
        """Test publishing when not connected."""
        event_store._is_connected = False

        with pytest.raises(ConnectionError, match="Not connected to NATS"):
            await event_store.publish_event(sample_event)

    @pytest.mark.asyncio
    async def test_publish_event_invalid_event(self, event_store):
        """Test publishing an invalid event."""
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Create invalid event
        invalid_event = Event(event_type="", data={})

        with pytest.raises(ValueError, match="Invalid event structure"):
            await event_store.publish_event(invalid_event)

    @pytest.mark.asyncio
    async def test_publish_event_timeout(
        self, event_store, mock_jetstream, sample_event
    ):
        """Test publish timeout."""
        event_store._js = mock_jetstream
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Mock timeout
        mock_jetstream.publish = AsyncMock(side_effect=nats.errors.TimeoutError())

        with pytest.raises(PublishError, match="Event publish timeout"):
            await event_store.publish_event(sample_event)

    @pytest.mark.asyncio
    async def test_publish_batch(self, event_store, mock_jetstream, sample_events):
        """Test batch publishing."""
        event_store._js = mock_jetstream
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Mock successful publishes
        ack = MagicMock()
        ack.seq = 42
        mock_jetstream.publish = AsyncMock(return_value=ack)

        sequences = await event_store.publish_batch(sample_events)

        assert len(sequences) == len(sample_events)
        assert all(seq == 42 for seq in sequences)
        assert mock_jetstream.publish.call_count == len(sample_events)

    @pytest.mark.asyncio
    async def test_get_stream_info(self, event_store, mock_jetstream):
        """Test getting stream information."""
        event_store._js = mock_jetstream
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Mock stream info
        stream_info = MagicMock()
        stream_info.config.name = "test-events"
        stream_info.config.subjects = ["test.events.*"]
        stream_info.state.messages = 100
        stream_info.state.bytes = 10000
        stream_info.state.first_seq = 1
        stream_info.state.last_seq = 100
        stream_info.state.consumer_count = 2
        stream_info.created = "2024-01-01T00:00:00Z"
        stream_info.cluster = MagicMock()
        stream_info.cluster.name = "test-cluster"
        stream_info.cluster.leader = "node-1"

        mock_jetstream.stream_info.return_value = stream_info

        info = await event_store.get_stream_info()

        assert info["name"] == "test-events"
        assert info["messages"] == 100
        assert info["bytes"] == 10000
        assert info["first_seq"] == 1
        assert info["last_seq"] == 100
        assert info["consumer_count"] == 2
        assert info["cluster"]["name"] == "test-cluster"
        assert info["cluster"]["leader"] == "node-1"

    @pytest.mark.asyncio
    async def test_create_consumer(self, event_store, mock_jetstream):
        """Test creating a consumer."""
        event_store._js = mock_jetstream
        mock_jetstream.add_consumer = AsyncMock()

        config = await event_store.create_consumer("test-consumer")

        assert config.name == "test-consumer"
        assert config.durable_name == "test-consumer"
        assert config.ack_policy == "explicit"
        mock_jetstream.add_consumer.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_consumer(self, event_store, mock_jetstream):
        """Test deleting a consumer."""
        event_store._js = mock_jetstream
        mock_jetstream.delete_consumer = AsyncMock()

        await event_store.delete_consumer("test-consumer")

        mock_jetstream.delete_consumer.assert_called_once_with(
            event_store.stream_name, "test-consumer"
        )

    @pytest.mark.asyncio
    async def test_disconnect(self, event_store, mock_nats_client):
        """Test disconnecting from NATS."""
        event_store._is_connected = True
        event_store._nc = mock_nats_client
        mock_nats_client.drain = AsyncMock()
        mock_nats_client.close = AsyncMock()

        await event_store.disconnect()

        assert not event_store._is_connected
        assert event_store._nc is None
        assert event_store._js is None
        mock_nats_client.drain.assert_called_once()
        mock_nats_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, event_store):
        """Test using event store as context manager."""
        with patch.object(
            event_store, "connect", new_callable=AsyncMock
        ) as mock_connect:
            with patch.object(
                event_store, "disconnect", new_callable=AsyncMock
            ) as mock_disconnect:
                async with event_store as store:
                    assert store == event_store
                    mock_connect.assert_called_once()

                mock_disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_transaction_context(self, event_store, mock_jetstream):
        """Test transaction context manager."""
        event_store._js = mock_jetstream
        event_store._is_connected = True
        event_store._nc = MagicMock(is_connected=True)

        # Mock successful publishes
        ack = MagicMock()
        ack.seq = 42
        mock_jetstream.publish = AsyncMock(return_value=ack)

        async with event_store.transaction() as events:
            events.append(Event(event_type="test.1", data={"id": "1"}))
            events.append(Event(event_type="test.2", data={"id": "2"}))

        # Should publish both events
        assert mock_jetstream.publish.call_count == 2

"""
Unit Tests for Durable Consumer
================================

Test the DurableEventConsumer class.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

import nats
import pytest

from tc_nats_events.consumers.durable_consumer import (
    ConsumerState,
    DurableEventConsumer,
)
from tc_nats_events.models.event import Event
from tc_nats_events.utils.config import NATSConfig
from tc_nats_events.utils.exceptions import ConnectionError, ConsumerError


class TestDurableEventConsumer:
    """Test DurableEventConsumer class."""

    @pytest.fixture
    def consumer(self, nats_config):
        """Create a consumer instance."""
        return DurableEventConsumer(
            service_name="test-service",
            config=nats_config,
            batch_size=5,
            fetch_timeout=1.0,
        )

    def test_consumer_initialization(self, consumer, nats_config):
        """Test consumer initialization."""
        assert consumer.service_name == "test-service"
        assert consumer.config == nats_config
        assert consumer.batch_size == 5
        assert consumer.fetch_timeout == 1.0
        assert consumer.consumer_name == "test-service-consumer"
        assert consumer.state == ConsumerState.IDLE
        assert not consumer.is_synced
        assert consumer.events_processed == 0
        assert consumer.events_failed == 0

    @pytest.mark.asyncio
    async def test_start_success(
        self, consumer, mock_nats_client, mock_jetstream, mock_subscription
    ):
        """Test successful consumer start."""
        with patch("nats.connect", return_value=mock_nats_client):
            mock_nats_client.jetstream.return_value = mock_jetstream
            mock_jetstream.consumer_info.side_effect = nats.js.errors.NotFoundError()
            mock_jetstream.add_consumer = AsyncMock()
            mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

            # Mock empty fetch to trigger sync completion
            mock_subscription.fetch = AsyncMock(return_value=[])

            await consumer.start()

            assert consumer._is_running
            assert consumer.state in [ConsumerState.SYNCING, ConsumerState.LIVE]
            assert consumer._processing_task is not None

            # Wait a bit for processing loop to start
            await asyncio.sleep(0.1)

            # Stop the consumer
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_start_already_running(self, consumer):
        """Test starting consumer that's already running."""
        consumer._is_running = True

        with patch("nats.connect") as mock_connect:
            await consumer.start()

            # Should not attempt to connect
            mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_failure(self, consumer):
        """Test handling connection failure."""
        with patch("nats.connect", side_effect=Exception("Connection failed")):
            with pytest.raises(ConsumerError, match="Consumer startup failed"):
                await consumer.start()

    @pytest.mark.asyncio
    async def test_setup_consumer_existing(
        self, consumer, mock_jetstream, mock_subscription
    ):
        """Test setting up consumer when it already exists."""
        consumer._js = mock_jetstream

        # Consumer exists
        consumer_info = MagicMock()
        consumer_info.delivered.stream_seq = 100
        consumer_info.num_ack_pending = 5
        mock_jetstream.consumer_info.return_value = consumer_info
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        await consumer._setup_consumer()

        assert consumer._subscription == mock_subscription
        mock_jetstream.add_consumer.assert_not_called()

    @pytest.mark.asyncio
    async def test_setup_consumer_new(
        self, consumer, mock_jetstream, mock_subscription
    ):
        """Test setting up new consumer."""
        consumer._js = mock_jetstream

        # Consumer doesn't exist
        mock_jetstream.consumer_info.side_effect = nats.js.errors.NotFoundError()
        mock_jetstream.add_consumer = AsyncMock()
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        await consumer._setup_consumer()

        assert consumer._subscription == mock_subscription
        mock_jetstream.add_consumer.assert_called_once()

        # Check consumer config
        call_args = mock_jetstream.add_consumer.call_args[0][1]
        assert call_args.name == consumer.consumer_name
        assert call_args.durable_name == consumer.consumer_name
        assert call_args.deliver_policy.value == "all"
        assert call_args.ack_policy.value == "explicit"

    @pytest.mark.asyncio
    async def test_event_processing_loop(
        self, consumer, mock_subscription, mock_nats_message
    ):
        """Test event processing loop."""
        consumer._subscription = mock_subscription
        consumer._is_running = True

        # Mock fetch to return messages then empty
        call_count = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [mock_nats_message]
            else:
                return []

        mock_subscription.fetch = mock_fetch

        # Register handler
        handler_called = False

        def test_handler(event: Event):
            nonlocal handler_called
            handler_called = True
            assert event.event_type == "test.created"

        consumer.register_handler("test.created", test_handler)

        # Run processing loop briefly
        task = asyncio.create_task(consumer._event_processing_loop())
        await asyncio.sleep(0.2)
        consumer._is_running = False
        await task

        assert handler_called
        assert consumer.events_processed == 1
        assert consumer.is_synced
        assert mock_nats_message.is_acked

    @pytest.mark.asyncio
    async def test_process_message_success(self, consumer, mock_nats_message):
        """Test successful message processing."""
        # Register handler
        handler_event = None

        def test_handler(event: Event):
            nonlocal handler_event
            handler_event = event

        consumer.register_handler("test.created", test_handler)

        await consumer._process_message(mock_nats_message)

        assert handler_event is not None
        assert handler_event.event_type == "test.created"
        assert handler_event.sequence == 1
        assert consumer.events_processed == 1
        assert consumer.last_processed_sequence == 1
        assert mock_nats_message.is_acked

    @pytest.mark.asyncio
    async def test_process_message_handler_error(self, consumer, mock_nats_message):
        """Test message processing with handler error."""

        # Register failing handler
        def failing_handler(event: Event):
            raise Exception("Handler error")

        consumer.register_handler("test.created", failing_handler)

        with pytest.raises(Exception, match="Handler error"):
            await consumer._process_message(mock_nats_message)

        assert consumer.events_failed == 1

    @pytest.mark.asyncio
    async def test_process_message_no_handler(self, consumer, mock_nats_message):
        """Test processing message with no handler."""
        # No handler registered
        await consumer._process_message(mock_nats_message)

        assert consumer.events_processed == 1
        assert mock_nats_message.is_acked

    @pytest.mark.asyncio
    async def test_process_event_async_handler(self, consumer):
        """Test processing event with async handler."""
        event = Event(event_type="test.async", data={"id": "123"})

        handler_called = False

        async def async_handler(evt: Event):
            nonlocal handler_called
            await asyncio.sleep(0.01)
            handler_called = True

        consumer.register_handler("test.async", async_handler)

        await consumer.process_event(event)

        assert handler_called

    @pytest.mark.asyncio
    async def test_default_handler(self, consumer):
        """Test default handler for unregistered events."""
        event = Event(event_type="unknown.event", data={"id": "123"})

        default_called = False

        def default_handler(evt: Event):
            nonlocal default_called
            default_called = True

        consumer.register_default_handler(default_handler)

        await consumer.process_event(event)

        assert default_called

    @pytest.mark.asyncio
    async def test_sync_completion(self, consumer):
        """Test initial sync completion."""
        assert not consumer.is_synced
        assert consumer._sync_start_time is None
        assert consumer._sync_complete_time is None

        consumer._sync_start_time = datetime.now(timezone.utc)
        consumer._consumer_state = ConsumerState.SYNCING

        await consumer._complete_sync()

        assert consumer.is_synced
        assert consumer.state == ConsumerState.LIVE
        assert consumer._sync_complete_time is not None

    def test_get_sync_status(self, consumer):
        """Test getting sync status."""
        consumer._consumer_state = ConsumerState.SYNCING
        consumer.events_processed = 100
        consumer.events_failed = 2
        consumer.last_processed_sequence = 100

        status = consumer.get_sync_status()

        assert status["service_name"] == "test-service"
        assert status["state"] == "syncing"
        assert status["is_synced"] is False
        assert status["events_processed"] == 100
        assert status["events_failed"] == 2
        assert status["last_sequence"] == 100

    @pytest.mark.asyncio
    async def test_stop(self, consumer, mock_nats_client):
        """Test stopping consumer."""
        consumer._is_running = True
        consumer._nc = mock_nats_client
        consumer._consumer_state = ConsumerState.LIVE

        # Create a mock task
        mock_task = AsyncMock()
        consumer._processing_task = mock_task

        mock_nats_client.drain = AsyncMock()
        mock_nats_client.close = AsyncMock()

        await consumer.stop()

        assert not consumer._is_running
        assert consumer.state == ConsumerState.STOPPED
        mock_task.cancel.assert_called_once()
        mock_nats_client.drain.assert_called_once()
        mock_nats_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, consumer):
        """Test using consumer as context manager."""
        with patch.object(consumer, "start", new_callable=AsyncMock) as mock_start:
            with patch.object(consumer, "stop", new_callable=AsyncMock) as mock_stop:
                async with consumer as c:
                    assert c == consumer
                    mock_start.assert_called_once()

                mock_stop.assert_called_once()

    def test_get_metrics(self, consumer):
        """Test getting consumer metrics."""
        consumer.events_processed = 50
        consumer.events_failed = 3
        consumer.last_processed_sequence = 50
        consumer.register_handler("test.created", lambda e: None)
        consumer.register_handler("test.updated", lambda e: None)

        metrics = consumer.get_metrics()

        assert metrics["service_name"] == "test-service"
        assert metrics["events_processed"] == 50
        assert metrics["events_failed"] == 3
        assert metrics["last_processed_sequence"] == 50
        assert metrics["state_size"] == 0
        assert set(metrics["registered_handlers"]) == {"test.created", "test.updated"}

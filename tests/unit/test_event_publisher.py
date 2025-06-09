"""
Unit Tests for Event Publisher
===============================

Test the EventPublisher class.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tc_nats_events.models.event import Event, EventMetadata, EventType
from tc_nats_events.publishers.event_publisher import EventPublisher
from tc_nats_events.utils.config import NATSConfig
from tc_nats_events.utils.exceptions import PublishError


class TestEventPublisher:
    """Test EventPublisher class."""

    @pytest.fixture
    def publisher(self, nats_config):
        """Create a publisher instance."""
        return EventPublisher(
            service_name="test-publisher", config=nats_config, environment="test"
        )

    @pytest.fixture
    def mock_event_store(self, mock_jetstream):
        """Create a mock event store."""
        store = AsyncMock()
        store.is_connected = True
        store.publish_event = AsyncMock(return_value=42)
        store.connect = AsyncMock()
        store.disconnect = AsyncMock()
        return store

    def test_publisher_initialization(self, publisher, nats_config):
        """Test publisher initialization."""
        assert publisher.service_name == "test-publisher"
        assert publisher.config == nats_config
        assert publisher.environment == "test"
        assert publisher.events_published == 0
        assert publisher.events_failed == 0
        assert publisher.max_retry_attempts == 3

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, publisher, mock_event_store):
        """Test connecting and disconnecting."""
        publisher._event_store = mock_event_store

        await publisher.connect()
        mock_event_store.connect.assert_called_once()

        await publisher.disconnect()
        mock_event_store.disconnect.assert_called_once()

    def test_set_context(self, publisher):
        """Test setting publishing context."""
        publisher.set_context(
            correlation_id="corr-123", causation_id="cause-456", user_id="user-789"
        )

        assert publisher._current_correlation_id == "corr-123"
        assert publisher._current_causation_id == "cause-456"
        assert publisher._current_user_id == "user-789"

    def test_clear_context(self, publisher):
        """Test clearing publishing context."""
        publisher.set_context(
            correlation_id="corr-123", causation_id="cause-456", user_id="user-789"
        )

        publisher.clear_context()

        assert publisher._current_correlation_id is None
        assert publisher._current_causation_id is None
        assert publisher._current_user_id is None

    @pytest.mark.asyncio
    async def test_publish_success(self, publisher, mock_event_store):
        """Test successful event publishing."""
        publisher._event_store = mock_event_store

        sequence = await publisher.publish(
            event_type="test.created", data={"id": "123", "name": "Test"}
        )

        assert sequence == 42
        assert publisher.events_published == 1
        assert publisher.last_publish_time is not None

        # Check event was created correctly
        call_args = mock_event_store.publish_event.call_args[0][0]
        assert isinstance(call_args, Event)
        assert call_args.event_type == "test.created"
        assert call_args.data == {"id": "123", "name": "Test"}
        assert call_args.metadata.source_service == "test-publisher"
        assert call_args.metadata.environment == "test"

    @pytest.mark.asyncio
    async def test_publish_with_metadata(self, publisher, mock_event_store):
        """Test publishing with custom metadata."""
        publisher._event_store = mock_event_store

        metadata = EventMetadata(event_id="custom-id", correlation_id="custom-corr")

        sequence = await publisher.publish(
            event_type="test.created", data={"id": "123"}, metadata=metadata
        )

        assert sequence == 42

        # Check metadata was enriched
        call_args = mock_event_store.publish_event.call_args[0][0]
        assert call_args.metadata.event_id == "custom-id"
        assert call_args.metadata.correlation_id == "custom-corr"
        assert call_args.metadata.source_service == "test-publisher"

    @pytest.mark.asyncio
    async def test_publish_with_context(self, publisher, mock_event_store):
        """Test publishing with context set."""
        publisher._event_store = mock_event_store

        publisher.set_context(
            correlation_id="ctx-corr", causation_id="ctx-cause", user_id="ctx-user"
        )

        await publisher.publish(event_type="test.created", data={"id": "123"})

        # Check context was applied
        call_args = mock_event_store.publish_event.call_args[0][0]
        assert call_args.metadata.correlation_id == "ctx-corr"
        assert call_args.metadata.causation_id == "ctx-cause"
        assert call_args.metadata.user_id == "ctx-user"

    @pytest.mark.asyncio
    async def test_publish_with_retry(self, publisher, mock_event_store):
        """Test publishing with retry on failure."""
        publisher._event_store = mock_event_store

        # Fail twice, then succeed
        call_count = 0

        async def publish_with_failures(event):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary failure")
            return 42

        mock_event_store.publish_event = publish_with_failures

        sequence = await publisher.publish(
            event_type="test.created", data={"id": "123"}
        )

        assert sequence == 42
        assert call_count == 3
        assert publisher.events_failed == 2  # Two failures
        assert publisher.events_published == 1

    @pytest.mark.asyncio
    async def test_publish_max_retries_exceeded(self, publisher, mock_event_store):
        """Test publishing failure after max retries."""
        publisher._event_store = mock_event_store
        publisher.max_retry_attempts = 2

        # Always fail
        mock_event_store.publish_event = AsyncMock(
            side_effect=Exception("Permanent failure")
        )

        with pytest.raises(
            PublishError, match="Failed to publish event after 2 attempts"
        ):
            await publisher.publish(event_type="test.created", data={"id": "123"})

        assert publisher.events_failed == 2

    @pytest.mark.asyncio
    async def test_publish_batch(self, publisher, mock_event_store):
        """Test batch publishing."""
        publisher._event_store = mock_event_store

        events = [
            ("test.created", {"id": "1"}),
            ("test.updated", {"id": "2"}),
            ("test.deleted", {"id": "3"}),
        ]

        sequences = await publisher.publish_batch(events)

        assert len(sequences) == 3
        assert all(seq == 42 for seq in sequences)
        assert publisher.events_published == 3
        assert mock_event_store.publish_event.call_count == 3

    @pytest.mark.asyncio
    async def test_publish_batch_with_failures(self, publisher, mock_event_store):
        """Test batch publishing with some failures."""
        publisher._event_store = mock_event_store

        # Make second event fail
        call_count = 0

        async def publish_selective_failure(event):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Failed")
            return 42

        mock_event_store.publish_event = publish_selective_failure

        events = [
            ("test.created", {"id": "1"}),
            ("test.updated", {"id": "2"}),
            ("test.deleted", {"id": "3"}),
        ]

        sequences = await publisher.publish_batch(events)

        assert sequences == [42, -1, 42]  # -1 for failed event
        assert publisher.events_published == 2
        assert publisher.events_failed > 0

    @pytest.mark.asyncio
    async def test_convenience_methods(self, publisher, mock_event_store):
        """Test convenience publishing methods."""
        publisher._event_store = mock_event_store

        # Test user created
        await publisher.publish_user_created(
            user_id="user-123",
            user_data={"email": "test@example.com", "name": "Test User"},
        )

        call_args = mock_event_store.publish_event.call_args[0][0]
        assert call_args.event_type == EventType.USER_CREATED
        assert call_args.data["user_id"] == "user-123"
        assert call_args.data["email"] == "test@example.com"

        # Test product updated
        await publisher.publish_product_updated(
            product_id="prod-456", updates={"price": 99.99, "stock": 100}
        )

        call_args = mock_event_store.publish_event.call_args[0][0]
        assert call_args.event_type == EventType.PRODUCT_UPDATED
        assert call_args.data["product_id"] == "prod-456"
        assert call_args.data["price"] == 99.99

        # Test scraper completed
        await publisher.publish_scraper_completed(
            scraper_id="scraper-789", items_extracted=150, duration_seconds=45.5
        )

        call_args = mock_event_store.publish_event.call_args[0][0]
        assert call_args.event_type == EventType.SCRAPER_COMPLETED
        assert call_args.data["scraper_id"] == "scraper-789"
        assert call_args.data["items_extracted"] == 150
        assert call_args.data["duration_seconds"] == 45.5

    def test_get_metrics(self, publisher):
        """Test getting publisher metrics."""
        publisher.events_published = 100
        publisher.events_failed = 5
        publisher.last_publish_time = datetime.now(timezone.utc)
        publisher._event_store = MagicMock()
        publisher._event_store.is_connected = True

        metrics = publisher.get_metrics()

        assert metrics["service_name"] == "test-publisher"
        assert metrics["events_published"] == 100
        assert metrics["events_failed"] == 5
        assert metrics["environment"] == "test"
        assert metrics["is_connected"] is True
        assert metrics["last_publish_time"] is not None

    @pytest.mark.asyncio
    async def test_context_manager(self, publisher, mock_event_store):
        """Test using publisher as context manager."""
        publisher._event_store = mock_event_store

        async with publisher as pub:
            assert pub == publisher
            mock_event_store.connect.assert_called_once()

        mock_event_store.disconnect.assert_called_once()

"""
Unit Tests for Durable Consumer
================================

Test the DurableEventConsumer class.
"""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import nats
import pytest

from tc_nats_events.adapters import FlexibleAdapter, GenericAdapter
from tc_nats_events.consumers.consumer_config import build_consumer_config
from tc_nats_events.consumers.durable_consumer import (
    ConsumerState,
    DurableEventConsumer,
)
from tc_nats_events.consumers.message_handler import MessageHandler
from tc_nats_events.idempotency import IdempotentProcessor, MemoryIdempotencyStore
from tc_nats_events.models.event import Event
from tc_nats_events.utils.exceptions import (
    ConsumerConfigMismatchError,
    ConsumerError,
    NonRetryableError,
)
from tests.conftest import MockNATSMessage


class TestDurableEventConsumer:
    """Test DurableEventConsumer class."""

    @pytest.fixture
    def config(self, nats_config):
        """Config without DLQ: DLQ behavior is tested separately with a mock."""
        return replace(nats_config, dlq_enabled=False)

    @staticmethod
    def _message_handler(consumer, dead_letters=None):
        """The MessageHandler that _setup_consumer would build for ``consumer``."""
        return MessageHandler(
            consumer_name=consumer.consumer_name,
            config=consumer.config,
            adapters=consumer.adapters,
            get_handler=consumer.get_handler,
            processor=consumer._processor,
            dead_letters=dead_letters,
            metrics=consumer._metrics,
            on_progress=consumer._mark_progress,
        )

    @pytest.fixture
    def consumer(self, config):
        """Create a consumer instance backed by an in-memory idempotency store."""
        consumer = DurableEventConsumer(
            service_name="test-service",
            config=config,
            batch_size=5,
            fetch_timeout=1.0,
            auto_adapt=False,  # Disable auto-adaptation for tests
            idempotency_store=MemoryIdempotencyStore(),
        )
        consumer._processor = IdempotentProcessor(
            MemoryIdempotencyStore(),
            lease_seconds=config.idempotency_lease_seconds,
            heartbeat_interval=config.heartbeat_interval_seconds,
            handler_timeout=config.handler_timeout_seconds,
        )
        consumer._handler = self._message_handler(consumer)
        yield consumer
        # Clean up handlers and state between tests
        consumer._handlers.clear()
        consumer._default_handler = None
        consumer.events_processed = 0
        consumer.events_failed = 0
        consumer.last_processed_sequence = 0
        consumer._metrics.reset()

    def test_consumer_initialization(self, consumer, config):
        """Test consumer initialization."""
        assert consumer.service_name == "test-service"
        assert consumer.config == config
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

            # Mock empty fetch to trigger sync completion with small delay
            async def mock_fetch(*args, **kwargs):
                await asyncio.sleep(0.001)  # Small delay to prevent tight loop
                return []

            mock_subscription.fetch = mock_fetch

            await consumer.start()

            assert consumer._is_running
            assert consumer._processing_task is not None

            # Wait a bit for processing loop to start and change state
            await asyncio.sleep(0.001)
            assert consumer.state in [ConsumerState.SYNCING, ConsumerState.LIVE]

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

    @staticmethod
    def _existing_info(config, consumer, **overrides):
        """consumer_info result carrying a realistic ConsumerConfig."""
        cfg = build_consumer_config(
            config, consumer.consumer_name, consumer.subject_filter
        )
        info = MagicMock()
        info.config = replace(cfg, **overrides)
        info.delivered.stream_seq = 100
        info.num_ack_pending = 5
        return info

    @pytest.mark.asyncio
    async def test_setup_consumer_existing(
        self, consumer, config, mock_jetstream, mock_subscription
    ):
        """An up-to-date existing durable is bound as-is (no add_consumer)."""
        consumer._js = mock_jetstream
        mock_jetstream.consumer_info.return_value = self._existing_info(
            config, consumer
        )
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        await consumer._setup_consumer()

        assert consumer._subscription == mock_subscription
        assert consumer._processor is not None
        mock_jetstream.add_consumer.assert_not_called()

    @pytest.mark.asyncio
    async def test_setup_consumer_existing_editable_diff_updates(
        self, consumer, config, mock_jetstream, mock_subscription
    ):
        """Editable drift is reconciled; deliver_policy of the durable is kept."""
        from nats.js.api import DeliverPolicy

        consumer._js = mock_jetstream
        mock_jetstream.consumer_info.return_value = self._existing_info(
            config, consumer, max_deliver=3, deliver_policy=DeliverPolicy.ALL
        )
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        await consumer._setup_consumer()

        mock_jetstream.add_consumer.assert_called_once()
        update = mock_jetstream.add_consumer.call_args[0][1]
        assert update.max_deliver == config.max_deliver_attempts
        assert update.deliver_policy == DeliverPolicy.ALL

    @pytest.mark.asyncio
    async def test_setup_consumer_existing_immutable_mismatch_fails(
        self, consumer, config, mock_jetstream, mock_subscription
    ):
        """A durable with a different filter cannot be bound silently."""
        consumer._js = mock_jetstream
        mock_jetstream.consumer_info.return_value = self._existing_info(
            config, consumer, filter_subject="other.events.>"
        )
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        with pytest.raises(ConsumerConfigMismatchError):
            await consumer._setup_consumer()

        mock_jetstream.add_consumer.assert_not_called()
        mock_jetstream.pull_subscribe.assert_not_called()

    @pytest.mark.asyncio
    async def test_setup_consumer_new(
        self, consumer, mock_jetstream, mock_subscription
    ):
        """New durables start at DeliverPolicy.NEW by default."""
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
        assert call_args.deliver_policy.value == "new"
        assert call_args.ack_policy.value == "explicit"
        assert call_args.max_deliver == 6

    @pytest.mark.asyncio
    async def test_setup_consumer_new_deliver_policy_all(
        self, config, mock_jetstream, mock_subscription
    ):
        """deliver_policy='all' replays the stream for a new durable."""
        consumer = DurableEventConsumer(
            service_name="replay-service",
            config=replace(config, deliver_policy="all"),
            auto_adapt=False,
            idempotency_store=MemoryIdempotencyStore(),
        )
        consumer._js = mock_jetstream
        mock_jetstream.consumer_info.side_effect = nats.js.errors.NotFoundError()
        mock_jetstream.add_consumer = AsyncMock()
        mock_jetstream.pull_subscribe = AsyncMock(return_value=mock_subscription)

        await consumer._setup_consumer()

        call_args = mock_jetstream.add_consumer.call_args[0][1]
        assert call_args.deliver_policy.value == "all"

    @pytest.mark.asyncio
    async def test_event_processing_loop(
        self, consumer, mock_subscription, mock_nats_message
    ):
        """Test event processing loop."""
        consumer._subscription = mock_subscription
        consumer._is_running = True
        # The supervisor (not the fetch loop) moves the state to SYNCING
        consumer._consumer_state = ConsumerState.SYNCING

        # Mock fetch to return messages then empty
        call_count = 0

        async def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [mock_nats_message]
            else:
                await asyncio.sleep(0.001)  # Small delay to prevent tight loop
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
        await asyncio.sleep(0.01)
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
    async def test_process_message_handler_error_naks_with_delay(
        self, consumer, mock_nats_message
    ):
        """A failing handler no longer raises: the message is nak'ed with a delay."""

        def failing_handler(event: Event):
            raise Exception("Handler error")

        consumer.register_handler("test.created", failing_handler)

        await consumer._process_message(mock_nats_message)

        assert consumer.events_failed == 1
        assert consumer.events_processed == 0
        assert mock_nats_message.is_nacked
        assert mock_nats_message.nak_delays == [consumer.config.nak_delays_seconds[0]]
        assert not mock_nats_message.is_acked
        assert not mock_nats_message.is_termed

    @pytest.mark.asyncio
    async def test_process_message_nak_delay_grows_with_deliveries(
        self, consumer, mock_nats_message
    ):
        def failing_handler(event: Event):
            raise Exception("boom")

        consumer.register_handler("test.created", failing_handler)
        mock_nats_message.metadata.num_delivered = 3

        await consumer._process_message(mock_nats_message)

        assert mock_nats_message.nak_delays == [consumer.config.nak_delays_seconds[2]]

    @pytest.mark.asyncio
    async def test_process_message_failure_is_retried_not_cached(
        self, consumer, mock_nats_message
    ):
        """A redelivery after failure re-runs the handler and then acks."""
        calls = 0

        def flaky(event: Event):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise Exception("transient")

        consumer.register_handler("test.created", flaky)

        await consumer._process_message(mock_nats_message)
        assert mock_nats_message.is_nacked and not mock_nats_message.is_acked

        await consumer._process_message(mock_nats_message)
        assert calls == 2
        assert mock_nats_message.is_acked
        assert consumer.events_processed == 1

    @pytest.mark.asyncio
    async def test_process_message_last_attempt_terminates_without_dlq(
        self, consumer, mock_nats_message
    ):
        """Last delivery with DLQ disabled: term() instead of a silent nak loop."""

        def failing_handler(event: Event):
            raise Exception("always")

        consumer.register_handler("test.created", failing_handler)
        mock_nats_message.metadata.num_delivered = consumer.config.max_deliver_attempts

        await consumer._process_message(mock_nats_message)

        assert mock_nats_message.is_termed
        assert not mock_nats_message.is_nacked
        assert not mock_nats_message.is_acked

    @pytest.mark.asyncio
    async def test_process_message_last_attempt_goes_to_dlq(
        self, consumer, mock_nats_message
    ):
        def failing_handler(event: Event):
            raise ValueError("always")

        consumer.register_handler("test.created", failing_handler)
        dlq = MagicMock(publish=AsyncMock())
        consumer._handler = self._message_handler(consumer, dlq)
        mock_nats_message.metadata.num_delivered = consumer.config.max_deliver_attempts

        await consumer._process_message(mock_nats_message)

        dlq.publish.assert_awaited_once()
        kwargs = dlq.publish.call_args.kwargs
        assert kwargs["reason"] == "max_deliveries"
        assert kwargs["stream_seq"] == 1
        assert "ValueError" in kwargs["error"]
        assert mock_nats_message.is_termed

    @pytest.mark.asyncio
    async def test_process_message_non_retryable_goes_to_dlq_immediately(
        self, consumer, mock_nats_message
    ):
        def failing_handler(event: Event):
            raise NonRetryableError("bad payload")

        consumer.register_handler("test.created", failing_handler)
        dlq = MagicMock(publish=AsyncMock())
        consumer._handler = self._message_handler(consumer, dlq)

        await consumer._process_message(mock_nats_message)  # first delivery

        assert dlq.publish.call_args.kwargs["reason"] == ("non_retryable")
        assert mock_nats_message.is_termed
        assert not mock_nats_message.is_nacked

    @pytest.mark.asyncio
    async def test_process_message_dlq_publish_failure_naks_instead_of_dropping(
        self, consumer, mock_nats_message
    ):
        """If the DLQ is unavailable the message is retried, never lost."""

        def failing_handler(event: Event):
            raise NonRetryableError("bad payload")

        consumer.register_handler("test.created", failing_handler)
        dlq = MagicMock(publish=AsyncMock(side_effect=RuntimeError("dlq down")))
        consumer._handler = self._message_handler(consumer, dlq)

        await consumer._process_message(mock_nats_message)

        assert mock_nats_message.is_nacked
        assert not mock_nats_message.is_termed

    @pytest.mark.asyncio
    async def test_process_message_poison_is_dead_lettered(self, consumer):
        """Unparseable payloads are terminated immediately, never retried."""
        msg = MockNATSMessage(b"not json at all", sequence=9)
        dlq = MagicMock(publish=AsyncMock())
        consumer._handler = self._message_handler(consumer, dlq)

        await consumer._process_message(msg)

        assert dlq.publish.call_args.kwargs["reason"] == "poison"
        assert msg.is_termed
        assert not msg.is_acked and not msg.is_nacked

    @pytest.mark.asyncio
    async def test_process_message_duplicate_is_skipped_and_acked(
        self, consumer, mock_nats_message
    ):
        """A message already handled successfully is acked without re-running."""
        calls = 0

        def handler(event: Event):
            nonlocal calls
            calls += 1

        consumer.register_handler("test.created", handler)

        await consumer._process_message(mock_nats_message)
        redelivery = MockNATSMessage(mock_nats_message.data, sequence=1)
        await consumer._process_message(redelivery)

        assert calls == 1
        assert redelivery.is_acked

    @pytest.mark.asyncio
    async def test_process_message_no_handler(self, consumer, mock_nats_message):
        """Messages with no registered handler are acked and not counted."""
        # No handler registered
        await consumer._process_message(mock_nats_message)

        assert mock_nats_message.is_acked
        assert consumer.events_processed == 0
        assert consumer.events_failed == 0

    @pytest.mark.asyncio
    async def test_process_event_async_handler(self, consumer):
        """Test processing event with async handler."""
        event = Event(event_type="test.async", data={"id": "123"})

        handler_called = False

        async def async_handler(evt: Event):
            nonlocal handler_called
            await asyncio.sleep(0.001)
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

        # Create a mock task that can be cancelled and awaited
        async def mock_task_coro():
            pass

        mock_task = asyncio.create_task(mock_task_coro())
        consumer._processing_task = mock_task

        mock_nats_client.drain = AsyncMock()
        mock_nats_client.close = AsyncMock()

        await consumer.stop()

        assert not consumer._is_running
        assert consumer.state == ConsumerState.STOPPED
        assert (
            mock_task.cancelled() or mock_task.done()
        )  # Task should be cancelled or completed
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

    def test_consumer_with_adapters(self, nats_config):
        """Test consumer initialization with adapters."""
        adapter = GenericAdapter()
        consumer = DurableEventConsumer(
            service_name="test-service",
            config=nats_config,
            adapters=[adapter],
            auto_adapt=False,
        )

        assert len(consumer.adapters) == 1
        assert isinstance(consumer.adapters[0], GenericAdapter)

    def test_consumer_with_auto_adapt(self, nats_config):
        """Test consumer initialization with auto-adaptation enabled."""
        consumer = DurableEventConsumer(
            service_name="test-service", config=nats_config, auto_adapt=True
        )

        # Should automatically include FlexibleAdapter
        assert len(consumer.adapters) == 1
        assert isinstance(consumer.adapters[0], FlexibleAdapter)

    def test_consumer_with_custom_adapters_and_auto_adapt(self, nats_config):
        """Test consumer with custom adapters and auto-adaptation."""
        custom_adapter = GenericAdapter()
        consumer = DurableEventConsumer(
            service_name="test-service",
            config=nats_config,
            adapters=[custom_adapter],
            auto_adapt=True,
        )

        # Should have custom adapter plus FlexibleAdapter
        assert len(consumer.adapters) == 2
        assert isinstance(consumer.adapters[0], GenericAdapter)
        assert isinstance(consumer.adapters[1], FlexibleAdapter)

    def test_consumer_auto_adapt_disabled(self, nats_config):
        """Test consumer with auto-adaptation disabled."""
        consumer = DurableEventConsumer(
            service_name="test-service", config=nats_config, auto_adapt=False
        )

        # Should have no adapters when auto_adapt=False and no adapters provided
        assert len(consumer.adapters) == 0

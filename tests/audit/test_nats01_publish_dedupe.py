"""
NATS-01 - JetStream publish dedupe never engages.

The stream declares ``duplicate_window`` but the publisher sends ``event-id``
instead of ``Nats-Msg-Id``, so a retry after a lost PubAck stores the event twice.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tc_nats_events import EventMetadata, EventPublisher
from tc_nats_events.core.event_store import NATSEventStore
from tc_nats_events.models.event import Event

from .conftest import make_config, requires_nats, stream_msg_count


class TestPublishHeaders:
    async def test_publish_sets_nats_msg_id(self, mock_jetstream):
        store = NATSEventStore(make_config())
        store._js = mock_jetstream
        store._nc = MagicMock(is_connected=True)
        store._is_connected = True
        event = Event(
            event_type="order.created",
            data={"id": 1},
            metadata=EventMetadata(event_id="evt-123"),
        )

        await store.publish_event(event)

        headers = mock_jetstream.publish.call_args.kwargs["headers"]
        assert headers.get("Nats-Msg-Id") == "evt-123"

    async def test_retry_reuses_msg_id(self):
        publisher = EventPublisher("svc", make_config())
        publisher.retry_delay_base = 0
        seen = []

        async def flaky(event):
            seen.append(event.metadata.event_id)
            if len(seen) < 3:
                raise RuntimeError("timeout")
            return 1

        publisher._event_store.publish_event = AsyncMock(side_effect=flaky)
        await publisher.publish("order.created", {"id": 1})

        assert len(seen) == 3 and len(set(seen)) == 1

    async def test_duplicate_ack_returns_original_sequence(self, mock_jetstream):
        mock_jetstream.publish.return_value = MagicMock(seq=7, duplicate=True)
        store = NATSEventStore(make_config())
        store._js = mock_jetstream
        store._nc = MagicMock(is_connected=True)
        store._is_connected = True

        seq = await store.publish_event(Event(event_type="a.b", data={}))

        assert seq == 7


@requires_nats
@pytest.mark.integration
class TestPublishDedupeIntegration:
    async def test_lost_ack_does_not_duplicate(self, js, cleanup, lossy_publish):
        config = make_config()
        cleanup(config)
        publisher = EventPublisher("svc", config)
        publisher.retry_delay_base = 0.1
        await publisher.connect()
        try:
            await publisher.publish("order.created", {"id": 1})
        finally:
            await publisher.disconnect()

        assert lossy_publish["calls"] >= 2, "publisher must have retried"
        assert await stream_msg_count(js, config.stream_name) == 1

    async def test_same_event_id_twice_is_one_message(self, js, cleanup):
        config = make_config()
        cleanup(config)
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            for _ in range(2):
                await publisher.publish(
                    "order.created",
                    {"id": 1},
                    metadata=EventMetadata(event_id="deterministic-order-1"),
                )
        finally:
            await publisher.disconnect()

        assert await stream_msg_count(js, config.stream_name) == 1

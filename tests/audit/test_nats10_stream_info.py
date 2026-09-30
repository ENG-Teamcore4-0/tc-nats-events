"""NATS-10 - ``stream_info`` round-trip before every publish."""

from unittest.mock import MagicMock

from tc_nats_events.core.event_store import NATSEventStore
from tc_nats_events.models.event import Event

from .conftest import make_config


async def test_publish_does_not_call_stream_info(mock_jetstream):
    store = NATSEventStore(make_config())
    store._js = mock_jetstream
    store._nc = MagicMock(is_connected=True)
    store._is_connected = True

    for i in range(5):
        await store.publish_event(Event(event_type="a.b", data={"i": i}))

    assert mock_jetstream.publish.await_count == 5
    assert mock_jetstream.stream_info.await_count == 0

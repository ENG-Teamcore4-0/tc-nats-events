"""
NATS-04 DeliverPolicy.ALL replays retention on a new durable.
NATS-05 Stream replicas default to 1 even in production.
NATS-09 Connecting rewrites the stream subjects of a shared stream.
NATS-11 ``"*"`` handler is looked up literally and never matches.
"""

import asyncio
import uuid

import pytest
from nats.js.api import StreamConfig

from tc_nats_events import DurableEventConsumer, EventPublisher
from tc_nats_events.core.event_store import NATSEventStore

from .conftest import make_config, requires_nats, wait_for
from .pods import start_pod, stop_pod


# --------------------------------------------------------------------- NATS-04
@requires_nats
@pytest.mark.integration
class TestDeliverPolicy:
    async def _run(self, cleanup, **overrides):
        config = make_config(**overrides)
        cleanup(config)
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        for i in range(5):
            await publisher.publish("old.event", {"id": i})
        received = []

        async def handler(event):
            received.append(event.data["id"])

        pod = await start_pod(config, {"old.event": handler, "new.event": handler})
        try:
            await publisher.publish("new.event", {"id": 99})
            await wait_for(lambda: 99 in received, timeout=10)
            await asyncio.sleep(0.5)
        finally:
            await publisher.disconnect()
            await stop_pod(pod)
        return received

    async def test_new_durable_does_not_replay_history_by_default(self, cleanup):
        assert await self._run(cleanup) == [99]

    async def test_explicit_all_replays_history(self, cleanup):
        assert await self._run(cleanup, deliver_policy="all") == [0, 1, 2, 3, 4, 99]


# --------------------------------------------------------------------- NATS-05
class TestReplicas:
    def test_production_requires_r3(self):
        config = make_config(environment="production", replicas=1)
        with pytest.raises(ValueError, match="replicas"):
            config.validate()

    def test_production_r3_is_valid(self):
        make_config(environment="production", replicas=3).validate()

    def test_development_allows_r1(self):
        make_config(environment="development", replicas=1).validate()


# --------------------------------------------------------------------- NATS-09
@requires_nats
@pytest.mark.integration
class TestStreamSubjectsNotOverwritten:
    async def _stream_with(self, js, config, subjects):
        await js.add_stream(StreamConfig(name=config.stream_name, subjects=subjects))

    async def test_foreign_stream_subjects_not_overwritten(self, js, cleanup):
        config = make_config()
        cleanup(config)
        foreign = [f"other.{uuid.uuid4().hex[:8]}.>"]
        await self._stream_with(js, config, foreign)

        store = NATSEventStore(config)
        with pytest.raises(Exception, match="subject"):
            await store.connect()
        await store.disconnect()

        info = await js.stream_info(config.stream_name)
        assert info.config.subjects == foreign

    async def test_superset_subjects_accepted(self, js, cleanup):
        config = make_config()
        cleanup(config)
        shared = [f"{config.subject_prefix}.>", f"b{uuid.uuid4().hex[:8]}.>"]
        await self._stream_with(js, config, shared)

        store = NATSEventStore(config)
        await store.connect()
        await store.disconnect()

        info = await js.stream_info(config.stream_name)
        assert sorted(info.config.subjects) == sorted(shared)


# --------------------------------------------------------------------- NATS-11
class TestStarHandler:
    def test_star_handler_is_catch_all(self):
        consumer = DurableEventConsumer("svc", make_config())

        async def catch_all(event):
            return None

        consumer.register_handler("*", catch_all)
        assert consumer.get_handler("user.created") is catch_all

    def test_exact_handler_wins_over_star(self):
        consumer = DurableEventConsumer("svc", make_config())

        async def exact(event):
            return None

        async def catch_all(event):
            return None

        consumer.register_handler("*", catch_all)
        consumer.register_handler("user.created", exact)
        assert consumer.get_handler("user.created") is exact

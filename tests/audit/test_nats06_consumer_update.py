"""
NATS-06 - Consumer config only applied on creation.

Changing ``max_deliver`` / ``ack_wait`` never reaches an existing durable.
Immutable differences must fail fast instead of binding silently.
"""

import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from .conftest import make_config, requires_nats
from .pods import start_pod, stop_pod


@requires_nats
@pytest.mark.integration
class TestConsumerReconcile:
    async def test_consumer_config_is_updated_on_start(self, js, cleanup):
        config = make_config(max_deliver_attempts=3, ack_wait_seconds=30)
        cleanup(config)
        pod = await start_pod(config, {})
        await stop_pod(pod)

        updated = make_config(
            stream_name=config.stream_name,
            subject_prefix=config.subject_prefix,
            max_deliver_attempts=6,
            ack_wait_seconds=10,
        )
        pod = await start_pod(updated, {})
        try:
            info = await js.consumer_info(config.stream_name, pod.consumer_name)
        finally:
            await stop_pod(pod)

        assert info.config.max_deliver == 6
        assert info.config.ack_wait == pytest.approx(10)

    async def test_immutable_change_fails_fast(self, js, cleanup):
        config = make_config()
        cleanup(config)
        pod = await start_pod(config, {})
        name = pod.consumer_name
        await stop_pod(pod)
        await js.delete_consumer(config.stream_name, name)
        await js.add_consumer(
            config.stream_name,
            ConsumerConfig(
                durable_name=name,
                name=name,
                ack_policy=AckPolicy.EXPLICIT,
                filter_subject=f"{config.subject_prefix}.only.this.>",
            ),
        )

        from tc_nats_events import DurableEventConsumer

        consumer = DurableEventConsumer("svc", config)
        with pytest.raises(Exception, match="filter_subject"):
            await consumer.start()
        await stop_pod(consumer)

    async def test_existing_all_consumer_not_broken_by_new_default(self, js, cleanup):
        config = make_config()
        cleanup(config)
        pod = await start_pod(config, {})
        name = pod.consumer_name
        await stop_pod(pod)
        await js.delete_consumer(config.stream_name, name)
        await js.add_consumer(
            config.stream_name,
            ConsumerConfig(
                durable_name=name,
                name=name,
                ack_policy=AckPolicy.EXPLICIT,
                deliver_policy=DeliverPolicy.ALL,
                filter_subject=f"{config.subject_prefix}.>",
                max_deliver=3,
            ),
        )

        pod = await start_pod(config, {})
        try:
            info = await js.consumer_info(config.stream_name, name)
        finally:
            await stop_pod(pod)

        assert info.config.deliver_policy == DeliverPolicy.ALL

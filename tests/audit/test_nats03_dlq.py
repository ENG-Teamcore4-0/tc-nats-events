"""
NATS-03 - No DLQ and no ``term()``.

Poison messages and messages that exhaust ``max_deliver`` disappear silently.
They must end up in ``<stream>-DLQ`` with diagnostic headers.
"""

import asyncio

import pytest

from tc_nats_events import EventPublisher

from .conftest import make_config, requires_nats, stream_exists, stream_msg_count
from .pods import start_pod, stop_pod


async def _dlq_count(js, config) -> int:
    name = f"{config.stream_name}-DLQ"
    if not await stream_exists(js, name):
        return 0
    return await stream_msg_count(js, name)


@requires_nats
@pytest.mark.integration
class TestDeadLetterQueue:
    async def test_poison_message_goes_to_dlq_and_is_terminated(self, js, cleanup):
        config = make_config(max_deliver_attempts=3, ack_wait_seconds=2)
        cleanup(config)
        pod = await start_pod(config, {})
        try:
            await js.publish(f"{config.subject_prefix}.broken", b"not json{")
            await asyncio.sleep(3)
            info = await js.consumer_info(config.stream_name, pod.consumer_name)
        finally:
            await stop_pod(pod)

        assert await _dlq_count(js, config) == 1, "poison message was dropped"
        dlq_msg = await js.get_msg(f"{config.stream_name}-DLQ", seq=1)
        assert dlq_msg.headers["X-Dlq-Reason"] == "poison"
        assert dlq_msg.data == b"not json{"
        assert info.delivered.consumer_seq == 1, "poison message must not be retried"
        assert info.num_ack_pending == 0

    async def test_exhausted_retries_go_to_dlq(self, js, cleanup):
        config = make_config(max_deliver_attempts=3)
        cleanup(config)
        attempts = []

        async def always_fails(event):
            attempts.append(1)
            raise RuntimeError("downstream permanently broken")

        pod = await start_pod(config, {"job.run": always_fails})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("job.run", {"id": 1})
            await asyncio.sleep(4)
        finally:
            await publisher.disconnect()
            await stop_pod(pod)

        assert await _dlq_count(js, config) == 1, "exhausted message was dropped"
        dlq_msg = await js.get_msg(f"{config.stream_name}-DLQ", seq=1)
        assert dlq_msg.headers["X-Dlq-Reason"] == "max_deliveries"
        assert dlq_msg.headers["X-Dlq-Deliveries"] == "3"
        assert "permanently broken" in dlq_msg.headers["X-Dlq-Error"]
        assert len(attempts) == 3

    async def test_advisory_safety_net(self, js, cleanup):
        """A pod crashes on every delivery: the MAX_DELIVERIES advisory feeds the DLQ."""
        from tc_nats_events.consumers.dlq import DeadLetterAdvisoryListener

        config = make_config(max_deliver_attempts=2, ack_wait_seconds=1)
        cleanup(config)
        pod = await start_pod(config, {})
        consumer_name = pod.consumer_name
        await stop_pod(pod)  # durable + DLQ stream now exist

        listener = DeadLetterAdvisoryListener(js, config, consumer_name)
        await listener.start()
        crashing = await js.pull_subscribe(
            "", durable=consumer_name, stream=config.stream_name
        )
        try:
            await js.publish(f"{config.subject_prefix}.job.run", b'{"x":1}')
            for _ in range(2):  # fetch and "crash" (never ack)
                await crashing.fetch(1, timeout=3)
                await asyncio.sleep(1.5)
            await asyncio.sleep(1)
            # The app-level path may also publish the same message: still one entry.
            await listener.handle_stream_seq(1, deliveries=2)
        finally:
            await crashing.unsubscribe()
            await listener.stop()

        assert await _dlq_count(js, config) == 1

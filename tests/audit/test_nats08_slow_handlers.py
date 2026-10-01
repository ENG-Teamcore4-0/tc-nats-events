"""
NATS-08 - ``ack_wait`` shorter than the handler: JetStream redelivers while the
first attempt is still running (plus queued batch messages time out).
"""

import asyncio

import pytest

from tc_nats_events import EventPublisher

from .conftest import make_config, requires_nats, wait_for
from .pods import start_pod, stop_pod


@requires_nats
@pytest.mark.integration
class TestSlowHandlers:
    async def test_slow_handler_not_redelivered(self, js, cleanup):
        config = make_config(ack_wait_seconds=2)
        cleanup(config)
        invocations = []

        async def slow(event):
            invocations.append(event.data["id"])
            await asyncio.sleep(5)

        pod_a = await start_pod(config, {"report.build": slow})
        pod_b = await start_pod(config, {"report.build": slow})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("report.build", {"id": 1})
            await asyncio.sleep(8)
            info = await js.consumer_info(config.stream_name, pod_a.consumer_name)
        finally:
            await publisher.disconnect()
            await stop_pod(pod_a)
            await stop_pod(pod_b)

        assert invocations == [1], f"handler ran {len(invocations)} times"
        assert info.delivered.consumer_seq == 1, "message was redelivered"

    async def test_batch_pending_msgs_not_redelivered(self, js, cleanup):
        config = make_config(ack_wait_seconds=2, deliver_policy="all")
        cleanup(config)
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        for i in range(5):
            await publisher.publish("report.build", {"id": i})
        await publisher.disconnect()

        done = []

        async def slowish(event):
            await asyncio.sleep(1.5)
            done.append(event.data["id"])

        pod = await start_pod(config, {"report.build": slowish}, batch_size=5)
        try:
            await wait_for(lambda: len(set(done)) == 5, timeout=20)
            await asyncio.sleep(2.5)  # let any expired message be re-fetched
            info = await js.consumer_info(config.stream_name, pod.consumer_name)
        finally:
            await stop_pod(pod)

        deliveries = info.delivered.consumer_seq
        assert (
            deliveries == 5
        ), f"queued batch messages hit ack_wait ({deliveries} deliveries)"

    async def test_hung_handler_times_out_and_retries(self, cleanup):
        config = make_config(handler_timeout_seconds=1.0)
        cleanup(config)
        attempts = []

        async def hangs_once(event):
            attempts.append(1)
            if len(attempts) == 1:
                await asyncio.sleep(3600)

        pod = await start_pod(config, {"job.run": hangs_once})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("job.run", {"id": 1})
            await wait_for(lambda: len(attempts) >= 2, timeout=8)
        finally:
            await publisher.disconnect()
            await stop_pod(pod)

        assert len(attempts) == 2, "hung handler blocked the message"

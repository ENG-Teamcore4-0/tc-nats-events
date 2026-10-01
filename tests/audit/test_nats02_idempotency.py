"""
NATS-02 - Idempotency caches failures, is per-pod and dies with the process.

A handler that fails once (e.g. notification service down for a few seconds)
must be retried with a delay, and a handler that already succeeded must not run
again on another pod or after a restart.
"""

import asyncio
import time

import pytest

from tc_nats_events import EventPublisher
from tc_nats_events.utils.idempotency import IdempotentEventProcessor

from .conftest import make_config, requires_nats, wait_for
from .pods import start_pod, stop_pod


class TestProcessorUnit:
    async def test_failed_handler_is_retried_not_cached(self):
        processor = IdempotentEventProcessor()
        calls = []

        async def handler(_event):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("notifications down")
            return "ok"

        with pytest.raises(RuntimeError):
            await processor.process_with_idempotency("e1", "h", handler, None)
        result = await processor.process_with_idempotency("e1", "h", handler, None)

        assert len(calls) == 2
        assert result == "ok"


@requires_nats
@pytest.mark.integration
class TestIdempotencyIntegration:
    async def test_notifications_down_then_recover(self, cleanup):
        """Handler fails on the first two deliveries, then the dependency recovers."""
        config = make_config()
        cleanup(config)
        attempts, delivered_at = [], []

        async def handler(event):
            delivered_at.append(time.monotonic())
            attempts.append(event.data["id"])
            if len(attempts) <= 2:
                raise RuntimeError("notification service unavailable")

        consumer = await start_pod(config, {"notify.requested": handler})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("notify.requested", {"id": 1})
            await wait_for(lambda: len(attempts) >= 3, timeout=15)
        finally:
            await publisher.disconnect()
            await stop_pod(consumer)

        assert len(attempts) == 3, f"handler ran {len(attempts)} times, event lost"
        gaps = [b - a for a, b in zip(delivered_at, delivered_at[1:])]
        assert all(g >= 0.15 for g in gaps), f"redelivery without backoff: {gaps}"

    async def test_dedupe_across_pods(self, cleanup, lossy_ack):
        """Pod A handles the event but its ack is lost and it crashes; pod B must skip it."""
        config = make_config(ack_wait_seconds=2)
        cleanup(config)
        effects = []
        handled = asyncio.Event()

        async def handler(event):
            effects.append(event.data["id"])
            handled.set()

        pod_a = await start_pod(config, {"order.paid": handler})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        pod_b = None
        try:
            await publisher.publish("order.paid", {"id": 42})
            await asyncio.wait_for(handled.wait(), timeout=10)
            await asyncio.sleep(0.3)
            await stop_pod(pod_a)  # crash after the lost ack
            pod_b = await start_pod(config, {"order.paid": handler})
            await asyncio.sleep(4)  # > ack_wait so any redelivery reaches pod B
        finally:
            await publisher.disconnect()
            await stop_pod(pod_b)

        assert lossy_ack["dropped"] == 1
        assert effects == [42], f"side effect executed {len(effects)} times"

    async def test_dedupe_survives_restart(self, cleanup, lossy_ack):
        """Same pod restarts after a lost ack; the processed event must not run again."""
        config = make_config(ack_wait_seconds=2)
        cleanup(config)
        effects = []

        async def handler(event):
            effects.append(event.data["id"])

        pod = await start_pod(config, {"order.paid": handler}, service="restart")
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("order.paid", {"id": 7})
            await wait_for(lambda: len(effects) >= 1, timeout=10)
            await asyncio.sleep(0.3)
            await stop_pod(pod)
            pod = await start_pod(config, {"order.paid": handler}, service="restart")
            await asyncio.sleep(4)
        finally:
            await publisher.disconnect()
            await stop_pod(pod)

        assert effects == [7], f"side effect executed {len(effects)} times"

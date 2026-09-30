"""
Regression tests for issues found while reviewing the 0.2.0 fixes
(code review + security review). They only apply to the new implementation.
"""

import asyncio
import json

import pytest

from tc_nats_events import EventPublisher

from .conftest import (
    make_config,
    requires_nats,
    stream_exists,
    stream_msg_count,
    wait_for,
)
from .pods import start_pod, stop_pod


@requires_nats
@pytest.mark.integration
class TestReviewHardening:
    async def test_advisory_for_processed_message_is_ignored(self, js, cleanup):
        """Ack lost after success -> MAX_DELIVERIES must not dead-letter it."""
        config = make_config()
        cleanup(config)
        done = []

        async def handler(event):
            done.append(event.data["id"])

        pod = await start_pod(config, {"order.paid": handler})
        publisher = EventPublisher("svc", config)
        await publisher.connect()
        try:
            await publisher.publish("order.paid", {"id": 1})
            await wait_for(lambda: done == [1], timeout=10)
            await pod._advisories.handle_stream_seq(1, deliveries=6)
        finally:
            await publisher.disconnect()
            await stop_pod(pod)

        dlq = config.dlq_stream_name
        assert await stream_exists(js, dlq)
        assert await stream_msg_count(js, dlq) == 0

    async def test_forged_advisory_is_rejected(self, js, nc, cleanup):
        config = make_config()
        cleanup(config)
        pod = await start_pod(config, {})
        try:
            await js.publish(f"{config.subject_prefix}.secret", b'{"x":1}')
            forged = {
                "type": "io.nats.jetstream.advisory.v1.max_deliver",
                "stream": "someone-else",
                "consumer": pod.consumer_name,
                "stream_seq": 1,
            }
            await nc.publish(pod._advisories.subject, json.dumps(forged).encode())
            await asyncio.sleep(1)
        finally:
            await stop_pod(pod)

        assert await stream_msg_count(js, config.dlq_stream_name) == 0

    async def test_lost_kv_write_response_keeps_lease(self, js, cleanup):
        """A write applied server-side whose reply was lost must not look like a takeover."""
        from tc_nats_events.idempotency import AcquireStatus, NatsKVIdempotencyStore

        config = make_config()
        cleanup(config)
        store = NatsKVIdempotencyStore(js, config.idempotency_bucket, ttl_seconds=600)
        await store.setup()
        acquired = await store.try_acquire("k1", lease_seconds=30)
        assert acquired.status == AcquireStatus.ACQUIRED

        # Our own renew reached the server but the reply was lost: revision moved.
        await store.kv.update(
            "k1", store._encode("processing", 30), last=acquired.lease.revision
        )

        await store.mark_done(acquired.lease)  # must adopt the new revision
        assert await store.status("k1") == "done"

    async def test_same_msg_id_on_other_subject_is_not_suppressed(self, js, cleanup):
        """
        A producer reusing an id on another event type cannot hide that event
        in the consumer. (``Nats-Msg-Id`` itself is deduplicated stream-wide by
        the server inside duplicate_window; that is documented as a producer
        trust assumption.) ``event-id`` is not deduplicated by the server, so it
        isolates the consumer-side key scoping.
        """
        config = make_config()
        cleanup(config)
        seen = []

        async def handler(event):
            seen.append(event.event_type)

        pod = await start_pod(config, {"a.happened": handler, "b.happened": handler})
        try:
            for event_type in ("a.happened", "b.happened"):
                body = json.dumps(
                    {
                        "event_type": event_type,
                        "data": {},
                        "timestamp": "2026-09-30T00:00:00Z",
                    }
                ).encode()
                await js.publish(
                    f"{config.subject_prefix}.{event_type}",
                    body,
                    headers={"event-id": "shared-id"},
                )
            await wait_for(lambda: len(seen) == 2, timeout=10)
        finally:
            await stop_pod(pod)

        assert sorted(seen) == ["a.happened", "b.happened"]

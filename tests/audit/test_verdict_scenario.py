"""
Audit verdict scenario (NATS-01 + NATS-02 together).

Two replicas consume; the notification dependency is down for ~3 seconds and
one publish loses its PubAck. Every event must produce exactly one side effect,
the stream must hold one message per event and nothing may land in the DLQ.
"""

import asyncio
import time
from collections import Counter

import pytest

from tc_nats_events import EventPublisher

from .conftest import make_config, requires_nats, stream_exists, stream_msg_count
from .pods import start_pod, stop_pod

EVENTS = 5
OUTAGE_SECONDS = 3.0


@requires_nats
@pytest.mark.integration
async def test_audit_verdict_scenario(js, cleanup, lossy_publish):
    config = make_config(nak_delays_seconds=(0.5, 1.0, 2.0, 4.0))
    cleanup(config)
    outage = {"until": float("inf")}  # armed once the pods are live
    effects: Counter = Counter()

    async def send_notification(event):
        if time.monotonic() < outage["until"]:
            raise RuntimeError("notification service down")
        effects[event.data["id"]] += 1

    pods = [
        await start_pod(config, {"notify.requested": send_notification}),
        await start_pod(config, {"notify.requested": send_notification}),
    ]
    publisher = EventPublisher("svc", config)
    publisher.retry_delay_base = 0.1
    await publisher.connect()
    outage["until"] = time.monotonic() + OUTAGE_SECONDS
    try:
        for i in range(EVENTS):
            await publisher.publish("notify.requested", {"id": i})
        deadline = time.monotonic() + 15
        while len(effects) < EVENTS and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        await asyncio.sleep(1)
    finally:
        await publisher.disconnect()
        for pod in pods:
            await stop_pod(pod)

    lost = [i for i in range(EVENTS) if effects[i] == 0]
    duplicated = {i: n for i, n in effects.items() if n > 1}
    assert not lost, f"events lost during outage: {lost}"
    assert not duplicated, f"side effects duplicated: {duplicated}"
    assert await stream_msg_count(js, config.stream_name) == EVENTS
    dlq = f"{config.stream_name}-DLQ"
    assert not await stream_exists(js, dlq) or await stream_msg_count(js, dlq) == 0

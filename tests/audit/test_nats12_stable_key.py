"""
NATS-12 - Idempotency key is random for adapter-parsed events (found in review).

tc-iam / generic payloads carry no event_id, so the adapters build a new uuid4
on every parse and a redelivered message is never recognised as a duplicate.
"""

import json

import pytest

from .conftest import make_config, requires_nats, wait_for
from .pods import start_pod, stop_pod

TC_IAM_PAYLOAD = json.dumps(
    {
        "event_type": "teamcore.tenant.registration",
        "environment": "test",
        "timestamp": "2026-09-30T10:00:00Z",
        "payload": json.dumps({"tenantID": 1}),
    }
).encode()


@requires_nats
@pytest.mark.integration
async def test_adapter_event_redelivery_runs_once(js, cleanup, lossy_ack):
    config = make_config(ack_wait_seconds=2)
    cleanup(config)
    effects = []

    async def handler(event):
        effects.append(event.data["tenantID"])

    pod = await start_pod(config, {"teamcore.tenant.registration": handler})
    try:
        # Published by an external producer (tc-iam), without Nats-Msg-Id.
        await js.publish(
            f"{config.subject_prefix}.teamcore.tenant.registration", TC_IAM_PAYLOAD
        )
        await wait_for(lambda: len(effects) >= 1, timeout=10)
        await wait_for(lambda: len(effects) >= 2, timeout=4)  # redelivery window
    finally:
        await stop_pod(pod)

    assert lossy_ack["dropped"] == 1
    assert effects == [1], f"redelivered tc-iam event executed {len(effects)} times"

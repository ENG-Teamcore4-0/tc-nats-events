"""
NATS-05 - R3 streams survive the loss of one node.

Requires the cluster from ``tests/cluster/docker-compose.yml`` and
``NATS_CLUSTER_URL`` (``make test-cluster``).
"""

import asyncio
import os
import subprocess
import uuid

import nats
import pytest

from tc_nats_events import EventPublisher, NATSConfig

from ..audit.conftest import wait_for
from ..audit.pods import start_pod, stop_pod

CLUSTER_URL = os.getenv("NATS_CLUSTER_URL")

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(not CLUSTER_URL, reason="NATS_CLUSTER_URL not set"),
]


def _config() -> NATSConfig:
    unique = uuid.uuid4().hex[:8]
    return NATSConfig(
        servers=CLUSTER_URL.split(","),
        environment="production",
        replicas=3,
        stream_name=f"r3-{unique}",
        subject_prefix=f"r3.{unique}",
        max_age_seconds=600,
        nak_delays_seconds=(0.2,),
    )


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


async def _leader(js, stream: str) -> str:
    return (await js.stream_info(stream)).cluster.leader


async def test_r3_stream_survives_node_loss():
    config = _config()
    config.validate()
    nc = await nats.connect(servers=config.servers)
    js = nc.jetstream()
    received = []

    async def handler(event):
        received.append(event.data["id"])

    pod = await start_pod(config, {"order.created": handler})
    publisher = EventPublisher("svc", config)
    await publisher.connect()
    stopped = None
    try:
        info = await js.stream_info(config.stream_name)
        assert info.config.num_replicas == 3
        assert len(info.cluster.replicas or []) == 2

        await publisher.publish("order.created", {"id": 1})
        assert await wait_for(lambda: received == [1], timeout=10)

        stopped = f"tc-nats-{await _leader(js, config.stream_name)}"
        _docker("stop", stopped)
        await asyncio.sleep(3)  # new leader election

        await publisher.publish("order.created", {"id": 2})
        assert await wait_for(lambda: received == [1, 2], timeout=20)
        assert (await js.stream_info(config.stream_name)).state.messages == 2
    finally:
        if stopped:
            _docker("start", stopped)
        await publisher.disconnect()
        await stop_pod(pod)
        for name in (
            config.stream_name,
            config.dlq_stream_name,
            f"KV_{config.idempotency_bucket}",
        ):
            try:
                await js.delete_stream(name)
            except Exception:
                pass
        await nc.close()


def test_r1_rejected_in_production():
    config = _config()
    config.replicas = 1
    with pytest.raises(ValueError, match="replicas"):
        config.validate()

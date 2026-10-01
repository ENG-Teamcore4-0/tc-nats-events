"""NATS-07 - The processing loop dies after 10 consecutive errors and nothing restarts it."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from nats.js.errors import NotFoundError

from tc_nats_events import DurableEventConsumer

from .conftest import make_config, wait_for

_real_sleep = asyncio.sleep


async def _fast_sleep(delay, *args, **kwargs):
    return await _real_sleep(min(delay, 0.01), *args, **kwargs)


def _mock_connection(fetch):
    subscription = MagicMock()
    subscription.fetch = fetch
    js = MagicMock()
    js.stream_info = AsyncMock(side_effect=NotFoundError())  # fresh stream
    js.consumer_info = AsyncMock(side_effect=NotFoundError())  # fresh durable
    js.add_consumer = AsyncMock()
    js.add_stream = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=subscription)
    js.key_value = AsyncMock(return_value=MagicMock())
    js.create_key_value = AsyncMock(return_value=MagicMock())
    nc = MagicMock()
    nc.is_connected = True
    nc.jetstream.return_value = js
    nc.connected_server_version = MagicMock(major=2, minor=10)
    nc.subscribe = AsyncMock()
    nc.drain = AsyncMock()
    nc.close = AsyncMock()
    return nc


async def test_loop_restarts_after_consecutive_errors():
    calls = {"n": 0}

    async def fetch(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] <= 12:
            raise RuntimeError("nats flapping")
        await _real_sleep(0.01)
        return []

    config = make_config(idempotency_backend="memory", dlq_enabled=False)
    consumer = DurableEventConsumer("svc", config)
    with patch("nats.connect", AsyncMock(return_value=_mock_connection(fetch))):
        with patch.object(asyncio, "sleep", _fast_sleep):
            await consumer.start()
            try:
                recovered = await wait_for(
                    lambda: calls["n"] > 13 and consumer.state.value == "live",
                    timeout=5,
                )
                health = consumer.health() if hasattr(consumer, "health") else {}
            finally:
                await consumer.stop()

    assert recovered, f"loop dead: fetch calls={calls['n']}, state={consumer.state}"
    assert health["restarts"] >= 1
    assert health["healthy"] is True

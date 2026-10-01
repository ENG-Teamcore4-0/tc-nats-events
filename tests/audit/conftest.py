"""
Audit regression fixtures
=========================

Tests under ``tests/audit`` prove each finding of the 2026-09-30 audit
(NATS-01..NATS-12). They are written against the *public* API that already
existed in 0.1.7 so the same file can be executed against ``main`` (RED) and
against the fix branch (GREEN).

``make_config`` silently drops fields that the running library version does
not know, so tuning knobs introduced by the fixes (e.g. ``nak_delays_seconds``)
are ignored on 0.1.7 and the test exercises the original behaviour.
"""

import asyncio
import dataclasses
import os
import time
import uuid
from typing import Any, Awaitable, Callable, List

import nats
import pytest
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js import JetStreamContext

import tc_nats_events.utils.idempotency as legacy_idempotency
from tc_nats_events import NATSConfig

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SKIP_INTEGRATION = os.getenv("SKIP_INTEGRATION_TESTS", "false").lower() == "true"

requires_nats = pytest.mark.skipif(
    SKIP_INTEGRATION,
    reason="Integration tests skipped (set SKIP_INTEGRATION_TESTS=false to run)",
)

# Fast retry knobs used by every audit test (ignored by versions without them).
FAST_RETRY = {
    "nak_delays_seconds": (0.2, 0.2, 0.2, 0.2, 0.2),
    "handler_timeout_seconds": 10.0,
    "publish_timeout_seconds": 2.0,
}


def make_config(**overrides: Any) -> NATSConfig:
    """Build a NATSConfig keeping only fields supported by this lib version."""
    known = {f.name for f in dataclasses.fields(NATSConfig)}
    unique = uuid.uuid4().hex[:8]
    base = {
        "servers": [NATS_URL],
        "stream_name": f"audit-{unique}",
        "subject_prefix": f"audit.{unique}",
        "max_messages": 10_000,
        "max_age_seconds": 600,
        **FAST_RETRY,
    }
    base.update(overrides)
    return NATSConfig(**{k: v for k, v in base.items() if k in known})


def new_pod_isolation() -> None:
    """Simulate a separate process: drop any process-wide idempotency cache."""
    legacy_idempotency._global_processor = None


async def wait_for(
    predicate: Callable[[], bool], timeout: float = 15.0, interval: float = 0.05
) -> bool:
    """Poll ``predicate`` until true or timeout. Returns the final value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


async def wait_for_async(
    predicate: Callable[[], Awaitable[bool]], timeout: float = 15.0
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.1)
    return await predicate()


@pytest.fixture
async def nc():
    """Raw NATS connection for arranging and asserting server state."""
    conn = await nats.connect(servers=[NATS_URL])
    yield conn
    await conn.close()


@pytest.fixture
async def js(nc) -> JetStreamContext:
    return nc.jetstream()


@pytest.fixture
async def cleanup(js):
    """Delete every stream created for a config (main, DLQ, KV) after the test."""
    configs: List[NATSConfig] = []
    yield configs.append
    for cfg in configs:
        for name in (
            cfg.stream_name,
            f"{cfg.stream_name}-DLQ",
            f"KV_{cfg.stream_name}-idem",
        ):
            try:
                await js.delete_stream(name)
            except Exception:
                pass


async def stream_msg_count(js: JetStreamContext, stream: str) -> int:
    info = await js.stream_info(stream)
    return info.state.messages


async def stream_exists(js: JetStreamContext, stream: str) -> bool:
    try:
        await js.stream_info(stream)
        return True
    except Exception:
        return False


@pytest.fixture
def lossy_publish(monkeypatch):
    """
    Simulate a lost PubAck: the first JetStream publish is stored by the
    server but the client sees a timeout, so the publisher retries.
    """
    original = JetStreamContext.publish
    state = {"calls": 0}

    async def publish(self, *args: Any, **kwargs: Any):
        state["calls"] += 1
        ack = await original(self, *args, **kwargs)
        if state["calls"] == 1:
            raise NATSTimeoutError()
        return ack

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    return state


@pytest.fixture
def lossy_ack(monkeypatch):
    """
    Simulate a lost consumer ack: the first ``msg.ack()`` is silently dropped
    on the wire, so the server redelivers after ``ack_wait``.
    """
    from nats.aio.msg import Msg

    original = Msg.ack
    state = {"dropped": 0}

    async def ack(self):
        if state["dropped"] == 0:
            state["dropped"] += 1
            self._ackd = True  # client believes it acked; the server never saw it
            return None
        return await original(self)

    monkeypatch.setattr(Msg, "ack", ack)
    return state

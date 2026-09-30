"""
Heartbeats for in-flight work (NATS-08)
=======================================

JetStream redelivers a message once ``ack_wait`` expires, even if the first
attempt is still running. ``periodic`` runs a beat (``msg.in_progress()`` and
lease renewal) while the handler works, and always stops when the block exits
so a finished or failed handler never keeps a message alive.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, Iterable

from nats.aio.msg import Msg

logger = logging.getLogger(__name__)

Beat = Callable[[], Awaitable[None]]


async def _beat_forever(interval: float, beat: Beat) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await beat()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # a failed beat must not kill the handler
            logger.warning(f"Heartbeat failed: {e}")


@asynccontextmanager
async def periodic(interval: float, beat: Beat) -> AsyncIterator[None]:
    task = asyncio.ensure_future(_beat_forever(interval, beat))
    try:
        yield
    finally:
        task.cancel()
        await asyncio.wait([task])  # never swallows a cancellation aimed at us


def touch_all(messages: Iterable[Msg]) -> Beat:
    """Beat that marks every given message as in progress."""

    async def beat() -> None:
        for msg in list(messages):
            try:
                await msg.in_progress()
            except Exception as e:  # one bad message must not starve the others
                logger.warning(f"in_progress() failed: {e}")

    return beat

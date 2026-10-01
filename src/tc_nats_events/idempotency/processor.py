"""
Idempotent processing (NATS-02)
===============================

Runs a handler at most once *successfully* per key across every replica:

* DONE          -> skip (the caller acks).
* IN_PROGRESS   -> another replica owns the lease: wait while touching the
                   message; never burn a delivery attempt nor dead-letter it.
* ACQUIRED      -> run the handler with heartbeats and a timeout; on success
                   mark done, on failure release the lease and re-raise.

Failures are never cached, so a redelivery always runs the handler again.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, List, Optional

from ..utils.heartbeat import Beat, periodic
from .base import AcquireStatus, IdempotencyStore, Lease, LeaseLostError

logger = logging.getLogger(__name__)

MARK_DONE_ATTEMPTS = 3
MARK_DONE_RETRY_DELAY = 0.2


class Outcome(str, Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class RetryLaterError(Exception):
    """The key is still leased by another replica; retry after a delay."""


async def _noop() -> None:
    return None


async def _quietly(beat: Beat) -> None:
    try:
        await beat()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Heartbeat touch failed: {e}")


class IdempotentProcessor:
    def __init__(
        self,
        store: IdempotencyStore,
        lease_seconds: float,
        heartbeat_interval: float,
        handler_timeout: float,
        on_event: Optional[Callable[[str], None]] = None,
    ):
        self._store = store
        self._lease_seconds = lease_seconds
        self._interval = heartbeat_interval
        self._timeout = handler_timeout
        self._on_event = on_event or (lambda _name: None)

    async def run(
        self,
        key: str,
        handler: Callable[[], Awaitable[Any]],
        touch: Beat = _noop,
    ) -> Outcome:
        result = await self._store.try_acquire(key, self._lease_seconds)
        if result.status == AcquireStatus.IN_PROGRESS:
            result = await self._wait_for_owner(key, touch)
        if result.status == AcquireStatus.DONE:
            self._on_event("duplicate_skipped")
            return Outcome.DUPLICATE
        assert result.lease is not None
        return await self._execute(result.lease, handler, touch)

    async def _wait_for_owner(self, key: str, touch: Beat) -> Any:
        """
        Another replica is working on it: keep our copy alive and re-check.

        The owner either finishes, fails (releases) or its lease lapses within
        ``handler_timeout + lease``; waiting that long avoids burning delivery
        attempts with naks for a message that is being handled.
        """
        deadline = time.monotonic() + self._timeout + self._lease_seconds
        while time.monotonic() < deadline:
            await _quietly(touch)
            await asyncio.sleep(self._interval)
            result = await self._store.try_acquire(key, self._lease_seconds)
            if result.status != AcquireStatus.IN_PROGRESS:
                return result
        raise RetryLaterError(f"idempotency key {key} still in progress elsewhere")

    async def _execute(
        self, lease: Lease, handler: Callable[[], Awaitable[Any]], touch: Beat
    ) -> Outcome:
        current: List[Lease] = [lease]
        lost = [False]

        async def beat() -> None:
            # Touching the message and renewing the lease are independent: a
            # failed in_progress() must not let the lease expire (and vice versa).
            await _quietly(touch)
            if lost[0]:
                return
            try:
                current[0] = await self._store.renew(current[0], self._lease_seconds)
            except LeaseLostError:
                lost[0] = True
                self._on_event("lease_lost")
                logger.error(f"Idempotency lease lost while running {lease.key}")

        try:
            async with periodic(self._interval, beat):
                await asyncio.wait_for(handler(), timeout=self._timeout)
        except BaseException:
            await asyncio.shield(self._store.release(current[0]))
            raise

        # The side effect happened: finish recording it even if we are cancelled.
        await asyncio.shield(self._mark_done(current[0]))
        return Outcome.PROCESSED

    async def _mark_done(self, lease: Lease) -> None:
        """The side effect already happened: never re-run it because of us."""
        for attempt in range(1, MARK_DONE_ATTEMPTS + 1):
            try:
                await self._store.mark_done(lease)
                return
            except LeaseLostError:
                self._on_event("lease_lost")
                logger.error(
                    f"Lease for {lease.key} was taken over; handler may have run twice"
                )
                return
            except Exception as e:
                logger.warning(
                    f"mark_done attempt {attempt} failed for {lease.key}: {e}"
                )
                await asyncio.sleep(MARK_DONE_RETRY_DELAY * attempt)
        self._on_event("mark_done_failed")
        logger.error(f"Could not record success for {lease.key}; acking anyway")

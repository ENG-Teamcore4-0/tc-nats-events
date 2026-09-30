"""In-process idempotency store. Local development and tests only."""

import asyncio
import time
import uuid
from dataclasses import dataclass, replace
from typing import Callable, Dict, Optional

from .base import AcquireResult, AcquireStatus, Lease, LeaseLostError

DEFAULT_MAX_KEYS = 100_000


@dataclass(frozen=True)
class _Record:
    state: str
    owner: str
    lease_until: float
    revision: int
    stored_at: float


class MemoryIdempotencyStore:
    """
    Same semantics as the KV store, scoped to one process.

    It does not deduplicate across replicas or restarts; use it only where a
    single process consumes (local development, unit tests). The private map is
    bounded: expired records are swept and the oldest are evicted past
    ``max_keys``.
    """

    def __init__(
        self,
        ttl_seconds: float = 24 * 3600,
        owner: Optional[str] = None,
        clock: Callable[[], float] = time.time,
        max_keys: int = DEFAULT_MAX_KEYS,
    ):
        self._ttl = ttl_seconds
        self._owner = owner or uuid.uuid4().hex
        self._clock = clock
        self._max_keys = max_keys
        # Private, lock-guarded state (records themselves are immutable).
        self._records: Dict[str, _Record] = {}
        self._lock = asyncio.Lock()
        self._revision = 0

    async def setup(self) -> None:
        return None

    def __len__(self) -> int:
        return len(self._records)

    def _next_revision(self) -> int:
        self._revision += 1
        return self._revision

    def _sweep(self, now: float) -> None:
        expired = [k for k, r in self._records.items() if now - r.stored_at > self._ttl]
        for key in expired:
            del self._records[key]
        overflow = len(self._records) - self._max_keys
        for key in list(self._records)[: max(overflow, 0)]:
            del self._records[key]  # dicts keep insertion order: oldest first

    def _live(self, key: str, now: float) -> Optional[_Record]:
        record = self._records.get(key)
        if record and now - record.stored_at > self._ttl:
            del self._records[key]
            return None
        return record

    async def try_acquire(self, key: str, lease_seconds: float) -> AcquireResult:
        async with self._lock:
            now = self._clock()
            record = self._live(key, now)
            if record and record.state == "done":
                return AcquireResult(AcquireStatus.DONE)
            if record and record.lease_until > now:
                return AcquireResult(AcquireStatus.IN_PROGRESS)
            revision = self._next_revision()
            self._records[key] = _Record(
                "processing", self._owner, now + lease_seconds, revision, now
            )
            if len(self._records) > self._max_keys:
                self._sweep(now)
            return AcquireResult(
                AcquireStatus.ACQUIRED, Lease(key, self._owner, revision)
            )

    def _owned(self, lease: Lease) -> _Record:
        record = self._records.get(lease.key)
        if record is None or record.revision != lease.revision:
            raise LeaseLostError(f"lease lost for {lease.key}")
        return record

    async def renew(self, lease: Lease, lease_seconds: float) -> Lease:
        async with self._lock:
            record = self._owned(lease)
            revision = self._next_revision()
            self._records[lease.key] = replace(
                record, lease_until=self._clock() + lease_seconds, revision=revision
            )
            return replace(lease, revision=revision)

    async def mark_done(self, lease: Lease) -> None:
        async with self._lock:
            record = self._owned(lease)
            self._records[lease.key] = replace(
                record, state="done", revision=self._next_revision()
            )

    async def release(self, lease: Lease) -> None:
        async with self._lock:
            record = self._records.get(lease.key)
            if record is not None and record.revision == lease.revision:
                del self._records[lease.key]

    async def status(self, key: str) -> Optional[str]:
        async with self._lock:
            now = self._clock()
            record = self._live(key, now)
            if record is None:
                return None
            if record.state == "done":
                return "done"
            return "processing" if record.lease_until > now else None

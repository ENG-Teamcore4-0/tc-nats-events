"""
NATS JetStream KV idempotency store
===================================

Shared by every replica and durable across restarts, without extra
infrastructure. Atomicity comes from KV optimistic concurrency:

* ``kv.create`` succeeds for exactly one replica (the lease owner).
* ``kv.update(last=revision)`` renews / completes only if nobody else wrote.

Per-key TTLs need nats-server 2.11+, so leases carry ``lease_until`` in the
value and the bucket-wide TTL bounds how long "done" markers are remembered.
"""

import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional

from nats.js import JetStreamContext
from nats.js.api import KeyValueConfig
from nats.js.errors import (
    APIError,
    BucketNotFoundError,
    KeyWrongLastSequenceError,
    NotFoundError,
)
from nats.js.kv import KeyValue

from .base import (
    AcquireResult,
    AcquireStatus,
    IdempotencyStoreError,
    Lease,
    LeaseLostError,
)

logger = logging.getLogger(__name__)

WRONG_LAST_SEQUENCE = 10071
MAX_ACQUIRE_ROUNDS = 5


class NatsKVIdempotencyStore:
    def __init__(
        self,
        js: JetStreamContext,
        bucket: str,
        ttl_seconds: float,
        replicas: int = 1,
        owner: Optional[str] = None,
        clock_skew_seconds: float = 2.0,
        clock: Callable[[], float] = time.time,
    ):
        self._js = js
        self._bucket = bucket
        self._ttl = ttl_seconds
        self._replicas = replicas
        self._owner = owner or uuid.uuid4().hex
        self._skew = clock_skew_seconds
        self._clock = clock
        self._kv: Optional[KeyValue] = None

    @property
    def kv(self) -> KeyValue:
        if self._kv is None:
            raise IdempotencyStoreError("idempotency store not set up")
        return self._kv

    async def setup(self) -> None:
        try:
            try:
                self._kv = await self._js.key_value(self._bucket)
                await self._warn_on_bucket_drift()
            except BucketNotFoundError:
                self._kv = await self._js.create_key_value(
                    KeyValueConfig(
                        bucket=self._bucket,
                        ttl=self._ttl,
                        history=1,
                        replicas=self._replicas,
                        description="tc-nats-events idempotency markers",
                    )
                )
                logger.info(f"Created idempotency KV bucket '{self._bucket}'")
        except Exception as e:
            raise IdempotencyStoreError(
                f"Cannot open idempotency KV bucket '{self._bucket}': {e}. "
                "The service account needs publish/subscribe on "
                f"'$KV.{self._bucket}.>' and JetStream API access to "
                f"'KV_{self._bucket}' (or set idempotency_backend='memory' "
                "for local development)."
            ) from e

    async def _warn_on_bucket_drift(self) -> None:
        try:
            status = await self.kv.status()
            ttl = getattr(status, "ttl", None)
            if not ttl:
                logger.warning(
                    f"Idempotency bucket '{self._bucket}' has no TTL: done markers "
                    "never expire and the bucket grows without bound"
                )
            elif float(ttl) + 1 < self._ttl:
                logger.warning(
                    f"Idempotency bucket '{self._bucket}' TTL is {ttl}s, shorter than "
                    f"the configured {self._ttl}s: dedupe horizon is reduced"
                )
        except Exception as e:  # informational only
            logger.debug(f"Could not inspect bucket '{self._bucket}': {e}")

    def _encode(self, state: str, lease_seconds: float = 0.0) -> bytes:
        record = {
            "state": state,
            "owner": self._owner,
            "lease_until": self._clock() + lease_seconds,
        }
        return json.dumps(record, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _decode(value: Optional[bytes]) -> Dict[str, Any]:
        """Parse a record; anything malformed is treated as an expired lease."""
        try:
            record = json.loads(value or b"{}")
        except (ValueError, UnicodeDecodeError):
            return {}
        if not isinstance(record, dict):
            return {}
        try:
            record["lease_until"] = float(record.get("lease_until", 0))
        except (TypeError, ValueError):
            record["lease_until"] = 0.0
        return record

    async def try_acquire(self, key: str, lease_seconds: float) -> AcquireResult:
        kv = self.kv
        for _ in range(MAX_ACQUIRE_ROUNDS):
            try:
                revision = await kv.create(
                    key, self._encode("processing", lease_seconds)
                )
                return AcquireResult(
                    AcquireStatus.ACQUIRED, Lease(key, self._owner, revision)
                )
            except KeyWrongLastSequenceError:
                pass  # someone holds (or held) the key

            try:
                entry = await kv.get(key)
            except NotFoundError:  # KeyNotFound / KeyDeleted: released meanwhile
                continue

            record = self._decode(entry.value)
            if record.get("state") == "done":
                return AcquireResult(AcquireStatus.DONE)
            if record["lease_until"] + self._skew > self._clock():
                return AcquireResult(AcquireStatus.IN_PROGRESS)

            # Stale lease (owner crashed): take over with compare-and-set.
            try:
                revision = await kv.update(
                    key, self._encode("processing", lease_seconds), last=entry.revision
                )
                logger.warning(
                    f"Took over stale idempotency lease {key} from {record.get('owner')}"
                )
                return AcquireResult(
                    AcquireStatus.ACQUIRED, Lease(key, self._owner, revision)
                )
            except KeyWrongLastSequenceError:
                continue
        raise IdempotencyStoreError(f"Could not acquire idempotency key {key}")

    async def _current_revision(self, lease: Lease) -> Optional[int]:
        """
        After a wrong-last-sequence error, check whether the key is still ours.

        A client-side timeout on a write that the server applied advances the
        revision without us knowing; re-reading avoids mistaking that for a
        takeover (which would skip mark_done and re-run the handler later).
        """
        try:
            entry = await self.kv.get(lease.key)
        except NotFoundError:
            return None
        record = self._decode(entry.value)
        if record.get("owner") == self._owner:
            return entry.revision
        return None

    async def _write(self, lease: Lease, value: bytes) -> int:
        try:
            return await self.kv.update(lease.key, value, last=lease.revision)
        except KeyWrongLastSequenceError as e:
            revision = await self._current_revision(lease)
            if revision is None:
                raise LeaseLostError(f"lease lost for {lease.key}") from e
            return await self.kv.update(lease.key, value, last=revision)

    async def renew(self, lease: Lease, lease_seconds: float) -> Lease:
        try:
            revision = await self._write(
                lease, self._encode("processing", lease_seconds)
            )
        except KeyWrongLastSequenceError as e:
            raise LeaseLostError(f"lease lost for {lease.key}") from e
        return Lease(lease.key, lease.owner, revision)

    async def mark_done(self, lease: Lease) -> None:
        try:
            await self._write(lease, self._encode("done"))
        except KeyWrongLastSequenceError as e:
            raise LeaseLostError(f"lease lost for {lease.key}") from e

    async def release(self, lease: Lease) -> None:
        revision: Optional[int] = lease.revision
        for _ in range(2):
            try:
                await self.kv.delete(lease.key, last=revision)
                return
            except (KeyWrongLastSequenceError, APIError) as e:
                wrong_last = isinstance(e, KeyWrongLastSequenceError) or (
                    getattr(e, "err_code", None) == WRONG_LAST_SEQUENCE
                )
                if not wrong_last:
                    logger.warning(
                        f"Could not release idempotency lease {lease.key}: {e}"
                    )
                    return
                revision = await self._current_revision(lease)
                if revision is None:
                    return  # someone else owns it now; nothing to release
            except Exception as e:
                # The lease expires on its own; releasing is an optimisation.
                logger.warning(f"Could not release idempotency lease {lease.key}: {e}")
                return

    async def status(self, key: str) -> Optional[str]:
        """``"done"``, ``"processing"`` (live lease) or ``None``."""
        try:
            entry = await self.kv.get(key)
        except NotFoundError:
            return None
        record = self._decode(entry.value)
        if record.get("state") == "done":
            return "done"
        if record["lease_until"] + self._skew > self._clock():
            return "processing"
        return None

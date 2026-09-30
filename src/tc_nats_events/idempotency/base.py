"""
Idempotency primitives
======================

A store records, per idempotency key, whether a message is being processed
(a lease owned by one replica) or has been processed successfully. Failures
are never recorded: a failed attempt releases its lease so the redelivery
runs the handler again (NATS-02).
"""

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Protocol

from ..utils.exceptions import TCNATSError


class IdempotencyStoreError(TCNATSError):
    """The idempotency store is unreachable or misconfigured."""


class LeaseLostError(TCNATSError):
    """Another replica took over the lease (it expired while we were working)."""


class AcquireStatus(str, Enum):
    ACQUIRED = "acquired"
    IN_PROGRESS = "in_progress"
    DONE = "done"


@dataclass(frozen=True)
class Lease:
    """Proof of ownership of a key at a given store revision."""

    key: str
    owner: str
    revision: int


@dataclass(frozen=True)
class AcquireResult:
    status: AcquireStatus
    lease: Optional[Lease] = None


class IdempotencyStore(Protocol):
    """Async store shared by every replica of a consumer."""

    async def setup(self) -> None:
        """Prepare backing resources. Fail fast on permissions."""

    async def try_acquire(self, key: str, lease_seconds: float) -> AcquireResult:
        """Atomically take the lease, or report IN_PROGRESS / DONE."""

    async def renew(self, lease: Lease, lease_seconds: float) -> Lease:
        """Extend a lease. Raises LeaseLostError if it is no longer ours."""

    async def mark_done(self, lease: Lease) -> None:
        """Record success. Raises LeaseLostError if it is no longer ours."""

    async def release(self, lease: Lease) -> None:
        """Drop the lease after a failure so a redelivery can retry."""

    async def status(self, key: str) -> Optional[str]:
        """``"done"``, ``"processing"`` (live lease) or ``None`` (free)."""


def idempotency_key(scope: str, message_id: str) -> str:
    """
    Build a store key that is safe for any backend and collision free.

    Hashing (instead of replacing characters) guarantees that distinct ids such
    as ``a/b`` and ``a_b`` never map to the same key.
    """
    digest = hashlib.sha256(f"{scope}\x00{message_id}".encode("utf-8")).hexdigest()
    return f"k{digest}"

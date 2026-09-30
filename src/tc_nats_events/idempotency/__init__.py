"""Idempotent consumption backed by a shared store (default: NATS JetStream KV)."""

from .base import (
    AcquireResult,
    AcquireStatus,
    IdempotencyStore,
    IdempotencyStoreError,
    Lease,
    LeaseLostError,
    idempotency_key,
)
from .memory import MemoryIdempotencyStore
from .nats_kv import NatsKVIdempotencyStore
from .processor import IdempotentProcessor, Outcome, RetryLaterError

__all__ = [
    "AcquireResult",
    "AcquireStatus",
    "IdempotencyStore",
    "IdempotencyStoreError",
    "IdempotentProcessor",
    "Lease",
    "LeaseLostError",
    "MemoryIdempotencyStore",
    "NatsKVIdempotencyStore",
    "Outcome",
    "RetryLaterError",
    "idempotency_key",
]

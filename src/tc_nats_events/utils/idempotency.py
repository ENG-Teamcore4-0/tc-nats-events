"""
Idempotency Utilities
====================

Utilities for ensuring idempotent event processing.
"""

import hashlib
import json
import threading
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set


class IdempotencyKey:
    """Idempotency key for event processing."""

    def __init__(self, event_id: str, handler_name: str):
        """
        Create idempotency key.

        Args:
            event_id: Unique event identifier
            handler_name: Name of the handler processing the event
        """
        self.event_id = event_id
        self.handler_name = handler_name
        self.key = f"{handler_name}:{event_id}"

    def __str__(self) -> str:
        return self.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other) -> bool:
        if isinstance(other, IdempotencyKey):
            return self.key == other.key
        return False


class ProcessingResult:
    """Result of event processing for idempotency tracking."""

    def __init__(
        self,
        success: bool,
        result: Any = None,
        error: Optional[Exception] = None,
        processed_at: Optional[datetime] = None,
    ):
        self.success = success
        self.result = result
        self.error = error
        self.processed_at = processed_at or datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "result": self.result,
            "error": str(self.error) if self.error else None,
            "processed_at": self.processed_at.isoformat(),
        }


class InMemoryIdempotencyStore:
    """In-memory store for idempotency tracking."""

    def __init__(self, max_size: int = 10000, ttl_hours: int = 24):
        """
        Initialize store.

        Args:
            max_size: Maximum number of keys to store
            ttl_hours: TTL for stored keys in hours
        """
        self.max_size = max_size
        self.ttl = timedelta(hours=ttl_hours)
        self._store: OrderedDict[IdempotencyKey, ProcessingResult] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: IdempotencyKey) -> Optional[ProcessingResult]:
        """Get processing result for key."""
        with self._lock:
            self._cleanup_expired()
            result = self._store.get(key)
            if result:
                # Move to end (LRU)
                self._store.move_to_end(key)
            return result

    def set(self, key: IdempotencyKey, result: ProcessingResult) -> None:
        """Store processing result for key."""
        with self._lock:
            self._store[key] = result
            self._store.move_to_end(key)

            # Evict oldest if over max size
            while len(self._store) > self.max_size:
                self._store.popitem(last=False)

    def _cleanup_expired(self) -> None:
        """Remove expired entries."""
        now = datetime.now(timezone.utc)
        expired_keys = [
            key
            for key, result in self._store.items()
            if now - result.processed_at > self.ttl
        ]

        for key in expired_keys:
            del self._store[key]

    def size(self) -> int:
        """Get current store size."""
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._store.clear()


class IdempotentEventProcessor:
    """Processor that ensures idempotent event handling."""

    def __init__(self, store: Optional[InMemoryIdempotencyStore] = None):
        """
        Initialize processor.

        Args:
            store: Idempotency store (creates default if None)
        """
        self.store = store or InMemoryIdempotencyStore()

    async def process_with_idempotency(
        self, event_id: str, handler_name: str, handler_func, *args, **kwargs
    ) -> Any:
        """
        Process event with idempotency guarantee.

        Args:
            event_id: Unique event identifier
            handler_name: Name of the handler
            handler_func: Function to execute
            *args: Arguments for handler
            **kwargs: Keyword arguments for handler

        Returns:
            Result of handler execution

        Raises:
            Exception: If handler fails after retries
        """
        key = IdempotencyKey(event_id, handler_name)

        # Check if already processed
        existing_result = self.store.get(key)
        if existing_result:
            if existing_result.success:
                return existing_result.result
            else:
                # Re-raise the previous error
                raise existing_result.error or Exception("Previous processing failed")

        # Process the event
        try:
            if asyncio.iscoroutinefunction(handler_func):
                result = await handler_func(*args, **kwargs)
            else:
                result = handler_func(*args, **kwargs)

            # Store successful result
            self.store.set(key, ProcessingResult(success=True, result=result))
            return result

        except Exception as e:
            # Store failed result
            self.store.set(key, ProcessingResult(success=False, error=e))
            raise


def generate_deterministic_id(
    data: Dict[str, Any], fields: Optional[list] = None
) -> str:
    """
    Generate deterministic ID from event data.

    Args:
        data: Event data
        fields: Specific fields to use (uses all if None)

    Returns:
        Deterministic ID based on data
    """
    if fields:
        relevant_data = {k: data.get(k) for k in fields if k in data}
    else:
        relevant_data = data

    # Sort keys for consistency
    json_str = json.dumps(relevant_data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(json_str.encode()).hexdigest()[:16]


# Global idempotent processor
_global_processor: Optional[IdempotentEventProcessor] = None
_processor_lock = threading.Lock()


def get_idempotent_processor() -> IdempotentEventProcessor:
    """Get global idempotent processor."""
    global _global_processor
    with _processor_lock:
        if _global_processor is None:
            _global_processor = IdempotentEventProcessor()
        return _global_processor


# Import asyncio here to avoid circular imports
import asyncio

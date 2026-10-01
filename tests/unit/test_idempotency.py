"""
Unit Tests for Idempotency
==========================

Test the idempotency utilities.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from tc_nats_events.utils.idempotency import (
    IdempotencyKey,
    IdempotentEventProcessor,
    InMemoryIdempotencyStore,
    ProcessingResult,
    generate_deterministic_id,
    get_idempotent_processor,
)


class TestIdempotencyKey:
    """Test IdempotencyKey class."""

    def test_key_creation(self):
        """Test creating idempotency keys."""
        key = IdempotencyKey("event-123", "handler-name")

        assert key.event_id == "event-123"
        assert key.handler_name == "handler-name"
        assert key.key == "handler-name:event-123"
        assert str(key) == "handler-name:event-123"

    def test_key_equality(self):
        """Test key equality and hashing."""
        key1 = IdempotencyKey("event-123", "handler-name")
        key2 = IdempotencyKey("event-123", "handler-name")
        key3 = IdempotencyKey("event-456", "handler-name")

        assert key1 == key2
        assert key1 != key3
        assert hash(key1) == hash(key2)
        assert hash(key1) != hash(key3)

    def test_key_in_set(self):
        """Test using keys in sets."""
        key1 = IdempotencyKey("event-123", "handler-name")
        key2 = IdempotencyKey("event-123", "handler-name")
        key3 = IdempotencyKey("event-456", "handler-name")

        key_set = {key1, key2, key3}
        assert len(key_set) == 2  # key1 and key2 are the same


class TestProcessingResult:
    """Test ProcessingResult class."""

    def test_successful_result(self):
        """Test successful processing result."""
        result = ProcessingResult(success=True, result="processed")

        assert result.success is True
        assert result.result == "processed"
        assert result.error is None
        assert isinstance(result.processed_at, datetime)

    def test_failed_result(self):
        """Test failed processing result."""
        error = ValueError("Processing failed")
        result = ProcessingResult(success=False, error=error)

        assert result.success is False
        assert result.result is None
        assert result.error == error

    def test_to_dict(self):
        """Test converting result to dictionary."""
        result = ProcessingResult(success=True, result="data")
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["result"] == "data"
        assert result_dict["error"] is None
        assert "processed_at" in result_dict


class TestInMemoryIdempotencyStore:
    """Test InMemoryIdempotencyStore class."""

    def test_store_and_get(self):
        """Test storing and retrieving results."""
        store = InMemoryIdempotencyStore()
        key = IdempotencyKey("event-123", "handler")
        result = ProcessingResult(success=True, result="data")

        # Initially not present
        assert store.get(key) is None

        # Store and retrieve
        store.set(key, result)
        retrieved = store.get(key)

        assert retrieved is not None
        assert retrieved.success is True
        assert retrieved.result == "data"

    def test_lru_eviction(self):
        """Test LRU eviction when max size exceeded."""
        store = InMemoryIdempotencyStore(max_size=2)

        key1 = IdempotencyKey("event-1", "handler")
        key2 = IdempotencyKey("event-2", "handler")
        key3 = IdempotencyKey("event-3", "handler")

        result = ProcessingResult(success=True)

        # Store 2 items (at max capacity)
        store.set(key1, result)
        store.set(key2, result)
        assert store.size() == 2

        # Store 3rd item, should evict oldest (key1)
        store.set(key3, result)
        assert store.size() == 2
        assert store.get(key1) is None  # Evicted
        assert store.get(key2) is not None
        assert store.get(key3) is not None

    def test_lru_access_updates_order(self):
        """Test that accessing an item updates its position in LRU."""
        store = InMemoryIdempotencyStore(max_size=2)

        key1 = IdempotencyKey("event-1", "handler")
        key2 = IdempotencyKey("event-2", "handler")
        key3 = IdempotencyKey("event-3", "handler")

        result = ProcessingResult(success=True)

        # Store 2 items
        store.set(key1, result)
        store.set(key2, result)

        # Access key1 (makes it most recent)
        store.get(key1)

        # Add key3, should evict key2 (oldest)
        store.set(key3, result)

        assert store.get(key1) is not None  # Still present
        assert store.get(key2) is None  # Evicted
        assert store.get(key3) is not None

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        store = InMemoryIdempotencyStore(ttl_hours=1)
        key = IdempotencyKey("event-123", "handler")

        # Create result with old timestamp
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        result = ProcessingResult(success=True, processed_at=old_time)

        # Manually add to store
        store._store[key] = result

        # Should be cleaned up on next get
        retrieved = store.get(key)
        assert retrieved is None
        assert store.size() == 0

    def test_clear(self):
        """Test clearing the store."""
        store = InMemoryIdempotencyStore()
        key = IdempotencyKey("event-123", "handler")
        result = ProcessingResult(success=True)

        store.set(key, result)
        assert store.size() == 1

        store.clear()
        assert store.size() == 0
        assert store.get(key) is None


class TestIdempotentEventProcessor:
    """Test IdempotentEventProcessor class."""

    @pytest.mark.asyncio
    async def test_first_execution(self):
        """Test first execution of a handler."""
        processor = IdempotentEventProcessor()

        call_count = 0

        def test_handler():
            nonlocal call_count
            call_count += 1
            return "result"

        result = await processor.process_with_idempotency(
            event_id="event-123", handler_name="test_handler", handler_func=test_handler
        )

        assert result == "result"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_idempotent_execution(self):
        """Test that second execution returns cached result."""
        processor = IdempotentEventProcessor()

        call_count = 0

        def test_handler():
            nonlocal call_count
            call_count += 1
            return "result"

        # First execution
        result1 = await processor.process_with_idempotency(
            event_id="event-123", handler_name="test_handler", handler_func=test_handler
        )

        # Second execution (should use cache)
        result2 = await processor.process_with_idempotency(
            event_id="event-123", handler_name="test_handler", handler_func=test_handler
        )

        assert result1 == result2 == "result"
        assert call_count == 1  # Handler called only once

    @pytest.mark.asyncio
    async def test_async_handler(self):
        """Test with async handler function."""
        processor = IdempotentEventProcessor()

        call_count = 0

        async def async_handler():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.001)
            return "async_result"

        result = await processor.process_with_idempotency(
            event_id="event-123",
            handler_name="async_handler",
            handler_func=async_handler,
        )

        assert result == "async_result"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_handler_with_arguments(self):
        """Test handler with arguments."""
        processor = IdempotentEventProcessor()

        def handler_with_args(arg1, arg2, kwarg1=None):
            return f"{arg1}-{arg2}-{kwarg1}"

        result = await processor.process_with_idempotency(
            "event-123",
            "handler_with_args",
            handler_with_args,
            "value1",
            "value2",
            kwarg1="kwvalue",
        )

        assert result == "value1-value2-kwvalue"

    @pytest.mark.asyncio
    async def test_handler_failure_not_cached(self):
        """Failures are never cached: a retry re-runs the handler (NATS-02)."""
        processor = IdempotentEventProcessor()

        call_count = 0

        def flaky_handler():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Handler failed")
            return "ok"

        with pytest.raises(ValueError, match="Handler failed"):
            await processor.process_with_idempotency(
                event_id="event-123",
                handler_name="flaky_handler",
                handler_func=flaky_handler,
            )

        # The retry runs the handler again instead of replaying the error
        result = await processor.process_with_idempotency(
            event_id="event-123",
            handler_name="flaky_handler",
            handler_func=flaky_handler,
        )

        assert result == "ok"
        assert call_count == 2

        # Success IS cached: a third call does not run the handler again
        await processor.process_with_idempotency(
            event_id="event-123",
            handler_name="flaky_handler",
            handler_func=flaky_handler,
        )
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_different_handlers_same_event(self):
        """Test different handlers for same event ID."""
        processor = IdempotentEventProcessor()

        def handler1():
            return "result1"

        def handler2():
            return "result2"

        # Same event ID, different handlers
        result1 = await processor.process_with_idempotency(
            event_id="event-123", handler_name="handler1", handler_func=handler1
        )

        result2 = await processor.process_with_idempotency(
            event_id="event-123", handler_name="handler2", handler_func=handler2
        )

        assert result1 == "result1"
        assert result2 == "result2"


class TestDeterministicId:
    """Test deterministic ID generation."""

    def test_same_data_same_id(self):
        """Test that same data produces same ID."""
        data = {"user_id": "123", "action": "create", "timestamp": "2024-01-01"}

        id1 = generate_deterministic_id(data)
        id2 = generate_deterministic_id(data)

        assert id1 == id2
        assert len(id1) == 16  # 16 character hash

    def test_different_data_different_id(self):
        """Test that different data produces different IDs."""
        data1 = {"user_id": "123", "action": "create"}
        data2 = {"user_id": "456", "action": "create"}

        id1 = generate_deterministic_id(data1)
        id2 = generate_deterministic_id(data2)

        assert id1 != id2

    def test_field_subset(self):
        """Test generating ID from specific fields."""
        data = {"user_id": "123", "action": "create", "timestamp": "2024-01-01"}

        id1 = generate_deterministic_id(data, fields=["user_id", "action"])
        id2 = generate_deterministic_id(data, fields=["user_id", "action"])

        # Same subset should produce same ID
        assert id1 == id2

        # Different subset should produce different ID
        id3 = generate_deterministic_id(data, fields=["user_id", "timestamp"])
        assert id1 != id3

    def test_key_order_independence(self):
        """Test that key order doesn't affect ID."""
        data1 = {"b": 2, "a": 1, "c": 3}
        data2 = {"a": 1, "b": 2, "c": 3}

        id1 = generate_deterministic_id(data1)
        id2 = generate_deterministic_id(data2)

        assert id1 == id2


class TestGlobalProcessor:
    """Test global processor registry."""

    def test_get_global_processor(self):
        """Test getting global processor."""
        processor1 = get_idempotent_processor()
        processor2 = get_idempotent_processor()

        # Should return same instance
        assert processor1 is processor2

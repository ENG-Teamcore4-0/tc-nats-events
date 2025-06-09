"""
Pytest Configuration and Fixtures
=================================

Common fixtures and configuration for all tests.
"""

import asyncio
import pytest
import pytest_asyncio
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

from tc_nats_events import NATSConfig, Event, EventMetadata


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def nats_config():
    """Create a test NATS configuration."""
    return NATSConfig(
        servers=["nats://localhost:4222"],
        client_name="test-client",
        stream_name="test-events",
        subject_prefix="test.events",
        max_messages=1000,
        max_age_seconds=3600,
        reconnect_time_wait=1,
        max_reconnect_attempts=3,
    )


@pytest.fixture
def sample_event():
    """Create a sample event for testing."""
    return Event(
        event_type="test.created",
        data={"id": "123", "name": "Test Item"},
        metadata=EventMetadata(
            event_id="event-123",
            correlation_id="corr-123",
            source_service="test-service",
            user_id="user-123",
            environment="test"
        )
    )


@pytest.fixture
def sample_events():
    """Create multiple sample events for testing."""
    events = []
    for i in range(5):
        events.append(
            Event(
                event_type=f"test.event_{i}",
                data={"id": str(i), "value": f"value_{i}"},
                metadata=EventMetadata(
                    event_id=f"event-{i}",
                    source_service="test-service"
                )
            )
        )
    return events


@pytest.fixture
def mock_nats_client():
    """Create a mock NATS client."""
    client = AsyncMock()
    client.is_connected = True
    client.jetstream = MagicMock()
    return client


@pytest.fixture
def mock_jetstream():
    """Create a mock JetStream context."""
    js = AsyncMock()
    
    # Mock stream info
    stream_info = MagicMock()
    stream_info.config.subjects = ["test.events.*"]
    stream_info.state.messages = 100
    stream_info.state.bytes = 10000
    stream_info.state.first_seq = 1
    stream_info.state.last_seq = 100
    stream_info.state.consumer_count = 2
    
    js.stream_info = AsyncMock(return_value=stream_info)
    
    # Mock publish
    ack = MagicMock()
    ack.seq = 101
    ack.stream = "test-events"
    js.publish = AsyncMock(return_value=ack)
    
    # Mock consumer info
    consumer_info = MagicMock()
    consumer_info.delivered.stream_seq = 50
    consumer_info.num_ack_pending = 5
    js.consumer_info = AsyncMock(return_value=consumer_info)
    
    return js


@pytest.fixture
def mock_subscription():
    """Create a mock subscription."""
    subscription = AsyncMock()
    
    # Mock message
    msg = AsyncMock()
    msg.data = b'{"event_type": "test.created", "data": {"id": "123"}, "timestamp": "2024-01-01T00:00:00Z"}'
    msg.metadata.sequence.stream = 1
    
    # Mock fetch
    subscription.fetch = AsyncMock(return_value=[msg])
    
    return subscription


class MockNATSMessage:
    """Mock NATS message for testing."""
    
    def __init__(self, data: bytes, sequence: int = 1):
        self.data = data
        self.metadata = MagicMock()
        self.metadata.sequence.stream = sequence
        self._acked = False
        self._nacked = False
    
    async def ack(self):
        """Mock acknowledgment."""
        self._acked = True
    
    async def nak(self):
        """Mock negative acknowledgment."""
        self._nacked = True
    
    @property
    def is_acked(self):
        """Check if message was acknowledged."""
        return self._acked
    
    @property
    def is_nacked(self):
        """Check if message was negatively acknowledged."""
        return self._nacked


@pytest.fixture
def mock_nats_message():
    """Create a mock NATS message."""
    event_data = {
        "event_type": "test.created",
        "data": {"id": "123", "name": "Test"},
        "timestamp": "2024-01-01T00:00:00Z",
        "metadata": {
            "event_id": "evt-123",
            "source_service": "test"
        }
    }
    
    import json
    return MockNATSMessage(json.dumps(event_data).encode('utf-8'))


@pytest.fixture
async def cleanup_streams():
    """Cleanup test streams after tests."""
    # This would connect to actual NATS and cleanup in integration tests
    yield
    # Cleanup code here if needed
"""
Unit Tests for Event Models
===========================

Test the Event and EventMetadata classes.
"""

import pytest
import json
from datetime import datetime, timezone

from tc_nats_events.models.event import Event, EventMetadata, EventType


class TestEventMetadata:
    """Test EventMetadata class."""
    
    def test_metadata_creation_with_defaults(self):
        """Test creating metadata with default values."""
        metadata = EventMetadata()
        
        assert metadata.event_id is not None
        assert len(metadata.event_id) > 0
        assert metadata.correlation_id is None
        assert metadata.causation_id is None
        assert metadata.source_service is None
        assert metadata.user_id is None
        assert metadata.environment is None
        assert metadata.version == "1.0"
    
    def test_metadata_creation_with_values(self):
        """Test creating metadata with all values."""
        metadata = EventMetadata(
            event_id="evt-123",
            correlation_id="corr-456",
            causation_id="cause-789",
            source_service="test-service",
            user_id="user-123",
            environment="production",
            version="2.0"
        )
        
        assert metadata.event_id == "evt-123"
        assert metadata.correlation_id == "corr-456"
        assert metadata.causation_id == "cause-789"
        assert metadata.source_service == "test-service"
        assert metadata.user_id == "user-123"
        assert metadata.environment == "production"
        assert metadata.version == "2.0"
    
    def test_metadata_immutability(self):
        """Test that metadata is immutable."""
        metadata = EventMetadata(event_id="evt-123")
        
        with pytest.raises(AttributeError):
            metadata.event_id = "new-id"
    
    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary."""
        metadata = EventMetadata(
            event_id="evt-123",
            correlation_id="corr-456",
            source_service="test-service"
        )
        
        result = metadata.to_dict()
        
        assert result["event_id"] == "evt-123"
        assert result["correlation_id"] == "corr-456"
        assert result["source_service"] == "test-service"
        assert result["version"] == "1.0"
        
        # None values should be excluded
        assert "causation_id" not in result
        assert "user_id" not in result
        assert "environment" not in result


class TestEvent:
    """Test Event class."""
    
    def test_event_creation_minimal(self):
        """Test creating event with minimal data."""
        event = Event(
            event_type="test.created",
            data={"id": "123"}
        )
        
        assert event.event_type == "test.created"
        assert event.data == {"id": "123"}
        assert event.timestamp is not None
        assert event.metadata is not None
        assert event.sequence is None
    
    def test_event_creation_full(self):
        """Test creating event with all data."""
        metadata = EventMetadata(
            event_id="evt-123",
            source_service="test-service"
        )
        
        event = Event(
            event_type="test.updated",
            data={"id": "123", "name": "Test"},
            timestamp="2024-01-01T00:00:00Z",
            metadata=metadata,
            sequence=42
        )
        
        assert event.event_type == "test.updated"
        assert event.data == {"id": "123", "name": "Test"}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata == metadata
        assert event.sequence == 42
    
    def test_event_auto_timestamp(self):
        """Test automatic timestamp generation."""
        event = Event(
            event_type="test.created",
            data={"id": "123"}
        )
        
        # Parse timestamp to verify it's valid ISO format
        timestamp = datetime.fromisoformat(event.timestamp.replace('Z', '+00:00'))
        assert timestamp.tzinfo is not None
    
    def test_event_immutability(self):
        """Test that event is immutable."""
        event = Event(
            event_type="test.created",
            data={"id": "123"}
        )
        
        with pytest.raises(AttributeError):
            event.event_type = "test.updated"
        
        with pytest.raises(AttributeError):
            event.data = {"id": "456"}
    
    def test_event_to_json(self):
        """Test serializing event to JSON."""
        event = Event(
            event_type="test.created",
            data={"id": "123", "name": "Test"},
            timestamp="2024-01-01T00:00:00Z"
        )
        
        json_bytes = event.to_json()
        json_data = json.loads(json_bytes.decode('utf-8'))
        
        assert json_data["event_type"] == "test.created"
        assert json_data["data"] == {"id": "123", "name": "Test"}
        assert json_data["timestamp"] == "2024-01-01T00:00:00Z"
        assert "metadata" in json_data
    
    def test_event_from_json(self):
        """Test deserializing event from JSON."""
        json_data = {
            "event_type": "test.created",
            "data": {"id": "123", "name": "Test"},
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {
                "event_id": "evt-123",
                "source_service": "test-service"
            },
            "sequence": 42
        }
        
        json_bytes = json.dumps(json_data).encode('utf-8')
        event = Event.from_json(json_bytes)
        
        assert event.event_type == "test.created"
        assert event.data == {"id": "123", "name": "Test"}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata.event_id == "evt-123"
        assert event.metadata.source_service == "test-service"
        assert event.sequence == 42
    
    def test_event_from_json_invalid(self):
        """Test deserializing invalid JSON."""
        # Invalid JSON
        with pytest.raises(ValueError, match="Invalid JSON"):
            Event.from_json(b"not json")
        
        # Missing required fields
        with pytest.raises(ValueError, match="Missing required fields"):
            Event.from_json(b'{"event_type": "test"}')
    
    def test_event_with_sequence(self):
        """Test creating event with sequence number."""
        event = Event(
            event_type="test.created",
            data={"id": "123"}
        )
        
        # Original event should not have sequence
        assert event.sequence is None
        
        # Create new event with sequence
        event_with_seq = event.with_sequence(42)
        
        assert event_with_seq.sequence == 42
        assert event_with_seq.event_type == event.event_type
        assert event_with_seq.data == event.data
        assert event_with_seq.timestamp == event.timestamp
        
        # Original event should remain unchanged
        assert event.sequence is None
    
    def test_event_is_valid(self):
        """Test event validation."""
        # Valid event
        event = Event(
            event_type="test.created",
            data={"id": "123"}
        )
        assert event.is_valid() is True
        
        # Invalid events (created through from_json)
        
        # Empty event type
        json_data = json.dumps({
            "event_type": "",
            "data": {"id": "123"},
            "timestamp": "2024-01-01T00:00:00Z"
        }).encode('utf-8')
        event = Event.from_json(json_data)
        assert event.is_valid() is False
        
        # Invalid data type
        json_data = json.dumps({
            "event_type": "test.created",
            "data": "not a dict",
            "timestamp": "2024-01-01T00:00:00Z"
        }).encode('utf-8')
        event = Event.from_json(json_data)
        assert event.is_valid() is False
    
    def test_event_string_representation(self):
        """Test string representations of event."""
        event = Event(
            event_type="test.created",
            data={"id": "123"},
            sequence=42
        )
        
        str_repr = str(event)
        assert "test.created" in str_repr
        assert "42" in str_repr
        
        repr_str = repr(event)
        assert "event_type='test.created'" in repr_str
        assert "sequence=42" in repr_str


class TestEventType:
    """Test EventType enum."""
    
    def test_event_type_values(self):
        """Test that event types have expected values."""
        assert EventType.USER_CREATED == "user.created"
        assert EventType.USER_UPDATED == "user.updated"
        assert EventType.USER_DELETED == "user.deleted"
        
        assert EventType.PRODUCT_CREATED == "product.created"
        assert EventType.PRODUCT_UPDATED == "product.updated"
        assert EventType.PRODUCT_PRICE_CHANGED == "product.price_changed"
        
        assert EventType.SCRAPER_STARTED == "scraper.started"
        assert EventType.SCRAPER_COMPLETED == "scraper.completed"
        assert EventType.SCRAPER_FAILED == "scraper.failed"
        
        assert EventType.CATALOG_ITEM_ADDED == "catalog.item_added"
        assert EventType.CATALOG_ITEM_UPDATED == "catalog.item_updated"
        
        assert EventType.CUSTOM == "custom"
    
    def test_event_type_usage(self):
        """Test using EventType enum in events."""
        event = Event(
            event_type=EventType.USER_CREATED,
            data={"user_id": "123", "email": "test@example.com"}
        )
        
        assert event.event_type == "user.created"
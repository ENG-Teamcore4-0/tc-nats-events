"""
Unit Tests for Event Adapters
==============================

Test the adapter system for handling different event formats.
"""

import json

import pytest

from tc_nats_events.adapters import EventAdapter, FlexibleAdapter, GenericAdapter
from tc_nats_events.models.event import Event


class TestGenericAdapter:
    """Test GenericAdapter class."""

    def test_can_handle_valid_json(self):
        """Test adapter can handle valid JSON."""
        adapter = GenericAdapter()

        valid_json = json.dumps({"event_type": "test", "data": {}}).encode("utf-8")
        assert adapter.can_handle(valid_json) is True

        invalid_json = b"not json"
        assert adapter.can_handle(invalid_json) is False

    def test_adapt_standard_format(self):
        """Test adapting standard tc-nats-events format."""
        adapter = GenericAdapter()

        original_data = {
            "event_type": "user.created",
            "data": {"user_id": "123", "email": "test@example.com"},
            "timestamp": "2024-01-01T00:00:00Z",
            "environment": "production",
        }

        json_bytes = json.dumps(original_data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "user.created"
        assert event.data == {"user_id": "123", "email": "test@example.com"}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata.environment == "production"
        assert event.metadata.source_service == "auto-detected"

    def test_adapt_tc_iam_format(self):
        """Test adapting tc-iam style format."""
        adapter = GenericAdapter()

        # tc-iam uses 'payload' instead of 'data'
        tc_iam_data = {
            "event_type": "teamcore.tenant.registration",
            "payload": json.dumps({"tenantShortName": "test", "tenantID": 1}),
            "timestamp": "2024-01-01T00:00:00Z",
            "environment": "localhost",
        }

        json_bytes = json.dumps(tc_iam_data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "teamcore.tenant.registration"
        assert event.data == {"tenantShortName": "test", "tenantID": 1}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata.environment == "localhost"

    def test_adapt_custom_field_names(self):
        """Test adapting with custom field mappings."""
        adapter = GenericAdapter(
            event_type_fields={"action", "operation"},
            data_fields={"body", "content"},
            timestamp_fields={"created_at"},
            environment_fields={"source"},
        )

        custom_data = {
            "action": "user_registered",
            "body": {"user_id": "456", "name": "John"},
            "created_at": "2024-01-01T00:00:00Z",
            "source": "api-service",
        }

        json_bytes = json.dumps(custom_data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "user_registered"
        assert event.data == {"user_id": "456", "name": "John"}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata.environment == "api-service"

    def test_adapt_multi_encoded_json(self):
        """Test adapting double/triple encoded JSON."""
        adapter = GenericAdapter(auto_decode_json_strings=True)

        # Triple encoded payload (like tc-iam sometimes does)
        inner_data = {"user": "test", "id": 123}
        double_encoded = json.dumps(json.dumps(inner_data))
        triple_encoded = json.dumps(double_encoded)

        data = {
            "event_type": "test.event",
            "payload": triple_encoded,
            "timestamp": "2024-01-01T00:00:00Z",
        }

        json_bytes = json.dumps(data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "test.event"
        assert event.data == inner_data  # Should be fully decoded

    def test_adapt_fallback_to_whole_object(self):
        """Test fallback when no explicit data field found."""
        adapter = GenericAdapter()

        # No explicit data/payload field
        data = {
            "type": "notification.sent",
            "recipient": "test@example.com",
            "subject": "Welcome",
            "body": "Hello world",
            "timestamp": "2024-01-01T00:00:00Z",
        }

        json_bytes = json.dumps(data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "notification.sent"
        # The adapter finds 'body' as a data field and extracts it
        assert event.data == {"value": "Hello world"}
        assert event.timestamp == "2024-01-01T00:00:00Z"

    def test_adapt_default_values(self):
        """Test adapter with missing fields."""
        adapter = GenericAdapter(default_event_type="unknown.event")

        # Minimal data with no standard fields
        data = {"message": "hello world"}

        json_bytes = json.dumps(data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "unknown.event"
        # The adapter finds 'message' as a data field and wraps it
        assert event.data == {"value": "hello world"}
        assert event.timestamp is not None  # Auto-generated
        assert event.metadata.source_service == "auto-detected"

    def test_adapt_error_handling(self):
        """Test adapter error handling."""
        adapter = GenericAdapter()

        # Invalid JSON that passes can_handle but fails in adapt
        invalid_data = b'{"event_type": "test", "data":'  # Truncated JSON

        # Should not raise exception, but create error event
        event = adapter.adapt_to_event(invalid_data)

        assert event.event_type == "generic.adapter.error"
        assert "error" in event.data
        assert "original_data" in event.data


class TestFlexibleAdapter:
    """Test FlexibleAdapter class."""

    def test_flexible_adapter_broad_matching(self):
        """Test FlexibleAdapter with very broad field matching."""
        adapter = FlexibleAdapter()

        # Test various field name patterns
        test_cases = [
            # Different event type fields
            {"action": "user_signup", "info": {"email": "test@example.com"}},
            {"operation": "data_sync", "params": {"count": 100}},
            {"method": "POST", "attributes": {"path": "/api/users"}},
            # Different data fields
            {"type": "log_entry", "properties": {"level": "INFO", "message": "Test"}},
            {"eventType": "alert", "fields": {"severity": "HIGH"}},
            # Different timestamp fields
            {
                "name": "task_completed",
                "data": {},
                "occurred_at": "2024-01-01T00:00:00Z",
            },
            {"type": "event", "info": {}, "happened_at": "2024-01-01T00:00:00Z"},
        ]

        for test_data in test_cases:
            json_bytes = json.dumps(test_data).encode("utf-8")
            event = adapter.adapt_to_event(json_bytes)

            # Should successfully create an event
            assert event.event_type != "flexible.event"  # Should detect the type
            assert isinstance(event.data, dict)
            assert event.metadata.source_service == "auto-detected"

    def test_flexible_adapter_fallback(self):
        """Test FlexibleAdapter fallback behavior."""
        adapter = FlexibleAdapter()

        # Data with no recognizable fields
        data = {"random_field": "value", "another_field": 123}

        json_bytes = json.dumps(data).encode("utf-8")
        event = adapter.adapt_to_event(json_bytes)

        assert event.event_type == "flexible.event"  # Uses default
        assert event.data == data  # Uses entire object as data


class TestEventWithAdapters:
    """Test Event.from_json with adapters."""

    def test_event_from_json_with_adapters(self):
        """Test Event.from_json using adapters."""
        # Create a custom format that requires adapter
        custom_data = {
            "action": "user_created",
            "payload": json.dumps({"user_id": "123", "email": "test@example.com"}),
            "when": "2024-01-01T00:00:00Z",
            "source": "user-service",
        }

        json_bytes = json.dumps(custom_data).encode("utf-8")

        # Without adapters - should fail
        with pytest.raises(ValueError, match="Missing required fields"):
            Event.from_json(json_bytes)

        # With generic adapter - should work
        adapter = GenericAdapter(
            event_type_fields={"action"},
            data_fields={"payload"},
            timestamp_fields={"when"},
            environment_fields={"source"},
        )

        event = Event.from_json(json_bytes, adapters=[adapter])

        assert event.event_type == "user_created"
        assert event.data == {"user_id": "123", "email": "test@example.com"}
        assert event.timestamp == "2024-01-01T00:00:00Z"
        assert event.metadata.environment == "user-service"

    def test_event_from_json_multiple_adapters(self):
        """Test Event.from_json with multiple adapters."""
        # Data that would match second adapter better
        data = {"type": "notification", "body": {"message": "hello"}}
        json_bytes = json.dumps(data).encode("utf-8")

        # First adapter will handle it but use default event type
        adapter1 = GenericAdapter(event_type_fields={"action"})  # Won't find 'action'

        # Second adapter would match better, but first adapter already handled it
        adapter2 = GenericAdapter(event_type_fields={"type"}, data_fields={"body"})

        event = Event.from_json(json_bytes, adapters=[adapter1, adapter2])

        # First adapter that can_handle returns True will be used
        assert event.event_type == "generic.event"  # adapter1's default
        assert event.data == {"message": "hello"}

    def test_event_from_json_adapter_fallback_to_standard(self):
        """Test Event.from_json falls back to standard format when adapters fail."""
        # Standard tc-nats-events format
        standard_data = {
            "event_type": "user.created",
            "data": {"user_id": "123"},
            "timestamp": "2024-01-01T00:00:00Z",
        }

        json_bytes = json.dumps(standard_data).encode("utf-8")

        # Adapter that can't handle this format
        adapter = GenericAdapter(event_type_fields={"action"})  # Won't find 'action'

        # Should fall back to standard parsing
        event = Event.from_json(json_bytes, adapters=[adapter])

        assert event.event_type == "user.created"
        assert event.data == {"user_id": "123"}
        assert event.timestamp == "2024-01-01T00:00:00Z"


class CustomTestAdapter(EventAdapter):
    """Custom adapter for testing."""

    def can_handle(self, raw_data: bytes) -> bool:
        try:
            data = json.loads(raw_data.decode("utf-8"))
            return "custom_field" in data
        except (json.JSONDecodeError, UnicodeDecodeError):
            return False

    def adapt_to_event(self, raw_data: bytes) -> Event:
        data = json.loads(raw_data.decode("utf-8"))
        return self._create_safe_event(
            event_type="custom.event", data={"custom": data["custom_field"]}
        )


class TestCustomAdapter:
    """Test custom adapter implementation."""

    def test_custom_adapter(self):
        """Test implementing a custom adapter."""
        adapter = CustomTestAdapter()

        # Data that should be handled by custom adapter
        data = {"custom_field": "test_value", "other": "ignored"}
        json_bytes = json.dumps(data).encode("utf-8")

        assert adapter.can_handle(json_bytes) is True

        event = adapter.adapt_to_event(json_bytes)
        assert event.event_type == "custom.event"
        assert event.data == {"custom": "test_value"}

    def test_custom_adapter_with_event_from_json(self):
        """Test custom adapter integration with Event.from_json."""
        adapter = CustomTestAdapter()

        data = {"custom_field": "test_value"}
        json_bytes = json.dumps(data).encode("utf-8")

        event = Event.from_json(json_bytes, adapters=[adapter])

        assert event.event_type == "custom.event"
        assert event.data == {"custom": "test_value"}

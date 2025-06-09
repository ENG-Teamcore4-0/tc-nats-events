"""
Unit Tests for Custom Events
============================

Test the flexibility of creating custom event types.
"""

from datetime import datetime, timezone

import pytest

from tc_nats_events.models.event import Event, EventMetadata, EventType, create_event


class TestCustomEventTypes:
    """Test creating events with custom types."""

    def test_custom_event_type_string(self):
        """Test creating event with completely custom event type."""
        event = Event(
            event_type="billing.invoice_generated",
            data={"invoice_id": "inv-123", "amount": 99.99, "customer_id": "cust-456"},
        )

        assert event.event_type == "billing.invoice_generated"
        assert event.data["invoice_id"] == "inv-123"
        assert event.is_valid()

    def test_predefined_event_type_usage(self):
        """Test using predefined event types."""
        event = Event(
            event_type=EventType.USER_CREATED,
            data={"user_id": "123", "email": "test@example.com"},
        )

        assert event.event_type == "user.created"
        assert event.data["user_id"] == "123"

    def test_predefined_event_type_as_string(self):
        """Test that predefined types work as strings."""
        event = Event(
            event_type="user.created", data={"user_id": "123"}  # String instead of enum
        )

        assert event.event_type == EventType.USER_CREATED
        assert event.event_type == "user.created"

    def test_domain_specific_event_types(self):
        """Test various domain-specific event types."""
        test_cases = [
            ("analytics.page_view", {"page": "/home", "user_id": "123"}),
            ("notification.email_sent", {"recipient": "user@example.com"}),
            ("inventory.stock_low", {"product_id": "prod-123", "quantity": 5}),
            ("payment.credit_card_charged", {"amount": 50.00, "card_last4": "1234"}),
            ("audit.user_login", {"user_id": "123", "ip": "192.168.1.1"}),
            ("ml.model_training_completed", {"model_id": "model-v2", "accuracy": 0.95}),
            (
                "integration.webhook_received",
                {"source": "stripe", "event": "payment_success"},
            ),
        ]

        for event_type, data in test_cases:
            event = Event(event_type=event_type, data=data)
            assert event.event_type == event_type
            assert event.data == data
            assert event.is_valid()

    def test_hierarchical_event_types(self):
        """Test hierarchical event type naming conventions."""
        # Multi-level hierarchies
        event_types = [
            "ecommerce.order.created",
            "ecommerce.order.payment.failed",
            "ecommerce.inventory.product.stock.updated",
            "auth.user.password.reset.requested",
            "monitoring.system.health.check.failed",
            "data.pipeline.etl.stage.completed",
        ]

        for event_type in event_types:
            event = Event(event_type=event_type, data={"test": "data"})
            assert event.event_type == event_type
            assert "." in event_type  # Verify hierarchical structure

    def test_event_create_method(self):
        """Test Event.create() factory method."""
        event = Event.create(
            event_type="custom.service.action",
            data={"action_id": "act-123", "result": "success"},
        )

        assert event.event_type == "custom.service.action"
        assert event.data["action_id"] == "act-123"
        assert event.timestamp is not None
        assert event.metadata is not None

    def test_event_create_with_metadata(self):
        """Test Event.create() with custom metadata."""
        metadata = EventMetadata(
            correlation_id="corr-123",
            user_id="user-456",
            source_service="billing-service",
        )

        event = Event.create(
            event_type="billing.charge.processed",
            data={"charge_id": "ch-789", "amount": 25.00},
            metadata=metadata,
        )

        assert event.event_type == "billing.charge.processed"
        assert event.metadata.correlation_id == "corr-123"
        assert event.metadata.user_id == "user-456"
        assert event.metadata.source_service == "billing-service"

    def test_create_event_function(self):
        """Test module-level create_event() function."""
        event = create_event(
            event_type="analytics.conversion_tracked",
            data={"conversion_id": "conv-123", "value": 150.00, "source": "google_ads"},
        )

        assert event.event_type == "analytics.conversion_tracked"
        assert event.data["conversion_id"] == "conv-123"
        assert isinstance(event, Event)

    def test_create_event_with_kwargs(self):
        """Test create_event() with additional kwargs."""
        custom_timestamp = "2024-01-15T10:30:00Z"
        metadata = EventMetadata(user_id="user-789")

        event = create_event(
            event_type="system.backup.completed",
            data={"backup_size_gb": 15.7},
            timestamp=custom_timestamp,
            metadata=metadata,
        )

        assert event.timestamp == custom_timestamp
        assert event.metadata.user_id == "user-789"

    def test_dynamic_event_type_generation(self):
        """Test dynamically generating event types."""
        service_name = "recommendation"
        action = "model_updated"
        version = "v2"

        # Dynamic event type construction
        event_type = f"{service_name}.{action}.{version}"

        event = Event(
            event_type=event_type,
            data={
                "model_version": version,
                "accuracy_improvement": 0.05,
                "training_duration_hours": 4.5,
            },
        )

        assert event.event_type == "recommendation.model_updated.v2"
        assert event.data["model_version"] == "v2"

    def test_event_type_validation(self):
        """Test that any non-empty string is valid as event type."""
        valid_event_types = [
            "a",  # Single character
            "simple",  # Simple word
            "with-dashes",  # With dashes
            "with_underscores",  # With underscores
            "with.dots",  # With dots
            "with123numbers",  # With numbers
            "UPPERCASE",  # Uppercase
            "MixedCase",  # Mixed case
            "very.long.hierarchical.event.type.with.many.levels",  # Very long
            "special!@#$%^&*()characters",  # Special characters (if needed)
        ]

        for event_type in valid_event_types:
            event = Event(event_type=event_type, data={"test": True})
            assert event.event_type == event_type
            assert event.is_valid()

    def test_invalid_event_types(self):
        """Test that empty event types are invalid."""
        # Empty string should be invalid
        event = Event(event_type="", data={"test": True})
        assert not event.is_valid()

        # None should cause type error (handled by type hints)
        with pytest.raises(TypeError):
            Event(event_type=None, data={"test": True})

    def test_serialization_with_custom_types(self):
        """Test JSON serialization/deserialization with custom event types."""
        original_event = Event(
            event_type="complex.custom.event.type",
            data={
                "nested": {"data": {"structure": True}},
                "array": [1, 2, 3],
                "string": "value",
                "number": 42.5,
            },
        )

        # Serialize to JSON
        json_bytes = original_event.to_json()

        # Deserialize from JSON
        deserialized_event = Event.from_json(json_bytes)

        assert deserialized_event.event_type == "complex.custom.event.type"
        assert deserialized_event.data == original_event.data
        assert deserialized_event.timestamp == original_event.timestamp


class TestEventTypeFlexibility:
    """Test various patterns of event type usage."""

    def test_microservice_event_patterns(self):
        """Test common microservice event patterns."""
        patterns = {
            # Command events (actions to be performed)
            "user.commands.create_account": {"email": "user@example.com"},
            "order.commands.process_payment": {"order_id": "123", "amount": 99.99},
            # Domain events (things that happened)
            "user.events.account_created": {"user_id": "123"},
            "order.events.payment_processed": {"order_id": "123", "status": "success"},
            # Integration events
            "integrations.stripe.webhook_received": {
                "event_type": "payment_intent.succeeded"
            },
            "integrations.salesforce.contact_synced": {"contact_id": "SF123"},
            # System events
            "system.health.check_failed": {"service": "database", "error": "timeout"},
            "system.performance.high_cpu_usage": {"cpu_percent": 95.5},
            # Business events
            "business.kpi.monthly_revenue_calculated": {
                "revenue": 150000,
                "month": "2024-01",
            },
            "business.promotion.campaign_started": {"campaign_id": "promo-123"},
        }

        for event_type, data in patterns.items():
            event = Event(event_type=event_type, data=data)
            assert event.is_valid()
            assert "." in event_type  # All follow hierarchical pattern

    def test_versioned_event_types(self):
        """Test versioned event types for evolution."""
        versions = ["v1", "v2", "v3"]
        base_event = "user.profile_updated"

        for version in versions:
            versioned_type = f"{base_event}.{version}"
            event = Event(
                event_type=versioned_type, data={"version": version, "user_id": "123"}
            )
            assert event.event_type == versioned_type
            assert event.data["version"] == version

    def test_environment_specific_events(self):
        """Test environment-specific event types."""
        environments = ["dev", "staging", "prod"]
        base_event = "deployment.completed"

        for env in environments:
            env_event_type = f"{env}.{base_event}"
            event = Event(
                event_type=env_event_type, data={"environment": env, "version": "1.2.3"}
            )
            assert event.event_type == env_event_type

    def test_team_specific_events(self):
        """Test team/service specific event types."""
        teams = ["frontend", "backend", "data", "ml", "devops"]

        for team in teams:
            event_type = f"{team}.deploy.success"
            event = Event(
                event_type=event_type,
                data={"team": team, "deployment_id": f"deploy-{team}-123"},
            )
            assert event.event_type == event_type
            assert event.data["team"] == team

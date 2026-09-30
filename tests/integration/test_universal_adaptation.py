"""
Integration Tests for Universal Event Adaptation
================================================

Test the complete adapter system integration with real NATS.
"""

import asyncio
import json

import nats
import pytest

from tc_nats_events import (
    DurableEventConsumer,
    Event,
    EventPublisher,
    GenericAdapter,
    NATSConfig,
)
from tests.conftest import delete_config_streams


@pytest.mark.integration
class TestUniversalAdaptation:
    """Test universal adaptation with real NATS server."""

    @pytest.fixture
    async def nats_server(self):
        """Setup NATS server for testing."""
        # This assumes NATS server is running on localhost:4222
        # In a real CI environment, you'd start a NATS container
        yield "nats://localhost:4222"

    @pytest.fixture
    async def test_config(self):
        """Create test configuration with cleanup."""
        import uuid

        unique_id = str(uuid.uuid4())[:8]

        config = NATSConfig(
            servers=["nats://localhost:4222"],
            stream_name=f"univ-adapt-test-{unique_id}",
            subject_prefix=f"univ.adapt.test.{unique_id}",
            # Events are published before the durable exists: replay them
            # (the library default is "new", which would skip them).
            deliver_policy="all",
        )

        # Clean up any existing streams (event, DLQ and idempotency KV)
        await delete_config_streams(config)

        yield config

        await delete_config_streams(config)

    async def publish_different_formats(self, config: NATSConfig):
        """Publish events in different formats using raw NATS."""
        nc = await nats.connect(servers=config.servers)
        js = nc.jetstream()

        # Ensure stream exists
        try:
            await js.add_stream(
                name=config.stream_name, subjects=[f"{config.subject_prefix}.>"]
            )
        except Exception:
            pass  # Stream might already exist

        # Format 1: Standard tc-nats-events
        standard_event = {
            "event_type": "user.created",
            "data": {"user_id": "123", "email": "user@test.com"},
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {"source_service": "user-service"},
        }
        await js.publish(
            f"{config.subject_prefix}.user.created", json.dumps(standard_event).encode()
        )

        # Format 2: tc-iam style (payload instead of data)
        tc_iam_event = {
            "event_type": "teamcore.tenant.registration",
            "payload": json.dumps({"tenantShortName": "test", "tenantID": 1}),
            "timestamp": "2024-01-01T01:00:00Z",
            "environment": "test",
        }
        await js.publish(
            f"{config.subject_prefix}.tenant.registration",
            json.dumps(tc_iam_event).encode(),
        )

        # Format 3: Custom microservice format
        custom_event = {
            "action": "order_placed",
            "body": {"order_id": "ord-456", "total": 99.99},
            "when": "2024-01-01T02:00:00Z",
            "source": "order-service",
        }
        await js.publish(
            f"{config.subject_prefix}.order.placed", json.dumps(custom_event).encode()
        )

        await nc.close()
        return 3  # Number of events published

    @pytest.mark.asyncio
    async def test_zero_config_universal_consumer(self, test_config):
        """Test universal consumer with zero configuration."""
        import time

        # Publish different formats
        expected_events = await self.publish_different_formats(test_config)

        # Create universal consumer (auto_adapt=True by default)
        import uuid

        consumer_name = f"test-universal-{str(uuid.uuid4())[:8]}"
        consumer = DurableEventConsumer(consumer_name, test_config)

        received_events = []

        async def collect_event(event: Event):
            received_events.append(event)

        consumer.register_default_handler(collect_event)

        # Run consumer
        async with consumer:
            # Wait for sync with timeout
            timeout = 2.0
            start_time = time.time()
            while not consumer.is_synced:
                if time.time() - start_time > timeout:
                    break
                await asyncio.sleep(0.02)

            # Give time to process all events
            await asyncio.sleep(0.3)

        # Verify all events were processed
        assert len(received_events) == expected_events

        # Verify different formats were correctly adapted
        event_types = [event.event_type for event in received_events]
        assert "user.created" in event_types
        assert "teamcore.tenant.registration" in event_types
        assert "order_placed" in event_types

        # Verify data was correctly extracted
        for event in received_events:
            assert isinstance(event.data, dict)
            assert len(event.data) > 0

    @pytest.mark.asyncio
    async def test_custom_adapter_consumer(self, test_config):
        import time

        """Test consumer with custom adapter configuration."""
        # Publish custom format
        nc = await nats.connect(servers=test_config.servers)
        js = nc.jetstream()

        try:
            await js.add_stream(
                name=test_config.stream_name,
                subjects=[f"{test_config.subject_prefix}.>"],
            )
        except Exception:
            pass

        custom_format = {
            "operation": "data_sync",
            "details": {"records": 1000, "tables": ["users", "orders"]},
            "completed_at": "2024-01-01T03:00:00Z",
            "system": "data-warehouse",
        }

        await js.publish(
            f"{test_config.subject_prefix}.data.sync",
            json.dumps(custom_format).encode(),
        )
        await nc.close()

        # Create custom adapter
        custom_adapter = GenericAdapter(
            event_type_fields={"operation"},
            data_fields={"details"},
            timestamp_fields={"completed_at"},
            environment_fields={"system"},
        )

        # Create consumer with custom adapter
        consumer = DurableEventConsumer(
            "test-custom-adapter",
            test_config,
            adapters=[custom_adapter],
            auto_adapt=False,  # Only use custom adapter
        )

        received_events = []

        async def collect_event(event: Event):
            received_events.append(event)

        consumer.register_default_handler(collect_event)

        # Run consumer
        async with consumer:
            # Wait for sync with timeout
            timeout = 2.0
            sync_start = time.time()
            while not consumer.is_synced:
                if time.time() - sync_start > timeout:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)

        # Verify event was processed correctly
        assert len(received_events) == 1
        event = received_events[0]

        assert event.event_type == "data_sync"
        assert event.data == {"records": 1000, "tables": ["users", "orders"]}
        assert event.timestamp == "2024-01-01T03:00:00Z"
        assert event.metadata.environment == "data-warehouse"

    @pytest.mark.asyncio
    async def test_mixed_format_processing(self, test_config):
        import time

        """Test processing mixed standard and custom formats."""
        nc = await nats.connect(servers=test_config.servers)
        js = nc.jetstream()

        try:
            await js.add_stream(
                name=test_config.stream_name,
                subjects=[f"{test_config.subject_prefix}.>"],
            )
        except Exception:
            pass

        # Publish standard tc-nats-events format
        standard_event = {
            "event_type": "payment.completed",
            "data": {"payment_id": "pay-123", "amount": 50.00},
            "timestamp": "2024-01-01T04:00:00Z",
        }
        await js.publish(
            f"{test_config.subject_prefix}.payment.completed",
            json.dumps(standard_event).encode(),
        )

        # Publish custom format
        custom_event = {
            "type": "alert_triggered",
            "message": {"severity": "HIGH", "component": "database"},
            "time": "2024-01-01T04:30:00Z",
            "origin": "monitoring",
        }
        await js.publish(
            f"{test_config.subject_prefix}.alert.triggered",
            json.dumps(custom_event).encode(),
        )

        await nc.close()

        # Create consumer with auto-adaptation
        consumer = DurableEventConsumer("test-mixed-formats", test_config)

        received_events = []

        async def collect_event(event: Event):
            received_events.append(event)

        consumer.register_default_handler(collect_event)

        # Run consumer
        async with consumer:
            # Wait for sync with timeout
            timeout = 2.0
            sync_start = time.time()
            while not consumer.is_synced:
                if time.time() - sync_start > timeout:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)

        # Verify both events were processed
        assert len(received_events) == 2

        # Find events by type
        payment_event = next(
            e for e in received_events if e.event_type == "payment.completed"
        )
        alert_event = next(
            e for e in received_events if e.event_type == "alert_triggered"
        )

        # Verify standard format was processed correctly
        assert payment_event.data == {"payment_id": "pay-123", "amount": 50.00}
        assert payment_event.timestamp == "2024-01-01T04:00:00Z"

        # Verify custom format was adapted correctly
        assert alert_event.data == {"severity": "HIGH", "component": "database"}
        assert alert_event.timestamp == "2024-01-01T04:30:00Z"
        assert alert_event.metadata.environment == "monitoring"

    @pytest.mark.asyncio
    async def test_adapter_with_standard_publisher(self, test_config):
        import time

        """Test that adapters work with standard EventPublisher."""
        # Publish using standard EventPublisher
        async with EventPublisher("test-publisher", test_config) as publisher:
            await publisher.publish(
                "user.updated", {"user_id": "456", "changes": {"email": "new@test.com"}}
            )

        # Consume with universal consumer
        consumer = DurableEventConsumer("test-standard-publisher", test_config)

        received_events = []

        async def collect_event(event: Event):
            received_events.append(event)

        consumer.register_handler("user.updated", collect_event)

        # Run consumer
        async with consumer:
            # Wait for sync with timeout
            timeout = 2.0
            sync_start = time.time()
            while not consumer.is_synced:
                if time.time() - sync_start > timeout:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)

        # Verify event was processed normally
        assert len(received_events) == 1
        event = received_events[0]

        assert event.event_type == "user.updated"
        assert event.data == {"user_id": "456", "changes": {"email": "new@test.com"}}
        assert event.metadata.source_service == "test-publisher"

    @pytest.mark.asyncio
    async def test_error_handling_in_adapters(self, test_config):
        import time

        """Test adapter error handling with malformed events."""
        nc = await nats.connect(servers=test_config.servers)
        js = nc.jetstream()

        try:
            await js.add_stream(
                name=test_config.stream_name,
                subjects=[f"{test_config.subject_prefix}.>"],
            )
        except Exception:
            pass

        # Publish malformed JSON within JSON (should trigger adapter error handling)
        malformed_event = {
            "event_type": "malformed.event",
            "payload": '{"incomplete": json data',  # Malformed JSON string
            "timestamp": "2024-01-01T05:00:00Z",
        }
        await js.publish(
            f"{test_config.subject_prefix}.malformed.event",
            json.dumps(malformed_event).encode(),
        )

        await nc.close()

        # Create consumer with error handling
        consumer = DurableEventConsumer("test-error-handling", test_config)

        received_events = []

        async def collect_event(event: Event):
            received_events.append(event)

        consumer.register_default_handler(collect_event)

        # Run consumer
        async with consumer:
            # Wait for sync with timeout
            timeout = 2.0
            sync_start = time.time()
            while not consumer.is_synced:
                if time.time() - sync_start > timeout:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.5)

        # Should still receive event (adapted as best as possible)
        assert len(received_events) == 1
        event = received_events[0]

        # Event should be processed, even if payload parsing failed
        assert event.event_type == "malformed.event"
        assert isinstance(event.data, dict)
        # Payload should be kept as string since JSON parsing failed
        assert "incomplete" in str(event.data)

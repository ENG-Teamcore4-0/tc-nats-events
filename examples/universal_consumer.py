"""
Universal Event Consumer Example
================================

This example demonstrates how tc-nats-events can automatically handle
ANY event format from ANY service with zero configuration.
"""

import asyncio
import json
import signal
from tc_nats_events import (
    DurableEventConsumer,
    EventPublisher,
    NATSConfig,
    Event,
    GenericAdapter,
    FlexibleAdapter,
    setup_logging,
)


async def handle_any_event(event: Event):
    """Handle ANY event from ANY service."""
    print(f"\n📨 Universal Event Handler:")
    print(f"   🏷️  Type: {event.event_type}")
    print(f"   📦 Data: {event.data}")
    print(f"   ⏰ Timestamp: {event.timestamp}")
    print(f"   🔢 Sequence: {event.sequence}")
    
    if event.metadata:
        print(f"   🌍 Environment: {event.metadata.environment}")
        print(f"   🔧 Source: {event.metadata.source_service}")


async def publish_various_formats(config: NATSConfig):
    """Publish events in different formats to test universal consumption."""
    print("\n🚀 Publishing events in various formats...")
    
    # Use raw NATS client to publish different formats
    import nats
    nc = await nats.connect(servers=config.servers)
    js = nc.jetstream()
    
    # Ensure stream exists
    try:
        await js.add_stream(name=config.stream_name, subjects=[f"{config.subject_prefix}.>"])
    except:
        pass  # Stream might already exist
    
    # Format 1: Standard tc-nats-events format
    standard_event = {
        "event_type": "user.created",
        "data": {"user_id": "123", "email": "user@example.com"},
        "timestamp": "2024-01-01T00:00:00Z",
        "metadata": {"source_service": "user-service"}
    }
    await js.publish(f"{config.subject_prefix}.user.created", 
                     json.dumps(standard_event).encode())
    print("   ✅ Published standard tc-nats-events format")
    
    # Format 2: tc-iam style format (payload instead of data)
    tc_iam_event = {
        "event_type": "teamcore.tenant.registration",
        "payload": json.dumps({"tenantShortName": "demo", "tenantID": 42}),
        "timestamp": "2024-01-01T01:00:00Z",
        "environment": "production"
    }
    await js.publish(f"{config.subject_prefix}.tenant.registration", 
                     json.dumps(tc_iam_event).encode())
    print("   ✅ Published tc-iam style format")
    
    # Format 3: Custom microservice format
    custom_event = {
        "action": "order_placed",
        "body": {"order_id": "ord-456", "total": 99.99},
        "when": "2024-01-01T02:00:00Z",
        "source": "order-service"
    }
    await js.publish(f"{config.subject_prefix}.order.placed", 
                     json.dumps(custom_event).encode())
    print("   ✅ Published custom microservice format")
    
    # Format 4: Legacy system format
    legacy_event = {
        "type": "inventory_updated",
        "message": {"product_id": "prod-789", "stock": 150},
        "created_at": "2024-01-01T03:00:00Z",
        "origin": "legacy-inventory"
    }
    await js.publish(f"{config.subject_prefix}.inventory.updated", 
                     json.dumps(legacy_event).encode())
    print("   ✅ Published legacy system format")
    
    # Format 5: Notification service format
    notification_event = {
        "operation": "email_sent",
        "details": {
            "recipient": "customer@example.com",
            "subject": "Order Confirmation",
            "template": "order_confirmation"
        },
        "occurred_at": "2024-01-01T04:00:00Z",
        "app": "notification-service"
    }
    await js.publish(f"{config.subject_prefix}.notification.sent", 
                     json.dumps(notification_event).encode())
    print("   ✅ Published notification service format")
    
    await nc.close()


async def universal_consumer_demo():
    """Demonstrate universal event consumption."""
    print("\n🌟 Universal Consumer Demo")
    print("🔧 This consumer works with ANY event format automatically!")
    print("=" * 60)
    
    # Setup logging
    setup_logging(level="INFO")
    
    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="universal-demo",
        subject_prefix="demo.events"
    )
    
    # Method 1: Zero Configuration (Recommended)
    print("\n🎯 Method 1: Zero Configuration Universal Consumer")
    consumer = DurableEventConsumer("universal-demo", config)
    # auto_adapt=True by default - handles ANY format!
    
    # Register one handler for all events
    consumer.register_handler("*", handle_any_event)
    
    # Method 2: Custom Configuration (if needed)
    print("\n🔧 Method 2: Custom Adapter Configuration")
    custom_adapter = GenericAdapter(
        event_type_fields={"action", "operation", "method"},
        data_fields={"body", "content", "details", "info"},
        timestamp_fields={"when", "occurred_at", "created_at"},
        environment_fields={"source", "origin", "app", "service"}
    )
    
    consumer_custom = DurableEventConsumer(
        "universal-custom", 
        config,
        adapters=[custom_adapter]  # Custom adapter + auto FlexibleAdapter
    )
    consumer_custom.register_handler("*", handle_any_event)
    
    # Publish various formats
    await publish_various_formats(config)
    
    print("\n🎯 Starting universal consumption...")
    print("This will handle ALL the different formats automatically!")
    
    # Start consumer
    async with consumer:
        # Wait for sync
        while not consumer.is_synced:
            await asyncio.sleep(0.5)
            status = consumer.get_sync_status()
            print(f"🔄 Syncing: {status['events_processed']} events")
        
        print(f"\n✅ Sync complete! Processed {consumer.get_sync_status()['events_processed']} events")
        print("🎉 Universal consumer successfully handled all different formats!")
        
        # Keep running for a bit to show real-time capabilities
        print("\n⏰ Running for 10 seconds to demonstrate real-time processing...")
        await asyncio.sleep(10)


async def adapter_comparison_demo():
    """Demonstrate different adapter configurations."""
    print("\n🔬 Adapter Comparison Demo")
    print("=" * 60)
    
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="adapter-demo",
        subject_prefix="adapter.test"
    )
    
    # Publish a custom format event
    import nats
    nc = await nats.connect(servers=config.servers)
    js = nc.jetstream()
    
    try:
        await js.add_stream(name=config.stream_name, subjects=[f"{config.subject_prefix}.>"])
    except:
        pass
    
    custom_format = {
        "action": "data_processed",
        "content": {"records": 1000, "duration_ms": 150},
        "happened_at": "2024-01-01T10:00:00Z",
        "service": "data-processor"
    }
    
    await js.publish(f"{config.subject_prefix}.data.processed", 
                     json.dumps(custom_format).encode())
    await nc.close()
    
    print("📨 Published custom format event")
    
    # Test different adapters
    adapters_to_test = [
        ("No Adapters (Standard Only)", []),
        ("Generic Adapter", [GenericAdapter()]),
        ("Flexible Adapter", [FlexibleAdapter()]),
        ("Custom Generic Adapter", [GenericAdapter(
            event_type_fields={"action"},
            data_fields={"content"},
            timestamp_fields={"happened_at"},
            environment_fields={"service"}
        )])
    ]
    
    for adapter_name, adapters in adapters_to_test:
        print(f"\n🧪 Testing: {adapter_name}")
        
        consumer = DurableEventConsumer(
            f"test-{adapter_name.lower().replace(' ', '-')}", 
            config,
            adapters=adapters,
            auto_adapt=False
        )
        
        events_received = []
        
        async def collect_event(event: Event):
            events_received.append(event)
            print(f"   📦 Received: {event.event_type} with data: {event.data}")
        
        consumer.register_handler("*", collect_event)
        
        try:
            async with consumer:
                await asyncio.sleep(2)  # Give time to process
                
                if events_received:
                    print(f"   ✅ Successfully processed {len(events_received)} events")
                else:
                    print(f"   ❌ No events processed (format not supported)")
        except Exception as e:
            print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    async def main():
        print("🌟 TC NATS Events - Universal Event Processing Demo")
        print("=" * 60)
        
        # Run demos
        await universal_consumer_demo()
        await adapter_comparison_demo()
        
        print("\n🎉 Demo completed!")
        print("💡 Key takeaways:")
        print("   • tc-nats-events can handle ANY JSON event format")
        print("   • Zero configuration needed for most use cases")
        print("   • Custom adapters available for specific needs")
        print("   • Backward compatible with existing code")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Demo interrupted by user")
    except Exception as e:
        print(f"💥 Demo error: {e}")
        import traceback
        traceback.print_exc()
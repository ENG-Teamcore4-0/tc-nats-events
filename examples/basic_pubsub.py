"""
Basic Publisher/Consumer Example
================================

This example demonstrates basic event publishing and consuming with TC NATS Events.
"""

import asyncio
import signal
from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    setup_logging,
)


async def handle_user_event(event: Event):
    """Handle user-related events."""
    print(f"\n📨 Received {event.event_type}:")
    print(f"   Event ID: {event.metadata.event_id}")
    print(f"   Sequence: {event.sequence}")
    print(f"   Data: {event.data}")
    print(f"   Timestamp: {event.timestamp}")


async def publisher_demo(config: NATSConfig):
    """Demonstrate event publishing."""
    print("\n🚀 Starting Publisher Demo...")
    
    async with EventPublisher("demo-publisher", config) as publisher:
        # Publish user created event
        seq1 = await publisher.publish_user_created(
            user_id="user-123",
            user_data={
                "email": "john.doe@example.com",
                "name": "John Doe",
                "role": "admin"
            }
        )
        print(f"✅ Published user.created event: sequence={seq1}")
        
        # Publish user updated event
        seq2 = await publisher.publish_user_updated(
            user_id="user-123",
            updates={
                "role": "super_admin",
                "last_login": "2024-01-15T10:30:00Z"
            }
        )
        print(f"✅ Published user.updated event: sequence={seq2}")
        
        # Publish custom event
        seq3 = await publisher.publish(
            event_type="user.logged_in",
            data={
                "user_id": "user-123",
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0..."
            }
        )
        print(f"✅ Published user.logged_in event: sequence={seq3}")
        
        # Get publisher metrics
        metrics = publisher.get_metrics()
        print(f"\n📊 Publisher Metrics: {metrics}")


async def consumer_demo(config: NATSConfig):
    """Demonstrate durable consumer with auto-sync."""
    print("\n🚀 Starting Consumer Demo...")
    
    # Create consumer
    consumer = DurableEventConsumer("demo-consumer", config)
    
    # Register handlers for different event types
    consumer.register_handler("user.created", handle_user_event)
    consumer.register_handler("user.updated", handle_user_event)
    consumer.register_handler("user.logged_in", handle_user_event)
    
    # Default handler for unregistered events
    async def default_handler(event: Event):
        print(f"⚠️  Unhandled event type: {event.event_type}")
    
    consumer.register_default_handler(default_handler)
    
    # Start consumer
    await consumer.start()
    print("✅ Consumer started, beginning synchronization...")
    
    # Monitor sync progress
    while not consumer.is_synced:
        await asyncio.sleep(1)
        status = consumer.get_sync_status()
        print(f"⏳ Syncing: {status['events_processed']} events processed...")
    
    print("✅ Synchronization complete! Now processing live events...")
    
    # Keep running until interrupted
    stop_event = asyncio.Event()
    
    def signal_handler():
        print("\n⚠️  Shutting down consumer...")
        stop_event.set()
    
    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await stop_event.wait()
    finally:
        await consumer.stop()
        print("✅ Consumer stopped")


async def main():
    """Run the demo."""
    # Setup logging
    setup_logging(level="INFO", structured=False)
    
    # Create configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="demo-events",
        subject_prefix="demo.events"
    )
    
    print("=" * 60)
    print("TC NATS Events - Basic Publisher/Consumer Demo")
    print("=" * 60)
    
    # Run publisher demo
    await publisher_demo(config)
    
    print("\n" + "=" * 60)
    
    # Run consumer demo
    await consumer_demo(config)


if __name__ == "__main__":
    asyncio.run(main())
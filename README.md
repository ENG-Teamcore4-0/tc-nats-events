# TC NATS Events

Event Sourcing and Durable Consumers for NATS JetStream - Teamcore Architecture Team

## 🚀 Overview

TC NATS Events is a Python package that implements Event Sourcing and Durable Consumer patterns using NATS JetStream. It's designed for building reliable microservices architectures with guaranteed message delivery and automatic synchronization.

### Key Features

- **Event Sourcing**: Immutable events with automatic metadata tracking
- **Durable Consumers**: Automatic synchronization from event #1 with recovery
- **At-least-once delivery**: Guaranteed message delivery with explicit acknowledgments
- **Horizontal scaling**: Multiple consumer instances with automatic load balancing
- **Automatic recovery**: Resilient connection handling and retry logic
- **Structured logging**: JSON-formatted logs with correlation tracking
- **Type safety**: Full type hints and validation
- **⭐ Complete Flexibility**: Use **any string** as event type - no restrictions!
- **Idempotency**: Built-in duplicate event handling
- **Rich Metrics**: Real-time performance monitoring with P95/P99 latencies

## 📦 Installation

```bash
pip install tc-nats-events
```

For development:
```bash
pip install tc-nats-events[dev]
```

## 🔧 Quick Start

### 1. Basic Event Publishing

```python
import asyncio
from tc_nats_events import EventPublisher, NATSConfig

async def main():
    # Create configuration
    config = NATSConfig.from_env()  # or NATSConfig()
    
    # Create publisher
    async with EventPublisher("my-service", config) as publisher:
        # Publish ANY custom event type - complete flexibility!
        sequence = await publisher.publish(
            event_type="billing.invoice_generated",  # Your custom type
            data={
                "invoice_id": "inv-123",
                "customer_id": "cust-456", 
                "amount": 99.99,
                "due_date": "2024-02-15"
            }
        )
        print(f"Custom event published with sequence: {sequence}")

asyncio.run(main())
```

### 2. Durable Consumer with Automatic Sync

```python
import asyncio
from tc_nats_events import DurableEventConsumer, NATSConfig, Event

async def handle_user_created(event: Event):
    """Handle user created events."""
    user_data = event.data
    print(f"New user created: {user_data['email']}")

async def main():
    # Create configuration
    config = NATSConfig.from_env()
    
    # Create consumer
    consumer = DurableEventConsumer("user-service", config)
    
    # Register event handlers
    consumer.register_handler("user.created", handle_user_created)
    
    # Start consumer (automatically syncs from event #1)
    async with consumer:
        print(f"Consumer started, syncing: {consumer.is_synced}")
        
        # Wait for initial sync
        while not consumer.is_synced:
            await asyncio.sleep(1)
            status = consumer.get_sync_status()
            print(f"Sync progress: {status['events_processed']} events")
        
        print("Sync complete! Now processing live events...")
        
        # Keep running
        await asyncio.sleep(3600)  # Run for 1 hour

asyncio.run(main())
```

### 3. Complete Flexibility - Any Event Type

```python
from tc_nats_events import create_event, Event, EventType

# Method 1: Use predefined types (optional)
event = Event(event_type=EventType.USER_CREATED, data={"user_id": "123"})

# Method 2: Use ANY custom string - no limitations!
event = Event(
    event_type="my_service.data_processed",  # Completely custom
    data={"records": 1500, "duration_ms": 234}
)

# Method 3: Convenience function
event = create_event(
    "analytics.conversion_tracked",
    {"value": 250.00, "source": "google_ads"}
)

# Method 4: Dynamic event types
service_name = "billing"
action = "payment_processed"
event_type = f"{service_name}.{action}"  # "billing.payment_processed"
```

### 4. Advanced Usage with Event Store

```python
import asyncio
from tc_nats_events import NATSEventStore, Event, EventMetadata, NATSConfig

async def main():
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="app-events",
        subject_prefix="app.events"
    )
    
    async with NATSEventStore(config) as store:
        # Create event with metadata
        event = Event(
            event_type="product.price_changed",
            data={
                "product_id": "SKU-123",
                "old_price": 99.99,
                "new_price": 79.99,
                "currency": "USD"
            },
            metadata=EventMetadata(
                correlation_id="promo-2024-01",
                source_service="pricing-engine",
                user_id="admin-456"
            )
        )
        
        # Publish event
        sequence = await store.publish_event(event)
        
        # Get stream info
        info = await store.get_stream_info()
        print(f"Stream has {info['messages']} messages")

asyncio.run(main())
```

## 🏗️ Architecture

### Event Flow

```
Publisher → NATS JetStream → Durable Consumers
    ↓           ↓                    ↓
  Event      Persistent         Guaranteed
 Created      Storage           Delivery
```

### Components

1. **Event Store**: Core abstraction over NATS JetStream
2. **Event Publisher**: High-level publisher with retry logic
3. **Durable Consumer**: Automatic synchronization and recovery
4. **Event Models**: Immutable events with metadata

## 🔍 Configuration

### Environment Variables

```bash
# NATS Connection
export NATS_SERVERS="nats://localhost:4222,nats://localhost:4223"
export NATS_USER="your-user"
export NATS_PASSWORD="your-password"

# Stream Configuration
export NATS_STREAM_NAME="app-events"
export NATS_SUBJECT_PREFIX="app.events"
export NATS_MAX_MESSAGES="1000000"
export NATS_MAX_AGE_SECONDS="2592000"  # 30 days

# Consumer Settings
export NATS_MAX_DELIVER_ATTEMPTS="3"
export NATS_ACK_WAIT_SECONDS="30"
export NATS_MAX_ACK_PENDING="1000"
```

### Programmatic Configuration

```python
from tc_nats_events import NATSConfig

# From environment
config = NATSConfig.from_env()

# Explicit configuration
config = NATSConfig(
    servers=["nats://localhost:4222"],
    stream_name="my-events",
    subject_prefix="my.events",
    max_messages=1_000_000,
    max_age_seconds=30 * 24 * 60 * 60,  # 30 days
    replicas=3  # For production
)

# From file
config = NATSConfig.from_file("config.yaml")
```

## 📊 Monitoring

### Consumer Metrics

```python
consumer = DurableEventConsumer("my-service", config)
await consumer.start()

# Get sync status
status = consumer.get_sync_status()
print(f"Sync Status: {status}")
# Output: {
#     "service_name": "my-service",
#     "state": "live",
#     "is_synced": true,
#     "events_processed": 12345,
#     "events_failed": 2,
#     "last_sequence": 12345,
#     "sync_duration_seconds": 45.2
# }

# Get metrics
metrics = consumer.get_metrics()
print(f"Metrics: {metrics}")
```

### Publisher Metrics

```python
publisher = EventPublisher("my-service", config)
await publisher.connect()

# Publish events...

metrics = publisher.get_metrics()
print(f"Publisher Metrics: {metrics}")
# Output: {
#     "service_name": "my-service",
#     "events_published": 1000,
#     "events_failed": 5,
#     "last_publish_time": "2024-01-15T10:30:00Z",
#     "environment": "production"
# }
```

## 🧪 Testing

Run the test suite:

```bash
# Unit tests
pytest tests/unit -v

# Integration tests (requires NATS server)
docker run -d -p 4222:4222 nats:latest -js
pytest tests/integration -v

# All tests with coverage
pytest --cov=tc_nats_events --cov-report=html
```

## 🔒 Production Considerations

### 1. Stream Replication

For production, use at least 3 replicas:

```python
config = NATSConfig(
    replicas=3,
    max_messages=10_000_000,
    max_bytes=10 * 1024 * 1024 * 1024  # 10GB
)
```

### 2. Consumer Groups

Scale horizontally with multiple instances:

```python
# All instances use the same consumer name
consumer1 = DurableEventConsumer("order-processor", config)
consumer2 = DurableEventConsumer("order-processor", config)
consumer3 = DurableEventConsumer("order-processor", config)

# NATS automatically load balances between them
```

### 3. Error Handling

```python
async def handle_with_retry(event: Event):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Process event
            await process_order(event.data)
            break
        except TemporaryError as e:
            if attempt == max_retries - 1:
                # Send to dead letter queue
                await dead_letter_queue.publish(event)
                raise
            await asyncio.sleep(2 ** attempt)
```

### 4. Monitoring and Alerting

```python
# Set up structured logging
from tc_nats_events import setup_logging

setup_logging(
    level="INFO",
    structured=True,  # JSON logs
    service_name="order-service"
)

# All logs will include service context
```

## 📚 Examples

See the [examples/](examples/) directory for complete examples:

- [Basic Publisher/Consumer](examples/basic_pubsub.py)
- [**Custom Events** - Complete Flexibility](examples/custom_events.py) ⭐
- [Advanced Features - Metrics & Idempotency](examples/advanced_features.py)
- [Scraper Integration](examples/scraper_integration.py)
- [Multi-Service Coordination](examples/multi_service_sync.py)

## 🎯 Custom Events Documentation

**Any service can create events with any event type!** See [Custom Events Guide](docs/CUSTOM_EVENTS.md) for:

- Creating custom event types
- Naming conventions and patterns
- Real-world service examples
- Event evolution and versioning
- Multi-service coordination

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built on top of [nats-py](https://github.com/nats-io/nats.py)
- Inspired by Event Sourcing and CQRS patterns
- Designed for Teamcore Architecture Team

# TC NATS Events

[![CI](https://github.com/ENG-Teamcore4-0/tc-nats-events/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ENG-Teamcore4-0/tc-nats-events/actions/workflows/ci.yml)
[![Auto Release](https://github.com/ENG-Teamcore4-0/tc-nats-events/actions/workflows/auto-release.yml/badge.svg?branch=main)](https://github.com/ENG-Teamcore4-0/tc-nats-events/actions/workflows/auto-release.yml)
[![Python versions](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)

Event Sourcing and Durable Consumers for NATS JetStream - Teamcore Architecture Team

## 🚀 Overview

TC NATS Events is a Python package that implements Event Sourcing and Durable Consumer patterns using NATS JetStream. It's designed for building reliable microservices architectures with guaranteed message delivery and automatic synchronization.

### When to Use TC NATS Events

| Use Case | TC NATS Events | Traditional Queue | Database Polling |
|----------|----------------|-------------------|------------------|
| Event Sourcing | ✅ Built-in support | ❌ Manual implementation | ❌ Not suitable |
| Multi-service coordination | ✅ Automatic sync | ⚠️ Manual coordination | ❌ Complex joins |
| Historical replay | ✅ From any point | ❌ Limited | ❌ Not available |
| Horizontal scaling | ✅ Automatic | ⚠️ Manual setup | ❌ Lock contention |
| At-least-once delivery | ✅ Guaranteed | ✅ Available | ❌ Manual tracking |
| Real-time updates | ✅ < 10ms latency | ✅ Low latency | ❌ Polling delay |

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

This is an internal Teamcore package. Install directly from GitHub:

### Latest Release (Recommended)
```bash
pip install git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git
```

### Specific Version
```bash
# Install a specific version tag
pip install git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@v0.1.1

# Or install from a specific branch
pip install git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@main
```

### With SSH (for authenticated access)
```bash
pip install git+ssh://git@github.com/ENG-Teamcore4-0/tc-nats-events.git
```

### For Development
```bash
# Clone and install in editable mode
git clone git@github.com:ENG-Teamcore4-0/tc-nats-events.git
cd tc-nats-events
pip install -e ".[dev,test]"
```

### In requirements.txt
```txt
# Add to your requirements.txt
git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@v0.1.1
```

### In pyproject.toml
```toml
[project.dependencies]
tc-nats-events = { git = "https://github.com/ENG-Teamcore4-0/tc-nats-events.git", tag = "v0.1.1" }
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

### 5. Error Handling and Recovery

```python
from tc_nats_events import DurableEventConsumer, Event

class ResilientService:
    def __init__(self, consumer: DurableEventConsumer):
        self.consumer = consumer
        self.consumer.register_handler("order.created", self.handle_order)
    
    async def handle_order(self, event: Event):
        """Handle order with retry logic."""
        order_data = event.data
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Process order
                await self.process_order(order_data)
                break
            except TemporaryError as e:
                if attempt == max_retries - 1:
                    # Send to dead letter queue
                    await self.send_to_dlq(event, str(e))
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
```

### 6. Multi-Service Coordination

```python
# User Service emits events
async with EventPublisher("user-service", config) as publisher:
    await publisher.publish("user.registered", {
        "user_id": "usr-123",
        "email": "user@example.com"
    })

# Multiple services react to the event
# Email Service
email_consumer = DurableEventConsumer("email-service", config)
email_consumer.register_handler("user.registered", send_welcome_email)

# Analytics Service
analytics_consumer = DurableEventConsumer("analytics-service", config)
analytics_consumer.register_handler("user.registered", track_registration)

# Billing Service
billing_consumer = DurableEventConsumer("billing-service", config)
billing_consumer.register_handler("user.registered", create_billing_account)
```

### 7. Performance Monitoring

```python
from tc_nats_events.utils.metrics import get_metrics_collector

# Monitor publisher performance
metrics = get_metrics_collector("my-service")
start_time = metrics.record_publish_start()

await publisher.publish("event.type", data)

metrics.record_publish_success(start_time)

# Get performance statistics
stats = metrics.get_metrics()
print(f"P95 latency: {stats['publish_stats']['p95_latency_ms']}ms")
```

### 8. FastAPI Microservice Consumer

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager
from tc_nats_events import DurableEventConsumer, EventPublisher, NATSConfig, Event

class NotificationService:
    def __init__(self):
        self.config = NATSConfig.from_env()
        self.consumer = DurableEventConsumer("notification-service", self.config)
        self.publisher = EventPublisher("notification-service", self.config)
        self.processed_count = 0
    
    async def start(self):
        """Start consuming events in background."""
        # Register event handlers
        self.consumer.register_handler("user.registered", self.handle_user_registered)
        self.consumer.register_handler("order.created", self.handle_order_created)
        
        # Connect publisher and start consumer
        await self.publisher.connect()
        await self.consumer.start()
        
        # Wait for initial sync
        while not self.consumer.is_synced:
            await asyncio.sleep(0.5)
        print("✅ Notification service ready")
    
    async def handle_user_registered(self, event: Event):
        """Process user registration - send welcome email."""
        user_data = event.data
        
        # Send welcome email (simulate)
        await self._send_email(
            email=user_data["email"],
            template="welcome",
            data={"name": user_data["name"]}
        )
        
        self.processed_count += 1
        print(f"📧 Welcome email sent to {user_data['email']}")
    
    async def handle_order_created(self, event: Event):
        """Process order creation - send confirmation."""
        order_data = event.data
        
        await self._send_email(
            email=order_data["customer_email"],
            template="order_confirmation", 
            data={
                "order_id": order_data["order_id"],
                "total": order_data["total_amount"]
            }
        )
        
        self.processed_count += 1
        print(f"📋 Order confirmation sent for {order_data['order_id']}")
    
    async def _send_email(self, email: str, template: str, data: dict):
        """Simulate sending email."""
        await asyncio.sleep(0.1)  # Simulate API call
        
        # Emit notification sent event
        await self.publisher.publish("notification.email.sent", {
            "email": email,
            "template": template,
            "sent_at": datetime.now(timezone.utc).isoformat()
        })
    
    async def stop(self):
        await self.consumer.stop()
        await self.publisher.disconnect()

# FastAPI app with background consumer
notification_service = NotificationService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await notification_service.start()
    yield
    # Shutdown  
    await notification_service.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "consumer_synced": notification_service.consumer.is_synced,
        "events_processed": notification_service.processed_count
    }

@app.get("/stats")  
async def stats():
    return {
        "processed_count": notification_service.processed_count,
        "consumer_state": notification_service.consumer.state,
        "sync_status": notification_service.consumer.get_sync_status()
    }

# Run with: uvicorn main:app --host 0.0.0.0 --port 8000
```

### 9. Complete Microservice Example

```python
import asyncio
from datetime import datetime
from tc_nats_events import (
    EventPublisher, DurableEventConsumer, NATSConfig,
    Event, EventMetadata, setup_logging
)

class OrderService:
    """Complete order processing microservice."""
    
    def __init__(self, config: NATSConfig):
        self.config = config
        self.publisher = EventPublisher("order-service", config)
        self.consumer = DurableEventConsumer("order-service", config)
        self.orders = {}
        
    async def start(self):
        """Start the service."""
        # Connect publisher
        await self.publisher.connect()
        
        # Register event handlers
        self.consumer.register_handler("api.order.requested", self.handle_order_request)
        self.consumer.register_handler("payment.completed", self.handle_payment_completed)
        self.consumer.register_handler("inventory.reserved", self.handle_inventory_reserved)
        
        # Start consumer
        await self.consumer.start()
        
        # Wait for sync
        while not self.consumer.is_synced:
            await asyncio.sleep(0.1)
            
        print("✅ Order service ready")
    
    async def handle_order_request(self, event: Event):
        """Process new order request."""
        order_data = event.data
        order_id = f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create order
        self.orders[order_id] = {
            **order_data,
            "order_id": order_id,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        
        # Set correlation context
        self.publisher.set_context(
            correlation_id=f"order-{order_id}",
            user_id=order_data.get("user_id")
        )
        
        # Emit order created event
        await self.publisher.publish("order.created", self.orders[order_id])
        
        # Request payment
        await self.publisher.publish("payment.requested", {
            "order_id": order_id,
            "amount": order_data["total_amount"],
            "currency": "USD"
        })
        
        # Reserve inventory
        await self.publisher.publish("inventory.reserve_requested", {
            "order_id": order_id,
            "items": order_data["items"]
        })
    
    async def handle_payment_completed(self, event: Event):
        """Handle successful payment."""
        order_id = event.data["order_id"]
        
        if order_id in self.orders:
            self.orders[order_id]["payment_status"] = "completed"
            self.orders[order_id]["payment_id"] = event.data["payment_id"]
            
            await self._check_order_completion(order_id)
    
    async def handle_inventory_reserved(self, event: Event):
        """Handle inventory reservation."""
        order_id = event.data["order_id"]
        
        if order_id in self.orders:
            self.orders[order_id]["inventory_status"] = "reserved"
            self.orders[order_id]["reservation_id"] = event.data["reservation_id"]
            
            await self._check_order_completion(order_id)
    
    async def _check_order_completion(self, order_id: str):
        """Check if order is ready to complete."""
        order = self.orders.get(order_id)
        if not order:
            return
            
        # Check if both payment and inventory are ready
        if (order.get("payment_status") == "completed" and 
            order.get("inventory_status") == "reserved"):
            
            order["status"] = "confirmed"
            order["confirmed_at"] = datetime.now().isoformat()
            
            # Emit order confirmed event
            await self.publisher.publish("order.confirmed", {
                "order_id": order_id,
                "customer_email": order["customer_email"],
                "items": order["items"],
                "total_amount": order["total_amount"]
            })
    
    async def stop(self):
        """Stop the service gracefully."""
        await self.consumer.stop()
        await self.publisher.disconnect()

# Run the service
async def main():
    setup_logging(level="INFO")
    config = NATSConfig.from_env()
    
    service = OrderService(config)
    await service.start()
    
    # Keep running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await service.stop()

if __name__ == "__main__":
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

### Core Examples
- [Basic Publisher/Consumer](examples/basic_pubsub.py) - Simple event publishing and consuming
- [**Custom Events** - Complete Flexibility](examples/custom_events.py) ⭐ - Creating any custom event types
- [Advanced Features](examples/advanced_features.py) - Metrics, idempotency, and monitoring
- [**FastAPI Microservice** - Production Ready](examples/fastapi_microservice.py) 🚀 - Complete microservice with FastAPI consuming events

### Integration Patterns
- [Multi-Service Coordination](examples/multi_service_sync.py) - Coordinating multiple services through events
- [Real-time Data Synchronization](examples/realtime_data_sync.py) - Keeping services in sync with event sourcing
- [Scraper Integration](examples/scraper_integration.py) - Event-driven web scraping system

### Production Patterns
- [Error Handling & Recovery](examples/error_handling_recovery.py) - Robust error handling with circuit breakers
- [Performance Testing](examples/performance_testing.py) - Load testing and performance measurement
- [Configuration Patterns](examples/configuration_patterns.py) - Environment configs and deployment strategies

## 💡 Common Patterns

### Event Naming Conventions

```
service.entity.action
```

Examples:
- `user.profile.updated`
- `payment.transaction.completed`
- `inventory.stock.depleted`
- `notification.email.sent`

### Idempotency Pattern

```python
from tc_nats_events.utils.idempotency import generate_deterministic_id

# Generate deterministic ID for idempotent processing
event_id = generate_deterministic_id(
    data={"order_id": "123", "amount": 99.99},
    fields=["order_id", "amount"]  # Fields that make event unique
)

await publisher.publish("order.created", {
    "id": event_id,
    "order_id": "123",
    "amount": 99.99
})
```

### Event Correlation

```python
# Set correlation context for related events
publisher.set_context(
    correlation_id="order-flow-123",
    user_id="user-456"
)

# All subsequent events will have this context
await publisher.publish("order.created", order_data)
await publisher.publish("payment.initiated", payment_data)
await publisher.publish("inventory.reserved", inventory_data)
```

### Circuit Breaker Pattern

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    async def call(self, func, *args, **kwargs):
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
            else:
                raise Exception("Circuit breaker is open")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
```

## 🎯 Best Practices

### 1. Event Design
- Keep events immutable and self-contained
- Include all necessary data in the event
- Use past tense for event names (e.g., `order.created` not `order.create`)
- Version your events when making breaking changes

### 2. Error Handling
- Implement retry logic with exponential backoff
- Use dead letter queues for permanent failures
- Set appropriate ACK timeouts based on processing time
- Monitor and alert on high failure rates

### 3. Performance
- Batch operations when possible
- Use connection pooling
- Monitor consumer lag and scale accordingly
- Set appropriate max_ack_pending based on throughput

### 4. Security
- Use TLS for NATS connections in production
- Implement proper authentication and authorization
- Never include sensitive data in events
- Use credentials files for NATS authentication

### 5. Monitoring
- Track event processing metrics (latency, throughput, errors)
- Set up alerts for consumer lag
- Monitor stream size and retention
- Use correlation IDs for distributed tracing

## 🎯 Custom Events Documentation

**Any service can create events with any event type!** See [Custom Events Guide](docs/CUSTOM_EVENTS.md) for:

- Creating custom event types
- Naming conventions and patterns
- Real-world service examples
- Event evolution and versioning
- Multi-service coordination

## 🔧 Troubleshooting

### Common Issues

#### Consumer not receiving events
```python
# Check if consumer is synced
if not consumer.is_synced:
    status = consumer.get_sync_status()
    print(f"Still syncing: {status['events_processed']} events processed")

# Ensure handler is registered before starting
consumer.register_handler("event.type", handler_function)
await consumer.start()  # Start AFTER registering handlers
```

#### Connection failures
```python
# Use multiple servers for redundancy
config = NATSConfig(
    servers=[
        "nats://nats-1:4222",
        "nats://nats-2:4222",
        "nats://nats-3:4222"
    ]
)

# Enable reconnection with backoff
config.reconnect_time_wait = 2  # seconds
config.max_reconnect_attempts = 60
```

#### High memory usage
```python
# Limit pending messages
config = NATSConfig(
    max_ack_pending=100,  # Limit concurrent processing
    max_messages=1_000_000  # Limit stream size
)

# Process events in batches
async def handle_batch(events: List[Event]):
    # Process events in chunks
    for chunk in chunks(events, 100):
        await process_chunk(chunk)
```

#### Duplicate event processing
```python
from tc_nats_events.utils.idempotency import get_idempotent_processor

processor = get_idempotent_processor()

async def handle_event(event: Event):
    # Process with idempotency guarantee
    result = await processor.process_with_idempotency(
        event_id=event.metadata.event_id,
        handler_name="my_handler",
        handler_func=actual_processing_function,
        event
    )
```

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

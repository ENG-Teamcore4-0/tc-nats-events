"""
TC NATS Events - Event Sourcing and Durable Consumers for NATS JetStream
=========================================================================

A Python package that implements Event Sourcing and Durable Consumer patterns
using NATS JetStream, designed for building reliable microservices architectures.

Main features:
- Event Sourcing with immutable events
- Durable Consumers with automatic synchronization
- At-least-once delivery guarantees
- Horizontal scaling support
- Automatic recovery and replay
- Structured logging and monitoring
"""

__version__ = "0.1.0"
__author__ = "TeamCore Platform Team"

from .models.event import Event, EventMetadata, EventType, create_event
from .core.event_store import NATSEventStore
from .consumers.durable_consumer import DurableEventConsumer
from .publishers.event_publisher import EventPublisher
from .utils.config import NATSConfig
from .utils.logging import setup_logging

__all__ = [
    # Models
    "Event",
    "EventMetadata",
    "EventType",
    "create_event",
    
    # Core
    "NATSEventStore",
    
    # Consumers
    "DurableEventConsumer",
    
    # Publishers
    "EventPublisher",
    
    # Utils
    "NATSConfig",
    "setup_logging",
]
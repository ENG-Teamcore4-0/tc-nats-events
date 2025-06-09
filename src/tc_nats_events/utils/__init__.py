"""Utilities for TC NATS Events package."""

from .config import NATSConfig
from .logging import setup_logging, get_logger
from .exceptions import (
    TCNATSError,
    ConnectionError,
    PublishError,
    ConsumerError,
    EventStoreError,
    StreamConfigError,
)
from .metrics import MetricsCollector, get_metrics_collector, get_all_metrics
from .idempotency import (
    IdempotentEventProcessor,
    IdempotencyKey,
    get_idempotent_processor,
    generate_deterministic_id
)

__all__ = [
    # Configuration
    "NATSConfig",
    
    # Logging
    "setup_logging",
    "get_logger",
    
    # Exceptions
    "TCNATSError",
    "ConnectionError",
    "PublishError",
    "ConsumerError",
    "EventStoreError",
    "StreamConfigError",
    
    # Metrics
    "MetricsCollector",
    "get_metrics_collector",
    "get_all_metrics",
    
    # Idempotency
    "IdempotentEventProcessor",
    "IdempotencyKey",
    "get_idempotent_processor",
    "generate_deterministic_id",
]
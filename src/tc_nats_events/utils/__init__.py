"""Utilities for TC NATS Events package."""

from .config import NATSConfig
from .exceptions import (
    ConnectionError,
    ConsumerError,
    EventStoreError,
    PublishError,
    StreamConfigError,
    TCNATSError,
)
from .idempotency import (
    IdempotencyKey,
    IdempotentEventProcessor,
    generate_deterministic_id,
    get_idempotent_processor,
)
from .logging import get_logger, setup_logging
from .metrics import MetricsCollector, get_all_metrics, get_metrics_collector

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

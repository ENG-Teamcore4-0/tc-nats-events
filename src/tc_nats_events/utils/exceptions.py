"""
Exceptions
==========

Custom exceptions for TC NATS Events package.
"""


class TCNATSError(Exception):
    """Base exception for TC NATS Events package."""

    pass


class ConnectionError(TCNATSError):
    """Raised when NATS connection fails."""

    pass


class PublishError(TCNATSError):
    """Raised when event publishing fails."""

    pass


class ConsumerError(TCNATSError):
    """Raised when consumer operations fail."""

    pass


class EventStoreError(TCNATSError):
    """Raised when event store operations fail."""

    pass


class StreamConfigError(TCNATSError):
    """Raised when stream configuration fails."""

    pass

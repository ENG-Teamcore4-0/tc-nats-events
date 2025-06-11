"""
Event Models
============

Immutable event models for Event Sourcing pattern.
"""

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from ..adapters.base_adapter import EventAdapter


class EventType(str, Enum):
    """Common event types for convenience. Services can use any string as event_type.

    These are provided as constants for common patterns, but the Event class
    accepts any string as event_type, allowing complete flexibility.

    Examples of custom event types:
    - "billing.invoice_generated"
    - "inventory.stock_low"
    - "notification.email_sent"
    - "analytics.user_action"
    """

    # User events
    USER_CREATED = "user.created"
    USER_UPDATED = "user.updated"
    USER_DELETED = "user.deleted"

    # Product events
    PRODUCT_CREATED = "product.created"
    PRODUCT_UPDATED = "product.updated"
    PRODUCT_DELETED = "product.deleted"
    PRODUCT_PRICE_CHANGED = "product.price_changed"

    # Scraper events
    SCRAPER_STARTED = "scraper.started"
    SCRAPER_COMPLETED = "scraper.completed"
    SCRAPER_FAILED = "scraper.failed"
    SCRAPER_DATA_EXTRACTED = "scraper.data_extracted"

    # Catalog events
    CATALOG_ITEM_ADDED = "catalog.item_added"
    CATALOG_ITEM_UPDATED = "catalog.item_updated"
    CATALOG_ITEM_REMOVED = "catalog.item_removed"


@dataclass(frozen=True)
class EventMetadata:
    """
    Metadata for tracking event lifecycle and origin.

    All fields are immutable to ensure event integrity.
    """

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    source_service: Optional[str] = None
    user_id: Optional[str] = None
    environment: Optional[str] = None
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass(frozen=True)
class Event:
    """
    Immutable event with metadata for Event Sourcing.

    Attributes:
        event_type: Type of the event (e.g., "user.created")
        data: Event payload containing the actual data
        timestamp: ISO format timestamp (auto-generated if not provided)
        metadata: Event metadata for tracking and correlation
        sequence: Stream sequence number (assigned by NATS)
    """

    event_type: str
    data: Dict[str, Any]
    timestamp: Optional[str] = None
    metadata: Optional[EventMetadata] = None
    sequence: Optional[int] = None

    def __post_init__(self) -> None:
        """Initialize default values for timestamp and metadata."""
        if self.event_type is None:
            raise TypeError("event_type cannot be None")

        if self.timestamp is None:
            object.__setattr__(
                self, "timestamp", datetime.now(timezone.utc).isoformat()
            )

        if self.metadata is None:
            object.__setattr__(self, "metadata", EventMetadata())

    def to_json(self) -> bytes:
        """
        Serialize event to JSON bytes for NATS.

        Returns:
            UTF-8 encoded JSON bytes
        """
        event_dict: Dict[str, Any] = {
            "event_type": self.event_type,
            "data": self.data,
            "timestamp": self.timestamp,
            "metadata": self.metadata.to_dict() if self.metadata else {},
        }

        if self.sequence is not None:
            event_dict["sequence"] = self.sequence

        return json.dumps(event_dict, separators=(",", ":")).encode("utf-8")

    @classmethod
    def _try_adapters(
        cls, json_data: bytes, adapters: List["EventAdapter"]
    ) -> Optional["Event"]:
        """Try to deserialize using provided adapters."""
        for adapter in adapters:
            try:
                if adapter.can_handle(json_data):
                    return adapter.adapt_to_event(json_data)
            except Exception:
                # Continue to next adapter if this one fails
                continue
        return None

    @classmethod
    def _validate_required_fields(cls, data: dict) -> None:
        """Validate that required fields are present."""
        required_fields = ["event_type", "data", "timestamp"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

    @classmethod
    def _build_metadata(cls, data: dict) -> Optional[EventMetadata]:
        """Build metadata object if present in data."""
        if "metadata" in data and data["metadata"]:
            return EventMetadata(**data["metadata"])
        return None

    @classmethod
    def from_json(
        cls, json_data: bytes, adapters: Optional[List["EventAdapter"]] = None
    ) -> "Event":
        """
        Deserialize event from JSON bytes with optional format adapters.

        Args:
            json_data: UTF-8 encoded JSON bytes
            adapters: List of EventAdapter instances for handling different formats

        Returns:
            Event instance

        Raises:
            ValueError: If JSON is invalid or missing required fields
        """
        # Try adapters first if provided
        if adapters:
            result = cls._try_adapters(json_data, adapters)
            if result:
                return result

        # Fall back to standard tc-nats-events format
        try:
            data = json.loads(json_data.decode("utf-8"))
            cls._validate_required_fields(data)
            metadata = cls._build_metadata(data)

            return cls(
                event_type=data["event_type"],
                data=data["data"],
                timestamp=data["timestamp"],
                metadata=metadata,
                sequence=data.get("sequence"),
            )

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
        except Exception as e:
            raise ValueError(f"Error deserializing event: {e}")

    def with_sequence(self, sequence: int) -> "Event":
        """
        Create a new Event instance with the given sequence number.

        Args:
            sequence: NATS stream sequence number

        Returns:
            New Event instance with sequence set
        """
        return Event(
            event_type=self.event_type,
            data=self.data,
            timestamp=self.timestamp,
            metadata=self.metadata,
            sequence=sequence,
        )

    def is_valid(self) -> bool:
        """
        Validate event structure and data.

        Returns:
            True if event is valid, False otherwise
        """
        return (
            bool(self.event_type)
            and isinstance(self.event_type, str)
            and isinstance(self.data, dict)
            and bool(self.timestamp)
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"Event(type={self.event_type}, "
            f"sequence={self.sequence}, "
            f"timestamp={self.timestamp})"
        )

    @classmethod
    def create(cls, event_type: str, data: Dict[str, Any], **kwargs: Any) -> "Event":
        """Factory method for creating events with any event type.

        Args:
            event_type: Any string representing the event type
            data: Event payload
            **kwargs: Additional fields (metadata, timestamp, etc.)

        Returns:
            Event instance

        Examples:
            # Using predefined types
            event = Event.create(EventType.USER_CREATED, {"user_id": "123"})

            # Using custom types
            event = Event.create("billing.invoice_generated", {
                "invoice_id": "inv-123",
                "amount": 99.99
            })

            # With custom metadata
            event = Event.create("analytics.user_action",
                data={"action": "click", "button": "signup"},
                metadata=EventMetadata(user_id="user-456")
            )
        """
        return cls(event_type=event_type, data=data, **kwargs)

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"Event(event_type={self.event_type!r}, "
            f"data={self.data!r}, "
            f"timestamp={self.timestamp!r}, "
            f"metadata={self.metadata!r}, "
            f"sequence={self.sequence!r})"
        )


def create_event(event_type: str, data: Dict[str, Any], **kwargs: Any) -> Event:
    """Convenience function for creating events with any event type.

    This is a module-level function that provides the same functionality
    as Event.create() for those who prefer functional style.

    Args:
        event_type: Any string representing the event type
        data: Event payload
        **kwargs: Additional fields (metadata, timestamp, etc.)

    Returns:
        Event instance

    Examples:
        # Simple custom event
        event = create_event("my_service.data_processed", {
            "records_count": 150,
            "processing_time_ms": 234
        })

        # With correlation tracking
        event = create_event("order.payment_failed",
            data={"order_id": "order-123", "reason": "insufficient_funds"},
            metadata=EventMetadata(
                correlation_id="order-session-456",
                user_id="user-789"
            )
        )
    """
    return Event.create(event_type, data, **kwargs)

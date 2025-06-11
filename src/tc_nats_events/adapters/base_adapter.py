"""
Base Event Adapter
==================

Abstract base class for event format adapters.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..models.event import Event, EventMetadata


class EventAdapter(ABC):
    """
    Abstract base class for converting different event formats to tc-nats-events Event format.

    This allows tc-nats-events to work with any event format by providing
    appropriate adapters.
    """

    @abstractmethod
    def can_handle(self, raw_data: bytes) -> bool:
        """
        Check if this adapter can handle the given raw data format.

        Args:
            raw_data: Raw event data from NATS

        Returns:
            True if this adapter can process the data, False otherwise
        """
        pass

    @abstractmethod
    def adapt_to_event(self, raw_data: bytes) -> Event:
        """
        Convert raw event data to tc-nats-events Event format.

        Args:
            raw_data: Raw event data from NATS

        Returns:
            Event instance compatible with tc-nats-events

        Raises:
            ValueError: If the data cannot be converted
        """
        pass

    def get_adapter_name(self) -> str:
        """Get a human-readable name for this adapter."""
        return self.__class__.__name__

    def _create_safe_event(
        self,
        event_type: str,
        data: Dict[str, Any],
        timestamp: Optional[str] = None,
        metadata: Optional[EventMetadata] = None,
    ) -> Event:
        """
        Helper method to create an Event safely.

        Args:
            event_type: Type of the event
            data: Event data
            timestamp: Event timestamp (ISO format)
            metadata: Event metadata

        Returns:
            Event instance
        """
        return Event(
            event_type=event_type,
            data=data,
            timestamp=timestamp,
            metadata=metadata or EventMetadata(),
        )

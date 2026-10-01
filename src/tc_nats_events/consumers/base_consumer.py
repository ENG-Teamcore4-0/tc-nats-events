"""
Base Consumer
=============

Abstract base class for event consumers.
"""

import asyncio
import inspect
import logging
import warnings
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional

from ..models.event import Event

logger = logging.getLogger(__name__)

# Type alias for event handlers (sync or async)
EventHandler = Callable[[Event], Any]

CATCH_ALL = "*"


async def invoke_handler(handler: EventHandler, event: Event) -> Any:
    """
    Call a sync or async handler and return its result.

    Sync handlers run in the default executor so they cannot block heartbeats,
    lease renewal or the handler timeout.
    """
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    ):
        return await handler(event)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, handler, event)
    if inspect.isawaitable(result):
        return await result
    return result


class BaseEventConsumer(ABC):
    """
    Abstract base class for event consumers.

    Provides common functionality and interface for all consumer types.
    """

    def __init__(self, service_name: str):
        """
        Initialize base consumer.

        Args:
            service_name: Name of the consuming service
        """
        self.service_name = service_name
        self._handlers: Dict[str, EventHandler] = {}
        self._default_handler: Optional[EventHandler] = None
        self._state: Dict[str, Any] = {}

        # Metrics
        self.events_processed = 0
        self.events_failed = 0
        self.last_processed_sequence = 0

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """
        Register a handler for a specific event type.

        Args:
            event_type: Type of event to handle. ``"*"`` is accepted as an
                alias of :meth:`register_default_handler` (deprecated).
            handler: Callable that processes the event
        """
        if event_type == CATCH_ALL:
            warnings.warn(
                'register_handler("*") is deprecated; use register_default_handler()',
                DeprecationWarning,
                stacklevel=2,
            )
            self.register_default_handler(handler)
            return
        self._handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")

    def register_default_handler(self, handler: EventHandler) -> None:
        """
        Register a default handler for unhandled event types.

        Args:
            handler: Callable that processes unhandled events
        """
        self._default_handler = handler
        logger.info("Registered default event handler")

    def get_handler(self, event_type: str) -> Optional[EventHandler]:
        """
        Get handler for a specific event type.

        Args:
            event_type: Type of event

        Returns:
            Handler function or default handler if not found
        """
        return self._handlers.get(event_type, self._default_handler)

    @abstractmethod
    async def start(self) -> None:
        """Start the consumer."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the consumer."""
        pass

    @abstractmethod
    async def process_event(self, event: Event) -> None:
        """
        Process a single event.

        Args:
            event: Event to process
        """
        pass

    def get_state(self) -> Dict[str, Any]:
        """Get current consumer state."""
        return self._state.copy()

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            "service_name": self.service_name,
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "last_processed_sequence": self.last_processed_sequence,
            "state_size": len(self._state),
            "registered_handlers": list(self._handlers.keys()),
        }

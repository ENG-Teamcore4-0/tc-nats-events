"""
Event Publisher
===============

High-level publisher with delivery confirmation and retry logic.
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import uuid

from ..core.event_store import NATSEventStore
from ..models.event import Event, EventMetadata, EventType
from ..utils.config import NATSConfig
from ..utils.exceptions import PublishError
from ..utils.metrics import get_metrics_collector

logger = logging.getLogger(__name__)


class EventPublisher:
    """
    Event publisher with automatic retry and monitoring.
    
    Features:
    - Automatic event metadata enrichment
    - Delivery confirmation
    - Retry logic with exponential backoff
    - Batch publishing support
    - Correlation and causation tracking
    """
    
    def __init__(
        self,
        service_name: str,
        config: NATSConfig,
        environment: Optional[str] = None
    ):
        """
        Initialize event publisher.
        
        Args:
            service_name: Name of the publishing service
            config: NATS configuration
            environment: Environment name (dev, staging, prod)
        """
        self.service_name = service_name
        self.config = config
        self.environment = environment or "development"
        
        # Event store
        self._event_store = NATSEventStore(config)
        
        # Metrics
        self.events_published = 0
        self.events_failed = 0
        self.last_publish_time: Optional[datetime] = None
        self._metrics = get_metrics_collector(service_name)
        
        # Retry configuration
        self.max_retry_attempts = 3
        self.retry_delay_base = 1.0  # seconds
        
        # Context tracking
        self._current_correlation_id: Optional[str] = None
        self._current_causation_id: Optional[str] = None
        self._current_user_id: Optional[str] = None
    
    async def connect(self) -> None:
        """Connect to NATS."""
        await self._event_store.connect()
        logger.info(f"Publisher {self.service_name} connected")
    
    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        await self._event_store.disconnect()
        logger.info(f"Publisher {self.service_name} disconnected")
    
    def set_context(
        self,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> None:
        """
        Set context for subsequent event publications.
        
        Args:
            correlation_id: ID to correlate related events
            causation_id: ID of the event that caused this event
            user_id: ID of the user triggering the event
        """
        self._current_correlation_id = correlation_id
        self._current_causation_id = causation_id
        self._current_user_id = user_id
    
    def clear_context(self) -> None:
        """Clear context information."""
        self._current_correlation_id = None
        self._current_causation_id = None
        self._current_user_id = None
    
    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        metadata: Optional[EventMetadata] = None
    ) -> int:
        """
        Publish an event with automatic retry.
        
        Args:
            event_type: Type of event to publish
            data: Event payload
            metadata: Optional event metadata (auto-generated if not provided)
            
        Returns:
            Stream sequence number
            
        Raises:
            PublishError: If publishing fails after retries
        """
        # Create or enrich metadata
        if metadata is None:
            metadata = self._create_metadata()
        else:
            metadata = self._enrich_metadata(metadata)
        
        # Create event
        event = Event(
            event_type=event_type,
            data=data,
            metadata=metadata
        )
        
        # Publish with retry
        sequence = await self._publish_with_retry(event)
        
        # Update metrics
        self.events_published += 1
        self.last_publish_time = datetime.now(timezone.utc)
        
        logger.info(
            f"Event published: type={event_type}, "
            f"sequence={sequence}, "
            f"event_id={metadata.event_id}",
            extra={
                "event_type": event_type,
                "sequence": sequence,
                "event_id": metadata.event_id,
                "service_name": self.service_name,
                "correlation_id": metadata.correlation_id,
                "environment": self.environment
            }
        )
        
        return sequence
    
    async def _publish_with_retry(self, event: Event) -> int:
        """Publish event with exponential backoff retry."""
        last_error = None
        
        for attempt in range(self.max_retry_attempts):
            try:
                return await self._event_store.publish_event(event)
                
            except Exception as e:
                last_error = e
                self.events_failed += 1
                
                if attempt < self.max_retry_attempts - 1:
                    delay = self.retry_delay_base * (2 ** attempt)
                    logger.warning(
                        f"Publish failed (attempt {attempt + 1}), "
                        f"retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Publish failed after {self.max_retry_attempts} attempts: {e}"
                    )
        
        raise PublishError(
            f"Failed to publish event after {self.max_retry_attempts} attempts: "
            f"{last_error}"
        )
    
    async def publish_batch(
        self,
        events: List[tuple[str, Dict[str, Any]]]
    ) -> List[int]:
        """
        Publish multiple events in a batch.
        
        Args:
            events: List of (event_type, data) tuples
            
        Returns:
            List of sequence numbers
        """
        sequences = []
        
        for event_type, data in events:
            try:
                seq = await self.publish(event_type, data)
                sequences.append(seq)
            except Exception as e:
                logger.error(f"Failed to publish event in batch: {e}")
                sequences.append(-1)
        
        return sequences
    
    def _create_metadata(self) -> EventMetadata:
        """Create metadata with context information."""
        return EventMetadata(
            event_id=str(uuid.uuid4()),
            correlation_id=self._current_correlation_id,
            causation_id=self._current_causation_id,
            source_service=self.service_name,
            user_id=self._current_user_id,
            environment=self.environment,
            version="1.0"
        )
    
    def _enrich_metadata(self, metadata: EventMetadata) -> EventMetadata:
        """Enrich existing metadata with context."""
        # Create new metadata with enriched values
        return EventMetadata(
            event_id=metadata.event_id,
            correlation_id=metadata.correlation_id or self._current_correlation_id,
            causation_id=metadata.causation_id or self._current_causation_id,
            source_service=metadata.source_service or self.service_name,
            user_id=metadata.user_id or self._current_user_id,
            environment=metadata.environment or self.environment,
            version=metadata.version
        )
    
    # Convenience methods for common event types
    
    async def publish_user_created(
        self,
        user_id: str,
        user_data: Dict[str, Any]
    ) -> int:
        """Publish user created event."""
        data = {"user_id": user_id, **user_data}
        return await self.publish(EventType.USER_CREATED, data)
    
    async def publish_user_updated(
        self,
        user_id: str,
        updates: Dict[str, Any]
    ) -> int:
        """Publish user updated event."""
        data = {"user_id": user_id, **updates}
        return await self.publish(EventType.USER_UPDATED, data)
    
    async def publish_user_deleted(self, user_id: str) -> int:
        """Publish user deleted event."""
        return await self.publish(EventType.USER_DELETED, {"user_id": user_id})
    
    async def publish_product_created(
        self,
        product_id: str,
        product_data: Dict[str, Any]
    ) -> int:
        """Publish product created event."""
        data = {"product_id": product_id, **product_data}
        return await self.publish(EventType.PRODUCT_CREATED, data)
    
    async def publish_product_updated(
        self,
        product_id: str,
        updates: Dict[str, Any]
    ) -> int:
        """Publish product updated event."""
        data = {"product_id": product_id, **updates}
        return await self.publish(EventType.PRODUCT_UPDATED, data)
    
    async def publish_scraper_started(
        self,
        scraper_id: str,
        target: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Publish scraper started event."""
        data = {
            "scraper_id": scraper_id,
            "target": target,
            "started_at": datetime.now(timezone.utc).isoformat(),
            **(metadata or {})
        }
        return await self.publish(EventType.SCRAPER_STARTED, data)
    
    async def publish_scraper_completed(
        self,
        scraper_id: str,
        items_extracted: int,
        duration_seconds: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Publish scraper completed event."""
        data = {
            "scraper_id": scraper_id,
            "items_extracted": items_extracted,
            "duration_seconds": duration_seconds,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            **(metadata or {})
        }
        return await self.publish(EventType.SCRAPER_COMPLETED, data)
    
    async def publish_scraper_data_extracted(
        self,
        scraper_id: str,
        data_type: str,
        items: List[Dict[str, Any]]
    ) -> int:
        """Publish scraper data extracted event."""
        data = {
            "scraper_id": scraper_id,
            "data_type": data_type,
            "item_count": len(items),
            "items": items,
            "extracted_at": datetime.now(timezone.utc).isoformat()
        }
        return await self.publish(EventType.SCRAPER_DATA_EXTRACTED, data)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get publisher metrics."""
        return {
            "service_name": self.service_name,
            "events_published": self.events_published,
            "events_failed": self.events_failed,
            "last_publish_time": (
                self.last_publish_time.isoformat() 
                if self.last_publish_time else None
            ),
            "environment": self.environment,
            "is_connected": self._event_store.is_connected
        }
    
    # Context manager support
    
    async def __aenter__(self) -> 'EventPublisher':
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()
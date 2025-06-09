"""
NATS Event Store
================

Core event store implementation using NATS JetStream for persistent event storage.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

import nats
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StorageType, StreamConfig

from ..models.event import Event
from ..utils.config import NATSConfig
from ..utils.exceptions import (
    ConnectionError,
    EventStoreError,
    PublishError,
    StreamConfigError,
)
from ..utils.metrics import get_metrics_collector

logger = logging.getLogger(__name__)


class NATSEventStore:
    """
    High-level abstraction over NATS JetStream for Event Sourcing.

    Features:
    - Automatic stream creation and configuration
    - Event publishing with delivery confirmation
    - Stream management and monitoring
    - Connection resilience and recovery
    """

    def __init__(self, config: NATSConfig):
        """
        Initialize NATS Event Store.

        Args:
            config: NATS configuration object
        """
        self.config = config
        self._nc: Optional[NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._is_connected = False
        self._lock = asyncio.Lock()

        # Stream configuration
        self.stream_name = config.stream_name
        self.subject_prefix = config.subject_prefix

        # Connection state
        self._reconnect_task: Optional[asyncio.Task] = None
        self._closed = False

        # Metrics
        self._metrics = get_metrics_collector(f"eventstore-{config.stream_name}")

    @property
    def is_connected(self) -> bool:
        """Check if connected to NATS."""
        return self._is_connected and self._nc is not None and self._nc.is_connected

    async def connect(self) -> None:
        """
        Connect to NATS server with automatic stream setup.

        Raises:
            ConnectionError: If connection fails
        """
        async with self._lock:
            if self._is_connected:
                logger.debug("Already connected to NATS")
                return

            try:
                logger.info(f"Connecting to NATS at {self.config.servers}")

                # Connection options with resilience
                options = {
                    "servers": self.config.servers,
                    "name": f"{self.config.client_name}-eventstore",
                    "reconnect_time_wait": self.config.reconnect_time_wait,
                    "max_reconnect_attempts": self.config.max_reconnect_attempts,
                    "error_cb": self._error_callback,
                    "disconnected_cb": self._disconnected_callback,
                    "reconnected_cb": self._reconnected_callback,
                    "closed_cb": self._closed_callback,
                }

                # Add authentication if configured
                if self.config.user and self.config.password:
                    options["user"] = self.config.user
                    options["password"] = self.config.password

                self._nc = await nats.connect(**options)
                self._js = self._nc.jetstream()

                # Ensure stream exists
                await self._ensure_stream_exists()

                self._is_connected = True
                logger.info("Connected to NATS successfully")

            except Exception as e:
                logger.error(f"Failed to connect to NATS: {e}")
                raise ConnectionError(f"NATS connection failed: {e}")

    async def _ensure_stream_exists(self) -> None:
        """
        Create or update stream configuration.

        Raises:
            StreamConfigError: If stream configuration fails
        """
        try:
            # Check if stream exists
            try:
                stream_info = await self._js.stream_info(self.stream_name)
                logger.info(f"Stream '{self.stream_name}' already exists")

                # Update stream if subjects changed
                current_subjects = set(stream_info.config.subjects)
                expected_subjects = {f"{self.subject_prefix}.*"}

                if current_subjects != expected_subjects:
                    await self._update_stream_config()

            except nats.js.errors.NotFoundError:
                # Create new stream
                await self._create_stream()

        except Exception as e:
            logger.error(f"Stream configuration failed: {e}")
            raise StreamConfigError(f"Failed to configure stream: {e}")

    async def _create_stream(self) -> None:
        """Create new stream with optimized configuration."""
        config = StreamConfig(
            name=self.stream_name,
            subjects=[f"{self.subject_prefix}.*"],
            # Persistence configuration for Event Sourcing
            retention=RetentionPolicy.LIMITS,
            storage=StorageType.FILE,  # Persistent storage
            # Retention limits
            max_msgs=self.config.max_messages,
            max_bytes=self.config.max_bytes,
            max_age=self.config.max_age_seconds * 1_000_000_000,  # nanoseconds
            # Performance settings
            max_msg_size=self.config.max_msg_size,
            duplicate_window=120 * 1_000_000_000,  # 2 minutes deduplication
            # Replication for durability (production should use 3+)
            replicas=self.config.replicas,
            # Allow direct access for fast reads
            allow_direct=True,
            # Ensure message ordering per subject
            discard_new_per_subject=False,
            # Enable mirroring if needed
            mirror_direct=True,
        )

        await self._js.add_stream(config)
        logger.info(f"Stream '{self.stream_name}' created successfully")

    async def _update_stream_config(self) -> None:
        """Update existing stream configuration."""
        stream_info = await self._js.stream_info(self.stream_name)

        config = stream_info.config
        config.subjects = [f"{self.subject_prefix}.*"]

        await self._js.update_stream(config)
        logger.info(f"Stream '{self.stream_name}' configuration updated")

    async def publish_event(self, event: Event) -> int:
        """
        Publish event with delivery confirmation.

        Args:
            event: Event to publish

        Returns:
            Stream sequence number

        Raises:
            PublishError: If publishing fails
            ConnectionError: If not connected
        """
        if not self.is_connected:
            raise ConnectionError("Not connected to NATS")

        if not event.is_valid():
            raise ValueError("Invalid event structure")

        # Build subject from event type
        subject = f"{self.subject_prefix}.{event.event_type}"

        start_time = self._metrics.record_publish_start()

        try:
            # Publish with acknowledgment and idempotency headers
            headers = {
                "event-id": event.metadata.event_id,
                "event-type": event.event_type,
                "source-service": event.metadata.source_service or "unknown",
                "correlation-id": event.metadata.correlation_id or "",
                "causation-id": event.metadata.causation_id or "",
                "timestamp": event.timestamp,
            }

            ack = await self._js.publish(
                subject=subject, payload=event.to_json(), headers=headers
            )

            # Record metrics
            self._metrics.record_publish_success(start_time)
            self._metrics.record_stream_sequence(self.stream_name, ack.seq)

            logger.info(
                f"Event published: type={event.event_type}, "
                f"sequence={ack.seq}, subject={subject}",
                extra={
                    "event_type": event.event_type,
                    "sequence": ack.seq,
                    "stream": self.stream_name,
                    "event_id": event.metadata.event_id,
                },
            )

            return ack.seq

        except NATSTimeoutError:
            self._metrics.record_publish_error("timeout")
            logger.error(f"Timeout publishing event: {event.event_type}")
            raise PublishError("Event publish timeout")
        except Exception as e:
            self._metrics.record_publish_error("general")
            logger.error(f"Failed to publish event: {e}")
            raise PublishError(f"Event publish failed: {e}")

    async def publish_batch(self, events: List[Event]) -> List[int]:
        """
        Publish multiple events efficiently.

        Args:
            events: List of events to publish

        Returns:
            List of sequence numbers
        """
        sequences = []

        for event in events:
            try:
                seq = await self.publish_event(event)
                sequences.append(seq)
            except Exception as e:
                logger.error(f"Failed to publish event in batch: {e}")
                # Continue with other events
                sequences.append(-1)

        return sequences

    async def get_stream_info(self) -> Dict[str, Any]:
        """
        Get current stream information and statistics.

        Returns:
            Stream information dictionary
        """
        if not self.is_connected:
            raise ConnectionError("Not connected to NATS")

        try:
            info = await self._js.stream_info(self.stream_name)

            return {
                "name": info.config.name,
                "subjects": info.config.subjects,
                "messages": info.state.messages,
                "bytes": info.state.bytes,
                "first_seq": info.state.first_seq,
                "last_seq": info.state.last_seq,
                "consumer_count": info.state.consumer_count,
                "created": str(info.created),
                "cluster": {
                    "name": info.cluster.name if info.cluster else None,
                    "leader": info.cluster.leader if info.cluster else None,
                },
            }

        except Exception as e:
            logger.error(f"Failed to get stream info: {e}")
            raise EventStoreError(f"Could not retrieve stream info: {e}")

    async def create_consumer(
        self, consumer_name: str, filter_subjects: Optional[List[str]] = None
    ) -> ConsumerConfig:
        """
        Create a durable consumer configuration.

        Args:
            consumer_name: Name of the consumer
            filter_subjects: Optional subject filters

        Returns:
            Consumer configuration
        """
        config = ConsumerConfig(
            name=consumer_name,
            durable_name=consumer_name,
            filter_subjects=filter_subjects or [f"{self.subject_prefix}.*"],
            ack_policy="explicit",
            replay_policy="instant",
            deliver_policy="all",
            max_deliver=3,
            ack_wait=30,
            max_ack_pending=1000,
        )

        await self._js.add_consumer(self.stream_name, config)
        logger.info(f"Consumer '{consumer_name}' created")

        return config

    async def delete_consumer(self, consumer_name: str) -> None:
        """Delete a consumer."""
        try:
            await self._js.delete_consumer(self.stream_name, consumer_name)
            logger.info(f"Consumer '{consumer_name}' deleted")
        except Exception as e:
            logger.error(f"Failed to delete consumer: {e}")
            raise EventStoreError(f"Could not delete consumer: {e}")

    async def disconnect(self) -> None:
        """Gracefully disconnect from NATS."""
        async with self._lock:
            if not self._is_connected:
                return

            self._closed = True

            if self._reconnect_task:
                self._reconnect_task.cancel()

            if self._nc:
                await self._nc.drain()
                await self._nc.close()

            self._is_connected = False
            self._nc = None
            self._js = None

            logger.info("Disconnected from NATS")

    # Callback methods for connection lifecycle

    async def _error_callback(self, e: Exception) -> None:
        """Handle NATS errors."""
        logger.error(f"NATS error: {e}")

    async def _disconnected_callback(self) -> None:
        """Handle disconnection."""
        logger.warning("Disconnected from NATS")
        self._is_connected = False

    async def _reconnected_callback(self) -> None:
        """Handle reconnection."""
        logger.info("Reconnected to NATS")
        self._is_connected = True

    async def _closed_callback(self) -> None:
        """Handle connection closed."""
        logger.info("NATS connection closed")
        self._is_connected = False

    # Context manager support

    async def __aenter__(self) -> "NATSEventStore":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()

    # Utility methods

    @asynccontextmanager
    async def transaction(self):
        """
        Context manager for transactional event publishing.

        Note: This is a logical transaction, not ACID.
        """
        events = []

        try:
            yield events
            # Publish all events
            await self.publish_batch(events)
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise

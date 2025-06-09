"""
Durable Event Consumer
======================

Implementation of durable consumer pattern with automatic synchronization and recovery.
"""

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, ReplayPolicy

from ..models.event import Event
from ..utils.config import NATSConfig
from ..utils.exceptions import ConnectionError, ConsumerError
from ..utils.idempotency import get_idempotent_processor
from ..utils.metrics import get_metrics_collector
from .base_consumer import BaseEventConsumer

logger = logging.getLogger(__name__)


class ConsumerState(str, Enum):
    """Consumer operational states."""

    IDLE = "idle"
    STARTING = "starting"
    SYNCING = "syncing"
    LIVE = "live"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class DurableEventConsumer(BaseEventConsumer):
    """
    Durable consumer with automatic synchronization and recovery.

    Features:
    - Automatic synchronization from event #1
    - At-least-once delivery guarantee
    - Horizontal scaling support
    - Automatic recovery on failure
    - Flow control and backpressure handling
    """

    def __init__(
        self,
        service_name: str,
        config: NATSConfig,
        batch_size: int = 10,
        fetch_timeout: float = 2.0,
    ):
        """
        Initialize durable event consumer.

        Args:
            service_name: Name of the consuming service
            config: NATS configuration
            batch_size: Number of messages to fetch per batch
            fetch_timeout: Timeout for fetch operations in seconds
        """
        super().__init__(service_name)

        self.config = config
        self.batch_size = batch_size
        self.fetch_timeout = fetch_timeout

        # NATS components
        self._nc: Optional[NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._subscription: Optional[Any] = None

        # Consumer configuration
        self.stream_name = config.stream_name
        self.consumer_name = f"{service_name}-consumer"
        self.subject_filter = f"{config.subject_prefix}.*"

        # State management
        self._consumer_state = ConsumerState.IDLE
        self._is_running = False
        self._processing_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # Synchronization tracking
        self._sync_start_time: Optional[datetime] = None
        self._sync_complete_time: Optional[datetime] = None
        self._initial_sync_complete = False

        # Error handling
        self._consecutive_errors = 0
        self._max_consecutive_errors = 10

        # Metrics and idempotency
        self._metrics = get_metrics_collector(service_name)
        self._idempotent_processor = get_idempotent_processor()

        # Processing counters
        self.events_processed: int = 0
        self.events_failed: int = 0
        self.last_processed_sequence: int = 0

    @property
    def state(self) -> ConsumerState:
        """Get current consumer state."""
        return self._consumer_state

    @property
    def is_synced(self) -> bool:
        """Check if initial synchronization is complete."""
        return self._initial_sync_complete

    async def start(self) -> None:
        """
        Start the consumer with automatic synchronization.

        Raises:
            ConsumerError: If startup fails
        """
        async with self._lock:
            if self._is_running:
                logger.warning(f"Consumer {self.service_name} already running")
                return

            try:
                self._consumer_state = ConsumerState.STARTING
                logger.info(f"Starting consumer: {self.service_name}")

                # Connect to NATS
                await self._connect()

                # Setup durable consumer
                await self._setup_consumer()

                # Start processing
                self._is_running = True
                self._processing_task = asyncio.create_task(
                    self._event_processing_loop()
                )

                logger.info(f"Consumer {self.service_name} started successfully")

            except Exception as e:
                self._consumer_state = ConsumerState.ERROR
                logger.error(f"Failed to start consumer: {e}")
                raise ConsumerError(f"Consumer startup failed: {e}")

    async def _connect(self) -> None:
        """Connect to NATS server."""
        try:
            connect_options = {
                "name": f"{self.config.client_name}-{self.service_name}",
                "reconnect_time_wait": self.config.reconnect_time_wait,
                "max_reconnect_attempts": self.config.max_reconnect_attempts,
            }

            if self.config.user and self.config.password:
                connect_options["user"] = self.config.user
                connect_options["password"] = self.config.password

            self._nc = await nats.connect(
                servers=self.config.servers, **connect_options
            )
            self._js = self._nc.jetstream()

            logger.info("Connected to NATS for consumer")

        except Exception as e:
            logger.error(f"NATS connection failed: {e}")
            raise ConnectionError(f"Failed to connect to NATS: {e}")

    async def _setup_consumer(self) -> None:
        """Setup durable consumer configuration."""
        try:
            # Check if consumer exists
            try:
                if self._js is None:
                    raise ConsumerError("JetStream context not initialized")
                info = await self._js.consumer_info(
                    self.stream_name, self.consumer_name
                )
                logger.info(
                    f"Durable consumer exists: {self.consumer_name}, "
                    f"delivered={info.delivered.stream_seq}, "
                    f"pending={info.num_ack_pending}"
                )

            except nats.js.errors.NotFoundError:
                # Create new durable consumer
                await self._create_consumer()

            # Create pull subscription
            if self._js is None:
                raise ConsumerError("JetStream context not initialized")
            self._subscription = await self._js.pull_subscribe(
                subject="",  # Empty - uses consumer's filter
                durable=self.consumer_name,
                stream=self.stream_name,
            )

        except Exception as e:
            logger.error(f"Consumer setup failed: {e}")
            raise ConsumerError(f"Failed to setup consumer: {e}")

    async def _create_consumer(self) -> None:
        """Create new durable consumer with optimal configuration."""
        config = ConsumerConfig(
            # Identity
            name=self.consumer_name,
            durable_name=self.consumer_name,
            # Delivery configuration
            deliver_policy=DeliverPolicy.ALL,  # Start from beginning
            ack_policy=AckPolicy.EXPLICIT,
            replay_policy=ReplayPolicy.INSTANT,
            # Subject filtering
            filter_subject=self.subject_filter,
            # Reliability settings
            max_deliver=self.config.max_deliver_attempts,
            ack_wait=self.config.ack_wait_seconds,
            # Flow control
            max_ack_pending=self.config.max_ack_pending,
            # Sampling (process all events)
            sample_freq=None,
        )

        if self._js is None:
            raise ConsumerError("JetStream context not initialized")
        await self._js.add_consumer(self.stream_name, config)
        logger.info(f"Created durable consumer: {self.consumer_name}")

    async def _event_processing_loop(self) -> None:
        """Main event processing loop."""
        logger.info(f"Starting event processing loop for {self.service_name}")

        self._consumer_state = ConsumerState.SYNCING
        self._sync_start_time = datetime.now(timezone.utc)

        while self._is_running:
            try:
                # Fetch batch of messages
                if self._subscription is None:
                    raise ConsumerError("Subscription not initialized")
                messages = await self._subscription.fetch(
                    batch=self.batch_size, timeout=self.fetch_timeout
                )

                if messages:
                    await self._process_batch(messages)
                    self._consecutive_errors = 0  # Reset error counter

                    # Log progress during sync
                    if self._consumer_state == ConsumerState.SYNCING:
                        logger.info(
                            f"Syncing: processed {len(messages)} events, "
                            f"total={self.events_processed}"
                        )
                else:
                    # No messages - check if sync complete
                    if self._consumer_state == ConsumerState.SYNCING:
                        await self._complete_sync()

            except asyncio.TimeoutError:
                # Normal timeout - no new messages
                if self._consumer_state == ConsumerState.SYNCING:
                    await self._complete_sync()

            except Exception as e:
                self._consecutive_errors += 1
                logger.error(f"Processing error (#{self._consecutive_errors}): {e}")

                if self._consecutive_errors >= self._max_consecutive_errors:
                    logger.error("Max consecutive errors reached, stopping")
                    self._consumer_state = ConsumerState.ERROR
                    break

                # Backoff on error
                await asyncio.sleep(min(self._consecutive_errors, 10))

        logger.info(f"Event processing loop ended for {self.service_name}")

    async def _process_batch(self, messages: List[Msg]) -> None:
        """Process a batch of messages."""
        for msg in messages:
            try:
                await self._process_message(msg)
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                await msg.nak()  # NACK for retry

    async def _process_message(self, msg: Msg) -> None:
        """Process a single message."""
        start_time = self._metrics.record_consume_start()

        try:
            # Parse event
            event = Event.from_json(msg.data)

            # Add sequence from NATS
            event = event.with_sequence(msg.metadata.sequence.stream)

            # Process through handlers with idempotency
            await self.process_event(event)

            # ACK after successful processing
            await msg.ack()

            # Update metrics
            self.events_processed += 1
            self.last_processed_sequence = event.sequence or 0
            self._metrics.record_consume_success(start_time)

            logger.debug(
                "Message processed successfully",
                extra={
                    "event_type": event.event_type,
                    "sequence": event.sequence,
                    "event_id": event.metadata.event_id if event.metadata else None,
                    "service_name": self.service_name,
                },
            )

        except Exception as e:
            self._metrics.record_consume_error("processing")
            logger.error(
                f"Message processing failed: {e}, "
                f"sequence={msg.metadata.sequence.stream}",
                extra={
                    "sequence": msg.metadata.sequence.stream,
                    "service_name": self.service_name,
                    "error": str(e),
                },
            )
            raise

    async def process_event(self, event: Event) -> None:
        """
        Process event through registered handlers with idempotency.

        Args:
            event: Event to process
        """
        handler = self.get_handler(event.event_type)

        if handler:
            try:
                # Process with idempotency guarantee
                if event.metadata is None:
                    raise ConsumerError("Event metadata is required")
                await self._idempotent_processor.process_with_idempotency(
                    event.metadata.event_id,
                    f"{self.service_name}.{event.event_type}",
                    handler,
                    event,
                )

            except Exception as e:
                self.events_failed += 1
                event_id = event.metadata.event_id if event.metadata else 'unknown'
                logger.error(
                    f"Handler failed for {event.event_type}: {e}, "
                    f"event_id={event_id}",
                    extra={
                        "event_type": event.event_type,
                        "event_id": event.metadata.event_id if event.metadata else None,
                        "service_name": self.service_name,
                        "error": str(e),
                    },
                )
                raise
        else:
            logger.warning(
                f"No handler for event type: {event.event_type}, "
                f"sequence={event.sequence}",
                extra={
                    "event_type": event.event_type,
                    "sequence": event.sequence,
                    "service_name": self.service_name,
                },
            )

    async def _complete_sync(self) -> None:
        """Mark initial synchronization as complete."""
        if not self._initial_sync_complete:
            self._initial_sync_complete = True
            self._consumer_state = ConsumerState.LIVE
            self._sync_complete_time = datetime.now(timezone.utc)

            sync_duration = (
                self._sync_complete_time
                - (self._sync_start_time or self._sync_complete_time)
            ).total_seconds()

            logger.info(
                f"Initial sync completed: "
                f"service={self.service_name}, "
                f"events={self.events_processed}, "
                f"duration={sync_duration:.2f}s, "
                f"last_seq={self.last_processed_sequence}"
            )

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        async with self._lock:
            if not self._is_running:
                return

            logger.info(f"Stopping consumer: {self.service_name}")
            self._consumer_state = ConsumerState.STOPPING
            self._is_running = False

            # Cancel processing task
            if self._processing_task:
                self._processing_task.cancel()
                try:
                    await self._processing_task
                except asyncio.CancelledError:
                    pass

            # Close NATS connection
            if self._nc:
                await self._nc.drain()
                await self._nc.close()

            self._consumer_state = ConsumerState.STOPPED
            logger.info(f"Consumer stopped: {self.service_name}")

    def get_sync_status(self) -> Dict[str, Any]:
        """Get synchronization status."""
        status = {
            "service_name": self.service_name,
            "state": self._consumer_state.value,
            "is_synced": self._initial_sync_complete,
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "last_sequence": self.last_processed_sequence,
            "state_size": len(self._state),
        }

        if self._sync_start_time:
            status["sync_start_time"] = self._sync_start_time.isoformat()

        if self._sync_complete_time:
            status["sync_complete_time"] = self._sync_complete_time.isoformat()
            status["sync_duration_seconds"] = int(
                (
                    self._sync_complete_time
                    - (self._sync_start_time or self._sync_complete_time)
                ).total_seconds()
            )

        return status

    # Context manager support

    async def __aenter__(self) -> "DurableEventConsumer":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.stop()

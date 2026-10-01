"""
Durable Event Consumer
======================

Pull-based durable consumer shared by every replica of a service.

Delivery guarantees:

* at-least-once: explicit ack only after the handler succeeds;
* effectively-once side effects: a shared idempotency store (NATS KV by
  default) skips messages another replica or a previous run already handled;
* failures are retried with delays (``nak_delays_seconds``) and end in the dead
  letter queue after ``max_deliver_attempts`` instead of being dropped;
* long handlers keep their messages alive with ``in_progress()`` heartbeats.
"""

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, cast

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from ..idempotency import (
    IdempotencyStore,
    IdempotentProcessor,
    MemoryIdempotencyStore,
    NatsKVIdempotencyStore,
    idempotency_key,
)
from ..models.event import Event
from ..utils.config import NATSConfig
from ..utils.exceptions import ConnectionError, ConsumerError
from ..utils.metrics import get_metrics_collector
from ..utils.server_version import ensure_server_version
from .base_consumer import BaseEventConsumer, invoke_handler
from .bootstrap import ensure_consumer, ensure_stream
from .dlq import DeadLetterAdvisoryListener, DeadLetterPublisher
from .message_handler import MessageHandler, Result
from .redelivery import message_identity
from .supervisor import (
    MAX_CONSECUTIVE_ERRORS,
    HealthSnapshot,
    error_delay,
    restart_delay,
)

logger = logging.getLogger(__name__)

DRAIN_TIMEOUT_SECONDS = 5.0


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
    """Durable, horizontally scalable consumer with retries, DLQ and dedupe."""

    def __init__(
        self,
        service_name: str,
        config: NATSConfig,
        batch_size: int = 1,
        fetch_timeout: float = 2.0,
        adapters: Optional[List[Any]] = None,
        auto_adapt: bool = True,
        idempotency_store: Optional[IdempotencyStore] = None,
    ):
        """
        Args:
            service_name: Name of the consuming service (durable = "<name>-consumer")
            config: NATS configuration
            batch_size: Messages per fetch. Queued messages are kept alive with
                heartbeats, but small batches keep redelivery latency low.
            fetch_timeout: Timeout for fetch operations in seconds
            adapters: EventAdapter instances for handling different event formats
            auto_adapt: If True, add FlexibleAdapter to handle any format
            idempotency_store: Override the store selected by
                ``config.idempotency_backend``
        """
        super().__init__(service_name)

        self.config = config
        self.batch_size = batch_size
        self.fetch_timeout = fetch_timeout

        self.adapters = list(adapters or [])
        if auto_adapt:
            from ..adapters.generic_adapter import FlexibleAdapter

            if not any(isinstance(a, FlexibleAdapter) for a in self.adapters):
                self.adapters.append(FlexibleAdapter())

        self._nc: Optional[NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._subscription: Optional[Any] = None
        self._custom_store = idempotency_store
        self._store: Optional[IdempotencyStore] = None
        self._processor: Optional[IdempotentProcessor] = None
        self._handler: Optional[MessageHandler] = None
        self._dead_letters: Optional[DeadLetterPublisher] = None
        self._advisories: Optional[DeadLetterAdvisoryListener] = None

        self.stream_name = config.stream_name
        self.consumer_name = f"{service_name}-consumer"
        self.subject_filter = f"{config.subject_prefix}.>"

        self._consumer_state = ConsumerState.IDLE
        self._is_running = False
        self._processing_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        self._sync_start_time: Optional[datetime] = None
        self._sync_complete_time: Optional[datetime] = None
        self._initial_sync_complete = False

        self._consecutive_errors = 0
        self._max_consecutive_errors = MAX_CONSECUTIVE_ERRORS
        self._health = HealthSnapshot()

        self._metrics = get_metrics_collector(service_name)

        self.events_processed: int = 0
        self.events_failed: int = 0
        self.last_processed_sequence: int = 0

    # ------------------------------------------------------------------ state
    @property
    def state(self) -> ConsumerState:
        return self._consumer_state

    @property
    def is_synced(self) -> bool:
        return self._initial_sync_complete

    def health(self) -> Dict[str, Any]:
        """Readiness information: healthy only while fetching successfully."""
        return self._health.as_dict(
            self._consumer_state.value,
            self._is_running,
            self.config.health_stale_seconds,
        )

    @property
    def is_healthy(self) -> bool:
        return bool(self.health()["healthy"])

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """
        Connect, reconcile stream/consumer/DLQ/idempotency and start the loop.

        Raises:
            ConsumerError: If startup fails
        """
        async with self._lock:
            if self._is_running:
                logger.warning(f"Consumer {self.service_name} already running")
                return
            try:
                self._consumer_state = ConsumerState.STARTING
                self.config.validate()
                await self._connect()
                await self._setup_consumer()
                self._is_running = True
                self._processing_task = asyncio.create_task(self._supervised_loop())
                self._processing_task.add_done_callback(self._on_loop_done)
                logger.info(f"Consumer {self.service_name} started successfully")
            except Exception as e:
                self._consumer_state = ConsumerState.ERROR
                logger.error(f"Failed to start consumer: {e}")
                await self._close_connection()
                raise ConsumerError(f"Consumer startup failed: {e}") from e

    async def _connect(self) -> None:
        try:
            options: Dict[str, Any] = {
                "name": f"{self.config.client_name}-{self.service_name}",
                "reconnect_time_wait": self.config.reconnect_time_wait,
                "max_reconnect_attempts": self.config.max_reconnect_attempts,
            }
            if self.config.user and self.config.password:
                options["user"] = self.config.user
                options["password"] = self.config.password
            self._nc = await nats.connect(servers=self.config.servers, **options)
        except Exception as e:
            logger.error(f"NATS connection failed: {e}")
            raise ConnectionError(f"Failed to connect to NATS: {e}")
        ensure_server_version(self._nc)
        self._js = self._nc.jetstream()
        logger.info("Connected to NATS for consumer")

    def _jetstream(self) -> JetStreamContext:
        if self._js is None:
            raise ConsumerError("JetStream context not initialized")
        return self._js

    def _build_store(self, js: JetStreamContext) -> IdempotencyStore:
        if self._custom_store is not None:
            return self._custom_store
        if self.config.idempotency_backend == "memory":
            logger.warning(
                "Using in-memory idempotency: no dedupe across replicas/restarts"
            )
            return MemoryIdempotencyStore(self.config.idempotency_ttl_seconds)
        return NatsKVIdempotencyStore(
            js,
            bucket=self.config.idempotency_bucket,
            ttl_seconds=self.config.idempotency_ttl_seconds,
            replicas=self.config.replicas,
            clock_skew_seconds=self.config.clock_skew_seconds,
        )

    async def _setup_consumer(self) -> None:
        js = self._jetstream()
        await ensure_stream(js, self.config)

        self._store = self._build_store(js)
        await self._store.setup()
        self._processor = IdempotentProcessor(
            self._store,
            lease_seconds=self.config.idempotency_lease_seconds,
            heartbeat_interval=self.config.heartbeat_interval_seconds,
            handler_timeout=self.config.handler_timeout_seconds,
            on_event=self._metrics.record_reliability_event,
        )

        if self.config.dlq_enabled:
            self._dead_letters = DeadLetterPublisher(
                js, self.config, self.consumer_name
            )
            await self._dead_letters.ensure_stream()

        self._handler = MessageHandler(
            consumer_name=self.consumer_name,
            config=self.config,
            adapters=self.adapters,
            get_handler=self.get_handler,
            processor=self._processor,
            dead_letters=self._dead_letters,
            metrics=self._metrics,
            on_progress=self._mark_progress,
        )

        # Reconcile BEFORE binding: pull_subscribe ignores config on existing
        # durables and silently creates a default one if it is missing.
        await ensure_consumer(js, self.config, self.consumer_name, self.subject_filter)

        if self.config.dlq_enabled:
            # Listen before pulling so no advisory is missed.
            self._advisories = DeadLetterAdvisoryListener(
                js,
                self.config,
                self.consumer_name,
                nc=self._nc,
                is_settled=self._is_settled,
            )
            await self._advisories.start()

        await self._subscribe()

    async def _is_settled(self, subject: str, headers: Any, stream_seq: int) -> bool:
        """True if the message is already done or leased by a live replica."""
        if self._store is None:
            return False
        identity = message_identity(subject, headers, self.stream_name, stream_seq)
        status = await self._store.status(idempotency_key(self.consumer_name, identity))
        return status is not None

    async def _subscribe(self) -> None:
        self._subscription = await self._jetstream().pull_subscribe(
            subject="",  # the durable's filter applies
            durable=self.consumer_name,
            stream=self.stream_name,
        )

    async def _resubscribe(self) -> None:
        """Drop the old pull subscription and re-reconcile before binding again."""
        old, self._subscription = self._subscription, None
        if old is not None:
            try:
                await old.unsubscribe()
            except Exception as e:
                logger.debug(f"Old subscription cleanup failed: {e}")
        js = self._jetstream()
        await ensure_stream(js, self.config)
        await ensure_consumer(js, self.config, self.consumer_name, self.subject_filter)
        await self._subscribe()

    def _stopping(self) -> bool:
        """stop() may have flipped the flag while we were awaiting."""
        return not self._is_running

    def _mark_progress(self) -> None:
        self._health = self._health.progress()

    async def _close_connection(self) -> None:
        if self._advisories is not None:
            await self._advisories.stop()
        # A cancelled fetch can leave an item in the pull subscription's queue,
        # which blocks drain() until its 30s timeout: unsubscribe it first.
        subscription, self._subscription = self._subscription, None
        if subscription is not None:
            try:
                await subscription.unsubscribe()
            except Exception as e:
                logger.debug(f"Pull subscription cleanup failed: {e}")
        if self._nc is None:
            return
        try:
            await asyncio.wait_for(self._nc.drain(), timeout=DRAIN_TIMEOUT_SECONDS)
        except Exception as e:
            logger.warning(f"NATS drain failed: {e!r}")
        finally:
            try:
                await self._nc.close()
            except Exception as e:
                logger.debug(f"NATS close failed: {e}")

    async def stop(self) -> None:
        """Gracefully stop: in-flight messages are nak'ed back, then drain."""
        async with self._lock:
            if not self._is_running:
                return
            logger.info(f"Stopping consumer: {self.service_name}")
            self._consumer_state = ConsumerState.STOPPING
            self._is_running = False
            try:
                task = self._processing_task
                if task is not None and not task.done():
                    task.cancel()
                    # asyncio.wait never swallows a cancellation aimed at stop().
                    await asyncio.wait([task])
            finally:
                await self._close_connection()
                self._consumer_state = ConsumerState.STOPPED
                logger.info(f"Consumer stopped: {self.service_name}")

    # ------------------------------------------------------------------- loop
    def _on_loop_done(self, task: "asyncio.Task[None]") -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._consumer_state = ConsumerState.ERROR
            logger.error(f"Consumer loop crashed unexpectedly: {error!r}")

    async def _supervised_loop(self) -> None:
        """Run the fetch loop forever, restarting it after repeated failures."""
        self._consumer_state = ConsumerState.SYNCING
        self._sync_start_time = datetime.now(timezone.utc)
        while self._is_running:
            await self._event_processing_loop()
            if self._stopping():
                break
            self._health = self._health.restarted()
            delay = restart_delay(self._health.restart_streak)
            logger.error(
                f"Consumer loop stopped after {self._max_consecutive_errors} errors; "
                f"restart #{self._health.restarts} in {delay:.0f}s"
            )
            await asyncio.sleep(delay)
            try:
                await self._resubscribe()
                self._consecutive_errors = 0
                self._consumer_state = (
                    ConsumerState.LIVE
                    if self._initial_sync_complete
                    else ConsumerState.SYNCING
                )
            except Exception as e:
                self._health = self._health.failed(e)
                logger.error(f"Resubscribe failed: {e}")

    async def _event_processing_loop(self) -> None:
        """Fetch/process until too many consecutive fetch errors (or stop)."""
        while self._is_running:
            try:
                messages = await self._fetch_messages()
            except asyncio.TimeoutError:
                # Only a *fetch* timeout means "no messages"; processing errors
                # are handled per message and never reach this branch.
                self._health = self._health.fetch_ok()
                await self._handle_timeout()
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._health = self._health.failed(e)
                if not await self._handle_processing_error(e):
                    return
                continue
            self._health = self._health.fetch_ok()
            await self._handle_messages(messages)

    async def _fetch_messages(self) -> List[Msg]:
        if self._subscription is None:
            raise ConsumerError("Subscription not initialized")
        return cast(
            List[Msg],
            await self._subscription.fetch(
                batch=self.batch_size, timeout=self.fetch_timeout
            ),
        )

    async def _handle_messages(self, messages: List[Msg]) -> None:
        if messages:
            await self._process_batch(messages)
            self._consecutive_errors = 0
            if self._consumer_state == ConsumerState.SYNCING:
                logger.info(
                    f"Syncing: processed {len(messages)} events, "
                    f"total={self.events_processed}"
                )
        elif self._consumer_state == ConsumerState.SYNCING:
            await self._complete_sync()

    async def _handle_timeout(self) -> None:
        if self._consumer_state == ConsumerState.SYNCING:
            await self._complete_sync()

    async def _handle_processing_error(self, error: Exception) -> bool:
        """Returns True to continue, False to hand over to the supervisor."""
        self._consecutive_errors += 1
        logger.error(f"Processing error (#{self._consecutive_errors}): {error}")
        if self._consecutive_errors >= self._max_consecutive_errors:
            self._consumer_state = ConsumerState.ERROR
            return False
        await asyncio.sleep(error_delay(self._consecutive_errors))
        return True

    async def _process_batch(self, messages: List[Msg]) -> None:
        """Process sequentially; queued messages get heartbeats meanwhile."""
        for index, msg in enumerate(messages):
            await self._process_message(msg, pending=messages[index + 1 :])

    async def _process_message(
        self, msg: Msg, pending: Optional[List[Msg]] = None
    ) -> None:
        if self._handler is None:
            raise ConsumerError("Consumer not set up")
        start_time = self._metrics.record_consume_start()
        try:
            result = await self._handler.handle(msg, list(pending or []))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # defensive: MessageHandler isolates its own errors
            logger.error(
                f"Unexpected error handling seq={msg.metadata.sequence.stream}: {e!r}"
            )
            return
        self._mark_progress()
        if result in (Result.PROCESSED, Result.DUPLICATE):
            self.events_processed += 1
            self.last_processed_sequence = msg.metadata.sequence.stream
            self._metrics.record_consume_success(start_time)
        elif result in (Result.RETRY, Result.DEAD_LETTERED):
            self.events_failed += 1

    async def process_event(self, event: Event) -> None:
        """
        Run the registered handler for ``event`` with idempotency by event_id.

        Used for direct invocation; the fetch loop goes through
        ``_process_message`` which also handles ack/nak/DLQ.
        """
        handler = self.get_handler(event.event_type)
        if handler is None:
            logger.warning(f"No handler for event type: {event.event_type}")
            return
        if event.metadata is None:
            raise ConsumerError("Event metadata is required")
        if self._processor is None:
            self._processor = IdempotentProcessor(
                (
                    self._custom_store
                    if self._custom_store is not None
                    else MemoryIdempotencyStore(self.config.idempotency_ttl_seconds)
                ),
                lease_seconds=self.config.idempotency_lease_seconds,
                heartbeat_interval=self.config.heartbeat_interval_seconds,
                handler_timeout=self.config.handler_timeout_seconds,
            )
        try:
            await self._processor.run(
                idempotency_key(self.consumer_name, event.metadata.event_id),
                lambda: invoke_handler(handler, event),
            )
        except Exception:
            self.events_failed += 1
            raise

    # ------------------------------------------------------------------- sync
    async def _complete_sync(self) -> None:
        if self._initial_sync_complete:
            return
        self._initial_sync_complete = True
        self._consumer_state = ConsumerState.LIVE
        self._sync_complete_time = datetime.now(timezone.utc)
        duration = (
            self._sync_complete_time
            - (self._sync_start_time or self._sync_complete_time)
        ).total_seconds()
        logger.info(
            f"Initial sync completed: service={self.service_name}, "
            f"events={self.events_processed}, duration={duration:.2f}s, "
            f"last_seq={self.last_processed_sequence}"
        )

    def get_sync_status(self) -> Dict[str, Any]:
        status: Dict[str, Any] = {
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

    async def __aenter__(self) -> "DurableEventConsumer":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()

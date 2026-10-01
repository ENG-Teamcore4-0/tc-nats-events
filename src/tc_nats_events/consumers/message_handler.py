"""
Per-message processing
======================

Turns one JetStream message into exactly one of: ack (processed / duplicate /
no handler), ``nak(delay)`` (retry later) or dead-letter + ``term()``.

Every outcome is isolated: a failing ack/nak/term/DLQ publish is logged and
never aborts the rest of the batch or the fetch loop.
"""

import asyncio
import logging
from enum import Enum
from typing import Any, Callable, List, Optional

from nats.aio.msg import Msg

from ..idempotency import IdempotentProcessor, Outcome, RetryLaterError, idempotency_key
from ..models.event import Event
from ..utils.config import NATSConfig
from ..utils.exceptions import NonRetryableError
from ..utils.heartbeat import Beat, touch_all
from ..utils.metrics import MetricsCollector
from .base_consumer import EventHandler, invoke_handler
from .dlq import DeadLetterPublisher
from .redelivery import is_last_attempt, message_id, nak_delay, num_delivered

logger = logging.getLogger(__name__)


class Result(str, Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    NO_HANDLER = "no_handler"
    RETRY = "retry"
    DEAD_LETTERED = "dead_lettered"


async def _quietly(action: str, seq: int, operation: Any) -> None:
    try:
        await operation
    except Exception as e:
        logger.warning(f"{action} failed for seq={seq}: {e!r}")


class MessageHandler:
    def __init__(
        self,
        *,
        consumer_name: str,
        config: NATSConfig,
        adapters: List[Any],
        get_handler: Callable[[str], Optional[EventHandler]],
        processor: IdempotentProcessor,
        dead_letters: Optional[DeadLetterPublisher],
        metrics: MetricsCollector,
        on_progress: Callable[[], None],
    ):
        self._consumer = consumer_name
        self._config = config
        self._adapters = adapters
        self._get_handler = get_handler
        self._processor = processor
        self._dead_letters = dead_letters
        self._metrics = metrics
        self._on_progress = on_progress

    def _touch(self, messages: List[Msg]) -> Beat:
        touch = touch_all(messages)

        async def beat() -> None:
            await touch()
            self._on_progress()  # long handlers must not look stale to liveness

        return beat

    async def handle(self, msg: Msg, pending: List[Msg]) -> Result:
        seq = msg.metadata.sequence.stream
        try:
            return await self._handle(msg, seq, pending)
        except asyncio.CancelledError:
            # Stopping mid-handler: hand the message back instead of waiting ack_wait.
            await asyncio.shield(_quietly("nak", seq, msg.nak()))
            raise

    async def _handle(self, msg: Msg, seq: int, pending: List[Msg]) -> Result:
        try:
            event = Event.from_json(msg.data, adapters=self._adapters).with_sequence(
                seq
            )
        except Exception as e:
            self._metrics.record_consume_error("poison")
            logger.error("Unparseable message seq=%s: %s", seq, type(e).__name__)
            return await self._dead_letter(msg, "poison", e)

        handler = self._get_handler(event.event_type)
        if handler is None:
            logger.warning(
                "No handler for event type %r, sequence=%s", event.event_type, seq
            )
            await _quietly("ack", seq, msg.ack())
            return Result.NO_HANDLER

        key = idempotency_key(self._consumer, message_id(msg, self._config.stream_name))
        try:
            outcome = await self._processor.run(
                key,
                lambda: invoke_handler(handler, event),
                touch=self._touch([msg, *pending]),
            )
        except RetryLaterError:
            await _quietly(
                "nak", seq, msg.nak(delay=self._config.heartbeat_interval_seconds)
            )
            return Result.RETRY
        except Exception as e:
            return await self._on_failure(msg, event, e)

        # Redelivery after a lost ack is harmless: the store has it as done.
        await _quietly("ack", seq, msg.ack())
        return Result.DUPLICATE if outcome == Outcome.DUPLICATE else Result.PROCESSED

    async def _on_failure(self, msg: Msg, event: Event, error: Exception) -> Result:
        self._metrics.record_consume_error("processing")
        deliveries = num_delivered(msg)
        logger.error(
            "Handler failed for %r: %s, sequence=%s, delivery=%s",
            event.event_type,
            type(error).__name__,
            event.sequence,
            deliveries,
        )
        if isinstance(error, NonRetryableError):
            return await self._dead_letter(msg, "non_retryable", error)
        if is_last_attempt(self._config, deliveries):
            return await self._dead_letter(msg, "max_deliveries", error)
        self._metrics.record_retry()
        await _quietly(
            "nak",
            msg.metadata.sequence.stream,
            msg.nak(delay=nak_delay(self._config, deliveries)),
        )
        return Result.RETRY

    async def _dead_letter(self, msg: Msg, reason: str, error: Exception) -> Result:
        seq = msg.metadata.sequence.stream
        deliveries = num_delivered(msg)
        if self._dead_letters is not None:
            try:
                await self._dead_letters.publish(
                    original_subject=msg.subject,
                    data=msg.data,
                    original_headers=msg.headers,
                    stream_seq=seq,
                    reason=reason,
                    error=f"{type(error).__name__}: {error}",
                    deliveries=deliveries,
                )
            except Exception as e:
                # Never drop it: retry later (the advisory also covers the last attempt).
                logger.error(f"DLQ publish failed for seq={seq}: {e!r}; retrying later")
                await _quietly(
                    "nak", seq, msg.nak(delay=nak_delay(self._config, deliveries))
                )
                return Result.RETRY
        else:
            logger.error(f"DLQ disabled; terminating seq={seq} ({reason})")
        await _quietly("term", seq, msg.term())
        return Result.DEAD_LETTERED

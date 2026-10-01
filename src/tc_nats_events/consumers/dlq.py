"""
Dead letter queue (NATS-03)
===========================

Primary path (application level): a poison message, a ``NonRetryableError`` or
a failure on the last allowed delivery is copied to ``<stream>-DLQ`` with
diagnostic headers and then ``term()``-ed.

Safety net: the server's ``MAX_DELIVERIES`` advisory covers deliveries that
expired without an answer (e.g. the pod crashed). Both paths use the same
deterministic ``Nats-Msg-Id`` so a message is dead-lettered once.

Advisories are core NATS messages, not persisted: they are only seen while a
replica is subscribed. The subscription uses a queue group so exactly one
replica handles each advisory. Subscribing requires permission on
``$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>``.
"""

import asyncio
import json
import logging
import re
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, cast

from nats.js import JetStreamContext
from nats.js.api import PubAck
from nats.js.errors import NotFoundError

from ..core.stream_config import build_dlq_stream_config
from ..utils.config import NATSConfig
from ..utils.exceptions import ConsumerError
from ..utils.metrics import get_metrics_collector

logger = logging.getLogger(__name__)

# Error text can echo payload data: keep it short (it is also in the DLQ ACL domain).
MAX_ERROR_HEADER_LENGTH = 200
ADVISORY_SUBJECT = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{consumer}"
ADVISORY_TYPE = "io.nats.jetstream.advisory.v1.max_deliver"
PUBLISH_ATTEMPTS = 3
PUBLISH_RETRY_DELAY = 0.5

# Returns True when the message was already handled (or is being handled).
SettledCheck = Callable[[str, Optional[Mapping[str, str]], int], Awaitable[bool]]


def _subject_token(value: str) -> str:
    return re.sub(r"[.\s*>]", "_", value) or "_"


def _header_value(value: str) -> str:
    return re.sub(r"[\r\n]+", " ", value)[:MAX_ERROR_HEADER_LENGTH]


def dlq_msg_id(consumer: str, stream_seq: int) -> str:
    return f"dlq-{consumer}-{stream_seq}"


class DeadLetterPublisher:
    def __init__(self, js: JetStreamContext, config: NATSConfig, consumer_name: str):
        self._js = js
        self._config = config
        self._consumer = consumer_name
        self._metrics = get_metrics_collector(consumer_name)

    async def ensure_stream(self) -> None:
        """Create the DLQ stream if missing. Never updates an existing one."""
        name = self._config.dlq_stream_name
        try:
            await self._js.stream_info(name)
            return
        except NotFoundError:
            pass
        try:
            await self._js.add_stream(build_dlq_stream_config(self._config))
            logger.info(f"Created dead letter stream '{name}'")
        except Exception as e:
            raise ConsumerError(
                f"Cannot create dead letter stream '{name}': {e}. Check that no other "
                f"stream captures 'dlq.{self._config.subject_prefix}.>' and that the "
                "account may create streams, or set dlq_enabled=False."
            ) from e

    def _subject(self, original_subject: str) -> str:
        prefix = f"{self._config.subject_prefix}."
        suffix = (
            original_subject[len(prefix) :]
            if original_subject.startswith(prefix)
            else "_"
        )
        return (
            f"dlq.{self._config.subject_prefix}.{_subject_token(self._consumer)}."
            f"{suffix or '_'}"
        )

    async def publish(
        self,
        *,
        original_subject: str,
        data: bytes,
        original_headers: Optional[Mapping[str, str]],
        stream_seq: int,
        reason: str,
        error: str,
        deliveries: int,
    ) -> None:
        # Reserved (Nats-*) and diagnostic (X-Dlq-*) headers from the producer
        # are dropped case-insensitively so they cannot be spoofed.
        headers: Dict[str, str] = {
            k: v
            for k, v in (original_headers or {}).items()
            if not k.lower().startswith(("nats-", "x-dlq-"))
        }
        headers.update(
            {
                "Nats-Msg-Id": dlq_msg_id(self._consumer, stream_seq),
                "X-Dlq-Reason": reason,
                "X-Dlq-Error": _header_value(error),
                "X-Dlq-Origin-Stream": self._config.stream_name,
                "X-Dlq-Origin-Seq": str(stream_seq),
                "X-Dlq-Origin-Subject": original_subject,
                "X-Dlq-Consumer": self._consumer,
                "X-Dlq-Deliveries": str(deliveries),
            }
        )
        ack = await self._publish_with_retry(
            self._subject(original_subject), data, headers
        )
        if getattr(ack, "duplicate", False) is True:
            logger.info(f"Message seq={stream_seq} already in the DLQ")
            return
        self._metrics.record_reliability_event(f"dead_lettered_{reason}")
        logger.error(
            "Message dead-lettered: reason=%s seq=%s deliveries=%s error=%r",
            reason,
            stream_seq,
            deliveries,
            _header_value(error),
        )

    async def _publish_with_retry(
        self, subject: str, data: bytes, headers: Dict[str, str]
    ) -> PubAck:
        for attempt in range(1, PUBLISH_ATTEMPTS + 1):
            try:
                return await self._js.publish(
                    subject,
                    data,
                    headers=headers,
                    timeout=self._config.publish_timeout_seconds,
                    stream=self._config.dlq_stream_name,
                )
            except Exception:
                if attempt == PUBLISH_ATTEMPTS:
                    raise
                await asyncio.sleep(PUBLISH_RETRY_DELAY * attempt)
        raise ConsumerError("unreachable: DLQ publish attempts exhausted")


class DeadLetterAdvisoryListener:
    """Feeds the DLQ from ``MAX_DELIVERIES`` advisories (crash safety net)."""

    def __init__(
        self,
        js: JetStreamContext,
        config: NATSConfig,
        consumer_name: str,
        nc: Optional[Any] = None,
        is_settled: Optional[SettledCheck] = None,
    ):
        self._js = js
        self._nc = nc if nc is not None else js._nc
        self._config = config
        self._consumer = consumer_name
        self._publisher = DeadLetterPublisher(js, config, consumer_name)
        self._is_settled = is_settled
        self._subscription: Optional[Any] = None

    @property
    def subject(self) -> str:
        return ADVISORY_SUBJECT.format(
            stream=self._config.stream_name, consumer=self._consumer
        )

    async def start(self) -> None:
        self._subscription = await self._nc.subscribe(
            self.subject, queue=f"{self._consumer}-dlq", cb=self._on_advisory
        )

    async def stop(self) -> None:
        if self._subscription is not None:
            try:
                await self._subscription.unsubscribe()
            except Exception as e:
                logger.debug(f"Advisory unsubscribe failed: {e}")
            self._subscription = None

    def _parse(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Accept only well-formed advisories about *our* stream and consumer."""
        try:
            advisory = json.loads(data)
            valid = (
                isinstance(advisory, dict)
                and advisory.get("type") == ADVISORY_TYPE
                and advisory.get("stream") == self._config.stream_name
                and advisory.get("consumer") == self._consumer
                and int(advisory.get("stream_seq", 0)) > 0
            )
        except (ValueError, TypeError):
            valid = False
        if not valid:
            logger.warning("Ignoring malformed or foreign MAX_DELIVERIES advisory")
            return None
        return cast(Dict[str, Any], advisory)

    async def _on_advisory(self, msg: Any) -> None:
        advisory = self._parse(msg.data)
        if advisory is None:
            return
        try:
            await self.handle_stream_seq(
                int(advisory["stream_seq"]), int(advisory.get("deliveries", 0) or 0)
            )
        except Exception as e:
            # Advisories are not redelivered: the message stays in the stream
            # until max_age; surface it loudly for manual replay.
            logger.error(
                f"Failed to dead-letter seq={advisory.get('stream_seq')} from advisory: {e}"
            )

    async def handle_stream_seq(self, stream_seq: int, deliveries: int) -> None:
        try:
            raw = await self._js.get_msg(self._config.stream_name, seq=stream_seq)
        except NotFoundError:
            logger.warning(f"Advisory for seq {stream_seq} but message is gone")
            return
        if self._is_settled is not None and await self._is_settled(
            raw.subject or "", raw.headers, stream_seq
        ):
            # Processed (ack lost) or still being processed by another replica.
            logger.info(f"Advisory for seq {stream_seq} ignored: already handled")
            return
        await self._publisher.publish(
            original_subject=raw.subject or "",
            data=raw.data or b"",
            original_headers=raw.headers,
            stream_seq=stream_seq,
            reason="max_deliveries",
            error="delivery attempts exhausted without acknowledgement",
            deliveries=deliveries,
        )

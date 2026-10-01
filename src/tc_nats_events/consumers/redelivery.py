"""
Redelivery policy (NATS-02 / NATS-03)
=====================================

A bare ``nak()`` makes JetStream redeliver immediately, so ``max_deliver``
attempts are burned in milliseconds while a dependency is down. Failures are
nak'ed with a delay taken from ``nak_delays_seconds``; the last allowed
delivery goes to the dead letter queue instead of being silently dropped.
"""

from typing import Mapping, Optional

from nats.aio.msg import Msg

from ..utils.config import NATSConfig


def nak_delay(config: NATSConfig, num_delivered: int) -> float:
    """Delay before the next attempt after delivery ``num_delivered`` failed."""
    delays = config.nak_delays_seconds
    index = min(max(num_delivered, 1) - 1, len(delays) - 1)
    return delays[index]


def is_last_attempt(config: NATSConfig, num_delivered: int) -> bool:
    limit = config.max_deliver_attempts
    return limit != -1 and num_delivered >= limit


def num_delivered(msg: Msg) -> int:
    try:
        return int(msg.metadata.num_delivered)
    except Exception:
        return 1


MAX_MESSAGE_ID_LENGTH = 256
_ID_HEADERS = ("Nats-Msg-Id", "event-id")


def _valid_id(value: str) -> bool:
    return bool(value) and len(value) <= MAX_MESSAGE_ID_LENGTH and value.isprintable()


def message_identity(
    subject: str, headers: Optional[Mapping[str, str]], stream: str, stream_seq: int
) -> str:
    """
    Stable identity of a stored message across redeliveries (NATS-12).

    Prefer the producer's dedupe id (``Nats-Msg-Id``), then our ``event-id``
    header, then the stream sequence. The id is scoped by subject so a producer
    reusing an id on another event type cannot suppress it; malformed ids fall
    back to the stream sequence.
    """
    for header in _ID_HEADERS:
        value = (headers or {}).get(header)
        if value is not None and _valid_id(value):
            return f"{subject}|{value}"
    return f"{subject}|{stream}:{stream_seq}"


def message_id(msg: Msg, stream: str) -> str:
    return message_identity(
        msg.subject, msg.headers, stream, msg.metadata.sequence.stream
    )

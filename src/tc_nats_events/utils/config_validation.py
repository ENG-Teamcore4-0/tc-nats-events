"""
Configuration validation
========================

Cross-field rules for :class:`NATSConfig`. Kept apart from the dataclass so the
rules can be read (and tested) as a single list of invariants.
"""

import math
import re
from typing import TYPE_CHECKING, Callable, List, Optional

if TYPE_CHECKING:  # pragma: no cover
    from .config import NATSConfig

PRODUCTION_ENVIRONMENTS = {"prod", "production"}
PRODUCTION_MIN_REPLICAS = 3
DELIVER_POLICIES = {"new", "all", "by_start_time", "last_per_subject"}
IDEMPOTENCY_BACKENDS = {"nats_kv", "memory"}
# Extra margin so a key never expires while a redelivery can still arrive.
IDEMPOTENCY_TTL_MARGIN_SECONDS = 3600
# Names end up in stream/bucket names and subjects: no wildcards, dots or spaces.
STREAM_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
SUBJECT_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$")


def _basic(c: "NATSConfig") -> Optional[str]:
    if not c.servers:
        return "At least one NATS server must be specified"
    if c.max_messages <= 0:
        return "max_messages must be positive"
    if c.max_bytes <= 0:
        return "max_bytes must be positive"
    if c.max_age_seconds <= 0:
        return "max_age_seconds must be positive"
    if c.replicas < 1:
        return "replicas must be at least 1"
    if not c.stream_name:
        return "stream_name cannot be empty"
    if not c.subject_prefix:
        return "subject_prefix cannot be empty"
    if not STREAM_NAME_PATTERN.match(c.stream_name):
        return "stream_name may only contain letters, digits, '_' and '-'"
    if not SUBJECT_PREFIX_PATTERN.match(c.subject_prefix):
        return "subject_prefix must be dot-separated tokens without wildcards"
    if bool(c.user) != bool(c.password):
        return "user and password must be set together"
    if c.max_ack_pending == 0 or c.max_ack_pending < -1:
        return "max_ack_pending must be > 0 or -1 (unlimited)"
    return None


def _replicas(c: "NATSConfig") -> Optional[str]:
    if (
        c.environment.strip().lower() in PRODUCTION_ENVIRONMENTS
        and c.replicas < PRODUCTION_MIN_REPLICAS
    ):
        return (
            f"replicas must be >= {PRODUCTION_MIN_REPLICAS} in production "
            f"(got {c.replicas}); a single node failure would lose the stream"
        )
    return None


def _delivery(c: "NATSConfig") -> Optional[str]:
    if c.deliver_policy not in DELIVER_POLICIES:
        return f"deliver_policy must be one of {sorted(DELIVER_POLICIES)}"
    if c.deliver_policy == "by_start_time" and c.opt_start_time is None:
        return "opt_start_time is required when deliver_policy='by_start_time'"
    if c.opt_start_time is not None:
        if c.deliver_policy != "by_start_time":
            return "opt_start_time is only valid with deliver_policy='by_start_time'"
        if c.opt_start_time.tzinfo is None:
            return "opt_start_time must be timezone-aware (e.g. ...+00:00 or ...Z)"
    if c.max_deliver_attempts != -1 and c.max_deliver_attempts < 1:
        return "max_deliver_attempts must be >= 1 or -1 (unlimited)"
    if c.ack_wait_seconds <= 0:
        return "ack_wait_seconds must be positive"
    if not c.nak_delays_seconds or any(
        d < 0 or not math.isfinite(d) for d in c.nak_delays_seconds
    ):
        return "nak_delays_seconds must be a non-empty list of finite delays >= 0"
    if c.handler_timeout_seconds <= 0:
        return "handler_timeout_seconds must be positive"
    return None


def _backoff(c: "NATSConfig") -> Optional[str]:
    backoff = c.consumer_backoff_seconds
    if not backoff:
        return None
    if backoff[0] < c.ack_wait_seconds:
        return (
            "consumer_backoff_seconds[0] replaces ack_wait on the server; "
            f"it must be >= ack_wait_seconds ({c.ack_wait_seconds})"
        )
    if c.max_deliver_attempts != -1 and c.max_deliver_attempts <= len(backoff):
        return "max_deliver_attempts must be greater than len(consumer_backoff_seconds)"
    return None


def _idempotency(c: "NATSConfig") -> Optional[str]:
    if c.idempotency_backend not in IDEMPOTENCY_BACKENDS:
        return f"idempotency_backend must be one of {sorted(IDEMPOTENCY_BACKENDS)}"
    if (
        c.idempotency_backend == "memory"
        and c.environment.strip().lower() in PRODUCTION_ENVIRONMENTS
    ):
        return "idempotency_backend='memory' does not dedupe across replicas; not allowed in production"
    if c.idempotency_ttl_seconds <= 0:
        return "idempotency_ttl_seconds must be positive"
    if c.clock_skew_seconds < 0 or c.clock_skew_seconds >= c.idempotency_lease_seconds:
        return (
            "clock_skew_seconds must be >= 0 and smaller than idempotency_lease_seconds"
        )
    if c.idempotency_lease_seconds < 3 * c.heartbeat_interval_seconds:
        return "idempotency_lease_seconds must be >= 3x the heartbeat interval"
    if c.max_deliver_attempts != -1:
        horizon = sum(c.nak_delays_seconds) + c.max_deliver_attempts * (
            c.ack_wait_seconds + c.handler_timeout_seconds
        )
        if c.idempotency_ttl_seconds < horizon + IDEMPOTENCY_TTL_MARGIN_SECONDS:
            return (
                "idempotency_ttl_seconds is shorter than the redelivery horizon "
                f"({int(horizon)}s + {IDEMPOTENCY_TTL_MARGIN_SECONDS}s margin)"
            )
    return None


def _health(c: "NATSConfig") -> Optional[str]:
    # Waiting for another replica can take handler_timeout + lease before the
    # next fetch; liveness must not flag that as stale.
    if c.health_stale_seconds <= c.handler_timeout_seconds:
        return "health_stale_seconds must be greater than handler_timeout_seconds"
    return None


def _dlq(c: "NATSConfig") -> Optional[str]:
    if c.dlq_duplicate_window_seconds <= 0:
        return "dlq_duplicate_window_seconds must be positive"
    if c.dlq_max_bytes == 0 or c.dlq_max_messages == 0:
        return "dlq_max_bytes / dlq_max_messages must be > 0 or -1 (unlimited)"
    return None


def _publishing(c: "NATSConfig") -> Optional[str]:
    if c.publish_timeout_seconds <= 0:
        return "publish_timeout_seconds must be positive"
    if c.duplicate_window_seconds <= 0:
        return "duplicate_window_seconds must be positive"
    if c.duplicate_window_seconds > c.max_age_seconds:
        return "duplicate_window_seconds cannot be larger than max_age_seconds"
    return None


RULES: List[Callable[["NATSConfig"], Optional[str]]] = [
    _basic,
    _replicas,
    _delivery,
    _backoff,
    _idempotency,
    _health,
    _dlq,
    _publishing,
]


def validate_config(config: "NATSConfig") -> None:
    """Raise ``ValueError`` with the first violated rule."""
    for rule in RULES:
        error = rule(config)
        if error:
            raise ValueError(error)

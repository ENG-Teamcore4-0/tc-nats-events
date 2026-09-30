"""
Durable consumer configuration
==============================

``build_consumer_config`` turns a :class:`NATSConfig` into the JetStream
consumer we want. ``reconcile`` compares it with an existing durable so that
editable settings are updated on start (NATS-06) and immutable differences fail
fast instead of binding silently.

``deliver_policy`` only matters when a durable is created, so it is never
compared: changing the library default (ALL -> NEW) does not break existing
durables (NATS-04).
"""

from dataclasses import replace
from typing import Dict, List, Optional, Set, Tuple

from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, ReplayPolicy

from ..utils.config import NATSConfig
from ..utils.exceptions import ConsumerConfigMismatchError

DELIVER_POLICIES: Dict[str, DeliverPolicy] = {
    "new": DeliverPolicy.NEW,
    "all": DeliverPolicy.ALL,
    "by_start_time": DeliverPolicy.BY_START_TIME,
    "last_per_subject": DeliverPolicy.LAST_PER_SUBJECT,
}

EDITABLE_FIELDS = ("ack_wait", "max_deliver", "backoff", "max_ack_pending")
FLOAT_TOLERANCE = 1e-3


def build_consumer_config(
    config: NATSConfig, name: str, filter_subject: str
) -> ConsumerConfig:
    backoff = (
        list(config.consumer_backoff_seconds)
        if config.consumer_backoff_seconds
        else None
    )
    return ConsumerConfig(
        name=name,
        durable_name=name,
        deliver_policy=DELIVER_POLICIES[config.deliver_policy],
        opt_start_time=config.opt_start_time,
        ack_policy=AckPolicy.EXPLICIT,
        replay_policy=ReplayPolicy.INSTANT,
        filter_subject=filter_subject,
        max_deliver=config.max_deliver_attempts,
        ack_wait=float(config.ack_wait_seconds),
        backoff=backoff,
        max_ack_pending=config.max_ack_pending,
    )


def _filters(cfg: ConsumerConfig) -> Set[str]:
    filters = set(cfg.filter_subjects or [])
    if cfg.filter_subject:
        filters.add(cfg.filter_subject)
    return filters


def _same(a: object, b: object) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < FLOAT_TOLERANCE
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    return (a or None) == (b or None)


def _immutable_diffs(existing: ConsumerConfig, desired: ConsumerConfig) -> List[str]:
    diffs = []
    if (existing.ack_policy or AckPolicy.EXPLICIT) != desired.ack_policy:
        diffs.append(f"ack_policy: {existing.ack_policy} != {desired.ack_policy}")
    if _filters(existing) != _filters(desired):
        diffs.append(
            f"filter_subject: {sorted(_filters(existing))} != {sorted(_filters(desired))}"
        )
    return diffs


def reconcile(
    existing: ConsumerConfig, desired: ConsumerConfig
) -> Tuple[Optional[ConsumerConfig], Dict[str, Tuple[object, object]]]:
    """
    Return ``(update, diff)``. ``update`` is the existing config with only the
    editable fields replaced (so immutable ones like deliver_policy are sent
    back unchanged), or ``None`` when nothing changed.
    """
    immutable = _immutable_diffs(existing, desired)
    if immutable:
        raise ConsumerConfigMismatchError(
            f"Durable '{existing.durable_name or existing.name}' differs in "
            f"immutable settings ({'; '.join(immutable)}). Delete the consumer or "
            "use a new service name; it cannot be updated in place."
        )
    diff = {
        field: (getattr(existing, field), getattr(desired, field))
        for field in EDITABLE_FIELDS
        if not _same(getattr(existing, field), getattr(desired, field))
    }
    if not diff:
        return None, {}
    changes = {field: getattr(desired, field) for field in diff}
    return replace(existing, **changes), diff

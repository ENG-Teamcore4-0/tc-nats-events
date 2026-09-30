"""
Stream configuration
====================

Single source of truth for the event stream and its dead letter stream, plus
validation of a stream that already exists. An existing stream is never
rewritten implicitly: other services may share it (NATS-09).
"""

import logging
from typing import List

from nats.js.api import RetentionPolicy, StorageType, StreamConfig

from ..utils.config import NATSConfig
from ..utils.exceptions import StreamConfigError

logger = logging.getLogger(__name__)


def event_subject_filter(config: NATSConfig) -> str:
    return f"{config.subject_prefix}.>"


def dlq_subject_filter(config: NATSConfig) -> str:
    # Outside "<prefix>.>" on purpose: overlapping subjects are rejected by NATS.
    return f"dlq.{config.subject_prefix}.>"


def build_stream_config(config: NATSConfig) -> StreamConfig:
    return StreamConfig(
        name=config.stream_name,
        subjects=[event_subject_filter(config)],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        max_msgs=config.max_messages,
        max_bytes=config.max_bytes,
        max_age=config.max_age_seconds,
        max_msg_size=config.max_msg_size,
        duplicate_window=config.duplicate_window_seconds,
        num_replicas=config.replicas,
    )


def build_dlq_stream_config(config: NATSConfig) -> StreamConfig:
    return StreamConfig(
        name=config.dlq_stream_name,
        subjects=[dlq_subject_filter(config)],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        max_age=config.max_age_seconds,
        # Bounded so a poison flood cannot fill the account storage (discard old).
        max_bytes=config.dlq_max_bytes,
        max_msgs=config.dlq_max_messages,
        max_msg_size=config.max_msg_size,
        # The server rejects a dedupe window longer than max_age.
        duplicate_window=min(
            config.dlq_duplicate_window_seconds, config.max_age_seconds
        ),
        num_replicas=config.replicas,
        description="Dead letters for tc-nats-events consumers",
    )


def subject_covers(pattern: str, subject: str) -> bool:
    """True if every subject matched by ``subject`` is matched by ``pattern``."""
    p_tokens, s_tokens = pattern.split("."), subject.split(".")
    for i, p in enumerate(p_tokens):
        if p == ">":
            return i < len(s_tokens)
        if i >= len(s_tokens):
            return False
        s = s_tokens[i]
        if p == "*":
            if s == ">":
                return False
            continue
        if p != s:
            return False
    return len(p_tokens) == len(s_tokens)


def _drift_warnings(existing: StreamConfig, desired: StreamConfig) -> List[str]:
    checks = {
        "num_replicas": (existing.num_replicas, desired.num_replicas),
        "duplicate_window": (existing.duplicate_window, desired.duplicate_window),
        "max_age": (existing.max_age, desired.max_age),
    }
    return [
        f"{name}: stream has {have}, config wants {want}"
        for name, (have, want) in checks.items()
        if have is not None and want is not None and float(have) != float(want)
    ]


def validate_existing_stream(existing: StreamConfig, config: NATSConfig) -> List[str]:
    """
    Check that an existing stream can serve this config without changing it.

    Returns non-fatal drift warnings; raises StreamConfigError when the stream
    does not capture our subjects.
    """
    expected = event_subject_filter(config)
    subjects = list(existing.subjects or [])
    if not any(subject_covers(s, expected) for s in subjects):
        raise StreamConfigError(
            f"Stream '{config.stream_name}' exists with subjects {subjects} that do "
            f"not cover '{expected}'. Refusing to overwrite a stream that may be "
            "shared; fix the stream or the subject_prefix."
        )
    return _drift_warnings(existing, build_stream_config(config))

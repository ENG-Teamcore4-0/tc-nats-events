"""
Configuration
=============

Configuration classes for NATS connection and stream settings.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

DEFAULT_NAK_DELAYS: Tuple[float, ...] = (1.0, 5.0, 30.0, 120.0, 600.0)


def parse_start_time(value: Union[None, str, datetime]) -> Optional[datetime]:
    """Accept ISO-8601 strings (``Z`` suffix included on Python < 3.11)."""
    if value is None or isinstance(value, datetime):
        return value
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as e:
        raise ValueError(f"opt_start_time is not ISO-8601: {value!r}") from e


@dataclass
class NATSConfig:
    """
    NATS configuration with sensible defaults.

    Can be initialized from environment variables or explicitly.
    """

    # Connection settings
    servers: List[str] = field(default_factory=lambda: ["nats://localhost:4222"])
    client_name: str = "tc-nats-events"
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)  # never logged
    environment: str = "development"

    # Connection resilience
    reconnect_time_wait: int = 2  # seconds
    max_reconnect_attempts: int = -1  # infinite
    connect_timeout: int = 10  # seconds

    # Stream configuration
    stream_name: str = "app-events"
    subject_prefix: str = "app.events"

    # Stream retention
    max_messages: int = 1_000_000
    max_bytes: int = 1024 * 1024 * 1024  # 1GB
    max_age_seconds: int = 7 * 24 * 60 * 60  # 7 days
    max_msg_size: int = 1024 * 1024  # 1MB
    replicas: int = 1
    duplicate_window_seconds: float = 120.0
    # Never rewrite an existing stream unless explicitly allowed (NATS-09).
    allow_stream_update: bool = False

    # Publishing
    publish_timeout_seconds: float = 5.0

    # Consumer defaults
    max_deliver_attempts: int = 6
    ack_wait_seconds: int = 30
    max_ack_pending: int = 1000
    # Where a *new* durable starts: new | all | by_start_time | last_per_subject
    deliver_policy: str = "new"
    opt_start_time: Optional[datetime] = None  # timezone-aware (ISO strings accepted)
    # Delays applied with nak(delay) after a handler failure (NATS-02).
    nak_delays_seconds: Tuple[float, ...] = DEFAULT_NAK_DELAYS
    # Server-side ConsumerConfig.backoff for ack_wait expiries. Its first value
    # replaces ack_wait, so it must start at or above ack_wait_seconds.
    consumer_backoff_seconds: Optional[Tuple[float, ...]] = None
    # Upper bound for a single handler run; heartbeats keep the message alive.
    handler_timeout_seconds: float = 25.0
    health_stale_seconds: float = 60.0

    # Idempotency (NATS-02): "nats_kv" (shared, durable) or "memory" (local only)
    idempotency_backend: str = "nats_kv"
    idempotency_ttl_seconds: int = 24 * 60 * 60
    idempotency_lease_seconds: float = 60.0
    clock_skew_seconds: float = 2.0

    # Dead letter queue (NATS-03)
    dlq_enabled: bool = True
    dlq_duplicate_window_seconds: float = 900.0
    dlq_max_bytes: int = 1024 * 1024 * 1024  # 1GB
    dlq_max_messages: int = 100_000

    def __post_init__(self) -> None:
        self.opt_start_time = parse_start_time(self.opt_start_time)
        # Normalise sequences to tuples so the config never shares mutable state.
        self.nak_delays_seconds = tuple(float(d) for d in self.nak_delays_seconds)
        if self.consumer_backoff_seconds is not None:
            self.consumer_backoff_seconds = tuple(
                float(d) for d in self.consumer_backoff_seconds
            )

    @property
    def dlq_stream_name(self) -> str:
        return f"{self.stream_name}-DLQ"

    @property
    def idempotency_bucket(self) -> str:
        return f"{self.stream_name}-idem"

    @property
    def heartbeat_interval_seconds(self) -> float:
        """How often in-flight messages are touched with in_progress()."""
        ack_wait = float(self.ack_wait_seconds)
        if self.consumer_backoff_seconds:
            ack_wait = min(ack_wait, self.consumer_backoff_seconds[0])
        return max(ack_wait / 3.0, 0.1)

    @classmethod
    def from_env(cls, prefix: str = "NATS_") -> "NATSConfig":
        """
        Create configuration from environment variables.

        Environment variables:
        - NATS_SERVERS: Comma-separated list of NATS URLs
        - NATS_CLIENT_NAME: Client identifier
        - NATS_USER: Authentication username
        - NATS_PASSWORD: Authentication password
        - NATS_ENVIRONMENT: development | staging | production
        - NATS_STREAM_NAME: Name of the stream
        - NATS_SUBJECT_PREFIX: Subject prefix for events
        - NATS_MAX_MESSAGES: Maximum messages in stream
        - NATS_MAX_BYTES: Maximum bytes in stream
        - NATS_MAX_AGE_SECONDS: Maximum age of messages
        - NATS_REPLICAS: Number of stream replicas
        - NATS_DELIVER_POLICY: new | all | by_start_time | last_per_subject
        - NATS_OPT_START_TIME: ISO-8601 start time for by_start_time
        - NATS_NAK_DELAYS_SECONDS: Comma-separated retry delays
        - NATS_IDEMPOTENCY_BACKEND: nats_kv | memory

        Args:
            prefix: Environment variable prefix

        Returns:
            NATSConfig instance
        """

        def get_env(key: str, default: Any = None) -> Any:
            return os.getenv(f"{prefix}{key}", default)

        def parse(key: str, value: str, kind: Any) -> Any:
            try:
                return kind(value)
            except ValueError as e:
                raise ValueError(f"Invalid value for {prefix}{key}: {value!r}") from e

        def get_env_int(key: str, default: int) -> int:
            value = get_env(key)
            return parse(key, value, int) if value else default

        def get_env_float(key: str, default: float) -> float:
            value = get_env(key)
            return parse(key, value, float) if value else default

        def get_env_bool(key: str, default: bool) -> bool:
            value = get_env(key)
            if not value:
                return default
            return value.strip().lower() in {"1", "true", "yes", "on"}

        def get_env_list(key: str, default: List[str]) -> List[str]:
            value = get_env(key)
            return value.split(",") if value else default

        def get_env_floats(key: str, default: Tuple[float, ...]) -> Tuple[float, ...]:
            value = get_env(key)
            if not value:
                return default
            return tuple(
                parse(key, v.strip(), float) for v in value.split(",") if v.strip()
            )

        return cls(
            servers=[
                s.strip() for s in get_env_list("SERVERS", ["nats://localhost:4222"])
            ],
            client_name=get_env("CLIENT_NAME", "tc-nats-events"),
            user=get_env("USER"),
            password=get_env("PASSWORD"),
            environment=get_env("ENVIRONMENT", "development"),
            reconnect_time_wait=get_env_int("RECONNECT_TIME_WAIT", 2),
            max_reconnect_attempts=get_env_int("MAX_RECONNECT_ATTEMPTS", -1),
            connect_timeout=get_env_int("CONNECT_TIMEOUT", 10),
            stream_name=get_env("STREAM_NAME", "app-events"),
            subject_prefix=get_env("SUBJECT_PREFIX", "app.events"),
            max_messages=get_env_int("MAX_MESSAGES", 1_000_000),
            max_bytes=get_env_int("MAX_BYTES", 1024 * 1024 * 1024),
            max_age_seconds=get_env_int("MAX_AGE_SECONDS", 7 * 24 * 60 * 60),
            max_msg_size=get_env_int("MAX_MSG_SIZE", 1024 * 1024),
            replicas=get_env_int("REPLICAS", 1),
            duplicate_window_seconds=get_env_float("DUPLICATE_WINDOW_SECONDS", 120.0),
            allow_stream_update=get_env_bool("ALLOW_STREAM_UPDATE", False),
            publish_timeout_seconds=get_env_float("PUBLISH_TIMEOUT_SECONDS", 5.0),
            max_deliver_attempts=get_env_int("MAX_DELIVER_ATTEMPTS", 6),
            ack_wait_seconds=get_env_int("ACK_WAIT_SECONDS", 30),
            max_ack_pending=get_env_int("MAX_ACK_PENDING", 1000),
            deliver_policy=get_env("DELIVER_POLICY", "new"),
            opt_start_time=get_env("OPT_START_TIME") or None,
            nak_delays_seconds=get_env_floats("NAK_DELAYS_SECONDS", DEFAULT_NAK_DELAYS),
            handler_timeout_seconds=get_env_float("HANDLER_TIMEOUT_SECONDS", 25.0),
            idempotency_backend=get_env("IDEMPOTENCY_BACKEND", "nats_kv"),
            idempotency_ttl_seconds=get_env_int("IDEMPOTENCY_TTL_SECONDS", 86400),
            dlq_enabled=get_env_bool("DLQ_ENABLED", True),
        )

    @classmethod
    def from_file(cls, config_file: Path) -> "NATSConfig":
        """
        Create configuration from a JSON or YAML file.

        Args:
            config_file: Path to configuration file

        Returns:
            NATSConfig instance
        """
        import json

        with open(config_file, "r") as f:
            if config_file.suffix in [".yaml", ".yml"]:
                import yaml

                config_dict = yaml.safe_load(f)
            else:
                config_dict = json.load(f)

        return cls(**config_dict)

    def validate(self) -> None:
        """
        Validate configuration values.

        Raises:
            ValueError: If configuration is invalid
        """
        from .config_validation import validate_config

        validate_config(self)

"""
Configuration
=============

Configuration classes for NATS connection and stream settings.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


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
    password: Optional[str] = None

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
    max_age_seconds: int = 30 * 24 * 60 * 60  # 30 days
    max_msg_size: int = 1024 * 1024  # 1MB
    replicas: int = 1

    # Consumer defaults
    max_deliver_attempts: int = 3
    ack_wait_seconds: int = 30
    max_ack_pending: int = 1000

    @classmethod
    def from_env(cls, prefix: str = "NATS_") -> "NATSConfig":
        """
        Create configuration from environment variables.

        Environment variables:
        - NATS_SERVERS: Comma-separated list of NATS URLs
        - NATS_CLIENT_NAME: Client identifier
        - NATS_USER: Authentication username
        - NATS_PASSWORD: Authentication password
        - NATS_STREAM_NAME: Name of the stream
        - NATS_SUBJECT_PREFIX: Subject prefix for events
        - NATS_MAX_MESSAGES: Maximum messages in stream
        - NATS_MAX_BYTES: Maximum bytes in stream
        - NATS_MAX_AGE_SECONDS: Maximum age of messages
        - NATS_REPLICAS: Number of stream replicas

        Args:
            prefix: Environment variable prefix

        Returns:
            NATSConfig instance
        """

        def get_env(key: str, default=None):
            return os.getenv(f"{prefix}{key}", default)

        def get_env_int(key: str, default: int) -> int:
            value = get_env(key)
            return int(value) if value else default

        def get_env_list(key: str, default: List[str]) -> List[str]:
            value = get_env(key)
            return value.split(",") if value else default

        return cls(
            servers=get_env_list("SERVERS", ["nats://localhost:4222"]),
            client_name=get_env("CLIENT_NAME", "tc-nats-events"),
            user=get_env("USER"),
            password=get_env("PASSWORD"),
            reconnect_time_wait=get_env_int("RECONNECT_TIME_WAIT", 2),
            max_reconnect_attempts=get_env_int("MAX_RECONNECT_ATTEMPTS", -1),
            connect_timeout=get_env_int("CONNECT_TIMEOUT", 10),
            stream_name=get_env("STREAM_NAME", "app-events"),
            subject_prefix=get_env("SUBJECT_PREFIX", "app.events"),
            max_messages=get_env_int("MAX_MESSAGES", 1_000_000),
            max_bytes=get_env_int("MAX_BYTES", 1024 * 1024 * 1024),
            max_age_seconds=get_env_int("MAX_AGE_SECONDS", 30 * 24 * 60 * 60),
            max_msg_size=get_env_int("MAX_MSG_SIZE", 1024 * 1024),
            replicas=get_env_int("REPLICAS", 1),
            max_deliver_attempts=get_env_int("MAX_DELIVER_ATTEMPTS", 3),
            ack_wait_seconds=get_env_int("ACK_WAIT_SECONDS", 30),
            max_ack_pending=get_env_int("MAX_ACK_PENDING", 1000),
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
        if not self.servers:
            raise ValueError("At least one NATS server must be specified")

        if self.max_messages <= 0:
            raise ValueError("max_messages must be positive")

        if self.max_bytes <= 0:
            raise ValueError("max_bytes must be positive")

        if self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")

        if self.replicas < 1:
            raise ValueError("replicas must be at least 1")

        if not self.stream_name:
            raise ValueError("stream_name cannot be empty")

        if not self.subject_prefix:
            raise ValueError("subject_prefix cannot be empty")

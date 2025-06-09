"""
Unit Tests for Configuration
============================

Test the NATSConfig class and configuration loading.
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from tc_nats_events.utils.config import NATSConfig


class TestNATSConfig:
    """Test NATSConfig class."""

    def test_config_defaults(self):
        """Test configuration with default values."""
        config = NATSConfig()

        assert config.servers == ["nats://localhost:4222"]
        assert config.client_name == "tc-nats-events"
        assert config.user is None
        assert config.password is None

        assert config.reconnect_time_wait == 2
        assert config.max_reconnect_attempts == -1
        assert config.connect_timeout == 10

        assert config.stream_name == "app-events"
        assert config.subject_prefix == "app.events"

        assert config.max_messages == 1_000_000
        assert config.max_bytes == 1024 * 1024 * 1024
        assert config.max_age_seconds == 30 * 24 * 60 * 60
        assert config.max_msg_size == 1024 * 1024
        assert config.replicas == 1

        assert config.max_deliver_attempts == 3
        assert config.ack_wait_seconds == 30
        assert config.max_ack_pending == 1000

    def test_config_custom_values(self):
        """Test configuration with custom values."""
        config = NATSConfig(
            servers=["nats://server1:4222", "nats://server2:4222"],
            client_name="my-client",
            user="testuser",
            password="testpass",
            stream_name="my-events",
            subject_prefix="my.events",
            replicas=3,
        )

        assert config.servers == ["nats://server1:4222", "nats://server2:4222"]
        assert config.client_name == "my-client"
        assert config.user == "testuser"
        assert config.password == "testpass"
        assert config.stream_name == "my-events"
        assert config.subject_prefix == "my.events"
        assert config.replicas == 3

    def test_config_from_env(self, monkeypatch):
        """Test loading configuration from environment variables."""
        # Set environment variables
        monkeypatch.setenv("NATS_SERVERS", "nats://env1:4222,nats://env2:4222")
        monkeypatch.setenv("NATS_CLIENT_NAME", "env-client")
        monkeypatch.setenv("NATS_USER", "envuser")
        monkeypatch.setenv("NATS_PASSWORD", "envpass")
        monkeypatch.setenv("NATS_STREAM_NAME", "env-stream")
        monkeypatch.setenv("NATS_SUBJECT_PREFIX", "env.events")
        monkeypatch.setenv("NATS_MAX_MESSAGES", "500000")
        monkeypatch.setenv("NATS_REPLICAS", "3")
        monkeypatch.setenv("NATS_ACK_WAIT_SECONDS", "60")

        config = NATSConfig.from_env()

        assert config.servers == ["nats://env1:4222", "nats://env2:4222"]
        assert config.client_name == "env-client"
        assert config.user == "envuser"
        assert config.password == "envpass"
        assert config.stream_name == "env-stream"
        assert config.subject_prefix == "env.events"
        assert config.max_messages == 500000
        assert config.replicas == 3
        assert config.ack_wait_seconds == 60

    def test_config_from_env_with_custom_prefix(self, monkeypatch):
        """Test loading configuration with custom env prefix."""
        monkeypatch.setenv("CUSTOM_SERVERS", "nats://custom:4222")
        monkeypatch.setenv("CUSTOM_STREAM_NAME", "custom-stream")

        config = NATSConfig.from_env(prefix="CUSTOM_")

        assert config.servers == ["nats://custom:4222"]
        assert config.stream_name == "custom-stream"

    def test_config_from_json_file(self):
        """Test loading configuration from JSON file."""
        config_data = {
            "servers": ["nats://file1:4222"],
            "client_name": "file-client",
            "stream_name": "file-stream",
            "subject_prefix": "file.events",
            "replicas": 2,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_file = Path(f.name)

        try:
            config = NATSConfig.from_file(config_file)

            assert config.servers == ["nats://file1:4222"]
            assert config.client_name == "file-client"
            assert config.stream_name == "file-stream"
            assert config.subject_prefix == "file.events"
            assert config.replicas == 2
        finally:
            config_file.unlink()

    def test_config_from_yaml_file(self):
        """Test loading configuration from YAML file."""
        yaml_content = """
            servers:
              - nats://yaml1:4222
              - nats://yaml2:4222
            client_name: yaml-client
            stream_name: yaml-stream
            subject_prefix: yaml.events
            max_messages: 2000000
            replicas: 3
        """

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_file = Path(f.name)

        try:
            config = NATSConfig.from_file(config_file)

            assert config.servers == ["nats://yaml1:4222", "nats://yaml2:4222"]
            assert config.client_name == "yaml-client"
            assert config.stream_name == "yaml-stream"
            assert config.subject_prefix == "yaml.events"
            assert config.max_messages == 2000000
            assert config.replicas == 3
        finally:
            config_file.unlink()

    def test_config_validation_valid(self):
        """Test validation with valid configuration."""
        config = NATSConfig()
        config.validate()  # Should not raise

        config = NATSConfig(
            servers=["nats://server:4222"],
            max_messages=1000,
            max_bytes=1000000,
            max_age_seconds=3600,
            replicas=3,
            stream_name="test",
            subject_prefix="test",
        )
        config.validate()  # Should not raise

    def test_config_validation_invalid(self):
        """Test validation with invalid configuration."""
        # Empty servers
        config = NATSConfig(servers=[])
        with pytest.raises(ValueError, match="At least one NATS server"):
            config.validate()

        # Invalid max_messages
        config = NATSConfig(max_messages=0)
        with pytest.raises(ValueError, match="max_messages must be positive"):
            config.validate()

        # Invalid max_bytes
        config = NATSConfig(max_bytes=-1)
        with pytest.raises(ValueError, match="max_bytes must be positive"):
            config.validate()

        # Invalid max_age_seconds
        config = NATSConfig(max_age_seconds=0)
        with pytest.raises(ValueError, match="max_age_seconds must be positive"):
            config.validate()

        # Invalid replicas
        config = NATSConfig(replicas=0)
        with pytest.raises(ValueError, match="replicas must be at least 1"):
            config.validate()

        # Empty stream_name
        config = NATSConfig(stream_name="")
        with pytest.raises(ValueError, match="stream_name cannot be empty"):
            config.validate()

        # Empty subject_prefix
        config = NATSConfig(subject_prefix="")
        with pytest.raises(ValueError, match="subject_prefix cannot be empty"):
            config.validate()

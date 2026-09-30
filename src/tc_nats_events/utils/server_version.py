"""Minimum nats-server version required by the reliability features."""

import logging
from typing import Any

from .exceptions import ConnectionError

logger = logging.getLogger(__name__)

MIN_SERVER_VERSION = (2, 10)


def ensure_server_version(nc: Any) -> None:
    """
    Fail fast on servers older than 2.10: consumer create-or-update semantics
    (NATS-06) and several JetStream APIs used here require it.
    """
    version = getattr(nc, "connected_server_version", None)
    major = getattr(version, "major", None)
    minor = getattr(version, "minor", None)
    if not isinstance(major, int) or not isinstance(minor, int):
        logger.debug("Unknown nats-server version; skipping version check")
        return
    if (major, minor) < MIN_SERVER_VERSION:
        raise ConnectionError(
            f"nats-server {major}.{minor} is not supported; "
            f"tc-nats-events requires >= {MIN_SERVER_VERSION[0]}.{MIN_SERVER_VERSION[1]}"
        )

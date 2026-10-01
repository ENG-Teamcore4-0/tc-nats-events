"""
Consumer bootstrap
==================

Creates the stream if it is missing (never rewrites it, NATS-09) and creates or
reconciles the durable consumer (NATS-04 / NATS-06).
"""

import logging

from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError

from ..core.stream_config import build_stream_config, validate_existing_stream
from ..utils.config import NATSConfig
from .consumer_config import build_consumer_config, reconcile

logger = logging.getLogger(__name__)


async def ensure_stream(js: JetStreamContext, config: NATSConfig) -> None:
    """Create the stream if missing; validate (never rewrite) an existing one."""
    try:
        info = await js.stream_info(config.stream_name)
    except NotFoundError:
        logger.info(f"Stream '{config.stream_name}' not found, creating it...")
        await js.add_stream(build_stream_config(config))
        return
    for warning in validate_existing_stream(info.config, config):
        logger.warning(f"Stream '{config.stream_name}' drift: {warning}")


async def ensure_consumer(
    js: JetStreamContext, config: NATSConfig, name: str, filter_subject: str
) -> None:
    desired = build_consumer_config(config, name, filter_subject)
    try:
        info = await js.consumer_info(config.stream_name, name)
    except NotFoundError:
        await _create(js, config, desired)
        return

    update, diff = reconcile(info.config, desired)
    if update is None:
        logger.info(
            f"Durable consumer '{name}' up to date "
            f"(delivered={info.delivered.stream_seq}, pending={info.num_ack_pending})"
        )
        return
    # Same durable name on nats-server >= 2.10 means create-or-update.
    await js.add_consumer(config.stream_name, update)
    changes = ", ".join(f"{k}: {old} -> {new}" for k, (old, new) in diff.items())
    logger.warning(f"Updated durable consumer '{name}': {changes}")


async def _create(
    js: JetStreamContext, config: NATSConfig, desired: ConsumerConfig
) -> None:
    await js.add_consumer(config.stream_name, desired)
    logger.info(
        f"Created durable consumer '{desired.durable_name}' "
        f"(deliver_policy={config.deliver_policy})"
    )
    if config.deliver_policy != "new":
        return
    try:
        state = (await js.stream_info(config.stream_name)).state
        if state.messages:
            logger.warning(
                f"New durable '{desired.durable_name}' starts with deliver_policy=new: "
                f"{state.messages} existing messages (seq {state.first_seq}.."
                f"{state.last_seq}) will NOT be processed. Use deliver_policy='all' "
                "or 'by_start_time' to replay them."
            )
    except Exception as e:  # informational only
        logger.debug(f"Could not inspect stream state: {e}")

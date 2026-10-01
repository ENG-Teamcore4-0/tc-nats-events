"""Helpers to run library consumers as independent "pods" inside a test."""

from typing import Any, Callable, Dict, Optional

from tc_nats_events import DurableEventConsumer, NATSConfig

from .conftest import new_pod_isolation, wait_for


async def start_pod(
    config: NATSConfig,
    handlers: Dict[str, Callable[..., Any]],
    service: str = "svc",
    **kwargs: Any,
) -> DurableEventConsumer:
    """Start a consumer with its own idempotency state and wait until live."""
    new_pod_isolation()
    consumer = DurableEventConsumer(service, config, **kwargs)
    for event_type, handler in handlers.items():
        consumer.register_handler(event_type, handler)
    await consumer.start()
    await wait_for(lambda: consumer.is_synced, timeout=10)
    return consumer


async def stop_pod(consumer: Optional[DurableEventConsumer]) -> None:
    if consumer is not None:
        try:
            await consumer.stop()
        except Exception:
            pass

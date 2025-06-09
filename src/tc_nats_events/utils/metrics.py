"""
Metrics Collection
==================

Metrics collection and monitoring utilities for TC NATS Events.
"""

import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from typing import Counter as CounterType
from typing import Dict, Optional


@dataclass
class EventMetrics:
    """Event processing metrics."""

    # Counters
    events_published: int = 0
    events_consumed: int = 0
    events_failed: int = 0
    events_retried: int = 0

    # Latency tracking
    publish_latencies: list = field(default_factory=list)
    consume_latencies: list = field(default_factory=list)

    # Error tracking
    error_counts: CounterType = field(default_factory=Counter)

    # Stream metrics
    stream_sequences: Dict[str, int] = field(default_factory=dict)
    consumer_lag: Dict[str, int] = field(default_factory=dict)

    # Timing
    last_event_time: Optional[datetime] = None
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def record_publish(self, latency_ms: float) -> None:
        """Record a successful publish."""
        self.events_published += 1
        self.publish_latencies.append(latency_ms)
        self.last_event_time = datetime.now(timezone.utc)

        # Keep only last 1000 latencies
        if len(self.publish_latencies) > 1000:
            self.publish_latencies = self.publish_latencies[-1000:]

    def record_consume(self, latency_ms: float) -> None:
        """Record a successful consume."""
        self.events_consumed += 1
        self.consume_latencies.append(latency_ms)
        self.last_event_time = datetime.now(timezone.utc)

        # Keep only last 1000 latencies
        if len(self.consume_latencies) > 1000:
            self.consume_latencies = self.consume_latencies[-1000:]

    def record_error(self, error_type: str) -> None:
        """Record an error."""
        self.events_failed += 1
        self.error_counts[error_type] += 1

    def record_retry(self) -> None:
        """Record a retry attempt."""
        self.events_retried += 1

    def record_sequence(self, stream: str, sequence: int) -> None:
        """Record stream sequence."""
        self.stream_sequences[stream] = sequence

    def record_lag(self, consumer: str, lag: int) -> None:
        """Record consumer lag."""
        self.consumer_lag[consumer] = lag

    def get_publish_stats(self) -> Dict[str, Any]:
        """Get publish statistics."""
        if not self.publish_latencies:
            return {
                "count": 0,
                "avg_latency_ms": 0,
                "p95_latency_ms": 0,
                "p99_latency_ms": 0,
            }

        sorted_latencies = sorted(self.publish_latencies)
        count = len(sorted_latencies)

        return {
            "count": self.events_published,
            "avg_latency_ms": sum(sorted_latencies) / count,
            "p95_latency_ms": sorted_latencies[int(count * 0.95)] if count > 0 else 0,
            "p99_latency_ms": sorted_latencies[int(count * 0.99)] if count > 0 else 0,
            "min_latency_ms": min(sorted_latencies),
            "max_latency_ms": max(sorted_latencies),
        }

    def get_consume_stats(self) -> Dict[str, Any]:
        """Get consume statistics."""
        if not self.consume_latencies:
            return {
                "count": 0,
                "avg_latency_ms": 0,
                "p95_latency_ms": 0,
                "p99_latency_ms": 0,
            }

        sorted_latencies = sorted(self.consume_latencies)
        count = len(sorted_latencies)

        return {
            "count": self.events_consumed,
            "avg_latency_ms": sum(sorted_latencies) / count,
            "p95_latency_ms": sorted_latencies[int(count * 0.95)] if count > 0 else 0,
            "p99_latency_ms": sorted_latencies[int(count * 0.99)] if count > 0 else 0,
            "min_latency_ms": min(sorted_latencies),
            "max_latency_ms": max(sorted_latencies),
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get complete metrics summary."""
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        return {
            "uptime_seconds": uptime,
            "events_published": self.events_published,
            "events_consumed": self.events_consumed,
            "events_failed": self.events_failed,
            "events_retried": self.events_retried,
            "publish_rate": self.events_published / uptime if uptime > 0 else 0,
            "consume_rate": self.events_consumed / uptime if uptime > 0 else 0,
            "error_rate": self.events_failed
            / max(self.events_consumed + self.events_published, 1),
            "publish_stats": self.get_publish_stats(),
            "consume_stats": self.get_consume_stats(),
            "error_counts": dict(self.error_counts),
            "stream_sequences": self.stream_sequences.copy(),
            "consumer_lag": self.consumer_lag.copy(),
            "last_event_time": (
                self.last_event_time.isoformat() if self.last_event_time else None
            ),
        }


class MetricsCollector:
    """Thread-safe metrics collector."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._metrics = EventMetrics()
        self._lock = threading.Lock()

    def record_publish_start(self) -> float:
        """Record publish start time."""
        return time.time() * 1000  # milliseconds

    def record_publish_success(self, start_time: float) -> None:
        """Record successful publish."""
        latency = (time.time() * 1000) - start_time
        with self._lock:
            self._metrics.record_publish(latency)

    def record_publish_error(self, error_type: str) -> None:
        """Record publish error."""
        with self._lock:
            self._metrics.record_error(f"publish_{error_type}")

    def record_consume_start(self) -> float:
        """Record consume start time."""
        return time.time() * 1000  # milliseconds

    def record_consume_success(self, start_time: float) -> None:
        """Record successful consume."""
        latency = (time.time() * 1000) - start_time
        with self._lock:
            self._metrics.record_consume(latency)

    def record_consume_error(self, error_type: str) -> None:
        """Record consume error."""
        with self._lock:
            self._metrics.record_error(f"consume_{error_type}")

    def record_retry(self) -> None:
        """Record retry attempt."""
        with self._lock:
            self._metrics.record_retry()

    def record_stream_sequence(self, stream: str, sequence: int) -> None:
        """Record stream sequence."""
        with self._lock:
            self._metrics.record_sequence(stream, sequence)

    def record_consumer_lag(self, consumer: str, lag: int) -> None:
        """Record consumer lag."""
        with self._lock:
            self._metrics.record_lag(consumer, lag)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        with self._lock:
            metrics = self._metrics.get_summary()
            metrics["service_name"] = self.service_name
            return metrics

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics = EventMetrics()


# Global metrics registry
_metrics_registry: Dict[str, MetricsCollector] = {}
_registry_lock = threading.Lock()


def get_metrics_collector(service_name: str) -> MetricsCollector:
    """Get or create metrics collector for a service."""
    with _registry_lock:
        if service_name not in _metrics_registry:
            _metrics_registry[service_name] = MetricsCollector(service_name)
        return _metrics_registry[service_name]


def get_all_metrics() -> Dict[str, Dict[str, Any]]:
    """Get metrics for all registered services."""
    with _registry_lock:
        return {
            service: collector.get_metrics()
            for service, collector in _metrics_registry.items()
        }

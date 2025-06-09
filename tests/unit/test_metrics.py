"""
Unit Tests for Metrics
======================

Test the metrics collection functionality.
"""

import time
from unittest.mock import patch

import pytest

from tc_nats_events.utils.metrics import (
    EventMetrics,
    MetricsCollector,
    get_all_metrics,
    get_metrics_collector,
)


class TestEventMetrics:
    """Test EventMetrics class."""

    def test_metrics_initialization(self):
        """Test metrics initialization."""
        metrics = EventMetrics()

        assert metrics.events_published == 0
        assert metrics.events_consumed == 0
        assert metrics.events_failed == 0
        assert metrics.events_retried == 0
        assert len(metrics.publish_latencies) == 0
        assert len(metrics.consume_latencies) == 0
        assert len(metrics.error_counts) == 0
        assert metrics.last_event_time is None

    def test_record_publish(self):
        """Test recording publish metrics."""
        metrics = EventMetrics()

        metrics.record_publish(10.5)
        metrics.record_publish(15.2)

        assert metrics.events_published == 2
        assert metrics.publish_latencies == [10.5, 15.2]
        assert metrics.last_event_time is not None

    def test_record_consume(self):
        """Test recording consume metrics."""
        metrics = EventMetrics()

        metrics.record_consume(5.3)
        metrics.record_consume(8.7)

        assert metrics.events_consumed == 2
        assert metrics.consume_latencies == [5.3, 8.7]
        assert metrics.last_event_time is not None

    def test_record_error(self):
        """Test recording errors."""
        metrics = EventMetrics()

        metrics.record_error("timeout")
        metrics.record_error("timeout")
        metrics.record_error("connection")

        assert metrics.events_failed == 3
        assert metrics.error_counts["timeout"] == 2
        assert metrics.error_counts["connection"] == 1

    def test_record_retry(self):
        """Test recording retries."""
        metrics = EventMetrics()

        metrics.record_retry()
        metrics.record_retry()

        assert metrics.events_retried == 2

    def test_record_sequence(self):
        """Test recording stream sequences."""
        metrics = EventMetrics()

        metrics.record_sequence("stream1", 100)
        metrics.record_sequence("stream2", 200)

        assert metrics.stream_sequences["stream1"] == 100
        assert metrics.stream_sequences["stream2"] == 200

    def test_record_lag(self):
        """Test recording consumer lag."""
        metrics = EventMetrics()

        metrics.record_lag("consumer1", 10)
        metrics.record_lag("consumer2", 5)

        assert metrics.consumer_lag["consumer1"] == 10
        assert metrics.consumer_lag["consumer2"] == 5

    def test_publish_stats(self):
        """Test publish statistics calculation."""
        metrics = EventMetrics()

        # Empty stats
        stats = metrics.get_publish_stats()
        assert stats["count"] == 0
        assert stats["avg_latency_ms"] == 0

        # With data
        latencies = [10, 20, 30, 40, 50]
        for latency in latencies:
            metrics.record_publish(latency)

        stats = metrics.get_publish_stats()
        assert stats["count"] == 5
        assert stats["avg_latency_ms"] == 30.0
        assert stats["min_latency_ms"] == 10
        assert stats["max_latency_ms"] == 50
        assert stats["p95_latency_ms"] == 50  # 95th percentile

    def test_consume_stats(self):
        """Test consume statistics calculation."""
        metrics = EventMetrics()

        latencies = [5, 15, 25, 35, 45]
        for latency in latencies:
            metrics.record_consume(latency)

        stats = metrics.get_consume_stats()
        assert stats["count"] == 5
        assert stats["avg_latency_ms"] == 25.0
        assert stats["min_latency_ms"] == 5
        assert stats["max_latency_ms"] == 45

    def test_latency_window_limit(self):
        """Test that latency windows are limited to 1000 entries."""
        metrics = EventMetrics()

        # Add more than 1000 latencies
        for i in range(1200):
            metrics.record_publish(i)

        # Should keep only last 1000
        assert len(metrics.publish_latencies) == 1000
        assert metrics.publish_latencies[0] == 200  # First kept entry
        assert metrics.publish_latencies[-1] == 1199  # Last entry

    def test_summary(self):
        """Test getting complete summary."""
        metrics = EventMetrics()

        # Add some data
        metrics.record_publish(10)
        metrics.record_consume(5)
        metrics.record_error("timeout")
        metrics.record_retry()
        metrics.record_sequence("stream1", 100)
        metrics.record_lag("consumer1", 5)

        summary = metrics.get_summary()

        assert summary["events_published"] == 1
        assert summary["events_consumed"] == 1
        assert summary["events_failed"] == 1
        assert summary["events_retried"] == 1
        assert summary["uptime_seconds"] > 0
        assert summary["publish_rate"] > 0
        assert summary["consume_rate"] > 0
        assert summary["error_rate"] == 0.5  # 1 error out of 2 events
        assert "publish_stats" in summary
        assert "consume_stats" in summary
        assert summary["stream_sequences"]["stream1"] == 100
        assert summary["consumer_lag"]["consumer1"] == 5


class TestMetricsCollector:
    """Test MetricsCollector class."""

    def test_collector_initialization(self):
        """Test collector initialization."""
        collector = MetricsCollector("test-service")

        assert collector.service_name == "test-service"
        metrics = collector.get_metrics()
        assert metrics["service_name"] == "test-service"
        assert metrics["events_published"] == 0

    def test_publish_flow(self):
        """Test publish metrics flow."""
        collector = MetricsCollector("test-service")

        start_time = collector.record_publish_start()
        assert isinstance(start_time, float)

        # Simulate some processing time
        time.sleep(0.01)

        collector.record_publish_success(start_time)

        metrics = collector.get_metrics()
        assert metrics["events_published"] == 1
        assert metrics["publish_stats"]["count"] > 0

    def test_consume_flow(self):
        """Test consume metrics flow."""
        collector = MetricsCollector("test-service")

        start_time = collector.record_consume_start()
        time.sleep(0.01)
        collector.record_consume_success(start_time)

        metrics = collector.get_metrics()
        assert metrics["events_consumed"] == 1

    def test_error_recording(self):
        """Test error recording."""
        collector = MetricsCollector("test-service")

        collector.record_publish_error("timeout")
        collector.record_consume_error("handler_failure")

        metrics = collector.get_metrics()
        assert metrics["events_failed"] == 2
        assert "publish_timeout" in metrics["error_counts"]
        assert "consume_handler_failure" in metrics["error_counts"]

    def test_thread_safety(self):
        """Test thread safety of metrics collection."""
        import threading

        collector = MetricsCollector("test-service")

        def worker():
            for _ in range(100):
                start_time = collector.record_publish_start()
                collector.record_publish_success(start_time)

        # Start multiple threads
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        metrics = collector.get_metrics()
        assert metrics["events_published"] == 500  # 5 threads * 100 events

    def test_reset(self):
        """Test metrics reset."""
        collector = MetricsCollector("test-service")

        # Add some metrics
        start_time = collector.record_publish_start()
        collector.record_publish_success(start_time)
        collector.record_retry()

        # Verify metrics exist
        metrics = collector.get_metrics()
        assert metrics["events_published"] == 1
        assert metrics["events_retried"] == 1

        # Reset and verify
        collector.reset()
        metrics = collector.get_metrics()
        assert metrics["events_published"] == 0
        assert metrics["events_retried"] == 0


class TestGlobalRegistry:
    """Test global metrics registry."""

    def test_get_metrics_collector(self):
        """Test getting metrics collector from registry."""
        collector1 = get_metrics_collector("service1")
        collector2 = get_metrics_collector("service1")  # Same service
        collector3 = get_metrics_collector("service2")  # Different service

        # Same service should return same collector
        assert collector1 is collector2
        assert collector1 is not collector3

        assert collector1.service_name == "service1"
        assert collector3.service_name == "service2"

    def test_get_all_metrics(self):
        """Test getting all metrics."""
        # Create collectors for different services
        collector1 = get_metrics_collector("service1")
        collector2 = get_metrics_collector("service2")

        # Add some metrics
        start_time = collector1.record_publish_start()
        collector1.record_publish_success(start_time)

        start_time = collector2.record_consume_start()
        collector2.record_consume_success(start_time)

        # Get all metrics
        all_metrics = get_all_metrics()

        assert "service1" in all_metrics
        assert "service2" in all_metrics
        assert all_metrics["service1"]["events_published"] == 1
        assert all_metrics["service2"]["events_consumed"] == 1

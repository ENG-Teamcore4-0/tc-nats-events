"""
Performance Testing Example
===========================

This example demonstrates performance testing, load simulation,
and metrics collection for TC NATS Events.
"""

import asyncio
import time
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventMetadata,
    setup_logging,
)
from tc_nats_events.utils.metrics import get_metrics_collector, get_all_metrics


@dataclass
class TestResult:
    """Performance test result."""
    test_name: str
    total_events: int
    duration_seconds: float
    events_per_second: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    success_rate: float
    error_count: int
    
    def print_summary(self):
        """Print test summary."""
        print(f"\n📊 {self.test_name} Results:")
        print(f"   Total events: {self.total_events:,}")
        print(f"   Duration: {self.duration_seconds:.2f}s")
        print(f"   Throughput: {self.events_per_second:,.2f} events/sec")
        print(f"   Avg latency: {self.avg_latency_ms:.2f}ms")
        print(f"   P95 latency: {self.p95_latency_ms:.2f}ms")
        print(f"   P99 latency: {self.p99_latency_ms:.2f}ms")
        print(f"   Success rate: {self.success_rate:.1%}")
        print(f"   Errors: {self.error_count}")


class LoadGenerator:
    """Generate load for performance testing."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.latencies: List[float] = []
        self.errors = 0
        self.events_sent = 0
    
    async def generate_burst_load(self, events_count: int, burst_size: int = 100):
        """Generate burst load - send events in bursts."""
        print(f"\n🚀 Generating burst load: {events_count} events in bursts of {burst_size}")
        
        start_time = time.time()
        
        for i in range(0, events_count, burst_size):
            burst_start = time.time()
            tasks = []
            
            # Create burst of events
            for j in range(min(burst_size, events_count - i)):
                event_data = {
                    "event_id": f"burst-{i+j}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "value": j,
                        "burst_id": i // burst_size,
                        "test_type": "burst"
                    }
                }
                
                task = self._send_event("load.burst_event", event_data)
                tasks.append(task)
            
            # Send burst concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Track results
            for result in results:
                if isinstance(result, Exception):
                    self.errors += 1
                else:
                    self.events_sent += 1
            
            # Small delay between bursts
            await asyncio.sleep(0.1)
            
            if (i + burst_size) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = self.events_sent / elapsed if elapsed > 0 else 0
                print(f"   Progress: {i + burst_size}/{events_count} events, {rate:.2f} events/sec")
        
        return time.time() - start_time
    
    async def generate_sustained_load(self, duration_seconds: int, target_rate: int):
        """Generate sustained load at target rate."""
        print(f"\n🔄 Generating sustained load: {target_rate} events/sec for {duration_seconds}s")
        
        start_time = time.time()
        interval = 1.0 / target_rate
        events_sent_this_second = 0
        second_start = time.time()
        
        while time.time() - start_time < duration_seconds:
            event_start = time.time()
            
            # Send event
            event_data = {
                "event_id": f"sustained-{self.events_sent}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "sequence": self.events_sent,
                    "test_type": "sustained"
                }
            }
            
            try:
                await self._send_event("load.sustained_event", event_data)
                self.events_sent += 1
                events_sent_this_second += 1
            except Exception:
                self.errors += 1
            
            # Rate limiting
            elapsed = time.time() - event_start
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)
            
            # Print rate every second
            if time.time() - second_start >= 1.0:
                print(f"   Current rate: {events_sent_this_second} events/sec")
                events_sent_this_second = 0
                second_start = time.time()
        
        return time.time() - start_time
    
    async def generate_variable_load(self, duration_seconds: int):
        """Generate variable load pattern."""
        print(f"\n📈 Generating variable load for {duration_seconds}s")
        
        patterns = [
            (10, 100),   # 10 seconds at 100 events/sec
            (5, 500),    # 5 seconds at 500 events/sec
            (10, 50),    # 10 seconds at 50 events/sec
            (5, 1000),   # 5 seconds at 1000 events/sec
        ]
        
        start_time = time.time()
        pattern_index = 0
        
        while time.time() - start_time < duration_seconds:
            duration, rate = patterns[pattern_index % len(patterns)]
            pattern_end = min(time.time() + duration, start_time + duration_seconds)
            
            print(f"   Pattern: {rate} events/sec for up to {duration}s")
            
            while time.time() < pattern_end:
                interval = 1.0 / rate
                event_start = time.time()
                
                event_data = {
                    "event_id": f"variable-{self.events_sent}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "pattern_rate": rate,
                        "test_type": "variable"
                    }
                }
                
                try:
                    await self._send_event("load.variable_event", event_data)
                    self.events_sent += 1
                except Exception:
                    self.errors += 1
                
                elapsed = time.time() - event_start
                if elapsed < interval:
                    await asyncio.sleep(interval - elapsed)
            
            pattern_index += 1
        
        return time.time() - start_time
    
    async def _send_event(self, event_type: str, data: Dict[str, Any]):
        """Send a single event and track latency."""
        start = time.time()
        
        await self.publisher.publish(event_type, data)
        
        latency = (time.time() - start) * 1000  # Convert to ms
        self.latencies.append(latency)
        
        # Keep only last 10000 latencies
        if len(self.latencies) > 10000:
            self.latencies = self.latencies[-10000:]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get performance statistics."""
        if not self.latencies:
            return {}
        
        sorted_latencies = sorted(self.latencies)
        
        return {
            "events_sent": self.events_sent,
            "errors": self.errors,
            "success_rate": self.events_sent / (self.events_sent + self.errors) if (self.events_sent + self.errors) > 0 else 0,
            "avg_latency_ms": statistics.mean(sorted_latencies),
            "min_latency_ms": min(sorted_latencies),
            "max_latency_ms": max(sorted_latencies),
            "p50_latency_ms": sorted_latencies[len(sorted_latencies) // 2],
            "p95_latency_ms": sorted_latencies[int(len(sorted_latencies) * 0.95)],
            "p99_latency_ms": sorted_latencies[int(len(sorted_latencies) * 0.99)],
        }


class PerformanceMonitor:
    """Monitor consumer performance."""
    
    def __init__(self):
        self.events_received = 0
        self.processing_times: List[float] = []
        self.event_types: Dict[str, int] = {}
        self.start_time = None
        self.first_event_time = None
        self.last_event_time = None
    
    async def handle_load_event(self, event: Event):
        """Handle load test events."""
        if self.start_time is None:
            self.start_time = time.time()
        
        if self.first_event_time is None:
            self.first_event_time = time.time()
        
        process_start = time.time()
        
        # Track event
        self.events_received += 1
        self.last_event_time = time.time()
        
        event_type = event.event_type
        if event_type not in self.event_types:
            self.event_types[event_type] = 0
        self.event_types[event_type] += 1
        
        # Simulate processing
        await asyncio.sleep(0.001)  # 1ms processing time
        
        # Track processing time
        processing_time = (time.time() - process_start) * 1000
        self.processing_times.append(processing_time)
        
        # Keep only last 10000 times
        if len(self.processing_times) > 10000:
            self.processing_times = self.processing_times[-10000:]
        
        # Print progress
        if self.events_received % 1000 == 0:
            elapsed = time.time() - self.start_time
            rate = self.events_received / elapsed if elapsed > 0 else 0
            print(f"   📥 Consumer: {self.events_received} events received, {rate:.2f} events/sec")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        if not self.processing_times or self.start_time is None:
            return {}
        
        elapsed = time.time() - self.start_time
        
        sorted_times = sorted(self.processing_times)
        
        return {
            "events_received": self.events_received,
            "duration": elapsed,
            "receive_rate": self.events_received / elapsed if elapsed > 0 else 0,
            "event_types": dict(self.event_types),
            "avg_processing_time_ms": statistics.mean(sorted_times),
            "p95_processing_time_ms": sorted_times[int(len(sorted_times) * 0.95)],
            "p99_processing_time_ms": sorted_times[int(len(sorted_times) * 0.99)],
            "first_to_last_duration": (self.last_event_time - self.first_event_time) if self.first_event_time and self.last_event_time else 0
        }


async def run_performance_tests(config: NATSConfig):
    """Run various performance tests."""
    results: List[TestResult] = []
    
    # Create publisher and consumer
    publisher = EventPublisher("load-generator", config)
    await publisher.connect()
    
    load_generator = LoadGenerator(publisher)
    monitor = PerformanceMonitor()
    
    consumer = DurableEventConsumer("performance-monitor", config)
    consumer.register_handler("load.burst_event", monitor.handle_load_event)
    consumer.register_handler("load.sustained_event", monitor.handle_load_event)
    consumer.register_handler("load.variable_event", monitor.handle_load_event)
    
    await consumer.start()
    
    # Wait for sync
    while not consumer.is_synced:
        await asyncio.sleep(0.5)
    print("✅ Consumer ready for performance testing")
    
    try:
        # Test 1: Burst Load
        print("\n" + "=" * 50)
        print("Test 1: Burst Load Performance")
        print("=" * 50)
        
        monitor.events_received = 0
        monitor.start_time = None
        load_generator.latencies.clear()
        
        duration = await load_generator.generate_burst_load(5000, burst_size=100)
        await asyncio.sleep(2)  # Wait for processing
        
        stats = load_generator.get_statistics()
        result = TestResult(
            test_name="Burst Load Test",
            total_events=stats["events_sent"],
            duration_seconds=duration,
            events_per_second=stats["events_sent"] / duration,
            avg_latency_ms=stats["avg_latency_ms"],
            p95_latency_ms=stats["p95_latency_ms"],
            p99_latency_ms=stats["p99_latency_ms"],
            success_rate=stats["success_rate"],
            error_count=stats["errors"]
        )
        results.append(result)
        result.print_summary()
        
        # Test 2: Sustained Load
        print("\n" + "=" * 50)
        print("Test 2: Sustained Load Performance")
        print("=" * 50)
        
        monitor.events_received = 0
        monitor.start_time = None
        load_generator.latencies.clear()
        load_generator.events_sent = 0
        load_generator.errors = 0
        
        duration = await load_generator.generate_sustained_load(30, target_rate=200)
        await asyncio.sleep(2)  # Wait for processing
        
        stats = load_generator.get_statistics()
        result = TestResult(
            test_name="Sustained Load Test",
            total_events=stats["events_sent"],
            duration_seconds=duration,
            events_per_second=stats["events_sent"] / duration,
            avg_latency_ms=stats["avg_latency_ms"],
            p95_latency_ms=stats["p95_latency_ms"],
            p99_latency_ms=stats["p99_latency_ms"],
            success_rate=stats["success_rate"],
            error_count=stats["errors"]
        )
        results.append(result)
        result.print_summary()
        
        # Test 3: Variable Load
        print("\n" + "=" * 50)
        print("Test 3: Variable Load Performance")
        print("=" * 50)
        
        monitor.events_received = 0
        monitor.start_time = None
        load_generator.latencies.clear()
        load_generator.events_sent = 0
        load_generator.errors = 0
        
        duration = await load_generator.generate_variable_load(30)
        await asyncio.sleep(2)  # Wait for processing
        
        stats = load_generator.get_statistics()
        result = TestResult(
            test_name="Variable Load Test",
            total_events=stats["events_sent"],
            duration_seconds=duration,
            events_per_second=stats["events_sent"] / duration,
            avg_latency_ms=stats["avg_latency_ms"],
            p95_latency_ms=stats["p95_latency_ms"],
            p99_latency_ms=stats["p99_latency_ms"],
            success_rate=stats["success_rate"],
            error_count=stats["errors"]
        )
        results.append(result)
        result.print_summary()
        
        # Consumer statistics
        consumer_stats = monitor.get_statistics()
        print("\n" + "=" * 50)
        print("Consumer Performance")
        print("=" * 50)
        print(f"Total events processed: {consumer_stats['events_received']:,}")
        print(f"Average processing time: {consumer_stats['avg_processing_time_ms']:.2f}ms")
        print(f"P95 processing time: {consumer_stats['p95_processing_time_ms']:.2f}ms")
        print(f"P99 processing time: {consumer_stats['p99_processing_time_ms']:.2f}ms")
        
        # Overall metrics
        all_metrics = get_all_metrics()
        print("\n" + "=" * 50)
        print("Overall System Metrics")
        print("=" * 50)
        
        for service_name, metrics in all_metrics.items():
            if metrics['events_published'] > 0 or metrics['events_consumed'] > 0:
                print(f"\n{service_name}:")
                print(f"  Published: {metrics['events_published']:,}")
                print(f"  Consumed: {metrics['events_consumed']:,}")
                print(f"  Failed: {metrics['events_failed']}")
                
                if metrics['publish_stats']['count'] > 0:
                    print(f"  Publish P95: {metrics['publish_stats']['p95_latency_ms']:.2f}ms")
                
                if metrics['consume_stats']['count'] > 0:
                    print(f"  Consume P95: {metrics['consume_stats']['p95_latency_ms']:.2f}ms")
        
        # Summary
        print("\n" + "=" * 50)
        print("Performance Test Summary")
        print("=" * 50)
        
        total_events = sum(r.total_events for r in results)
        total_duration = sum(r.duration_seconds for r in results)
        avg_throughput = total_events / total_duration if total_duration > 0 else 0
        
        print(f"\nTotal events: {total_events:,}")
        print(f"Total duration: {total_duration:.2f}s")
        print(f"Average throughput: {avg_throughput:,.2f} events/sec")
        
        # Find best/worst
        best_throughput = max(results, key=lambda r: r.events_per_second)
        worst_latency = max(results, key=lambda r: r.p99_latency_ms)
        
        print(f"\nBest throughput: {best_throughput.test_name} - {best_throughput.events_per_second:,.2f} events/sec")
        print(f"Worst P99 latency: {worst_latency.test_name} - {worst_latency.p99_latency_ms:.2f}ms")
        
    finally:
        await publisher.disconnect()
        await consumer.stop()


async def main():
    """Run performance testing demo."""
    print("=" * 70)
    print("TC NATS Events - Performance Testing Demo")
    print("=" * 70)
    print("\nThis demo runs performance tests including:")
    print("• Burst load testing")
    print("• Sustained load testing")
    print("• Variable load patterns")
    print("• Latency measurements (avg, P95, P99)")
    print("• Throughput analysis")
    print("• Consumer performance tracking")
    print("=" * 70)
    
    # Setup logging
    setup_logging(level="WARNING", structured=False)  # Less verbose for performance tests
    
    # Configuration optimized for performance
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="performance-test-events",
        subject_prefix="perf.events",
        max_messages=1_000_000,
        max_ack_pending=5000
    )
    
    try:
        await run_performance_tests(config)
        print("\n🎉 Performance testing completed successfully!")
    except KeyboardInterrupt:
        print("\n⚠️  Performance test interrupted by user")
    except Exception as e:
        print(f"\n❌ Performance test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
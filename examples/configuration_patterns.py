"""
Configuration and Deployment Patterns Example
=============================================

This example demonstrates various configuration patterns, environment-based
settings, and deployment strategies for TC NATS Events.
"""

import asyncio
import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    setup_logging,
)


def demonstrate_configuration_methods():
    """Show different ways to configure TC NATS Events."""
    print("\n" + "=" * 70)
    print("Configuration Methods")
    print("=" * 70)

    # Method 1: Default configuration
    print("\n1️⃣  Default Configuration:")
    default_config = NATSConfig()
    print(f"   Servers: {default_config.servers}")
    print(f"   Stream: {default_config.stream_name}")
    print(f"   Subject prefix: {default_config.subject_prefix}")

    # Method 2: Environment variables
    print("\n2️⃣  Environment Variable Configuration:")
    # Set environment variables
    os.environ["NATS_SERVERS"] = "nats://prod-nats-1:4222,nats://prod-nats-2:4222"
    os.environ["NATS_STREAM_NAME"] = "production-events"
    os.environ["NATS_SUBJECT_PREFIX"] = "prod.events"
    os.environ["NATS_MAX_MESSAGES"] = "5000000"
    os.environ["NATS_REPLICAS"] = "3"

    env_config = NATSConfig.from_env()
    print(f"   Servers: {env_config.servers}")
    print(f"   Stream: {env_config.stream_name}")
    print(f"   Subject prefix: {env_config.subject_prefix}")
    print(f"   Max messages: {env_config.max_messages:,}")
    print(f"   Replicas: {env_config.replicas}")

    # Method 3: Explicit configuration
    print("\n3️⃣  Explicit Configuration:")
    explicit_config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="custom-events",
        subject_prefix="custom.events",
        max_messages=1_000_000,
        max_age_seconds=7 * 24 * 60 * 60,  # 7 days
        replicas=1,
        max_ack_pending=1000,
        ack_wait_seconds=30
    )
    print(f"   Max age: {explicit_config.max_age_seconds / 86400} days")
    print(f"   Max ACK pending: {explicit_config.max_ack_pending}")
    print(f"   ACK wait: {explicit_config.ack_wait_seconds}s")

    # Method 4: Configuration from file
    print("\n4️⃣  File-based Configuration:")
    config_data = {
        "nats": {
            "servers": ["nats://nats-1:4222", "nats://nats-2:4222"],
            "stream_name": "app-events",
            "subject_prefix": "app.events",
            "credentials_file": "/path/to/nats.creds"
        },
        "stream": {
            "max_messages": 10_000_000,
            "max_bytes": 10 * 1024 * 1024 * 1024,  # 10GB
            "max_age_seconds": 30 * 24 * 60 * 60,   # 30 days
            "replicas": 3
        },
        "consumer": {
            "max_deliver_attempts": 5,
            "ack_wait_seconds": 60,
            "max_ack_pending": 5000
        }
    }

    # Save to YAML file
    config_path = Path("config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    # Load from file
    file_config = NATSConfig.from_file(str(config_path))
    print(f"   Loaded from: {config_path}")
    print(f"   Servers: {file_config.servers}")
    print(f"   Max bytes: {file_config.max_bytes / (1024**3):.1f}GB")

    # Clean up
    config_path.unlink()

    # Reset environment
    for key in ["NATS_SERVERS", "NATS_STREAM_NAME", "NATS_SUBJECT_PREFIX",
                "NATS_MAX_MESSAGES", "NATS_REPLICAS"]:
        os.environ.pop(key, None)


class ConfigurableService:
    """Example service with environment-specific configuration."""

    def __init__(self, environment: str):
        self.environment = environment
        self.config = self._load_config_for_environment(environment)
        self.features = self._load_feature_flags(environment)

    def _load_config_for_environment(self, env: str) -> Dict[str, Any]:
        """Load configuration based on environment."""
        configs = {
            "development": {
                "nats_config": NATSConfig(
                    servers=["nats://localhost:4222"],
                    stream_name="dev-events",
                    subject_prefix="dev.events",
                    replicas=1,
                    max_messages=10_000
                ),
                "retry_policy": {
                    "max_attempts": 3,
                    "backoff_ms": 100
                },
                "monitoring": {
                    "enabled": True,
                    "verbose": True,
                    "sample_rate": 1.0  # 100% sampling
                }
            },
            "staging": {
                "nats_config": NATSConfig(
                    servers=["nats://staging-nats:4222"],
                    stream_name="staging-events",
                    subject_prefix="staging.events",
                    replicas=2,
                    max_messages=100_000
                ),
                "retry_policy": {
                    "max_attempts": 5,
                    "backoff_ms": 500
                },
                "monitoring": {
                    "enabled": True,
                    "verbose": False,
                    "sample_rate": 0.1  # 10% sampling
                }
            },
            "production": {
                "nats_config": NATSConfig(
                    servers=[
                        "nats://prod-nats-1:4222",
                        "nats://prod-nats-2:4222",
                        "nats://prod-nats-3:4222"
                    ],
                    stream_name="production-events",
                    subject_prefix="prod.events",
                    replicas=3,
                    max_messages=10_000_000,
                    max_bytes=100 * 1024 * 1024 * 1024,  # 100GB
                    max_age_seconds=90 * 24 * 60 * 60     # 90 days
                ),
                "retry_policy": {
                    "max_attempts": 10,
                    "backoff_ms": 1000,
                    "max_backoff_ms": 60000
                },
                "monitoring": {
                    "enabled": True,
                    "verbose": False,
                    "sample_rate": 0.01,  # 1% sampling
                    "alert_threshold": 100
                }
            }
        }

        return configs.get(env, configs["development"])

    def _load_feature_flags(self, env: str) -> Dict[str, bool]:
        """Load feature flags for environment."""
        flags = {
            "development": {
                "enable_debug_events": True,
                "enable_performance_tracking": True,
                "enable_data_validation": True,
                "enable_circuit_breaker": False,
                "enable_rate_limiting": False
            },
            "staging": {
                "enable_debug_events": True,
                "enable_performance_tracking": True,
                "enable_data_validation": True,
                "enable_circuit_breaker": True,
                "enable_rate_limiting": True
            },
            "production": {
                "enable_debug_events": False,
                "enable_performance_tracking": True,
                "enable_data_validation": True,
                "enable_circuit_breaker": True,
                "enable_rate_limiting": True
            }
        }

        return flags.get(env, flags["development"])

    def print_configuration(self):
        """Print current configuration."""
        print(f"\n🔧 Configuration for {self.environment.upper()} environment:")
        print(f"\nNATS Configuration:")
        nats_config = self.config["nats_config"]
        print(f"  Servers: {nats_config.servers}")
        print(f"  Stream: {nats_config.stream_name}")
        print(f"  Replicas: {nats_config.replicas}")

        print(f"\nRetry Policy:")
        retry = self.config["retry_policy"]
        print(f"  Max attempts: {retry['max_attempts']}")
        print(f"  Backoff: {retry['backoff_ms']}ms")

        print(f"\nMonitoring:")
        monitoring = self.config["monitoring"]
        print(f"  Enabled: {monitoring['enabled']}")
        print(f"  Sample rate: {monitoring['sample_rate'] * 100}%")

        print(f"\nFeature Flags:")
        for flag, enabled in self.features.items():
            print(f"  {flag}: {'✓' if enabled else '✗'}")


async def demonstrate_deployment_patterns(config: NATSConfig):
    """Demonstrate various deployment patterns."""
    print("\n" + "=" * 70)
    print("Deployment Patterns")
    print("=" * 70)

    # Pattern 1: Single Service, Multiple Instances
    print("\n1️⃣  Horizontal Scaling Pattern:")
    print("   Multiple instances of the same service for load balancing")

    # Simulate multiple instances
    consumers = []
    for i in range(3):
        consumer = DurableEventConsumer(
            service_name="order-processor",  # Same name for load balancing
            config=config,
            instance_id=f"instance-{i+1}"  # Different instance IDs
        )

        async def handle_order(event: Event):
            print(f"   Instance {i+1} processing order: {event.data.get('order_id')}")

        consumer.register_handler("order.created", handle_order)
        consumers.append(consumer)

    # Start all instances
    for consumer in consumers:
        await consumer.start()

    # Publish some orders
    publisher = EventPublisher("order-api", config)
    await publisher.connect()

    print("   Publishing 10 orders...")
    for j in range(10):
        await publisher.publish("order.created", {"order_id": f"ORDER-{j+1:03d}"})

    await asyncio.sleep(2)

    # Cleanup
    for consumer in consumers:
        await consumer.stop()
    await publisher.disconnect()

    # Pattern 2: Blue-Green Deployment
    print("\n2️⃣  Blue-Green Deployment Pattern:")
    print("   Seamless switchover between versions")

    # Blue version (current)
    blue_consumer = DurableEventConsumer("payment-processor-blue", config)

    async def handle_payment_v1(event: Event):
        print(f"   BLUE (v1.0) processing payment: {event.data.get('payment_id')}")

    blue_consumer.register_handler("payment.requested", handle_payment_v1)
    await blue_consumer.start()

    # Deploy green version (new)
    green_consumer = DurableEventConsumer("payment-processor-green", config)

    async def handle_payment_v2(event: Event):
        print(f"   GREEN (v2.0) processing payment: {event.data.get('payment_id')} with new features!")

    green_consumer.register_handler("payment.requested", handle_payment_v2)
    await green_consumer.start()

    print("   Both versions running, green catching up...")
    await asyncio.sleep(2)

    # Switch traffic to green
    print("   Switching to GREEN version...")
    await blue_consumer.stop()

    # Cleanup
    await green_consumer.stop()

    # Pattern 3: Canary Deployment
    print("\n3️⃣  Canary Deployment Pattern:")
    print("   Gradual rollout with percentage-based routing")

    # Stable version
    stable_consumer = DurableEventConsumer("analytics-processor-stable", config)
    canary_consumer = DurableEventConsumer("analytics-processor-canary", config)

    canary_percentage = 20  # 20% to canary

    async def route_analytics_event(event: Event):
        import random
        if random.randint(1, 100) <= canary_percentage:
            print(f"   CANARY (20%) processing: {event.data.get('event_id')}")
        else:
            print(f"   STABLE (80%) processing: {event.data.get('event_id')}")

    # In real deployment, you'd have separate handlers
    stable_consumer.register_handler("analytics.event", route_analytics_event)

    # Pattern 4: Multi-Region Deployment
    print("\n4️⃣  Multi-Region Deployment Pattern:")
    print("   Services deployed across regions with region-specific config")

    regions = {
        "us-east": NATSConfig(
            servers=["nats://us-east-1:4222", "nats://us-east-2:4222"],
            stream_name="events-us-east"
        ),
        "eu-west": NATSConfig(
            servers=["nats://eu-west-1:4222", "nats://eu-west-2:4222"],
            stream_name="events-eu-west"
        ),
        "ap-south": NATSConfig(
            servers=["nats://ap-south-1:4222", "nats://ap-south-2:4222"],
            stream_name="events-ap-south"
        )
    }

    for region, region_config in regions.items():
        print(f"   Region {region}: {region_config.servers[0]}")


async def demonstrate_health_checks(config: NATSConfig):
    """Demonstrate health check patterns."""
    print("\n" + "=" * 70)
    print("Health Check Patterns")
    print("=" * 70)

    class HealthCheckService:
        """Service with comprehensive health checks."""

        def __init__(self, publisher: EventPublisher, consumer: DurableEventConsumer):
            self.publisher = publisher
            self.consumer = consumer
            self.last_event_time = None
            self.events_processed = 0

        async def health_check(self) -> Dict[str, Any]:
            """Perform health check."""
            health = {
                "status": "healthy",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "checks": {}
            }

            # Check NATS connection
            try:
                # Publish health check event
                await self.publisher.publish("health.check", {"timestamp": health["timestamp"]})
                health["checks"]["nats_publish"] = "healthy"
            except Exception as e:
                health["checks"]["nats_publish"] = f"unhealthy: {e}"
                health["status"] = "unhealthy"

            # Check consumer status
            if self.consumer.is_synced:
                health["checks"]["consumer_sync"] = "healthy"
            else:
                health["checks"]["consumer_sync"] = "syncing"
                health["status"] = "degraded"

            # Check processing lag
            if self.last_event_time:
                lag = (datetime.now(timezone.utc) - self.last_event_time).total_seconds()
                if lag > 60:  # More than 1 minute lag
                    health["checks"]["processing_lag"] = f"unhealthy: {lag}s lag"
                    health["status"] = "degraded"
                else:
                    health["checks"]["processing_lag"] = f"healthy: {lag}s lag"

            # Add metrics
            health["metrics"] = {
                "events_processed": self.events_processed,
                "consumer_state": self.consumer.state,
                "sync_status": self.consumer.get_sync_status()
            }

            return health

        async def liveness_probe(self) -> bool:
            """Simple liveness check."""
            try:
                # Just check if we can publish
                await self.publisher.publish("health.liveness", {"ping": "pong"})
                return True
            except Exception:
                return False

        async def readiness_probe(self) -> bool:
            """Readiness check."""
            # Ready if synced and processing
            return (self.consumer.is_synced and
                    self.consumer.state == "processing" and
                    await self.liveness_probe())

    # Create service with health checks
    publisher = EventPublisher("health-check-demo", config)
    consumer = DurableEventConsumer("health-check-demo", config)

    await publisher.connect()
    await consumer.start()

    health_service = HealthCheckService(publisher, consumer)

    # Perform health checks
    print("\n🏥 Health Check Results:")

    # Liveness
    is_alive = await health_service.liveness_probe()
    print(f"  Liveness: {'✓ ALIVE' if is_alive else '✗ DEAD'}")

    # Readiness
    is_ready = await health_service.readiness_probe()
    print(f"  Readiness: {'✓ READY' if is_ready else '✗ NOT READY'}")

    # Full health check
    health = await health_service.health_check()
    print(f"  Overall Status: {health['status'].upper()}")
    print(f"  Checks:")
    for check, status in health["checks"].items():
        print(f"    - {check}: {status}")

    await publisher.disconnect()
    await consumer.stop()


async def demonstrate_graceful_shutdown():
    """Demonstrate graceful shutdown patterns."""
    print("\n" + "=" * 70)
    print("Graceful Shutdown Patterns")
    print("=" * 70)

    class GracefulService:
        """Service with graceful shutdown."""

        def __init__(self, name: str, config: NATSConfig):
            self.name = name
            self.publisher = EventPublisher(name, config)
            self.consumer = DurableEventConsumer(name, config)
            self.shutdown_event = asyncio.Event()
            self.processing_tasks = set()

        async def start(self):
            """Start the service."""
            await self.publisher.connect()

            # Register handlers
            self.consumer.register_handler("task.process", self._handle_task)

            await self.consumer.start()
            print(f"✅ {self.name} started")

        async def _handle_task(self, event: Event):
            """Handle task with tracking."""
            task_id = event.data.get("task_id")

            # Track processing task
            task = asyncio.create_task(self._process_task(task_id))
            self.processing_tasks.add(task)
            task.add_done_callback(self.processing_tasks.discard)

            await task

        async def _process_task(self, task_id: str):
            """Process a task."""
            print(f"  {self.name} processing task: {task_id}")
            await asyncio.sleep(2)  # Simulate work
            print(f"  {self.name} completed task: {task_id}")

        async def shutdown(self, timeout: int = 30):
            """Gracefully shutdown the service."""
            print(f"\n🛑 {self.name} shutting down gracefully...")

            # Step 1: Stop accepting new work
            print(f"  1. Stopping consumer...")
            await self.consumer.stop()

            # Step 2: Wait for in-flight requests
            if self.processing_tasks:
                print(f"  2. Waiting for {len(self.processing_tasks)} tasks to complete...")
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.processing_tasks, return_exceptions=True),
                        timeout=timeout
                    )
                    print(f"  3. All tasks completed")
                except asyncio.TimeoutError:
                    print(f"  3. Timeout waiting for tasks, cancelling {len(self.processing_tasks)} tasks")
                    for task in self.processing_tasks:
                        task.cancel()

            # Step 3: Emit shutdown event
            await self.publisher.publish("service.shutdown", {
                "service": self.name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "graceful": True
            })

            # Step 4: Disconnect
            await self.publisher.disconnect()
            print(f"✅ {self.name} shutdown complete")

    # Demonstrate graceful shutdown
    config = NATSConfig()
    service = GracefulService("worker-service", config)

    await service.start()

    # Simulate some work
    publisher = EventPublisher("task-generator", config)
    await publisher.connect()

    for i in range(5):
        await publisher.publish("task.process", {"task_id": f"TASK-{i+1}"})

    await asyncio.sleep(1)

    # Trigger shutdown
    await service.shutdown()
    await publisher.disconnect()


async def main():
    """Run configuration and deployment patterns demo."""
    print("=" * 70)
    print("TC NATS Events - Configuration and Deployment Patterns")
    print("=" * 70)

    # Setup logging
    setup_logging(level="INFO", structured=False)

    # 1. Configuration methods
    demonstrate_configuration_methods()

    # 2. Environment-specific configuration
    print("\n" + "=" * 70)
    print("Environment-Specific Configuration")
    print("=" * 70)

    for env in ["development", "staging", "production"]:
        service = ConfigurableService(env)
        service.print_configuration()

    # 3. Deployment patterns
    config = NATSConfig()
    await demonstrate_deployment_patterns(config)

    # 4. Health checks
    await demonstrate_health_checks(config)

    # 5. Graceful shutdown
    await demonstrate_graceful_shutdown()

    print("\n🎉 Configuration and deployment patterns demo completed!")


if __name__ == "__main__":
    asyncio.run(main())

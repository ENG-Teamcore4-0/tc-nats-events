"""
Advanced Features Example
=========================

Demonstrates advanced features including metrics, idempotency, and structured logging.
"""

import asyncio
import signal
from datetime import datetime, timezone
from typing import Dict, Any

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventType,
    setup_logging,
)
from tc_nats_events.utils.metrics import get_metrics_collector, get_all_metrics
from tc_nats_events.utils.idempotency import generate_deterministic_id

class OrderProcessor:
    """Example order processing service with idempotency and metrics."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.processed_orders = set()
        self.metrics = get_metrics_collector(service_name)

    async def handle_order_created(self, event: Event):
        """Handle order created event with idempotency."""
        order_data = event.data
        order_id = order_data["order_id"]

        print(f"\n🔄 Processing order: {order_id}")

        # Simulate order processing
        await asyncio.sleep(0.1)

        # Simulate business logic
        if order_data.get("amount", 0) > 1000:
            await self._process_high_value_order(order_data)
        else:
            await self._process_regular_order(order_data)

        self.processed_orders.add(order_id)

        print(f"✅ Order processed: {order_id}")

        # Log with structured data
        print(f"📊 Orders processed so far: {len(self.processed_orders)}")

    async def handle_order_updated(self, event: Event):
        """Handle order updated event."""
        order_data = event.data
        order_id = order_data["order_id"]

        print(f"\n🔄 Updating order: {order_id}")
        await asyncio.sleep(0.05)
        print(f"✅ Order updated: {order_id}")

    async def handle_payment_processed(self, event: Event):
        """Handle payment processed event."""
        payment_data = event.data
        order_id = payment_data["order_id"]
        amount = payment_data["amount"]

        print(f"\n💳 Payment processed: Order {order_id}, Amount ${amount}")

        # Simulate payment validation
        if amount < 0:
            raise ValueError(f"Invalid payment amount: {amount}")

        await asyncio.sleep(0.1)
        print(f"✅ Payment validated: ${amount}")

    async def _process_high_value_order(self, order_data: Dict[str, Any]):
        """Process high-value orders with special handling."""
        print(f"🔥 High-value order detected: ${order_data['amount']}")
        # Special processing for high-value orders
        await asyncio.sleep(0.2)

    async def _process_regular_order(self, order_data: Dict[str, Any]):
        """Process regular orders."""
        print(f"📦 Regular order: ${order_data['amount']}")
        await asyncio.sleep(0.1)


class InventoryService:
    """Example inventory service."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.inventory = {
            "product-1": 100,
            "product-2": 50,
            "product-3": 25
        }

    async def handle_order_created(self, event: Event):
        """Update inventory when order is created."""
        order_data = event.data
        items = order_data.get("items", [])

        print(f"\n📦 Updating inventory for order: {order_data['order_id']}")

        for item in items:
            product_id = item["product_id"]
            quantity = item["quantity"]

            if product_id in self.inventory:
                self.inventory[product_id] = max(0, self.inventory[product_id] - quantity)
                print(f"   📉 {product_id}: {self.inventory[product_id]} remaining")
            else:
                print(f"   ⚠️  Unknown product: {product_id}")

        print(f"✅ Inventory updated")

    async def handle_order_cancelled(self, event: Event):
        """Restore inventory when order is cancelled."""
        order_data = event.data
        items = order_data.get("items", [])

        print(f"\n📦 Restoring inventory for cancelled order: {order_data['order_id']}")

        for item in items:
            product_id = item["product_id"]
            quantity = item["quantity"]

            if product_id in self.inventory:
                self.inventory[product_id] += quantity
                print(f"   📈 {product_id}: {self.inventory[product_id]} available")


async def simulate_order_processing():
    """Simulate order processing with multiple services."""
    print("\n🚀 Starting Order Processing Simulation")

    # Setup structured logging
    setup_logging(level="INFO", structured=True, service_name="order-processing")

    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="order-events",
        subject_prefix="order.events",
        max_messages=10000
    )

    # Create services
    order_processor = OrderProcessor("order-processor")
    inventory_service = InventoryService("inventory-service")

    # Create consumers
    order_consumer = DurableEventConsumer("order-processor", config)
    inventory_consumer = DurableEventConsumer("inventory-service", config)

    # Register handlers for order processor
    order_consumer.register_handler("order.created", order_processor.handle_order_created)
    order_consumer.register_handler("order.updated", order_processor.handle_order_updated)
    order_consumer.register_handler("payment.processed", order_processor.handle_payment_processed)

    # Register handlers for inventory service
    inventory_consumer.register_handler("order.created", inventory_service.handle_order_created)
    inventory_consumer.register_handler("order.cancelled", inventory_service.handle_order_cancelled)

    # Start consumers
    await order_consumer.start()
    await inventory_consumer.start()

    # Wait for sync
    print("⏳ Waiting for initial synchronization...")
    while not (order_consumer.is_synced and inventory_consumer.is_synced):
        await asyncio.sleep(0.1)
    print("✅ All consumers synchronized")

    # Publisher for generating events
    publisher = EventPublisher("order-api", config, environment="demo")
    await publisher.connect()

    # Set correlation context for related events
    correlation_id = "order-session-123"
    publisher.set_context(correlation_id=correlation_id, user_id="user-456")

    try:
        # Simulate order creation
        print("\n📝 Creating orders...")

        orders = [
            {
                "order_id": "order-001",
                "customer_id": "customer-123",
                "amount": 250.00,
                "items": [
                    {"product_id": "product-1", "quantity": 2, "price": 100.00},
                    {"product_id": "product-2", "quantity": 1, "price": 50.00}
                ]
            },
            {
                "order_id": "order-002",
                "customer_id": "customer-456",
                "amount": 1500.00,
                "items": [
                    {"product_id": "product-3", "quantity": 10, "price": 150.00}
                ]
            },
            {
                "order_id": "order-003",
                "customer_id": "customer-789",
                "amount": 75.00,
                "items": [
                    {"product_id": "product-1", "quantity": 1, "price": 75.00}
                ]
            }
        ]

        for order in orders:
            # Use deterministic ID for idempotency
            order["deterministic_id"] = generate_deterministic_id(
                order,
                fields=["order_id", "customer_id", "amount"]
            )

            await publisher.publish("order.created", order)
            await asyncio.sleep(0.5)  # Small delay

        # Simulate order updates
        print("\n🔄 Updating orders...")
        for i, order in enumerate(orders[:2]):  # Update first 2 orders
            update_data = {
                "order_id": order["order_id"],
                "status": "confirmed",
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            await publisher.publish("order.updated", update_data)
            await asyncio.sleep(0.3)

        # Simulate payments
        print("\n💳 Processing payments...")
        for order in orders:
            payment_data = {
                "payment_id": f"pay-{order['order_id']}",
                "order_id": order["order_id"],
                "amount": order["amount"],
                "method": "credit_card",
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
            await publisher.publish("payment.processed", payment_data)
            await asyncio.sleep(0.4)

        # Simulate order cancellation
        print("\n❌ Cancelling an order...")
        cancel_data = {
            "order_id": orders[2]["order_id"],
            "reason": "customer_request",
            "cancelled_at": datetime.now(timezone.utc).isoformat(),
            "items": orders[2]["items"]  # For inventory restoration
        }
        await publisher.publish("order.cancelled", cancel_data)

        # Wait for processing
        print("\n⏳ Waiting for event processing...")
        await asyncio.sleep(3)

        # Show metrics
        print("\n📊 === METRICS SUMMARY ===")
        all_metrics = get_all_metrics()

        for service_name, metrics in all_metrics.items():
            print(f"\n🔧 {service_name}:")
            print(f"   Events consumed: {metrics['events_consumed']}")
            print(f"   Events failed: {metrics['events_failed']}")
            print(f"   Consume rate: {metrics['consume_rate']:.2f} events/sec")
            print(f"   Error rate: {metrics['error_rate']:.2%}")

            if metrics['consume_stats']['count'] > 0:
                stats = metrics['consume_stats']
                print(f"   Avg latency: {stats['avg_latency_ms']:.2f}ms")
                print(f"   P95 latency: {stats['p95_latency_ms']:.2f}ms")

        # Show final inventory state
        print(f"\n📦 Final Inventory State:")
        for product_id, quantity in inventory_service.inventory.items():
            print(f"   {product_id}: {quantity} units")

        print(f"\n✅ Processed {len(order_processor.processed_orders)} unique orders")

        # Test idempotency by republishing same events
        print("\n🔄 Testing idempotency - republishing events...")
        for order in orders[:2]:
            await publisher.publish("order.created", order)

        await asyncio.sleep(2)
        print(f"📊 Orders processed after republish: {len(order_processor.processed_orders)} (should be same)")

    finally:
        await publisher.disconnect()
        await order_consumer.stop()
        await inventory_consumer.stop()


async def main():
    """Run the advanced features demo."""
    print("=" * 70)
    print("TC NATS Events - Advanced Features Demo")
    print("=" * 70)
    print("\nThis demo showcases:")
    print("✨ Idempotent event processing")
    print("📊 Comprehensive metrics collection")
    print("🔗 Event correlation and causation")
    print("⚡ Multi-service event choreography")
    print("🛡️  Error handling and recovery")
    print("📋 Structured logging")
    print("=" * 70)

    try:
        await simulate_order_processing()
        print("\n🎉 Demo completed successfully!")

    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

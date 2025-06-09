"""
Custom Events Example
=====================

Demonstrates how any service can create and use completely custom event types.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventType,  # Optional: predefined types
    EventMetadata,
    create_event,  # Convenience function
    setup_logging,
)


# Example 1: Billing Service with Custom Events
class BillingService:
    """Example billing service with completely custom event types."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
    
    async def process_payment(self, payment_data: Dict[str, Any]):
        """Process payment and emit custom events."""
        payment_id = payment_data["payment_id"]
        amount = payment_data["amount"]
        
        print(f"💳 Processing payment: {payment_id} for ${amount}")
        
        # Custom event type - not predefined anywhere
        await self.publisher.publish(
            event_type="billing.payment_initiated",  # Custom event type
            data={
                "payment_id": payment_id,
                "amount": amount,
                "currency": "USD",
                "method": "credit_card",
                "initiated_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Simulate payment processing
        await asyncio.sleep(0.2)
        
        if amount > 0:
            # Success event
            await self.publisher.publish(
                event_type="billing.payment_completed",  # Another custom type
                data={
                    "payment_id": payment_id,
                    "amount": amount,
                    "status": "completed",
                    "transaction_id": f"txn-{payment_id}",
                    "completed_at": datetime.now(timezone.utc).isoformat()
                }
            )
            print(f"✅ Payment completed: {payment_id}")
        else:
            # Failure event
            await self.publisher.publish(
                event_type="billing.payment_failed",
                data={
                    "payment_id": payment_id,
                    "amount": amount,
                    "error_code": "INVALID_AMOUNT",
                    "error_message": "Amount must be greater than 0",
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }
            )
            print(f"❌ Payment failed: {payment_id}")
    
    async def generate_invoice(self, customer_id: str, items: list):
        """Generate invoice and emit custom events."""
        invoice_id = f"inv-{int(datetime.now().timestamp())}"
        total = sum(item["price"] * item["quantity"] for item in items)
        
        print(f"📄 Generating invoice: {invoice_id}")
        
        # Using Event.create() method
        invoice_event = Event.create(
            event_type="billing.invoice_generated",  # Custom type
            data={
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "items": items,
                "total_amount": total,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "due_date": "2024-02-15T00:00:00Z"
            },
            metadata=EventMetadata(
                correlation_id=f"billing-session-{customer_id}",
                user_id=customer_id
            )
        )
        
        # Publish the event object directly
        sequence = await self.publisher._event_store.publish_event(invoice_event)
        print(f"✅ Invoice generated: {invoice_id}, sequence: {sequence}")


# Example 2: Analytics Service with Custom Events
class AnalyticsService:
    """Analytics service tracking user behavior."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
    
    async def track_user_action(self, user_id: str, action: str, context: Dict[str, Any]):
        """Track user actions with custom analytics events."""
        
        # Using create_event() convenience function
        event = create_event(
            event_type=f"analytics.user_{action}",  # Dynamic event type
            data={
                "user_id": user_id,
                "action": action,
                "context": context,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id": context.get("session_id"),
                "ip_address": context.get("ip_address", "unknown")
            },
            metadata=EventMetadata(
                source_service="analytics-service",
                user_id=user_id,
                correlation_id=context.get("session_id")
            )
        )
        
        sequence = await self.publisher._event_store.publish_event(event)
        print(f"📊 Tracked {action} for user {user_id}, sequence: {sequence}")
    
    async def track_performance_metric(self, metric_name: str, value: float, tags: Dict[str, str]):
        """Track performance metrics."""
        
        await self.publisher.publish(
            event_type="analytics.metric_recorded",
            data={
                "metric_name": metric_name,
                "value": value,
                "tags": tags,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "host": "server-001"
            }
        )
        print(f"📈 Metric recorded: {metric_name} = {value}")


# Example 3: Notification Service
class NotificationService:
    """Service for handling notifications."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
    
    async def send_email(self, recipient: str, subject: str, template: str):
        """Send email and emit notification events."""
        
        # Email sending initiated
        await self.publisher.publish(
            event_type="notification.email_queued",
            data={
                "recipient": recipient,
                "subject": subject,
                "template": template,
                "queued_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Simulate email sending
        await asyncio.sleep(0.1)
        
        # Email sent successfully
        await self.publisher.publish(
            event_type="notification.email_sent",
            data={
                "recipient": recipient,
                "subject": subject,
                "template": template,
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "delivery_id": f"email-{int(datetime.now().timestamp())}"
            }
        )
        print(f"📧 Email sent to {recipient}: {subject}")


# Event Handlers for consuming custom events
class EventHandlers:
    """Handlers for processing custom events."""
    
    async def handle_payment_completed(self, event: Event):
        """Handle payment completion."""
        data = event.data
        print(f"🎉 Payment handler: Payment {data['payment_id']} completed for ${data['amount']}")
    
    async def handle_payment_failed(self, event: Event):
        """Handle payment failure."""
        data = event.data
        print(f"⚠️  Payment handler: Payment {data['payment_id']} failed - {data['error_message']}")
    
    async def handle_invoice_generated(self, event: Event):
        """Handle invoice generation."""
        data = event.data
        print(f"📋 Invoice handler: Invoice {data['invoice_id']} for ${data['total_amount']}")
    
    async def handle_user_action(self, event: Event):
        """Handle user analytics events."""
        data = event.data
        action = data['action']
        user_id = data['user_id']
        print(f"👤 Analytics handler: User {user_id} performed {action}")
    
    async def handle_metric(self, event: Event):
        """Handle performance metrics."""
        data = event.data
        print(f"📊 Metrics handler: {data['metric_name']} = {data['value']}")
    
    async def handle_email_sent(self, event: Event):
        """Handle email notifications."""
        data = event.data
        print(f"📬 Email handler: Email sent to {data['recipient']}")
    
    async def handle_any_custom_event(self, event: Event):
        """Generic handler for any custom event."""
        print(f"🔧 Generic handler: Received {event.event_type} with {len(event.data)} fields")


async def demonstrate_custom_events():
    """Demonstrate creating and handling completely custom events."""
    
    print("=" * 60)
    print("Custom Events Demonstration")
    print("=" * 60)
    
    setup_logging(level="INFO", structured=False)
    
    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="custom-events",
        subject_prefix="custom.events"
    )
    
    # Services
    publisher = EventPublisher("multi-service", config)
    await publisher.connect()
    
    billing_service = BillingService(publisher)
    analytics_service = AnalyticsService(publisher)
    notification_service = NotificationService(publisher)
    
    # Consumer with custom event handlers
    consumer = DurableEventConsumer("custom-events-consumer", config)
    handlers = EventHandlers()
    
    # Register handlers for custom event types
    consumer.register_handler("billing.payment_completed", handlers.handle_payment_completed)
    consumer.register_handler("billing.payment_failed", handlers.handle_payment_failed)
    consumer.register_handler("billing.invoice_generated", handlers.handle_invoice_generated)
    
    # Handle analytics events with pattern matching
    consumer.register_handler("analytics.user_login", handlers.handle_user_action)
    consumer.register_handler("analytics.user_logout", handlers.handle_user_action)
    consumer.register_handler("analytics.user_purchase", handlers.handle_user_action)
    consumer.register_handler("analytics.metric_recorded", handlers.handle_metric)
    
    consumer.register_handler("notification.email_sent", handlers.handle_email_sent)
    
    # Default handler for any unregistered custom events
    consumer.register_default_handler(handlers.handle_any_custom_event)
    
    try:
        # Start consumer
        await consumer.start()
        
        # Wait for sync
        while not consumer.is_synced:
            await asyncio.sleep(0.1)
        print("✅ Consumer ready")
        
        # Demonstrate billing events
        print("\n🏦 === BILLING SERVICE DEMO ===")
        await billing_service.process_payment({
            "payment_id": "pay-001",
            "amount": 99.99
        })
        
        await billing_service.process_payment({
            "payment_id": "pay-002", 
            "amount": -10  # This will fail
        })
        
        await billing_service.generate_invoice("customer-123", [
            {"name": "Product A", "price": 50.00, "quantity": 2},
            {"name": "Product B", "price": 25.00, "quantity": 1}
        ])
        
        # Demonstrate analytics events
        print("\n📊 === ANALYTICS SERVICE DEMO ===")
        await analytics_service.track_user_action("user-456", "login", {
            "session_id": "sess-789",
            "ip_address": "192.168.1.100"
        })
        
        await analytics_service.track_user_action("user-456", "purchase", {
            "session_id": "sess-789",
            "product_id": "prod-123",
            "amount": 99.99
        })
        
        await analytics_service.track_performance_metric("api_response_time", 250.5, {
            "endpoint": "/api/users",
            "method": "GET"
        })
        
        # Demonstrate notification events
        print("\n📧 === NOTIFICATION SERVICE DEMO ===")
        await notification_service.send_email(
            "user@example.com",
            "Welcome to our service!",
            "welcome_template"
        )
        
        # Demonstrate mixed predefined and custom events
        print("\n🔀 === MIXED EVENT TYPES DEMO ===")
        
        # Using predefined event type
        await publisher.publish(EventType.USER_CREATED, {
            "user_id": "user-789",
            "email": "newuser@example.com"
        })
        
        # Using completely custom event type
        await publisher.publish("custom_service.data_backup_completed", {
            "backup_id": "backup-001",
            "size_gb": 15.7,
            "duration_minutes": 45,
            "storage_location": "s3://backups/backup-001"
        })
        
        # Wait for processing
        await asyncio.sleep(2)
        
        print(f"\n📈 Total events processed: {consumer.events_processed}")
        
    finally:
        await publisher.disconnect()
        await consumer.stop()


async def main():
    """Run the custom events demonstration."""
    print("\n🎯 This example shows how ANY service can:")
    print("   ✨ Create completely custom event types")
    print("   🔧 Use predefined types for convenience")
    print("   📝 Mix custom and predefined events")
    print("   🎭 Handle events with pattern matching")
    print("   🚀 Extend the system without modifying the package")
    
    await demonstrate_custom_events()
    
    print("\n" + "=" * 60)
    print("🎉 Custom Events Demo Complete!")
    print("\nKey Takeaways:")
    print("• Event types are just strings - use ANY string you want")
    print("• EventType enum provides common patterns, not restrictions")
    print("• Event.create() and create_event() for easy creation")
    print("• Register handlers for your custom event types")
    print("• Use default handlers for catch-all processing")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
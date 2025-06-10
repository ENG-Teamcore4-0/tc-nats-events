"""
Multi-Service Coordination Example
==================================

This example demonstrates how multiple services can coordinate through events,
showing eventual consistency and synchronized processing across services.
"""

import asyncio
import signal
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventMetadata,
    setup_logging,
)


class UserService:
    """Service responsible for user management."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.users: Dict[str, Dict[str, Any]] = {}
    
    async def create_user(self, user_data: Dict[str, Any]) -> str:
        """Create a new user and emit event."""
        user_id = f"user-{len(self.users) + 1:04d}"
        
        # Store user
        self.users[user_id] = {
            **user_data,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "active"
        }
        
        # Emit user created event
        await self.publisher.publish(
            event_type="user.created",
            data=self.users[user_id],
            metadata=EventMetadata(
                correlation_id=f"user-creation-{user_id}",
                source_service="user-service"
            )
        )
        
        print(f"👤 UserService: Created user {user_id}")
        return user_id
    
    async def update_user_preferences(self, user_id: str, preferences: Dict[str, Any]):
        """Update user preferences."""
        if user_id not in self.users:
            raise ValueError(f"User {user_id} not found")
        
        self.users[user_id]["preferences"] = preferences
        
        await self.publisher.publish(
            event_type="user.preferences_updated",
            data={
                "user_id": user_id,
                "preferences": preferences,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        print(f"⚙️  UserService: Updated preferences for {user_id}")


class NotificationService:
    """Service responsible for managing user notification settings."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.notification_settings: Dict[str, Dict[str, bool]] = {}
        self.welcome_emails_sent: List[str] = []
    
    async def handle_user_created(self, event: Event):
        """Initialize notification settings for new users."""
        user_data = event.data
        user_id = user_data["user_id"]
        
        # Set default notification preferences
        self.notification_settings[user_id] = {
            "email": True,
            "sms": False,
            "push": True,
            "newsletter": True
        }
        
        print(f"📧 NotificationService: Initialized settings for {user_id}")
        
        # Send welcome email
        await self.send_welcome_email(user_id, user_data["email"])
        
        # Emit event for notification settings created
        await self.publisher.publish(
            event_type="notification.settings_initialized",
            data={
                "user_id": user_id,
                "settings": self.notification_settings[user_id]
            },
            metadata=EventMetadata(
                correlation_id=event.metadata.correlation_id,
                causation_id=event.metadata.event_id,
                source_service="notification-service"
            )
        )
    
    async def send_welcome_email(self, user_id: str, email: str):
        """Send welcome email to new user."""
        await asyncio.sleep(0.1)  # Simulate email sending
        
        self.welcome_emails_sent.append(user_id)
        
        await self.publisher.publish(
            event_type="notification.email_sent",
            data={
                "user_id": user_id,
                "email": email,
                "template": "welcome",
                "sent_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        print(f"📬 NotificationService: Welcome email sent to {email}")
    
    async def handle_preferences_updated(self, event: Event):
        """Update notification settings based on user preferences."""
        data = event.data
        user_id = data["user_id"]
        preferences = data["preferences"]
        
        if user_id in self.notification_settings and "notifications" in preferences:
            self.notification_settings[user_id].update(preferences["notifications"])
            print(f"🔔 NotificationService: Updated settings for {user_id}")


class BillingService:
    """Service responsible for billing and subscriptions."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.free_trial_days = 30
    
    async def handle_user_created(self, event: Event):
        """Create billing account for new users."""
        user_data = event.data
        user_id = user_data["user_id"]
        
        # Create billing account
        self.accounts[user_id] = {
            "user_id": user_id,
            "plan": "free_trial",
            "status": "active",
            "trial_ends_at": self._calculate_trial_end(),
            "payment_method": None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        print(f"💳 BillingService: Created billing account for {user_id}")
        
        # Emit billing account created event
        await self.publisher.publish(
            event_type="billing.account_created",
            data=self.accounts[user_id],
            metadata=EventMetadata(
                correlation_id=event.metadata.correlation_id,
                causation_id=event.metadata.event_id,
                source_service="billing-service"
            )
        )
    
    async def handle_notification_settings(self, event: Event):
        """Send billing-related notifications based on settings."""
        data = event.data
        user_id = data["user_id"]
        settings = data["settings"]
        
        if settings.get("email", False) and user_id in self.accounts:
            # Simulate sending trial information
            await self.publisher.publish(
                event_type="billing.trial_info_sent",
                data={
                    "user_id": user_id,
                    "trial_ends_at": self.accounts[user_id]["trial_ends_at"],
                    "plan": self.accounts[user_id]["plan"]
                }
            )
            print(f"💰 BillingService: Trial information sent to {user_id}")
    
    def _calculate_trial_end(self) -> str:
        """Calculate trial end date."""
        from datetime import timedelta
        end_date = datetime.now(timezone.utc) + timedelta(days=self.free_trial_days)
        return end_date.isoformat()


class AnalyticsService:
    """Service responsible for tracking user analytics."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.user_events: Dict[str, List[Dict[str, Any]]] = {}
        self.funnel_stats = {
            "users_created": 0,
            "emails_sent": 0,
            "settings_initialized": 0,
            "billing_accounts_created": 0
        }
    
    async def handle_any_event(self, event: Event):
        """Track all events for analytics."""
        # Extract user_id from event data
        user_id = event.data.get("user_id")
        if not user_id:
            return
        
        # Store event
        if user_id not in self.user_events:
            self.user_events[user_id] = []
        
        self.user_events[user_id].append({
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "correlation_id": event.metadata.correlation_id if event.metadata else None
        })
        
        # Update funnel stats
        if event.event_type == "user.created":
            self.funnel_stats["users_created"] += 1
        elif event.event_type == "notification.email_sent":
            self.funnel_stats["emails_sent"] += 1
        elif event.event_type == "notification.settings_initialized":
            self.funnel_stats["settings_initialized"] += 1
        elif event.event_type == "billing.account_created":
            self.funnel_stats["billing_accounts_created"] += 1
        
        # Periodically emit analytics events
        if sum(self.funnel_stats.values()) % 10 == 0:
            await self._emit_analytics_summary()
    
    async def _emit_analytics_summary(self):
        """Emit analytics summary event."""
        await self.publisher.publish(
            event_type="analytics.funnel_summary",
            data={
                "stats": self.funnel_stats.copy(),
                "total_users_tracked": len(self.user_events),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
        print(f"📊 AnalyticsService: Funnel stats - {self.funnel_stats}")
    
    def get_user_journey(self, user_id: str) -> List[Dict[str, Any]]:
        """Get complete event journey for a user."""
        return self.user_events.get(user_id, [])


class AuditService:
    """Service responsible for maintaining audit logs."""
    
    def __init__(self):
        self.audit_log: List[Dict[str, Any]] = []
        self.correlation_chains: Dict[str, List[str]] = {}
    
    async def handle_any_event(self, event: Event):
        """Log all events for audit purposes."""
        audit_entry = {
            "event_id": event.metadata.event_id if event.metadata else None,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "correlation_id": event.metadata.correlation_id if event.metadata else None,
            "causation_id": event.metadata.causation_id if event.metadata else None,
            "source_service": event.metadata.source_service if event.metadata else None,
            "user_id": event.data.get("user_id")
        }
        
        self.audit_log.append(audit_entry)
        
        # Track correlation chains
        if event.metadata and event.metadata.correlation_id:
            if event.metadata.correlation_id not in self.correlation_chains:
                self.correlation_chains[event.metadata.correlation_id] = []
            self.correlation_chains[event.metadata.correlation_id].append(
                event.metadata.event_id
            )
        
        # Log every 5th event
        if len(self.audit_log) % 5 == 0:
            print(f"📝 AuditService: {len(self.audit_log)} events logged, "
                  f"{len(self.correlation_chains)} correlation chains tracked")


async def simulate_user_onboarding(config: NATSConfig):
    """Simulate a complete user onboarding flow across multiple services."""
    print("\n" + "=" * 70)
    print("Multi-Service User Onboarding Simulation")
    print("=" * 70)
    
    # Create publishers for each service
    user_publisher = EventPublisher("user-service", config)
    notification_publisher = EventPublisher("notification-service", config)
    billing_publisher = EventPublisher("billing-service", config)
    analytics_publisher = EventPublisher("analytics-service", config)
    
    await user_publisher.connect()
    await notification_publisher.connect()
    await billing_publisher.connect()
    await analytics_publisher.connect()
    
    # Create services
    user_service = UserService(user_publisher)
    notification_service = NotificationService(notification_publisher)
    billing_service = BillingService(billing_publisher)
    analytics_service = AnalyticsService(analytics_publisher)
    audit_service = AuditService()
    
    # Create consumers for each service
    notification_consumer = DurableEventConsumer("notification-service", config)
    billing_consumer = DurableEventConsumer("billing-service", config)
    analytics_consumer = DurableEventConsumer("analytics-service", config)
    audit_consumer = DurableEventConsumer("audit-service", config)
    
    # Register handlers
    notification_consumer.register_handler("user.created", notification_service.handle_user_created)
    notification_consumer.register_handler("user.preferences_updated", notification_service.handle_preferences_updated)
    
    billing_consumer.register_handler("user.created", billing_service.handle_user_created)
    billing_consumer.register_handler("notification.settings_initialized", billing_service.handle_notification_settings)
    
    # Analytics and audit consume all events
    analytics_consumer.register_default_handler(analytics_service.handle_any_event)
    audit_consumer.register_default_handler(audit_service.handle_any_event)
    
    # Start all consumers
    await notification_consumer.start()
    await billing_consumer.start()
    await analytics_consumer.start()
    await audit_consumer.start()
    
    # Wait for synchronization
    print("\n⏳ Waiting for all services to synchronize...")
    consumers = [notification_consumer, billing_consumer, analytics_consumer, audit_consumer]
    while not all(c.is_synced for c in consumers):
        await asyncio.sleep(0.5)
    print("✅ All services synchronized and ready!")
    
    try:
        # Simulate user onboarding flow
        print("\n🚀 Starting user onboarding flow...")
        
        # Create multiple users
        users_data = [
            {"email": "alice@example.com", "name": "Alice Johnson", "plan": "premium"},
            {"email": "bob@example.com", "name": "Bob Smith", "plan": "basic"},
            {"email": "charlie@example.com", "name": "Charlie Brown", "plan": "premium"}
        ]
        
        created_user_ids = []
        for user_data in users_data:
            print(f"\n--- Onboarding {user_data['name']} ---")
            user_id = await user_service.create_user(user_data)
            created_user_ids.append(user_id)
            
            # Give services time to react
            await asyncio.sleep(1)
            
            # Update preferences for some users
            if user_data["plan"] == "premium":
                await user_service.update_user_preferences(user_id, {
                    "theme": "dark",
                    "language": "en",
                    "notifications": {
                        "email": True,
                        "sms": True,
                        "push": True,
                        "newsletter": True
                    }
                })
                await asyncio.sleep(0.5)
        
        # Wait for all events to propagate
        print("\n⏳ Waiting for all events to propagate...")
        await asyncio.sleep(3)
        
        # Show final state
        print("\n" + "=" * 70)
        print("Final Service States")
        print("=" * 70)
        
        print(f"\n👥 UserService: {len(user_service.users)} users created")
        print(f"📧 NotificationService: {len(notification_service.notification_settings)} settings initialized")
        print(f"💳 BillingService: {len(billing_service.accounts)} billing accounts created")
        print(f"📊 AnalyticsService: Tracked {len(analytics_service.user_events)} users")
        print(f"📝 AuditService: {len(audit_service.audit_log)} total events logged")
        
        # Show user journey for first user
        if created_user_ids:
            journey = analytics_service.get_user_journey(created_user_ids[0])
            print(f"\n🛤️  User Journey for {created_user_ids[0]}:")
            for event in journey:
                print(f"   - {event['timestamp']}: {event['event_type']}")
        
        # Show correlation chain
        if audit_service.correlation_chains:
            first_correlation = list(audit_service.correlation_chains.keys())[0]
            chain = audit_service.correlation_chains[first_correlation]
            print(f"\n🔗 Correlation Chain ({first_correlation}):")
            print(f"   Events in chain: {len(chain)}")
        
    finally:
        # Cleanup
        await user_publisher.disconnect()
        await notification_publisher.disconnect()
        await billing_publisher.disconnect()
        await analytics_publisher.disconnect()
        
        await notification_consumer.stop()
        await billing_consumer.stop()
        await analytics_consumer.stop()
        await audit_consumer.stop()


async def main():
    """Run the multi-service coordination demo."""
    print("=" * 70)
    print("TC NATS Events - Multi-Service Coordination Demo")
    print("=" * 70)
    print("\nThis demo shows how multiple services coordinate through events:")
    print("• UserService: Creates users and manages preferences")
    print("• NotificationService: Handles notifications and sends emails")
    print("• BillingService: Creates billing accounts and manages trials")
    print("• AnalyticsService: Tracks all events and user journeys")
    print("• AuditService: Maintains complete audit log with correlation tracking")
    print("=" * 70)
    
    # Setup logging
    setup_logging(level="INFO", structured=False)
    
    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="coordination-events",
        subject_prefix="coord.events"
    )
    
    try:
        await simulate_user_onboarding(config)
        print("\n🎉 Multi-service coordination demo completed successfully!")
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
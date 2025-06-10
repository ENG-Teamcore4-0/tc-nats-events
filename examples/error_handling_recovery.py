"""
Error Handling and Recovery Example
===================================

This example demonstrates robust error handling, retry mechanisms,
and recovery patterns in TC NATS Events.
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from enum import Enum

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventMetadata,
    setup_logging,
)
from tc_nats_events.utils.exceptions import (
    EventPublishError,
    EventProcessingError,
    ConnectionError as NATSConnectionError,
)


class ErrorType(Enum):
    """Types of errors to simulate."""
    TEMPORARY_NETWORK = "temporary_network"
    DATABASE_TIMEOUT = "database_timeout"
    VALIDATION_ERROR = "validation_error"
    RATE_LIMIT = "rate_limit"
    EXTERNAL_API_ERROR = "external_api_error"
    POISON_MESSAGE = "poison_message"


class PaymentProcessor:
    """Payment processor with various error scenarios."""
    
    def __init__(self, failure_rate: float = 0.3):
        self.failure_rate = failure_rate
        self.processed_payments = {}
        self.failed_payments = {}
        self.retry_counts = {}
        self.dead_letter_queue = []
    
    async def process_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process payment with potential failures."""
        payment_id = payment_data["payment_id"]
        
        # Track retry attempts
        if payment_id not in self.retry_counts:
            self.retry_counts[payment_id] = 0
        self.retry_counts[payment_id] += 1
        
        # Simulate various error conditions
        if random.random() < self.failure_rate:
            error_type = random.choice(list(ErrorType))
            
            if error_type == ErrorType.TEMPORARY_NETWORK:
                print(f"🌐 Temporary network error for payment {payment_id}")
                raise ConnectionError("Network timeout - retry possible")
            
            elif error_type == ErrorType.DATABASE_TIMEOUT:
                print(f"🗄️  Database timeout for payment {payment_id}")
                raise TimeoutError("Database connection timeout")
            
            elif error_type == ErrorType.VALIDATION_ERROR:
                print(f"❌ Validation error for payment {payment_id}")
                raise ValueError(f"Invalid payment amount: {payment_data.get('amount')}")
            
            elif error_type == ErrorType.RATE_LIMIT:
                print(f"🚦 Rate limit exceeded for payment {payment_id}")
                raise Exception("API rate limit exceeded - retry after cooldown")
            
            elif error_type == ErrorType.EXTERNAL_API_ERROR:
                print(f"🔌 External API error for payment {payment_id}")
                raise Exception("Payment gateway unavailable")
            
            elif error_type == ErrorType.POISON_MESSAGE:
                print(f"☠️  Poison message detected: {payment_id}")
                raise Exception("Malformed payment data - cannot process")
        
        # Successful processing
        await asyncio.sleep(0.1)  # Simulate processing time
        
        result = {
            "payment_id": payment_id,
            "status": "completed",
            "transaction_id": f"txn-{payment_id}",
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "retry_count": self.retry_counts[payment_id]
        }
        
        self.processed_payments[payment_id] = result
        print(f"✅ Payment {payment_id} processed successfully (attempt #{self.retry_counts[payment_id]})")
        
        return result


class ResilientPaymentService:
    """Payment service with comprehensive error handling."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.processor = PaymentProcessor(failure_rate=0.4)
        self.circuit_breaker_state = "closed"  # closed, open, half-open
        self.failure_count = 0
        self.failure_threshold = 5
        self.recovery_timeout = 5  # seconds
        self.last_failure_time = None
    
    async def handle_payment_request(self, event: Event):
        """Handle payment request with error recovery."""
        payment_data = event.data
        payment_id = payment_data["payment_id"]
        
        # Check circuit breaker
        if await self._check_circuit_breaker():
            print(f"🚫 Circuit breaker OPEN - rejecting payment {payment_id}")
            await self._send_to_dead_letter_queue(event, "Circuit breaker open")
            return
        
        try:
            # Attempt to process payment
            result = await self._process_with_retry(payment_data, event)
            
            if result:
                # Reset circuit breaker on success
                self._reset_circuit_breaker()
                
                # Publish success event
                await self.publisher.publish(
                    event_type="payment.processed",
                    data=result,
                    metadata=EventMetadata(
                        correlation_id=event.metadata.correlation_id,
                        causation_id=event.metadata.event_id,
                        source_service="payment-service"
                    )
                )
            
        except Exception as e:
            print(f"❌ Failed to process payment {payment_id} after all retries: {e}")
            self._record_failure()
            await self._send_to_dead_letter_queue(event, str(e))
    
    async def _process_with_retry(self, payment_data: Dict[str, Any], event: Event) -> Optional[Dict[str, Any]]:
        """Process payment with retry logic."""
        max_retries = 3
        retry_delays = [1, 3, 5]  # Exponential backoff
        
        for attempt in range(max_retries):
            try:
                result = await self.processor.process_payment(payment_data)
                return result
                
            except (ConnectionError, TimeoutError) as e:
                # Retryable errors
                if attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    print(f"🔄 Retrying payment {payment_data['payment_id']} in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    raise
                    
            except ValueError as e:
                # Non-retryable validation error
                print(f"🚫 Non-retryable error: {e}")
                raise
                
            except Exception as e:
                # Other errors - check if retryable
                if "rate limit" in str(e).lower():
                    # Rate limit - wait longer
                    await asyncio.sleep(10)
                elif "poison" in str(e).lower():
                    # Poison message - don't retry
                    raise
                else:
                    # Generic error - retry with backoff
                    if attempt < max_retries - 1:
                        delay = retry_delays[attempt]
                        await asyncio.sleep(delay)
                    else:
                        raise
        
        return None
    
    async def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is open."""
        if self.circuit_breaker_state == "closed":
            return False
        
        if self.circuit_breaker_state == "open":
            # Check if recovery timeout has passed
            if self.last_failure_time:
                time_since_failure = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
                if time_since_failure > self.recovery_timeout:
                    print("🔧 Circuit breaker entering HALF-OPEN state")
                    self.circuit_breaker_state = "half-open"
                    return False
            return True
        
        return False
    
    def _record_failure(self):
        """Record failure for circuit breaker."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        
        if self.failure_count >= self.failure_threshold:
            print(f"⚡ Circuit breaker OPEN after {self.failure_count} failures")
            self.circuit_breaker_state = "open"
    
    def _reset_circuit_breaker(self):
        """Reset circuit breaker on success."""
        if self.circuit_breaker_state == "half-open":
            print("✅ Circuit breaker CLOSED - recovery successful")
        
        self.circuit_breaker_state = "closed"
        self.failure_count = 0
        self.last_failure_time = None
    
    async def _send_to_dead_letter_queue(self, event: Event, error: str):
        """Send failed event to dead letter queue."""
        dlq_event = {
            "original_event": {
                "event_type": event.event_type,
                "data": event.data,
                "metadata": event.metadata.to_dict() if event.metadata else {}
            },
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "retry_count": self.processor.retry_counts.get(event.data.get("payment_id"), 0)
        }
        
        self.processor.dead_letter_queue.append(dlq_event)
        
        # Publish to DLQ topic
        await self.publisher.publish(
            event_type="payment.failed_permanently",
            data=dlq_event
        )
        
        print(f"💀 Event sent to dead letter queue: {event.data.get('payment_id')}")


class MonitoringService:
    """Service that monitors system health and errors."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.error_counts = {}
        self.success_counts = {}
        self.dlq_items = []
        self.alerts_sent = []
    
    async def handle_payment_processed(self, event: Event):
        """Track successful payments."""
        payment_id = event.data["payment_id"]
        retry_count = event.data.get("retry_count", 1)
        
        if payment_id not in self.success_counts:
            self.success_counts[payment_id] = 0
        self.success_counts[payment_id] += 1
        
        if retry_count > 1:
            print(f"📊 Monitoring: Payment {payment_id} succeeded after {retry_count} attempts")
    
    async def handle_payment_failed(self, event: Event):
        """Track failed payments and send alerts."""
        dlq_data = event.data
        original_event = dlq_data["original_event"]
        error = dlq_data["error"]
        
        self.dlq_items.append(dlq_data)
        
        # Track error types
        if error not in self.error_counts:
            self.error_counts[error] = 0
        self.error_counts[error] += 1
        
        # Send alert if too many failures
        if len(self.dlq_items) % 5 == 0:
            await self._send_alert(f"High failure rate detected: {len(self.dlq_items)} payments failed")
        
        print(f"🚨 Monitoring: Payment permanently failed - {error}")
    
    async def _send_alert(self, message: str):
        """Send monitoring alert."""
        alert = {
            "message": message,
            "severity": "high",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error_summary": dict(self.error_counts),
            "total_failures": len(self.dlq_items),
            "total_successes": sum(self.success_counts.values())
        }
        
        self.alerts_sent.append(alert)
        
        await self.publisher.publish(
            event_type="monitoring.alert",
            data=alert
        )
        
        print(f"🚨 ALERT: {message}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get system health status."""
        total_attempts = sum(self.success_counts.values()) + len(self.dlq_items)
        success_rate = sum(self.success_counts.values()) / total_attempts if total_attempts > 0 else 0
        
        return {
            "success_rate": success_rate,
            "total_successes": sum(self.success_counts.values()),
            "total_failures": len(self.dlq_items),
            "error_breakdown": dict(self.error_counts),
            "alerts_sent": len(self.alerts_sent),
            "health": "healthy" if success_rate > 0.7 else "degraded" if success_rate > 0.5 else "unhealthy"
        }


async def simulate_payment_processing_with_errors(config: NATSConfig):
    """Simulate payment processing with various error scenarios."""
    print("\n" + "=" * 70)
    print("Payment Processing with Error Handling")
    print("=" * 70)
    
    # Create publishers
    payment_publisher = EventPublisher("payment-service", config)
    monitoring_publisher = EventPublisher("monitoring-service", config)
    
    await payment_publisher.connect()
    await monitoring_publisher.connect()
    
    # Create services
    payment_service = ResilientPaymentService(payment_publisher)
    monitoring_service = MonitoringService(monitoring_publisher)
    
    # Create consumers
    payment_consumer = DurableEventConsumer("payment-service", config)
    monitoring_consumer = DurableEventConsumer("monitoring-service", config)
    
    # Register handlers
    payment_consumer.register_handler("payment.requested", payment_service.handle_payment_request)
    
    monitoring_consumer.register_handler("payment.processed", monitoring_service.handle_payment_processed)
    monitoring_consumer.register_handler("payment.failed_permanently", monitoring_service.handle_payment_failed)
    
    # Start consumers
    await payment_consumer.start()
    await monitoring_consumer.start()
    
    # Wait for sync
    while not (payment_consumer.is_synced and monitoring_consumer.is_synced):
        await asyncio.sleep(0.5)
    print("✅ Services ready!")
    
    try:
        # Generate payment requests
        print("\n🚀 Generating payment requests...")
        
        for i in range(20):
            payment_request = {
                "payment_id": f"pay-{i+1:03d}",
                "amount": random.uniform(10, 1000),
                "currency": "USD",
                "customer_id": f"cust-{random.randint(1, 5):03d}",
                "method": random.choice(["credit_card", "debit_card", "paypal"])
            }
            
            # Some payments have invalid data
            if i % 7 == 0:
                payment_request["amount"] = -100  # Invalid amount
            
            await payment_publisher.publish(
                event_type="payment.requested",
                data=payment_request,
                metadata=EventMetadata(
                    correlation_id=f"payment-session-{i+1}",
                    source_service="api-gateway"
                )
            )
            
            # Stagger requests
            await asyncio.sleep(0.2)
        
        # Wait for processing
        print("\n⏳ Processing payments...")
        await asyncio.sleep(15)
        
        # Show results
        print("\n" + "=" * 70)
        print("Processing Results")
        print("=" * 70)
        
        # Payment service stats
        print(f"\n💳 Payment Service:")
        print(f"   Processed: {len(payment_service.processor.processed_payments)}")
        print(f"   Failed: {len(payment_service.processor.dead_letter_queue)}")
        print(f"   Circuit breaker: {payment_service.circuit_breaker_state}")
        
        # Show retry statistics
        retry_stats = {}
        for payment_id, count in payment_service.processor.retry_counts.items():
            if count not in retry_stats:
                retry_stats[count] = 0
            retry_stats[count] += 1
        
        print(f"\n🔄 Retry Statistics:")
        for attempts, count in sorted(retry_stats.items()):
            print(f"   {attempts} attempt(s): {count} payments")
        
        # Monitoring service stats
        health = monitoring_service.get_health_status()
        print(f"\n📊 System Health:")
        print(f"   Status: {health['health']}")
        print(f"   Success rate: {health['success_rate']:.1%}")
        print(f"   Total successes: {health['total_successes']}")
        print(f"   Total failures: {health['total_failures']}")
        
        if health['error_breakdown']:
            print(f"\n❌ Error Breakdown:")
            for error, count in health['error_breakdown'].items():
                print(f"   {error}: {count}")
        
        # Show some DLQ items
        if payment_service.processor.dead_letter_queue:
            print(f"\n💀 Dead Letter Queue (first 3):")
            for item in payment_service.processor.dead_letter_queue[:3]:
                print(f"   Payment {item['original_event']['data']['payment_id']}: {item['error']}")
        
    finally:
        await payment_publisher.disconnect()
        await monitoring_publisher.disconnect()
        await payment_consumer.stop()
        await monitoring_consumer.stop()


async def main():
    """Run the error handling and recovery demo."""
    print("=" * 70)
    print("TC NATS Events - Error Handling and Recovery Demo")
    print("=" * 70)
    print("\nThis demo showcases:")
    print("• Retry mechanisms with exponential backoff")
    print("• Circuit breaker pattern for fault tolerance")
    print("• Dead letter queue for unprocessable messages")
    print("• Different error types and appropriate handling")
    print("• System monitoring and alerting")
    print("• Graceful degradation under failure conditions")
    print("=" * 70)
    
    # Setup logging
    setup_logging(level="INFO", structured=False)
    
    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="error-handling-events",
        subject_prefix="error.events"
    )
    
    try:
        await simulate_payment_processing_with_errors(config)
        print("\n🎉 Error handling demo completed successfully!")
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
"""
Real-time Data Synchronization Example
======================================

This example demonstrates how to keep multiple services' data in sync
in real-time using event sourcing patterns.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    EventMetadata,
    setup_logging,
)


@dataclass
class Product:
    """Product model."""
    product_id: str
    name: str
    price: float
    stock: int
    category: str
    active: bool = True
    last_updated: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class MasterCatalogService:
    """Master catalog service - source of truth for product data."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.products: Dict[str, Product] = {}
        self.version = 0
    
    async def create_product(self, product_data: Dict[str, Any]) -> Product:
        """Create a new product."""
        product_id = f"PROD-{len(self.products) + 1:04d}"
        
        product = Product(
            product_id=product_id,
            name=product_data["name"],
            price=product_data["price"],
            stock=product_data["stock"],
            category=product_data["category"],
            last_updated=datetime.now(timezone.utc).isoformat()
        )
        
        self.products[product_id] = product
        self.version += 1
        
        # Emit product created event
        await self.publisher.publish(
            event_type="catalog.product_created",
            data={
                **product.to_dict(),
                "catalog_version": self.version
            }
        )
        
        print(f"📦 MasterCatalog: Created product {product_id} - {product.name}")
        return product
    
    async def update_product(self, product_id: str, updates: Dict[str, Any]):
        """Update an existing product."""
        if product_id not in self.products:
            raise ValueError(f"Product {product_id} not found")
        
        product = self.products[product_id]
        old_values = {}
        
        # Track what changed
        for field, new_value in updates.items():
            if hasattr(product, field):
                old_value = getattr(product, field)
                if old_value != new_value:
                    old_values[field] = old_value
                    setattr(product, field, new_value)
        
        if old_values:
            product.last_updated = datetime.now(timezone.utc).isoformat()
            self.version += 1
            
            # Emit product updated event
            await self.publisher.publish(
                event_type="catalog.product_updated",
                data={
                    "product_id": product_id,
                    "updates": updates,
                    "old_values": old_values,
                    "current_state": product.to_dict(),
                    "catalog_version": self.version
                }
            )
            
            print(f"🔄 MasterCatalog: Updated {product_id} - {list(updates.keys())}")
    
    async def update_stock(self, product_id: str, quantity_change: int):
        """Update product stock level."""
        if product_id not in self.products:
            raise ValueError(f"Product {product_id} not found")
        
        product = self.products[product_id]
        old_stock = product.stock
        new_stock = max(0, product.stock + quantity_change)
        
        product.stock = new_stock
        product.last_updated = datetime.now(timezone.utc).isoformat()
        self.version += 1
        
        # Emit stock updated event
        await self.publisher.publish(
            event_type="catalog.stock_updated",
            data={
                "product_id": product_id,
                "old_stock": old_stock,
                "new_stock": new_stock,
                "quantity_change": quantity_change,
                "catalog_version": self.version
            }
        )
        
        print(f"📊 MasterCatalog: Stock updated for {product_id}: {old_stock} → {new_stock}")
        
        # Check for low stock
        if new_stock < 10 and old_stock >= 10:
            await self.publisher.publish(
                event_type="catalog.low_stock_alert",
                data={
                    "product_id": product_id,
                    "product_name": product.name,
                    "current_stock": new_stock
                }
            )
    
    async def deactivate_product(self, product_id: str):
        """Deactivate a product."""
        if product_id not in self.products:
            raise ValueError(f"Product {product_id} not found")
        
        product = self.products[product_id]
        product.active = False
        product.last_updated = datetime.now(timezone.utc).isoformat()
        self.version += 1
        
        await self.publisher.publish(
            event_type="catalog.product_deactivated",
            data={
                "product_id": product_id,
                "catalog_version": self.version
            }
        )
        
        print(f"🚫 MasterCatalog: Deactivated product {product_id}")
    
    async def emit_snapshot(self):
        """Emit a complete catalog snapshot."""
        snapshot_data = {
            "version": self.version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "products": {
                pid: p.to_dict() for pid, p in self.products.items()
            },
            "total_products": len(self.products),
            "active_products": sum(1 for p in self.products.values() if p.active)
        }
        
        await self.publisher.publish(
            event_type="catalog.snapshot",
            data=snapshot_data
        )
        
        print(f"📸 MasterCatalog: Emitted snapshot v{self.version} with {len(self.products)} products")


class SearchIndexService:
    """Search index service that maintains synchronized product search data."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.search_index: Dict[str, Dict[str, Any]] = {}
        self.index_version = 0
        self.last_sync_version = 0
    
    async def handle_product_created(self, event: Event):
        """Index new products."""
        data = event.data
        product_id = data["product_id"]
        
        # Create search index entry
        self.search_index[product_id] = {
            "product_id": product_id,
            "name": data["name"].lower(),
            "category": data["category"].lower(),
            "price": data["price"],
            "active": data.get("active", True),
            "keywords": self._extract_keywords(data["name"])
        }
        
        self.index_version += 1
        self.last_sync_version = data.get("catalog_version", 0)
        
        print(f"🔍 SearchIndex: Indexed new product {product_id}")
        
        # Emit index updated event
        await self.publisher.publish(
            event_type="search.index_updated",
            data={
                "product_id": product_id,
                "action": "created",
                "index_version": self.index_version
            }
        )
    
    async def handle_product_updated(self, event: Event):
        """Update search index for product changes."""
        data = event.data
        product_id = data["product_id"]
        
        if product_id in self.search_index:
            # Update relevant fields
            current_state = data["current_state"]
            self.search_index[product_id].update({
                "name": current_state["name"].lower(),
                "category": current_state["category"].lower(),
                "price": current_state["price"],
                "keywords": self._extract_keywords(current_state["name"])
            })
            
            self.index_version += 1
            self.last_sync_version = data.get("catalog_version", 0)
            
            print(f"🔍 SearchIndex: Updated index for {product_id}")
    
    async def handle_product_deactivated(self, event: Event):
        """Remove deactivated products from search."""
        product_id = event.data["product_id"]
        
        if product_id in self.search_index:
            self.search_index[product_id]["active"] = False
            self.index_version += 1
            self.last_sync_version = event.data.get("catalog_version", 0)
            
            print(f"🔍 SearchIndex: Deactivated {product_id} in search")
    
    async def handle_catalog_snapshot(self, event: Event):
        """Rebuild index from catalog snapshot."""
        snapshot = event.data
        catalog_version = snapshot["version"]
        
        # Check if we need to rebuild
        if catalog_version > self.last_sync_version:
            print(f"🔍 SearchIndex: Rebuilding from snapshot v{catalog_version}")
            
            # Clear and rebuild index
            self.search_index.clear()
            
            for product_id, product_data in snapshot["products"].items():
                if product_data.get("active", True):
                    self.search_index[product_id] = {
                        "product_id": product_id,
                        "name": product_data["name"].lower(),
                        "category": product_data["category"].lower(),
                        "price": product_data["price"],
                        "active": True,
                        "keywords": self._extract_keywords(product_data["name"])
                    }
            
            self.last_sync_version = catalog_version
            self.index_version += 1
            
            print(f"🔍 SearchIndex: Rebuilt with {len(self.search_index)} products")
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract search keywords from text."""
        # Simple keyword extraction
        words = text.lower().split()
        return [w for w in words if len(w) > 2]
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """Search products."""
        query_lower = query.lower()
        results = []
        
        for product_id, index_data in self.search_index.items():
            if not index_data.get("active", True):
                continue
                
            # Search in name and keywords
            if (query_lower in index_data["name"] or
                any(query_lower in kw for kw in index_data["keywords"])):
                results.append({
                    "product_id": product_id,
                    "name": index_data["name"],
                    "price": index_data["price"],
                    "category": index_data["category"]
                })
        
        return results


class PricingService:
    """Pricing service that maintains price history and calculations."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.current_prices: Dict[str, float] = {}
        self.price_history: Dict[str, List[Dict[str, Any]]] = {}
        self.discounts: Dict[str, float] = {}
    
    async def handle_product_created(self, event: Event):
        """Track initial product price."""
        data = event.data
        product_id = data["product_id"]
        price = data["price"]
        
        self.current_prices[product_id] = price
        self.price_history[product_id] = [{
            "price": price,
            "timestamp": data["last_updated"],
            "event": "created"
        }]
        
        print(f"💰 PricingService: Tracking price for {product_id}: ${price}")
    
    async def handle_product_updated(self, event: Event):
        """Track price changes."""
        data = event.data
        product_id = data["product_id"]
        
        if "price" in data["updates"]:
            old_price = data["old_values"]["price"]
            new_price = data["updates"]["price"]
            
            self.current_prices[product_id] = new_price
            
            if product_id not in self.price_history:
                self.price_history[product_id] = []
            
            self.price_history[product_id].append({
                "price": new_price,
                "old_price": old_price,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": "updated",
                "change_percent": ((new_price - old_price) / old_price) * 100
            })
            
            print(f"💰 PricingService: Price changed for {product_id}: ${old_price} → ${new_price}")
            
            # Emit price change event
            await self.publisher.publish(
                event_type="pricing.price_changed",
                data={
                    "product_id": product_id,
                    "old_price": old_price,
                    "new_price": new_price,
                    "change_percent": ((new_price - old_price) / old_price) * 100
                }
            )
    
    async def apply_discount(self, product_id: str, discount_percent: float):
        """Apply discount to a product."""
        if product_id not in self.current_prices:
            return
        
        self.discounts[product_id] = discount_percent
        original_price = self.current_prices[product_id]
        discounted_price = original_price * (1 - discount_percent / 100)
        
        await self.publisher.publish(
            event_type="pricing.discount_applied",
            data={
                "product_id": product_id,
                "original_price": original_price,
                "discount_percent": discount_percent,
                "discounted_price": discounted_price
            }
        )
        
        print(f"🏷️  PricingService: Applied {discount_percent}% discount to {product_id}")
    
    def get_price_history(self, product_id: str) -> List[Dict[str, Any]]:
        """Get price history for a product."""
        return self.price_history.get(product_id, [])


class InventoryService:
    """Inventory service that tracks stock levels and movements."""
    
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.stock_levels: Dict[str, int] = {}
        self.stock_movements: Dict[str, List[Dict[str, Any]]] = {}
        self.low_stock_threshold = 10
    
    async def handle_product_created(self, event: Event):
        """Initialize stock tracking."""
        data = event.data
        product_id = data["product_id"]
        initial_stock = data["stock"]
        
        self.stock_levels[product_id] = initial_stock
        self.stock_movements[product_id] = [{
            "type": "initial",
            "quantity": initial_stock,
            "timestamp": data["last_updated"],
            "balance": initial_stock
        }]
        
        print(f"📦 InventoryService: Tracking stock for {product_id}: {initial_stock} units")
    
    async def handle_stock_updated(self, event: Event):
        """Track stock movements."""
        data = event.data
        product_id = data["product_id"]
        
        self.stock_levels[product_id] = data["new_stock"]
        
        if product_id not in self.stock_movements:
            self.stock_movements[product_id] = []
        
        movement_type = "addition" if data["quantity_change"] > 0 else "deduction"
        
        self.stock_movements[product_id].append({
            "type": movement_type,
            "quantity": abs(data["quantity_change"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "balance": data["new_stock"],
            "old_balance": data["old_stock"]
        })
        
        print(f"📦 InventoryService: Stock {movement_type} for {product_id}: {data['quantity_change']}")
    
    async def handle_low_stock_alert(self, event: Event):
        """Handle low stock alerts."""
        data = event.data
        
        # Trigger reorder
        await self.publisher.publish(
            event_type="inventory.reorder_required",
            data={
                "product_id": data["product_id"],
                "product_name": data["product_name"],
                "current_stock": data["current_stock"],
                "suggested_quantity": 50  # Simple reorder logic
            }
        )
        
        print(f"⚠️  InventoryService: Reorder triggered for {data['product_id']}")


class ReportingService:
    """Reporting service that generates real-time analytics."""
    
    def __init__(self):
        self.event_count = 0
        self.event_types: Dict[str, int] = {}
        self.sync_status: Dict[str, Dict[str, Any]] = {}
    
    async def handle_any_event(self, event: Event):
        """Track all events for reporting."""
        self.event_count += 1
        
        event_type = event.event_type
        if event_type not in self.event_types:
            self.event_types[event_type] = 0
        self.event_types[event_type] += 1
        
        # Track service sync status
        if event.metadata and event.metadata.source_service:
            service = event.metadata.source_service
            if service not in self.sync_status:
                self.sync_status[service] = {
                    "last_event": None,
                    "event_count": 0
                }
            
            self.sync_status[service]["last_event"] = event.timestamp
            self.sync_status[service]["event_count"] += 1
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate sync status report."""
        return {
            "total_events": self.event_count,
            "event_types": dict(self.event_types),
            "service_status": dict(self.sync_status),
            "report_time": datetime.now(timezone.utc).isoformat()
        }


async def simulate_realtime_sync(config: NATSConfig):
    """Simulate real-time data synchronization."""
    print("\n" + "=" * 70)
    print("Real-time Data Synchronization Simulation")
    print("=" * 70)
    
    # Create publishers
    catalog_publisher = EventPublisher("catalog-service", config)
    search_publisher = EventPublisher("search-service", config)
    pricing_publisher = EventPublisher("pricing-service", config)
    inventory_publisher = EventPublisher("inventory-service", config)
    
    await catalog_publisher.connect()
    await search_publisher.connect()
    await pricing_publisher.connect()
    await inventory_publisher.connect()
    
    # Create services
    catalog = MasterCatalogService(catalog_publisher)
    search = SearchIndexService(search_publisher)
    pricing = PricingService(pricing_publisher)
    inventory = InventoryService(inventory_publisher)
    reporting = ReportingService()
    
    # Create consumers
    search_consumer = DurableEventConsumer("search-service", config)
    pricing_consumer = DurableEventConsumer("pricing-service", config)
    inventory_consumer = DurableEventConsumer("inventory-service", config)
    reporting_consumer = DurableEventConsumer("reporting-service", config)
    
    # Register handlers
    search_consumer.register_handler("catalog.product_created", search.handle_product_created)
    search_consumer.register_handler("catalog.product_updated", search.handle_product_updated)
    search_consumer.register_handler("catalog.product_deactivated", search.handle_product_deactivated)
    search_consumer.register_handler("catalog.snapshot", search.handle_catalog_snapshot)
    
    pricing_consumer.register_handler("catalog.product_created", pricing.handle_product_created)
    pricing_consumer.register_handler("catalog.product_updated", pricing.handle_product_updated)
    
    inventory_consumer.register_handler("catalog.product_created", inventory.handle_product_created)
    inventory_consumer.register_handler("catalog.stock_updated", inventory.handle_stock_updated)
    inventory_consumer.register_handler("catalog.low_stock_alert", inventory.handle_low_stock_alert)
    
    reporting_consumer.register_default_handler(reporting.handle_any_event)
    
    # Start consumers
    await search_consumer.start()
    await pricing_consumer.start()
    await inventory_consumer.start()
    await reporting_consumer.start()
    
    # Wait for sync
    while not all([
        search_consumer.is_synced,
        pricing_consumer.is_synced,
        inventory_consumer.is_synced,
        reporting_consumer.is_synced
    ]):
        await asyncio.sleep(0.5)
    print("✅ All services synchronized!")
    
    try:
        # Create initial products
        print("\n📦 Creating initial product catalog...")
        
        products_data = [
            {"name": "Laptop Pro X1", "price": 1299.99, "stock": 25, "category": "Electronics"},
            {"name": "Wireless Mouse", "price": 49.99, "stock": 100, "category": "Electronics"},
            {"name": "Office Chair Deluxe", "price": 399.99, "stock": 15, "category": "Furniture"},
            {"name": "Standing Desk", "price": 599.99, "stock": 8, "category": "Furniture"},
            {"name": "USB-C Hub", "price": 79.99, "stock": 50, "category": "Electronics"}
        ]
        
        created_products = []
        for data in products_data:
            product = await catalog.create_product(data)
            created_products.append(product)
            await asyncio.sleep(0.5)
        
        # Wait for sync
        await asyncio.sleep(2)
        
        # Test search functionality
        print("\n🔍 Testing search synchronization...")
        search_results = search.search("laptop")
        print(f"   Search for 'laptop': {len(search_results)} results")
        
        # Update some products
        print("\n🔄 Updating products...")
        
        # Price change
        await catalog.update_product(
            created_products[0].product_id,
            {"price": 1199.99}  # Price reduction
        )
        
        # Stock update
        await catalog.update_stock(
            created_products[3].product_id,
            -5  # Sold 5 units
        )
        
        # Multiple updates
        await catalog.update_product(
            created_products[1].product_id,
            {"name": "Wireless Gaming Mouse", "price": 59.99}
        )
        
        await asyncio.sleep(2)
        
        # Apply discounts
        print("\n🏷️  Applying discounts...")
        await pricing.apply_discount(created_products[0].product_id, 10)  # 10% off
        await pricing.apply_discount(created_products[2].product_id, 15)  # 15% off
        
        # Trigger low stock
        print("\n📉 Simulating low stock...")
        await catalog.update_stock(created_products[3].product_id, -3)  # Now at 0
        
        await asyncio.sleep(2)
        
        # Emit catalog snapshot
        print("\n📸 Emitting catalog snapshot...")
        await catalog.emit_snapshot()
        
        await asyncio.sleep(2)
        
        # Show synchronization results
        print("\n" + "=" * 70)
        print("Synchronization Results")
        print("=" * 70)
        
        # Catalog state
        print(f"\n📦 Master Catalog:")
        print(f"   Products: {len(catalog.products)}")
        print(f"   Version: {catalog.version}")
        
        # Search index state
        print(f"\n🔍 Search Index:")
        print(f"   Indexed products: {len(search.search_index)}")
        print(f"   Index version: {search.index_version}")
        print(f"   Sync version: {search.last_sync_version}")
        
        # Pricing state
        print(f"\n💰 Pricing Service:")
        print(f"   Tracked prices: {len(pricing.current_prices)}")
        print(f"   Active discounts: {len(pricing.discounts)}")
        
        # Show price history for first product
        if created_products:
            history = pricing.get_price_history(created_products[0].product_id)
            print(f"   Price history for {created_products[0].product_id}: {len(history)} entries")
        
        # Inventory state
        print(f"\n📦 Inventory Service:")
        print(f"   Tracked products: {len(inventory.stock_levels)}")
        low_stock = [pid for pid, level in inventory.stock_levels.items() if level < 10]
        print(f"   Low stock items: {len(low_stock)}")
        
        # Reporting
        report = reporting.generate_report()
        print(f"\n📊 Event Statistics:")
        print(f"   Total events: {report['total_events']}")
        print(f"   Event types: {len(report['event_types'])}")
        print(f"   Active services: {len(report['service_status'])}")
        
        # Test final search
        print(f"\n🔍 Final search test:")
        gaming_results = search.search("gaming")
        print(f"   Search for 'gaming': {len(gaming_results)} results")
        
    finally:
        await catalog_publisher.disconnect()
        await search_publisher.disconnect()
        await pricing_publisher.disconnect()
        await inventory_publisher.disconnect()
        
        await search_consumer.stop()
        await pricing_consumer.stop()
        await inventory_consumer.stop()
        await reporting_consumer.stop()


async def main():
    """Run the real-time synchronization demo."""
    print("=" * 70)
    print("TC NATS Events - Real-time Data Synchronization Demo")
    print("=" * 70)
    print("\nThis demo showcases:")
    print("• Real-time data synchronization across services")
    print("• Event-driven state management")
    print("• Search index synchronization")
    print("• Price tracking and history")
    print("• Inventory management with alerts")
    print("• Snapshot-based recovery")
    print("• Multi-service data consistency")
    print("=" * 70)
    
    # Setup logging
    setup_logging(level="INFO", structured=False)
    
    # Configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="realtime-sync-events",
        subject_prefix="sync.events"
    )
    
    try:
        await simulate_realtime_sync(config)
        print("\n🎉 Real-time synchronization demo completed successfully!")
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
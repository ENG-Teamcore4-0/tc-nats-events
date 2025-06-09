"""
Scraper Integration Example
===========================

This example shows how to integrate TC NATS Events with a scraping system,
demonstrating the pattern described in the architecture documents.
"""

import asyncio
import random
from datetime import datetime, timezone
from typing import List, Dict, Any

from tc_nats_events import (
    EventPublisher,
    DurableEventConsumer,
    NATSConfig,
    Event,
    setup_logging,
)


# Simulated scraper that publishes events
class ScraperSimulator:
    """Simulates a web scraper that publishes events."""
    
    def __init__(self, scraper_id: str, publisher: EventPublisher):
        self.scraper_id = scraper_id
        self.publisher = publisher
        self.items_scraped = 0
    
    async def scrape_products(self, store_id: str, category: str):
        """Simulate scraping products from a store."""
        print(f"\n🕷️  Scraper {self.scraper_id} starting for {store_id}/{category}")
        
        # Publish scraper started event
        start_time = datetime.now(timezone.utc)
        await self.publisher.publish_scraper_started(
            scraper_id=self.scraper_id,
            target=f"{store_id}/{category}",
            metadata={
                "store_id": store_id,
                "category": category,
                "scraper_version": "2.1.0"
            }
        )
        
        # Simulate scraping products
        num_products = random.randint(50, 200)
        products = []
        
        for i in range(num_products):
            product = {
                "product_id": f"SKU-{store_id}-{i:04d}",
                "name": f"Product {i}",
                "price": round(random.uniform(10.0, 500.0), 2),
                "currency": "USD",
                "in_stock": random.choice([True, False]),
                "store_id": store_id,
                "category": category,
                "scraped_at": datetime.now(timezone.utc).isoformat()
            }
            products.append(product)
            
            # Simulate scraping delay
            if i % 10 == 0:
                await asyncio.sleep(0.1)
        
        # Publish scraped data in batches
        batch_size = 50
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            await self.publisher.publish_scraper_data_extracted(
                scraper_id=self.scraper_id,
                data_type="products",
                items=batch
            )
            print(f"   📦 Published batch of {len(batch)} products")
        
        # Calculate duration
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        # Publish scraper completed event
        await self.publisher.publish_scraper_completed(
            scraper_id=self.scraper_id,
            items_extracted=len(products),
            duration_seconds=duration,
            metadata={
                "store_id": store_id,
                "category": category
            }
        )
        
        self.items_scraped += len(products)
        print(f"✅ Scraper {self.scraper_id} completed: {len(products)} items in {duration:.2f}s")


# Data processing service that consumes scraper events
class DataProcessor:
    """Processes scraped data from events."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.products_processed = 0
        self.active_scrapers = {}
    
    async def handle_scraper_started(self, event: Event):
        """Handle scraper started events."""
        data = event.data
        scraper_id = data["scraper_id"]
        
        self.active_scrapers[scraper_id] = {
            "started_at": data["started_at"],
            "target": data["target"],
            "items_received": 0
        }
        
        print(f"\n🔧 [{self.service_name}] Scraper started: {scraper_id} -> {data['target']}")
    
    async def handle_scraper_data(self, event: Event):
        """Process scraped data."""
        data = event.data
        scraper_id = data["scraper_id"]
        items = data["items"]
        
        # Update scraper stats
        if scraper_id in self.active_scrapers:
            self.active_scrapers[scraper_id]["items_received"] += len(items)
        
        # Simulate data processing
        for item in items:
            # In a real system, this would:
            # - Validate data
            # - Transform to internal format
            # - Store in database
            # - Update search indices
            # - Trigger downstream events
            self.products_processed += 1
        
        print(f"   🔄 [{self.service_name}] Processed {len(items)} items from {scraper_id}")
    
    async def handle_scraper_completed(self, event: Event):
        """Handle scraper completion."""
        data = event.data
        scraper_id = data["scraper_id"]
        
        if scraper_id in self.active_scrapers:
            scraper_info = self.active_scrapers.pop(scraper_id)
            print(f"\n✅ [{self.service_name}] Scraper completed: {scraper_id}")
            print(f"   - Items extracted: {data['items_extracted']}")
            print(f"   - Items received: {scraper_info['items_received']}")
            print(f"   - Duration: {data['duration_seconds']:.2f}s")


# Catalog service that maintains product catalog
class CatalogService:
    """Maintains product catalog from scraper events."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.catalog = {}  # product_id -> product_data
        self.publisher = None
    
    def set_publisher(self, publisher: EventPublisher):
        """Set event publisher for catalog events."""
        self.publisher = publisher
    
    async def handle_scraper_data(self, event: Event):
        """Update catalog with scraped products."""
        data = event.data
        items = data["items"]
        
        for item in items:
            product_id = item["product_id"]
            
            # Check if product exists
            if product_id in self.catalog:
                # Update existing product
                old_price = self.catalog[product_id].get("price")
                new_price = item["price"]
                
                self.catalog[product_id] = item
                
                # Publish price change event if price changed
                if old_price != new_price and self.publisher:
                    await self.publisher.publish(
                        event_type="catalog.price_changed",
                        data={
                            "product_id": product_id,
                            "old_price": old_price,
                            "new_price": new_price,
                            "store_id": item["store_id"]
                        }
                    )
            else:
                # Add new product
                self.catalog[product_id] = item
                
                # Publish new product event
                if self.publisher:
                    await self.publisher.publish(
                        event_type="catalog.item_added",
                        data={
                            "product_id": product_id,
                            "product_data": item
                        }
                    )
        
        print(f"   📚 [{self.service_name}] Catalog updated: {len(self.catalog)} total products")


async def run_scraper_demo(config: NATSConfig):
    """Run the scraper simulation."""
    print("\n" + "=" * 60)
    print("Starting Scraper Simulation")
    print("=" * 60)
    
    async with EventPublisher("scraper-service", config) as publisher:
        # Create multiple scrapers
        scrapers = [
            ScraperSimulator(f"scraper-{i}", publisher)
            for i in range(1, 4)
        ]
        
        # Simulate scraping different stores/categories
        tasks = [
            scrapers[0].scrape_products("walmart", "electronics"),
            scrapers[1].scrape_products("target", "home"),
            scrapers[2].scrape_products("amazon", "books"),
        ]
        
        await asyncio.gather(*tasks)
        
        print(f"\n📊 Total items scraped: {sum(s.items_scraped for s in scrapers)}")


async def run_processor_demo(config: NATSConfig):
    """Run the data processor service."""
    print("\n" + "=" * 60)
    print("Starting Data Processor Service")
    print("=" * 60)
    
    # Create processor
    processor = DataProcessor("data-processor")
    
    # Create consumer
    consumer = DurableEventConsumer("data-processor", config)
    
    # Register handlers
    consumer.register_handler("scraper.started", processor.handle_scraper_started)
    consumer.register_handler("scraper.data_extracted", processor.handle_scraper_data)
    consumer.register_handler("scraper.completed", processor.handle_scraper_completed)
    
    # Start consumer
    await consumer.start()
    
    # Wait for sync
    while not consumer.is_synced:
        await asyncio.sleep(0.5)
    
    print("✅ Data processor synchronized and ready")
    
    # Run for a while to process events
    await asyncio.sleep(10)
    
    await consumer.stop()
    print(f"\n📊 Total products processed: {processor.products_processed}")


async def run_catalog_demo(config: NATSConfig):
    """Run the catalog service."""
    print("\n" + "=" * 60)
    print("Starting Catalog Service")
    print("=" * 60)
    
    # Create catalog service
    catalog = CatalogService("catalog-service")
    
    # Create publisher for catalog events
    async with EventPublisher("catalog-service", config) as publisher:
        catalog.set_publisher(publisher)
        
        # Create consumer
        consumer = DurableEventConsumer("catalog-service", config)
        
        # Register handler for scraper data
        consumer.register_handler("scraper.data_extracted", catalog.handle_scraper_data)
        
        # Start consumer
        await consumer.start()
        
        # Wait for sync
        while not consumer.is_synced:
            await asyncio.sleep(0.5)
        
        print("✅ Catalog service synchronized and ready")
        
        # Run for a while to process events
        await asyncio.sleep(10)
        
        await consumer.stop()
        print(f"\n📊 Total products in catalog: {len(catalog.catalog)}")


async def main():
    """Run the complete scraper integration demo."""
    # Setup logging
    setup_logging(level="INFO", structured=False)
    
    # Create configuration
    config = NATSConfig(
        servers=["nats://localhost:4222"],
        stream_name="scraper-events",
        subject_prefix="scraper.events"
    )
    
    print("=" * 60)
    print("TC NATS Events - Scraper Integration Demo")
    print("=" * 60)
    print("\nThis demo simulates a scraping system with:")
    print("- Multiple scrapers publishing events")
    print("- Data processor consuming and processing scraped data")
    print("- Catalog service maintaining product catalog")
    print("=" * 60)
    
    # Run services in parallel
    await asyncio.gather(
        run_scraper_demo(config),
        run_processor_demo(config),
        run_catalog_demo(config)
    )


if __name__ == "__main__":
    asyncio.run(main())
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of TC NATS Events package
- Event Sourcing implementation with immutable events
- Durable Consumer pattern with automatic synchronization
- Event Publisher with delivery confirmation and retry logic
- Complete flexibility for custom event types
- Idempotent event processing
- Comprehensive metrics collection with P95/P99 latencies
- Structured logging with correlation tracking
- Configuration from environment variables, files, or programmatic
- Horizontal scaling support with load balancing
- At-least-once delivery guarantees
- Automatic recovery and connection resilience
- Type safety with full type hints
- Comprehensive test suite (unit and integration)
- Documentation and examples

### Features

#### Core Components
- `Event` - Immutable event model with metadata
- `EventMetadata` - Rich metadata for correlation and tracking
- `NATSEventStore` - High-level abstraction over NATS JetStream
- `DurableEventConsumer` - Consumer with automatic sync and recovery
- `EventPublisher` - Publisher with retry and delivery confirmation

#### Event Flexibility
- Support for any string as event type (no restrictions)
- `EventType` enum for common patterns (optional)
- `Event.create()` and `create_event()` factory methods
- Hierarchical event naming conventions
- Dynamic event type generation

#### Advanced Features
- `IdempotentEventProcessor` for duplicate handling
- `MetricsCollector` for performance monitoring
- Structured logging with JSON formatter
- Context tracking with correlation/causation IDs
- Stream configuration with persistence
- Consumer states (IDLE → SYNCING → LIVE)

#### Developer Experience
- Multiple configuration sources (env, files, programmatic)
- Comprehensive examples and documentation
- Type hints for IDE support
- Async/await throughout
- Context manager support
- Detailed error messages and logging

### Configuration
- `NATSConfig` class with validation
- Environment variable support with `NATS_` prefix
- JSON/YAML file configuration
- Sensible defaults for development
- Production-ready settings

### Examples
- Basic publisher/consumer usage
- Custom events demonstration
- Advanced features showcase
- Scraper integration pattern
- Multi-service coordination
- Error handling patterns

### Testing
- Unit tests with comprehensive mocking
- Integration tests with real NATS server
- Custom event flexibility tests
- Metrics and idempotency tests
- CI/CD pipeline with GitHub Actions

### Documentation
- Comprehensive README with examples
- Custom events guide
- Contributing guidelines
- Security policy
- Compliance review document

## [0.1.0] - 2024-01-15

### Added
- Initial release (see Unreleased section above)

---

## Template for Future Releases

### [X.Y.Z] - YYYY-MM-DD

#### Added
- New features

#### Changed
- Changes in existing functionality

#### Deprecated
- Soon-to-be removed features

#### Removed
- Removed features

#### Fixed
- Bug fixes

#### Security
- Security-related changes
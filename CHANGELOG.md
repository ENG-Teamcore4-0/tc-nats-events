# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-30

Reliability release fixing the 2026-09-30 audit (NATS-01..NATS-11, plus NATS-12
found during the fix review). Evidence: `docs/audit/0.2.0-evidence.md`.
Upgrade steps: `docs/MIGRATION-0.2.md`.

### ⚠️ Breaking changes
- New durables start with `deliver_policy="new"` (was ALL). Existing durables keep their position. (NATS-04)
- `max_deliver_attempts` default is 6 (was 3); failures are retried with `nak_delays_seconds` (1s, 5s, 30s, 2m, 10m). (NATS-02)
- Idempotency defaults to a shared NATS KV bucket `<stream>-idem`; the service needs `$KV` permissions (or `idempotency_backend="memory"` for local). (NATS-02)
- A dead letter stream `<stream>-DLQ` (subjects `dlq.<prefix>.>`) is created by consumers. (NATS-03)
- An existing stream is never rewritten: non-covering subjects now fail with `StreamConfigError`. Opt in to retention/replica updates with `allow_stream_update=True`. (NATS-09)
- `register_handler("*", h)` now registers a catch-all default handler (deprecated alias of `register_default_handler`). (NATS-11)
- `NATSConfig.validate()` runs on connect/start and rejects `environment="production"` with `replicas < 3`. (NATS-05)
- `DurableEventConsumer(batch_size=...)` default is 1 (was 10).
- Requires Python >= 3.11 (was 3.8; 3.8 and 3.9 are end-of-life), nats-server >= 2.10 and nats-py >= 2.10.
- The legacy `utils.idempotency.IdempotentEventProcessor` no longer caches failures.

### Fixed
- NATS-01: publishes carry `Nats-Msg-Id = event_id`, so retries after a lost PubAck are deduplicated; duplicate acks return the original sequence. The publisher refuses to connect if its retry horizon exceeds the stream `duplicate_window`.
- NATS-02: failures are never cached; retries use `nak(delay)`; the idempotency store is shared across replicas and survives restarts (KV leases with compare-and-set).
- NATS-03: poison messages, `NonRetryableError` and exhausted retries go to the DLQ with `X-Dlq-*` headers and are `term()`-ed; `MAX_DELIVERIES` advisories are a safety net.
- NATS-06: editable consumer settings (`ack_wait`, `max_deliver`, `backoff`, `max_ack_pending`) are reconciled on start; immutable differences fail fast.
- NATS-07: the processing loop is supervised and restarted; `consumer.health()` / `is_healthy` for readiness probes.
- NATS-08: `in_progress()` heartbeats for the running message and its batch; `handler_timeout_seconds` bounds hung handlers.
- NATS-10: no `stream_info` round-trip per publish.
- NATS-12: the idempotency key uses the message identity (`Nats-Msg-Id` / `event-id` / stream sequence), so tc-iam and generic payloads without `event_id` are deduplicated.
- Publish timeout is configurable (`publish_timeout_seconds`, default 5s; was a hardcoded 30s).

### Added
- `NonRetryableError`, `NATSConfig` fields: `environment`, `deliver_policy`, `opt_start_time`, `nak_delays_seconds`, `consumer_backoff_seconds`, `handler_timeout_seconds`, `idempotency_*`, `dlq_*`, `duplicate_window_seconds`, `allow_stream_update`, `health_stale_seconds`.
- `tc_nats_events.idempotency` package (`NatsKVIdempotencyStore`, `MemoryIdempotencyStore`, `IdempotentProcessor`).
- Reliability counters in metrics (`reliability_counts`: `publish_duplicate`, `dead_lettered_*`, `duplicate_skipped`, `lease_lost`, `mark_done_failed`).
- Audit regression suite (`tests/audit`) and 3-node R3 cluster test (`tests/cluster`, CI job `cluster`); CI enforces 80% coverage.


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
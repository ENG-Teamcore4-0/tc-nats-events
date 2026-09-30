"""
Unit Tests for the 0.2.0 reliability modules
============================================

Pure logic only (no NATS): redelivery policy, consumer/stream reconciliation,
config validation, idempotency store/processor and loop supervision.
"""

import asyncio
import time
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StreamConfig

from tc_nats_events import NATSConfig
from tc_nats_events.consumers.consumer_config import (
    build_consumer_config,
    reconcile,
)
from tc_nats_events.consumers.redelivery import (
    is_last_attempt,
    message_id,
    nak_delay,
    num_delivered,
)
from tc_nats_events.consumers.supervisor import (
    MAX_RESTART_DELAY_SECONDS,
    HealthSnapshot,
    error_delay,
    restart_delay,
)
from tc_nats_events.core.stream_config import (
    build_dlq_stream_config,
    build_stream_config,
    subject_covers,
    validate_existing_stream,
)
from tc_nats_events.idempotency import (
    AcquireStatus,
    IdempotentProcessor,
    LeaseLostError,
    MemoryIdempotencyStore,
    Outcome,
    RetryLaterError,
    idempotency_key,
)
from tc_nats_events.utils.exceptions import (
    ConsumerConfigMismatchError,
    StreamConfigError,
)


@pytest.fixture
def config() -> NATSConfig:
    return NATSConfig(stream_name="unit-events", subject_prefix="unit.events")


def make_msg(
    headers=None, sequence=7, delivered=1, subject="unit.events.created"
) -> MagicMock:
    msg = MagicMock()
    msg.subject = subject
    msg.headers = headers
    msg.metadata.sequence.stream = sequence
    msg.metadata.num_delivered = delivered
    return msg


# --------------------------------------------------------------- redelivery
class TestRedelivery:
    def test_nak_delay_follows_schedule(self, config):
        assert [nak_delay(config, n) for n in range(1, 6)] == [
            1.0,
            5.0,
            30.0,
            120.0,
            600.0,
        ]

    def test_nak_delay_clamps_to_last_value(self, config):
        assert nak_delay(config, 6) == 600.0
        assert nak_delay(config, 1000) == 600.0

    def test_nak_delay_clamps_low_deliveries_to_first(self, config):
        assert nak_delay(config, 0) == 1.0
        assert nak_delay(config, -5) == 1.0

    def test_nak_delay_custom_schedule(self):
        cfg = NATSConfig(nak_delays_seconds=(0.5,))
        assert nak_delay(cfg, 1) == 0.5
        assert nak_delay(cfg, 9) == 0.5

    def test_is_last_attempt(self, config):
        assert config.max_deliver_attempts == 6
        assert not is_last_attempt(config, 5)
        assert is_last_attempt(config, 6)
        assert is_last_attempt(config, 7)

    def test_is_last_attempt_unlimited_never_last(self):
        cfg = NATSConfig(max_deliver_attempts=-1)
        assert not is_last_attempt(cfg, 1)
        assert not is_last_attempt(cfg, 10_000)

    def test_num_delivered(self):
        assert num_delivered(make_msg(delivered=4)) == 4

    def test_num_delivered_defaults_to_one_on_bad_metadata(self):
        msg = MagicMock()
        msg.metadata.num_delivered = "not-a-number"
        assert num_delivered(msg) == 1

    def test_message_id_prefers_nats_msg_id(self):
        msg = make_msg({"Nats-Msg-Id": "producer-1", "event-id": "evt-1"})
        assert message_id(msg, "s") == "unit.events.created|producer-1"

    def test_message_id_uses_event_id_header(self):
        msg = make_msg({"event-id": "evt-1"})
        assert message_id(msg, "s") == "unit.events.created|evt-1"

    def test_message_id_skips_empty_headers(self):
        msg = make_msg({"Nats-Msg-Id": "", "event-id": "evt-2"})
        assert message_id(msg, "s") == "unit.events.created|evt-2"

    @pytest.mark.parametrize("headers", [None, {}, {"other": "x"}])
    def test_message_id_falls_back_to_stream_sequence(self, headers):
        msg = make_msg(headers, sequence=42)
        assert message_id(msg, "my-stream") == "unit.events.created|my-stream:42"

    def test_message_id_is_scoped_by_subject(self):
        """A reused producer id on another event type must not collide."""
        a = make_msg({"Nats-Msg-Id": "same"}, subject="unit.events.a")
        b = make_msg({"Nats-Msg-Id": "same"}, subject="unit.events.b")
        assert message_id(a, "s") != message_id(b, "s")

    def test_message_id_malformed_header_falls_back_to_sequence(self):
        too_long = make_msg({"Nats-Msg-Id": "x" * 1000}, sequence=3)
        unprintable = make_msg({"Nats-Msg-Id": "bad\x00id"}, sequence=3)
        assert message_id(too_long, "s") == "unit.events.created|s:3"
        assert message_id(unprintable, "s") == "unit.events.created|s:3"

    def test_message_id_stable_across_redeliveries(self):
        first = make_msg({"event-id": "e"}, sequence=5, delivered=1)
        again = make_msg({"event-id": "e"}, sequence=5, delivered=4)
        assert message_id(first, "s") == message_id(again, "s")


# ---------------------------------------------------------- consumer config
class TestConsumerConfig:
    NAME = "svc-consumer"
    FILTER = "unit.events.>"

    def desired(self, config) -> ConsumerConfig:
        return build_consumer_config(config, self.NAME, self.FILTER)

    def test_build_uses_new_defaults(self, config):
        built = self.desired(config)
        assert built.deliver_policy == DeliverPolicy.NEW
        assert built.ack_policy == AckPolicy.EXPLICIT
        assert built.durable_name == built.name == self.NAME
        assert built.filter_subject == self.FILTER
        assert built.max_deliver == 6
        assert built.ack_wait == 30.0
        assert built.backoff is None

    def test_build_maps_deliver_policy_and_backoff(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        cfg = NATSConfig(
            deliver_policy="by_start_time",
            opt_start_time=start,
            consumer_backoff_seconds=(30, 60),
        )
        built = build_consumer_config(cfg, self.NAME, self.FILTER)
        assert built.deliver_policy == DeliverPolicy.BY_START_TIME
        assert built.opt_start_time == start
        assert built.backoff == [30.0, 60.0]

    def test_reconcile_no_diff_returns_none(self, config):
        update, diff = reconcile(self.desired(config), self.desired(config))
        assert update is None
        assert diff == {}

    def test_reconcile_ignores_float_noise_and_empty_backoff(self, config):
        existing = replace(self.desired(config), ack_wait=30.0004, backoff=[])
        update, _ = reconcile(existing, self.desired(config))
        assert update is None

    def test_reconcile_editable_diff_keeps_existing_deliver_policy(self, config):
        existing = replace(
            self.desired(config),
            deliver_policy=DeliverPolicy.ALL,
            max_deliver=3,
            ack_wait=10.0,
        )
        update, diff = reconcile(existing, self.desired(config))

        assert set(diff) == {"max_deliver", "ack_wait"}
        assert diff["max_deliver"] == (3, 6)
        assert update is not None
        assert update.max_deliver == 6
        assert update.ack_wait == 30.0
        # deliver_policy is immutable on the server: it must be sent back as-is
        assert update.deliver_policy == DeliverPolicy.ALL

    def test_reconcile_backoff_diff(self, config):
        desired = build_consumer_config(
            NATSConfig(consumer_backoff_seconds=(30, 60), max_deliver_attempts=6),
            self.NAME,
            self.FILTER,
        )
        update, diff = reconcile(self.desired(config), desired)
        assert "backoff" in diff
        assert update.backoff == [30.0, 60.0]

    def test_reconcile_deliver_policy_is_never_compared(self):
        all_cfg = build_consumer_config(
            NATSConfig(deliver_policy="all"), self.NAME, self.FILTER
        )
        new_cfg = build_consumer_config(NATSConfig(), self.NAME, self.FILTER)
        update, diff = reconcile(all_cfg, new_cfg)
        assert update is None and diff == {}

    def test_reconcile_filter_mismatch_raises(self, config):
        existing = replace(self.desired(config), filter_subject="other.>")
        with pytest.raises(ConsumerConfigMismatchError, match="filter_subject"):
            reconcile(existing, self.desired(config))

    def test_reconcile_filter_subjects_list_equivalent_to_single(self, config):
        existing = replace(
            self.desired(config), filter_subject=None, filter_subjects=[self.FILTER]
        )
        update, _ = reconcile(existing, self.desired(config))
        assert update is None

    def test_reconcile_ack_policy_mismatch_raises(self, config):
        existing = replace(self.desired(config), ack_policy=AckPolicy.NONE)
        with pytest.raises(ConsumerConfigMismatchError, match="ack_policy"):
            reconcile(existing, self.desired(config))


# ------------------------------------------------------------ stream config
class TestStreamConfig:
    @pytest.mark.parametrize(
        "pattern,subject,expected",
        [
            ("a.b.>", "a.b.>", True),
            ("a.b.c", "a.b.c", True),
            ("a.b.c", "a.b.d", False),
            ("a.>", "a.b.>", True),  # superset
            (">", "a.b.>", True),
            (">", "a", True),
            ("a.b.>", "a.>", False),  # subset pattern does not cover
            ("a.b.>", "a.b", False),  # '>' needs at least one more token
            ("a.*", "a.b", True),
            ("a.*", "a.b.c", False),
            ("a.*.c", "a.b.c", True),
            ("a.*.c", "a.b.d", False),
            ("a.*", "a.>", False),  # '*' cannot cover a multi-token tail
            ("a.b", "a.b.c", False),
            ("a.b.c", "a.b", False),
            ("*.>", "a.b", True),
        ],
    )
    def test_subject_covers(self, pattern, subject, expected):
        assert subject_covers(pattern, subject) is expected

    def test_build_stream_config(self, config):
        built = build_stream_config(config)
        assert built.name == "unit-events"
        assert built.subjects == ["unit.events.>"]
        assert built.duplicate_window == 120.0
        assert built.num_replicas == 1

    def test_dlq_subjects_do_not_overlap_event_subjects(self, config):
        dlq = build_dlq_stream_config(config)
        assert dlq.name == "unit-events-DLQ"
        assert dlq.subjects == ["dlq.unit.events.>"]
        assert not subject_covers("unit.events.>", dlq.subjects[0])

    def test_dlq_duplicate_window_default(self, config):
        assert build_dlq_stream_config(config).duplicate_window == 900.0

    def test_dlq_duplicate_window_capped_by_max_age(self):
        cfg = NATSConfig(max_age_seconds=300, duplicate_window_seconds=120.0)
        assert build_dlq_stream_config(cfg).duplicate_window == 300

    def existing(self, config, **overrides) -> StreamConfig:
        params = {
            "name": config.stream_name,
            "subjects": ["unit.events.>"],
            "duplicate_window": config.duplicate_window_seconds,
            "max_age": float(config.max_age_seconds),
            "num_replicas": config.replicas,
        }
        params.update(overrides)
        return StreamConfig(**params)

    def test_validate_existing_no_drift(self, config):
        assert validate_existing_stream(self.existing(config), config) == []

    def test_validate_existing_superset_subjects_ok(self, config):
        existing = self.existing(config, subjects=["unit.>", "x.>"])
        assert validate_existing_stream(existing, config) == []

    def test_validate_existing_non_covering_raises(self, config):
        existing = self.existing(config, subjects=["other.>"])
        with pytest.raises(StreamConfigError, match="do not cover"):
            validate_existing_stream(existing, config)

    def test_validate_existing_no_subjects_raises(self, config):
        with pytest.raises(StreamConfigError):
            validate_existing_stream(self.existing(config, subjects=None), config)

    def test_validate_existing_returns_drift_warnings(self, config):
        existing = self.existing(config, num_replicas=3, duplicate_window=30.0)
        warnings = validate_existing_stream(existing, config)
        assert len(warnings) == 2
        assert any(w.startswith("num_replicas") for w in warnings)
        assert any(w.startswith("duplicate_window") for w in warnings)


# -------------------------------------------------------- config validation
class TestConfigValidation:
    def test_defaults_are_valid(self):
        NATSConfig().validate()

    @pytest.mark.parametrize("env", ["production", "prod", " PRODUCTION "])
    def test_production_requires_three_replicas(self, env):
        with pytest.raises(ValueError, match="replicas must be >= 3"):
            NATSConfig(environment=env, replicas=1).validate()

    def test_production_with_three_replicas_ok(self):
        NATSConfig(environment="production", replicas=3).validate()

    def test_non_production_single_replica_ok(self):
        NATSConfig(environment="staging", replicas=1).validate()

    def test_by_start_time_requires_opt_start_time(self):
        with pytest.raises(ValueError, match="opt_start_time"):
            NATSConfig(deliver_policy="by_start_time").validate()

    def test_by_start_time_with_start_time_ok(self):
        NATSConfig(
            deliver_policy="by_start_time",
            opt_start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ).validate()

    def test_unknown_deliver_policy(self):
        with pytest.raises(ValueError, match="deliver_policy"):
            NATSConfig(deliver_policy="bogus").validate()

    def test_backoff_first_value_must_not_be_below_ack_wait(self):
        with pytest.raises(ValueError, match=r"consumer_backoff_seconds\[0\]"):
            NATSConfig(ack_wait_seconds=30, consumer_backoff_seconds=(5, 10)).validate()

    def test_max_deliver_must_exceed_backoff_length(self):
        with pytest.raises(ValueError, match="greater than len"):
            NATSConfig(
                max_deliver_attempts=3, consumer_backoff_seconds=(30, 60, 120)
            ).validate()

    def test_valid_backoff_ok(self):
        NATSConfig(
            max_deliver_attempts=6, consumer_backoff_seconds=(30, 60, 120)
        ).validate()

    def test_unlimited_deliveries_skip_backoff_length_rule(self):
        NATSConfig(
            max_deliver_attempts=-1, consumer_backoff_seconds=(30, 60, 120)
        ).validate()

    @pytest.mark.parametrize("value", [0, -2])
    def test_invalid_max_deliver(self, value):
        with pytest.raises(ValueError, match="max_deliver_attempts"):
            NATSConfig(max_deliver_attempts=value).validate()

    def test_lease_must_cover_three_heartbeats(self):
        # heartbeat = ack_wait / 3 = 10s -> lease must be >= 30s
        with pytest.raises(ValueError, match="idempotency_lease_seconds"):
            NATSConfig(idempotency_lease_seconds=20.0).validate()

    def test_lease_at_three_heartbeats_ok(self):
        NATSConfig(idempotency_lease_seconds=30.0).validate()

    def test_ttl_must_cover_redelivery_horizon(self):
        with pytest.raises(ValueError, match="redelivery horizon"):
            NATSConfig(idempotency_ttl_seconds=60).validate()

    def test_unlimited_deliveries_skip_ttl_rule(self):
        NATSConfig(max_deliver_attempts=-1, idempotency_ttl_seconds=60).validate()

    def test_unknown_idempotency_backend(self):
        with pytest.raises(ValueError, match="idempotency_backend"):
            NATSConfig(idempotency_backend="redis").validate()

    def test_duplicate_window_cannot_exceed_max_age(self):
        with pytest.raises(ValueError, match="duplicate_window_seconds"):
            NATSConfig(max_age_seconds=60, duplicate_window_seconds=120.0).validate()

    @pytest.mark.parametrize(
        "kwargs,match",
        [
            ({"servers": []}, "server"),
            ({"max_messages": 0}, "max_messages"),
            ({"replicas": 0}, "replicas"),
            ({"stream_name": ""}, "stream_name"),
            ({"nak_delays_seconds": ()}, "nak_delays_seconds"),
            ({"nak_delays_seconds": (1.0, -1.0)}, "nak_delays_seconds"),
            ({"handler_timeout_seconds": 0}, "handler_timeout_seconds"),
            ({"publish_timeout_seconds": 0}, "publish_timeout_seconds"),
            ({"duplicate_window_seconds": 0}, "duplicate_window_seconds"),
        ],
    )
    def test_other_rules(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            NATSConfig(**kwargs).validate()


# ------------------------------------------------------------ memory store
class TestMemoryIdempotencyStore:
    async def test_acquire_then_in_progress_then_done(self):
        store = MemoryIdempotencyStore()
        first = await store.try_acquire("k", 30)
        assert first.status == AcquireStatus.ACQUIRED
        assert first.lease is not None

        assert (await store.try_acquire("k", 30)).status == AcquireStatus.IN_PROGRESS

        await store.mark_done(first.lease)
        assert (await store.try_acquire("k", 30)).status == AcquireStatus.DONE

    async def test_release_allows_reacquire(self):
        store = MemoryIdempotencyStore()
        lease = (await store.try_acquire("k", 30)).lease
        await store.release(lease)
        assert (await store.try_acquire("k", 30)).status == AcquireStatus.ACQUIRED

    async def test_release_with_stale_lease_is_ignored(self):
        clock = [1000.0]
        store = MemoryIdempotencyStore(clock=lambda: clock[0])
        stale = (await store.try_acquire("k", 10)).lease
        clock[0] += 11  # lease expired; another worker takes over
        fresh = (await store.try_acquire("k", 10)).lease

        await store.release(stale)

        assert (await store.try_acquire("k", 10)).status == AcquireStatus.IN_PROGRESS
        await store.mark_done(fresh)

    async def test_expired_lease_can_be_taken_over(self):
        clock = [0.0]
        store = MemoryIdempotencyStore(clock=lambda: clock[0])
        await store.try_acquire("k", 10)
        clock[0] = 11
        assert (await store.try_acquire("k", 10)).status == AcquireStatus.ACQUIRED

    async def test_mark_done_after_takeover_raises_lease_lost(self):
        clock = [0.0]
        store = MemoryIdempotencyStore(clock=lambda: clock[0])
        stale = (await store.try_acquire("k", 10)).lease
        clock[0] = 11
        await store.try_acquire("k", 10)
        with pytest.raises(LeaseLostError):
            await store.mark_done(stale)

    async def test_renew_extends_lease_and_bumps_revision(self):
        clock = [0.0]
        store = MemoryIdempotencyStore(clock=lambda: clock[0])
        lease = (await store.try_acquire("k", 10)).lease
        clock[0] = 8
        renewed = await store.renew(lease, 10)
        assert renewed.revision > lease.revision
        clock[0] = 15  # past the original lease, inside the renewed one
        assert (await store.try_acquire("k", 10)).status == AcquireStatus.IN_PROGRESS

    async def test_renew_with_stale_lease_raises(self):
        store = MemoryIdempotencyStore()
        lease = (await store.try_acquire("k", 10)).lease
        await store.renew(lease, 10)
        with pytest.raises(LeaseLostError):
            await store.renew(lease, 10)  # old revision

    async def test_ttl_expires_done_records(self):
        clock = [0.0]
        store = MemoryIdempotencyStore(ttl_seconds=100, clock=lambda: clock[0])
        lease = (await store.try_acquire("k", 10)).lease
        await store.mark_done(lease)
        clock[0] = 101
        assert (await store.try_acquire("k", 10)).status == AcquireStatus.ACQUIRED

    async def test_setup_is_noop(self):
        assert await MemoryIdempotencyStore().setup() is None

    def test_idempotency_key_is_safe_and_collision_free(self):
        assert idempotency_key("s", "a/b") != idempotency_key("s", "a_b")
        assert idempotency_key("s1", "x") != idempotency_key("s2", "x")
        key = idempotency_key("svc", "id with spaces/and.dots")
        assert key == idempotency_key("svc", "id with spaces/and.dots")
        assert key.isalnum()


# ---------------------------------------------------------------- processor
class FlakyStore(MemoryIdempotencyStore):
    """Store whose mark_done can be made to fail in a chosen way."""

    def __init__(self, mark_done_error=None, fail_times=None):
        super().__init__()
        self.mark_done_error = mark_done_error
        self.fail_times = fail_times
        self.mark_done_calls = 0
        self.released = 0

    async def mark_done(self, lease):
        self.mark_done_calls += 1
        if self.mark_done_error and (
            self.fail_times is None or self.mark_done_calls <= self.fail_times
        ):
            raise self.mark_done_error
        await super().mark_done(lease)

    async def release(self, lease):
        self.released += 1
        await super().release(lease)


def make_processor(store=None, lease=5.0, interval=0.05, timeout=5.0, on_event=None):
    store = MemoryIdempotencyStore() if store is None else store
    return store, IdempotentProcessor(
        store,
        lease_seconds=lease,
        heartbeat_interval=interval,
        handler_timeout=timeout,
        on_event=on_event,
    )


class TestIdempotentProcessor:
    async def test_runs_handler_and_marks_done(self):
        store, processor = make_processor()
        calls = []

        async def handler():
            calls.append(1)

        assert await processor.run("k", handler) == Outcome.PROCESSED
        assert calls == [1]
        assert (await store.try_acquire("k", 5)).status == AcquireStatus.DONE

    async def test_done_key_is_skipped(self):
        events = []
        _, processor = make_processor(on_event=events.append)
        calls = []

        async def handler():
            calls.append(1)

        await processor.run("k", handler)
        assert await processor.run("k", handler) == Outcome.DUPLICATE
        assert calls == [1]
        assert events == ["duplicate_skipped"]

    async def test_failure_releases_so_retry_reruns_handler(self):
        store = FlakyStore()
        _, processor = make_processor(store)
        attempts = []

        async def handler():
            attempts.append(1)
            if len(attempts) == 1:
                raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await processor.run("k", handler)
        assert store.released == 1

        assert await processor.run("k", handler) == Outcome.PROCESSED
        assert len(attempts) == 2

    async def test_mark_done_lease_lost_still_returns_processed(self):
        events = []
        store = FlakyStore(mark_done_error=LeaseLostError("taken"))
        _, processor = make_processor(store, on_event=events.append)

        async def handler():
            return None

        assert await processor.run("k", handler) == Outcome.PROCESSED
        assert store.mark_done_calls == 1  # no retry on lease loss
        assert events == ["lease_lost"]

    async def test_mark_done_transient_errors_are_retried(self, monkeypatch):
        monkeypatch.setattr(
            "tc_nats_events.idempotency.processor.MARK_DONE_RETRY_DELAY", 0.0
        )
        store = FlakyStore(mark_done_error=RuntimeError("kv down"), fail_times=2)
        _, processor = make_processor(store)

        async def handler():
            return None

        assert await processor.run("k", handler) == Outcome.PROCESSED
        assert store.mark_done_calls == 3
        assert (await store.try_acquire("k", 5)).status == AcquireStatus.DONE

    async def test_mark_done_persistent_failure_still_processed(self, monkeypatch):
        monkeypatch.setattr(
            "tc_nats_events.idempotency.processor.MARK_DONE_RETRY_DELAY", 0.0
        )
        events = []
        store = FlakyStore(mark_done_error=RuntimeError("kv down"))
        _, processor = make_processor(store, on_event=events.append)

        async def handler():
            return None

        assert await processor.run("k", handler) == Outcome.PROCESSED
        assert store.mark_done_calls == 3
        assert events == ["mark_done_failed"]

    async def test_handler_timeout_raises_and_releases(self):
        store = FlakyStore()
        _, processor = make_processor(store, timeout=0.05)

        async def slow():
            await asyncio.sleep(5)

        with pytest.raises(asyncio.TimeoutError):
            await processor.run("k", slow)

        assert store.released == 1
        assert (await store.try_acquire("k", 5)).status == AcquireStatus.ACQUIRED

    async def test_cancellation_releases_lease(self):
        store = FlakyStore()
        _, processor = make_processor(store)
        started = asyncio.Event()

        async def handler():
            started.set()
            await asyncio.sleep(5)

        task = asyncio.create_task(processor.run("k", handler))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert store.released == 1

    async def test_heartbeat_touches_and_renews_while_handler_runs(self):
        store, processor = make_processor(interval=0.02)
        touches = []

        async def touch():
            touches.append(1)

        async def handler():
            await asyncio.sleep(0.15)

        await processor.run("k", handler, touch=touch)
        assert len(touches) >= 3

    async def test_in_progress_waits_then_sees_done(self):
        store, processor = make_processor(interval=0.02)
        owner = (await store.try_acquire("k", 5)).lease
        touches = []
        calls = []

        async def touch():
            touches.append(1)

        async def handler():
            calls.append(1)

        async def finish_elsewhere():
            await asyncio.sleep(0.1)
            await store.mark_done(owner)

        finisher = asyncio.create_task(finish_elsewhere())
        outcome = await processor.run("k", handler, touch=touch)
        await finisher

        assert outcome == Outcome.DUPLICATE
        assert calls == []
        assert touches  # our copy was kept alive while waiting

    async def test_in_progress_owner_released_then_we_run(self):
        store, processor = make_processor(interval=0.02)
        owner = (await store.try_acquire("k", 5)).lease
        calls = []

        async def handler():
            calls.append(1)

        async def fail_elsewhere():
            await asyncio.sleep(0.1)
            await store.release(owner)

        releaser = asyncio.create_task(fail_elsewhere())
        assert await processor.run("k", handler) == Outcome.PROCESSED
        await releaser
        assert calls == [1]

    async def test_in_progress_too_long_raises_retry_later(self):
        store, processor = make_processor(lease=0.15, interval=0.02)
        await store.try_acquire("k", 30)  # someone else holds it for long

        async def handler():
            raise AssertionError("must not run")

        with pytest.raises(RetryLaterError):
            await processor.run("k", handler)


# --------------------------------------------------------------- supervisor
class TestSupervisor:
    def test_restart_delay_is_capped_exponential(self):
        assert [restart_delay(n) for n in range(0, 5)] == [1.0, 1.0, 2.0, 4.0, 8.0]
        assert restart_delay(6) == 32.0
        assert restart_delay(8) == MAX_RESTART_DELAY_SECONDS
        assert restart_delay(50) == MAX_RESTART_DELAY_SECONDS

    def test_error_delay_grows_then_caps(self):
        assert error_delay(1) == 1.0
        assert error_delay(5) == 5.0
        assert error_delay(99) == 10.0

    def test_snapshot_is_immutable_and_transitions_return_copies(self):
        snap = HealthSnapshot()
        restarted = snap.restarted()
        failed = restarted.failed(ValueError("x"))
        ok = failed.fetch_ok()

        assert snap.restarts == 0
        assert restarted.restarts == 1
        assert failed.last_error == "ValueError: x"
        assert ok.last_progress_at is not None
        assert snap.last_progress_at is None

    def test_fetch_ok_resets_restart_streak_but_keeps_total(self):
        snap = HealthSnapshot().restarted().restarted()
        assert (snap.restarts, snap.restart_streak) == (2, 2)
        ok = snap.fetch_ok()
        assert (ok.restarts, ok.restart_streak) == (2, 0)
        assert restart_delay(ok.restarted().restart_streak) == 1.0

    def test_as_dict_healthy_when_fresh_and_running(self):
        data = HealthSnapshot().fetch_ok().as_dict("live", True, stale_after=60)
        assert data["healthy"] is True
        assert data["state"] == "live"
        assert data["restarts"] == 0

    def test_as_dict_syncing_counts_as_healthy(self):
        assert HealthSnapshot().fetch_ok().as_dict("syncing", True, 60)["healthy"]

    def test_as_dict_stale_fetch_is_unhealthy(self):
        snap = HealthSnapshot(last_progress_monotonic=time.monotonic() - 120)
        assert snap.as_dict("live", True, stale_after=60)["healthy"] is False

    def test_as_dict_never_fetched_is_unhealthy(self):
        assert HealthSnapshot().as_dict("live", True, 60)["healthy"] is False

    @pytest.mark.parametrize("state", ["error", "stopped", "starting", "idle"])
    def test_as_dict_bad_state_is_unhealthy(self, state):
        assert HealthSnapshot().fetch_ok().as_dict(state, True, 60)["healthy"] is False

    def test_as_dict_not_running_is_unhealthy(self):
        assert (
            HealthSnapshot().fetch_ok().as_dict("live", False, 60)["healthy"] is False
        )

    def test_as_dict_reports_last_error(self):
        snap = HealthSnapshot().fetch_ok().failed(RuntimeError("nope"))
        data = snap.as_dict("live", True, 60)
        assert data["last_error"] == "RuntimeError: nope"

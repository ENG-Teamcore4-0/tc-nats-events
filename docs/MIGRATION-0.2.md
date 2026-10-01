# Migrating to tc-nats-events 0.2.0

## 1. Before deploying

1. **nats-server >= 2.10** on every environment (`nats server info`).
2. **Permissions** for each consumer account (see README → *Required NATS permissions*):
   - KV bucket `<stream>-idem` (`$KV.<stream>-idem.>` and the `KV_<stream>-idem` stream API),
   - publish on `dlq.<subject_prefix>.>` and the `<stream>-DLQ` stream API,
   - subscribe on `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`.
   Pre-create the KV bucket and DLQ stream with IaC if runtime accounts must not create streams.
3. **Production replicas**: set `NATS_ENVIRONMENT=production` and `NATS_REPLICAS=3`
   (validation fails otherwise). To raise an existing stream from R1 to R3 run
   `nats stream edit <stream> --replicas 3` or start once with `NATS_ALLOW_STREAM_UPDATE=true`.
4. **Python >= 3.11** in the service image.
5. **Pin the dependency**: `tc-nats-events @ git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@v0.2.0`.

## 2. Deploy all replicas at once

0.1.x pods rewrite stream subjects on connect and deduplicate only in memory.
Mixing versions keeps those bugs alive. Scale 0.1.x to zero (or use a recreate
strategy) and then start 0.2.0.

## 3. Behaviour changes to review

| Change | Action |
|---|---|
| New durables start at `deliver_policy="new"` | Existing durables are untouched. If a *new* service must process history, set `NATS_DELIVER_POLICY=all` or `by_start_time` + `NATS_OPT_START_TIME`. A warning is logged when a new durable skips existing messages. |
| Retries with delay, then DLQ | Remove retry loops and "swallow and ack" code from handlers: raise instead. Raise `NonRetryableError` for permanent failures. Monitor `<stream>-DLQ`. |
| Existing durables are reconciled | On first start the durable gets `max_deliver=6` (and any `ack_wait`/`max_ack_pending` change). The log shows the diff. |
| `register_handler("*")` is a catch-all | It now receives every unmatched event type. Remove it if that is not wanted. |
| Readiness | Wire `consumer.health()["healthy"]` into the readiness probe. |
| Stream subjects are never rewritten | If startup fails with `StreamConfigError`, the stream is shared or misconfigured: fix it explicitly. |

## 4. Local development

```bash
export NATS_IDEMPOTENCY_BACKEND=memory   # optional; KV works on a local nats-server -js too
export NATS_DLQ_ENABLED=false            # optional
```

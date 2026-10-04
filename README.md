# Emergency Broadcast System

A delivery-guaranteed, event-driven emergency notification platform built on **Apache Kafka**. It pushes an alert to a large group of users across four channels — push, SMS, email, voice — with automatic cross-channel fallback on failure, exponential-backoff retries, a dead-letter queue for undeliverable messages, and a fully queryable audit trail proving what was actually delivered.

> Most notification systems are "fire and forget." This one isn't — every delivery attempt resolves to a recorded, auditable outcome: delivered, retried, or routed through a fallback chain until it either succeeds or is explicitly flagged for manual review.

---

## Overview

**The problem:** when an emergency happens — an outage, a security incident, a regional alert — affected users need to be notified reliably, at scale, within minutes. A "best effort" notification system isn't good enough: silent failures mean people never find out.

**What this system guarantees:**
- Every delivery attempt is classified as **transient** (retry) or **permanent** (stop retrying, escalate) — enforced by the compiler via a checked-exception hierarchy, not left to developer discipline.
- A permanently failing channel automatically triggers the **next** channel in a fallback chain (Push → SMS → Email → Voice), so one bad channel doesn't mean the user never gets the alert.
- A scheduled **reconciliation job** catches deliveries that go silent due to a worker crash or any failure mode the normal retry/DLT path can't see.
- Every status change is independently, asynchronously recorded, so `GET /broadcast/{id}/status` returns the real, current state of every delivery — not a guess.

---

## Architecture

The system is a **multi-module Maven project**, with each module an independently deployable Spring Boot service (except `core` and `provider`, which are plain libraries):

```
broadcast-system/
├── core/           Shared models (BroadcastEvent, DeliveryTask, TrackingEvent)
│                   and the TransientDeliveryException / PermanentDeliveryException
│                   checked-exception hierarchy. No Spring dependency.
│
├── provider/       NotificationProvider interface + a configurable mock
│                   implementation that simulates realistic provider failure
│                   rates and latency, standing in for Twilio/SendGrid/FCM.
│
├── dispatcher/     REST entry point (POST /broadcast). Publishes the raw
│                   event, then fans it out into one DeliveryTask per user.
│
├── worker/         Four channel workers (SMS/Email/Push/Voice), each with
│                   its own thread pool and provider. Owns the retry
│                   consumer and all failure-classification logic.
│
├── tracking/       Consumes tracking events, writes the audit trail to
│                   Postgres, exposes GET /broadcast/{id}/status, and runs
│                   the reconciliation sweep for stuck deliveries.
│
├── kafka-infra/    docker-compose for Kafka (KRaft), Postgres, Redis, akhq.
└── loadtest/       A standalone Java tool for throughput testing.
```

**Why separate services, not one monolith:** each channel scales independently — if an SMS provider is rate-limiting at 2am, that never blocks push or email. Each module can be deployed, scaled, and restarted on its own.

---

## End-to-End Flow

```
POST /broadcast
      │
      ▼
BroadcastEvent ──► broadcast.emergency.raw (12 partitions, keyed by region)
      │
      ▼
Dispatcher fans out: one DeliveryTask per user, starting on PUSH
      │
      ▼
broadcast.emergency.dispatch.push ──► PushChannelWorker
      │
      ▼
Thread pool hands off (Kafka thread never blocks) ──► provider.send(...)
      │
      ├─ Success ───────────────► DELIVERED (tracking event published)
      │
      ├─ TransientDeliveryException ─► retry topic, exponential backoff,
      │                                 republished back to the SAME channel
      │
      └─ PermanentDeliveryException ─► DLT topic + a NEW DeliveryTask
                                         published to the NEXT channel (SMS)
```

A task only moves to the next channel after its *current* channel is genuinely exhausted — a transient failure gets every retry chance first; only a true permanent failure (or exhausted retries) triggers escalation.

---

## Kafka Design

Seven topics, each sized from real throughput math rather than defaults — see [Throughput & Scaling](#throughput--scaling) for the derivation.

| Topic | Partitions | Purpose |
|---|---|---|
| `broadcast.emergency.raw` | 12 | Incoming broadcasts, keyed by region |
| `broadcast.emergency.dispatch.push` | 8 | Push delivery tasks |
| `broadcast.emergency.dispatch.sms` | 6 | SMS delivery tasks |
| `broadcast.emergency.dispatch.email` | 4 | Email delivery tasks |
| `broadcast.emergency.dispatch.voice` | 3 | Voice delivery tasks (lowest volume, last-resort channel) |
| `broadcast.emergency.retry` | 6 | Tasks awaiting their backoff window |
| `broadcast.emergency.DLT` | 3 | Permanently failed tasks |
| `broadcast.emergency.tracking-events` | 6 | Status-change events consumed by `tracking` |

Each channel worker runs as its own named **consumer group**, so channels scale independently — adding instances to `sms-worker-group` has zero effect on `push-worker-group`.

---

## Channel Workers

Each of the four channel workers follows the same shape (`AbstractChannelWorker` + a thin subclass per channel):

1. `@KafkaListener` receives a `DeliveryTask`.
2. The listener thread immediately hands off to a **bounded `ThreadPoolTaskExecutor`** and returns — it never blocks waiting on a provider call.
3. A pool thread checks Redis for a duplicate (idempotency), then calls `provider.send(...)`.

| Channel | Thread pool size | Reasoning |
|---|---|---|
| Push | 50 | Fast provider calls, high volume |
| SMS | 50 | Fast provider calls, high volume |
| Email | 100 | Higher provider concurrency limit |
| Voice | 20 | Slow provider calls (seconds, not ms), low volume, last resort |

Rejected tasks (pool and queue both full) use **`CallerRunsPolicy`** — the Kafka thread itself is forced to run the task, which naturally throttles consumption under backpressure instead of OOMing.

`NotificationProvider` is an interface; the current implementation is `MockNotificationProvider`, with configurable failure rates and latency per channel — it stands in for a real Twilio/SendGrid/FCM integration, which would be a single new class with zero changes anywhere else in the system.

---

## Retry & Failure Handling

Every provider call result is classified via a **checked exception hierarchy**:

```java
public interface NotificationProvider {
    DeliveryResult send(String recipient, String message)
            throws TransientDeliveryException, PermanentDeliveryException;
}
```

Because these are checked, not unchecked, the compiler forces every provider implementation to explicitly classify every failure path — there's no way to accidentally throw an unclassified error.

- **`TransientDeliveryException`** (timeout, rate-limit) → requeued with exponential backoff: `5 × 3^attempt` seconds (5s, 15s, 45s, 135s), capped at 4 attempts.
- **`PermanentDeliveryException`** (invalid recipient, unsubscribed) → no retry wasted — straight to the DLT, and the fallback chain advances to the next channel.
- Retries that exhaust all 4 attempts are treated as permanent failures from that point on.

A separate **reconciliation job** sweeps Postgres every few minutes for anything still `PENDING`/`RETRYING` past a 10-minute threshold — catching deliveries where the normal signal-generating path itself silently failed (e.g. a worker crash mid-processing), which neither retry nor DLT logic alone can see.

---

## Redis Idempotency

Kafka guarantees **at-least-once** delivery, not exactly-once — the same message can be redelivered after a consumer rebalance or a crash before offset commit. Before calling the provider, each worker claims an idempotency key in Redis via an atomic `SETNX`-style check:

```java
broadcastId + ":" + userId + ":" + channel + ":" + attemptCount
```

**A real bug this caught during load testing:** the key originally omitted `attemptCount`. A genuine retry (a *new* attempt, same user/channel) computed the same key as the original attempt, got incorrectly treated as a duplicate of it, and was silently dropped — the delivery got permanently stuck at `RETRYING` with no further action ever taken. Including `attemptCount` in the key fixes this: a true Kafka redelivery (same attempt number) is still deduped correctly, but a legitimate next attempt is no longer mistaken for one.

---

## Database Optimization

The audit-trail table (`delivery_log`) is written far more often than it's read, so every write was deliberately optimized:

- **No read-before-write.** Status transitions use `@Modifying @Query` direct `UPDATE` statements instead of `find → mutate → save`, cutting every status change from 2 database round-trips to 1.
- **Bounded queue + batched flush for inserts.** New `PENDING` rows are pushed into an in-memory `BlockingQueue` and flushed every 500ms via `saveAll()` with JDBC batching (`hibernate.jdbc.batch_size=100`), instead of one `INSERT` per user — observed consolidating up to 1,000 writes into a single batch during load testing.
- **A genuine race condition, found and fixed via load testing:** under real concurrent load, a delivery outcome (e.g. `DELIVERED`) could resolve and attempt its `UPDATE` *before* the row's initial `PENDING` insert had been flushed from the queue — the `UPDATE` would match zero rows and silently do nothing, permanently stranding that delivery at `PENDING`. Fixed with an upsert-style fallback: if the `UPDATE` matches nothing, the service inserts the row directly with the correct final status already set.

---

## Throughput & Scaling

**Design target:** push a broadcast to 500,000 users within 5 minutes → ~1,667 messages/sec at peak, split roughly Push 50% / SMS 30% / Email 15% / Voice 5%. Partition counts and thread-pool sizes above were derived from:

```
per-consumer throughput = threadPoolSize / avgProviderLatency
partitions ≈ target throughput / per-consumer throughput
```

**Measured result (single local instance, single-broker Kafka):** a custom Java load-testing tool (`loadtest/`) drove 1,000 concurrent users through the full pipeline — REST trigger → Kafka fan-out → delivery → status verification — sustaining roughly **250 messages/sec** during active processing.

**The honest gap:** the design target assumed horizontally scaled consumer instances (e.g. ~3 push consumers to hit 833/sec); this was tested with exactly one instance of each service. ~250/sec from one instance is consistent with needing ~3 scaled instances to reach the full peak target — the architecture is designed to scale that way (independent consumer groups per channel), it just hasn't been load-tested at that multi-instance scale yet.

---

## Technology Stack

Java 17 · Spring Boot 3 · Apache Kafka · PostgreSQL · Redis · Spring Data JPA · Maven (multi-module) · Docker Compose

---

## How to Run Locally

**1. Start infrastructure**
```bash
cd kafka-infra
docker compose up -d
```
This brings up Kafka (KRaft mode), Postgres, Redis, and akhq (Kafka UI, `localhost:8082`).

**2. Build everything**
```bash
mvn clean install
```

**3. Start each service** (separate terminals), in any order:
```bash
cd dispatcher && mvn spring-boot:run   # port 8080
cd worker      && mvn spring-boot:run
cd tracking    && mvn spring-boot:run  # port 8090
```

**4. Trigger a broadcast**
```bash
curl -X POST http://localhost:8080/broadcast \
  -H "Content-Type: application/json" \
  -d '{"region":"Pune","message":"Test alert","userIds":["user-1","user-2","user-3"]}'
```

**5. Check delivery status**
```bash
curl http://localhost:8090/broadcast/{broadcastId}/status
```

Returns a per-user, per-channel breakdown: delivered, retrying, or permanently failed — with any automatic channel fallback already reflected.

**6. (Optional) Load test**
```bash
cd loadtest
javac LoadTest.java
java LoadTest
```

# spring-transactional-kafka

Transaction-safe Kafka publishing for Spring applications.

---

## Why not just use KafkaTemplate?

Spring's `KafkaTemplate` supports Kafka transactions natively via `executeInTransaction()` and `@Transactional` on the producer side. But Kafka transactions are not the same as database transactions — and that distinction matters.

When you use Spring's built-in Kafka transactional support, you are coordinating **Kafka's own transaction protocol** (producer idempotence, atomic multi-partition writes). This is useful for Kafka-to-Kafka pipelines (consume → process → produce), but it does **not** coordinate with your database transaction.

The result is a classic dual-write problem:

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);         // DB write
    kafkaTemplate.send("orders", order); // Kafka write — separate system
}
```

```
What you think happens:
  [DB commit] ──── [Kafka publish] ✓

What can actually happen:
  [DB commit] ──── [Kafka publish fails] ✗  → DB has the order, Kafka does not
  [DB rollback] ── [Kafka publish succeeds] ✗ → Kafka has the order, DB does not
```

There is no atomicity guarantee between the two. A crash, network blip, or broker timeout after the DB commits but before Kafka publishes leaves your systems permanently out of sync.

---

## What about Spring Kafka's transaction-aware sending?

Spring Kafka's `KafkaTemplate` does detect an active `DataSourceTransactionManager` or `JpaTransactionManager` transaction via `TransactionSynchronizationManager` and defers the send until `afterCommit()`. If the transaction rolls back, the send is discarded.

So Spring does provide this out of the box — but whether it actually kicks in depends on three conditions that are easy to get wrong silently:

**1. `spring.kafka.producer.transaction-id-prefix` must NOT be set**

This is the most counterintuitive one. Setting `transactionIdPrefix` does not enhance the deferral behavior — it replaces it entirely with Kafka's own transaction protocol. The simple `afterCommit()` deferral is the default behavior for a non-transactional producer.

```
No transaction-id-prefix set:
  DB tx active → KafkaTemplate defers send to afterCommit() ✓

transaction-id-prefix set:
  DB tx active → KafkaTemplate uses Kafka producer transactions instead
               → deferral behavior gone, Kafka tx commits independently of DB ✗
```

If you've set `transactionIdPrefix` anywhere in your config for any reason, you've silently opted out of the behavior you actually wanted.

**2. Spring's transaction synchronization infrastructure must be active**

The deferral only works if `TransactionSynchronizationManager.isSynchronizationActive()` is true — which requires an active `DataSourceTransactionManager` or `JpaTransactionManager` transaction. Calling `send()` outside a `@Transactional` boundary, or within a transaction that doesn't activate synchronization, publishes immediately with no warning.

**3. A `KafkaTransactionManager` must not be in play**

If you have both a `DataSourceTransactionManager` and a `KafkaTransactionManager` configured, the behavior depends on which is primary and how they interact. The deferral behavior does not apply when a `KafkaTransactionManager` is the active transaction manager.

### The actual problem

Each of these conditions fails silently. If `transactionIdPrefix` is set, or synchronization isn't active, or a `KafkaTransactionManager` takes precedence, `KafkaTemplate` publishes immediately with no error and no indication that the deferral you expected didn't happen. In a test environment these conditions are often met; in production with a fully wired Spring context, they may not be.

`TransactionalKafkaTemplate` does one thing explicitly: if `TransactionSynchronizationManager.isSynchronizationActive()` is true, buffer the record and publish in `afterCommit()`. If not, publish immediately. No dependency on producer configuration. No interaction with `KafkaTransactionManager`. No silent fallback.

---

## What TransactionalKafkaTemplate does differently

`TransactionalKafkaTemplate` solves the dual-write problem using the **transactional outbox pattern**. Instead of writing to Kafka directly, it hooks into Spring's `TransactionSynchronizationManager` and defers publishing until **after** the database transaction has successfully committed.

```
TransactionalKafkaTemplate approach:

  @Transactional method starts
  │
  ├── DB writes happen normally
  │
  ├── kafkaTemplate.send(...) called
  │     └── detects active transaction
  │     └── buffers the record in-memory (does NOT publish yet)
  │
  ├── @Transactional method returns
  │
  ├── DB transaction commits ✓
  │
  └── afterCommit() fires → records published to Kafka ✓

  If DB rolls back → afterCommit() never fires → Kafka is never written ✓
```

### In-memory deferral (TransactionalKafkaTemplate)

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    transactionalKafkaTemplate.send("orders", order); // buffered, not sent yet
}
// DB commits → Kafka publish fires automatically after
```

No schema changes. No extra tables. Works out of the box.

### Durable outbox (OutboxEventStager)

For stronger guarantees — surviving application crashes between DB commit and Kafka publish — the library also provides a durable outbox. The event is written to an `outbox_events` table **inside the same DB transaction**, and a separate relay process picks it up and publishes to Kafka.

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    outboxEventStager.stage(
        OutboxMessage.to("orders.placed")
            .key(order.getId().toString())
            .payload(order)
            .build()
    );
    // Both the order and the outbox row commit atomically
}
// Relay process reads outbox_events → publishes → marks PUBLISHED
```

```
DB transaction:
┌─────────────────────────────────────────┐
│  INSERT INTO orders ...                 │
│  INSERT INTO outbox_events (PENDING) ...|  ← atomic with the order
└─────────────────────────────────────────┘
           │
           ▼
    Relay process
           │
           ▼
    Kafka publish → mark PUBLISHED
```

---

## The honest tradeoff

| | Spring KafkaTemplate | TransactionalKafkaTemplate | Durable Outbox |
|---|---|---|---|
| **Atomicity with DB** | No | Best-effort | Yes (atomic) |
| **Survives app crash** | N/A | No | Yes |
| **Schema changes needed** | No | No | Yes (`outbox_events` table) |
| **Operational complexity** | Low | Low | Medium (relay process) |
| **Delivery guarantee** | At-most-once | At-least-once (post-commit) | At-least-once |
| **Best for** | Kafka-to-Kafka pipelines | Simple DB + Kafka sync | Critical event reliability |

### When TransactionalKafkaTemplate is enough

- Your application is unlikely to crash in the milliseconds between DB commit and the `afterCommit()` callback
- Occasional missed events are acceptable (e.g. cache invalidation, non-critical notifications)
- You want zero infrastructure overhead

### When to use the durable outbox

- Lost events are not acceptable (payments, orders, audit trails)
- You need guaranteed at-least-once delivery with crash recovery
- You can tolerate slightly higher latency and the operational cost of a relay process

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

Spring Kafka's `KafkaTemplate` actually does detect an active `DataSourceTransactionManager` or `JpaTransactionManager` transaction via `TransactionSynchronizationManager` and defers the send until `afterCommit()`. If the transaction rolls back, the send is discarded.

This sounds like exactly what you want. The problem is that **this behavior only activates under a specific set of conditions that are easy to get wrong**:

**1. The `ProducerFactory` must be configured as transactional**

```java
@Bean
public ProducerFactory<String, String> producerFactory() {
    DefaultKafkaProducerFactory<String, String> factory =
        new DefaultKafkaProducerFactory<>(props);
    factory.setTransactionIdPrefix("my-app-"); // required — without this, no deferral
    return factory;
}
```

Without `transactionIdPrefix`, `KafkaTemplate.send()` publishes immediately regardless of any active DB transaction. There is no warning, no exception — it silently bypasses the deferral behavior.

**2. This drags in Kafka producer transactions whether you want them or not**

Configuring a `transactionIdPrefix` enables Kafka's idempotent producer and transaction protocol on the broker side. This has real operational implications: it requires `IDEMPOTENT_WRITE` ACLs, affects producer performance, and means your broker must support transactions. You may not want or need any of this — you just wanted to not publish on rollback.

**3. The interaction with `KafkaTransactionManager` is non-obvious**

If you also have a `KafkaTransactionManager` in your context, Spring may attempt to synchronize Kafka transactions with your DB transaction using a chained approach. Chained transaction managers are not true two-phase commit — they commit each resource sequentially:

```
Chained commit order:

  [DB commit] → [Kafka commit]
                      ↑
               crash here? DB committed, Kafka did not. Still inconsistent.
```

**4. The deferral only applies when `@Transactional` uses a DB transaction manager**

If you mix transaction managers, use `@Transactional` without specifying a `transactionManager`, or call `send()` outside a transaction boundary, the behavior silently changes. It's hard to tell from reading the code whether deferral is actually happening.

### The actual gap

Spring Kafka *can* do deferred-after-commit sending, but getting there requires configuring Kafka producer transactions as a side effect, understanding the interaction between multiple transaction managers, and trusting that the right conditions are all met at runtime.

`TransactionalKafkaTemplate` does one thing explicitly: if a Spring transaction is active, buffer the record and publish it in `afterCommit()`. If there is no active transaction, publish immediately. No Kafka transaction protocol. No broker-side configuration. No silent fallback.

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

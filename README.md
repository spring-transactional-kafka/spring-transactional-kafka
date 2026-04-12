# spring-transactional-kafka

Guaranteed Kafka publishing for Spring applications — from simple transaction-aware sending to a full durable outbox.

---

## The problem

When you publish to Kafka inside a `@Transactional` method, by default (without Kafka transaction coordination) the send happens immediately — before the DB transaction commits. If the transaction rolls back, the event has already been sent to Kafka. If the broker is unavailable after the DB commits, the event is lost.

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    kafkaTemplate.send("orders", order); // sends immediately — before DB commits
}
// DB rolls back? Event already published.
// Broker down after DB commits? Event lost.
```

This library solves that by ensuring Kafka publishes happen **after** the DB transaction commits successfully — and not at all if it rolls back.

---

## TransactionalKafkaTemplate — drop-in transaction-aware sending

A drop-in replacement for `KafkaTemplate.send()` that buffers records during an active transaction and publishes them in `afterCommit()`. Outside a transaction it behaves identically to `KafkaTemplate`.

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    transactionalKafkaTemplate.send("orders", order); // buffered until DB commits
}
// DB rolls back → send discarded.
// DB commits → send fires.
```

No extra configuration. No Kafka transaction protocol. Works with any standard `KafkaTemplate`.

> **Note:** Spring Kafka supports transactional producers (`transactionIdPrefix`) and coordination via `KafkaTransactionManager`. However:
> - Kafka transactions only guarantee atomic writes within Kafka
> - Coordinating Kafka with a database transaction requires multiple transaction managers and is not truly atomic
> - Configuration is non-trivial and tightly coupled to Kafka's transaction protocol
>
> `TransactionalKafkaTemplate` instead uses Spring's transaction synchronization to defer publishing until after a successful DB commit, without requiring Kafka transactions.

**Limitation:** if the application crashes between the DB commit and the `afterCommit()` callback, the send is lost.

---

## Durable Outbox — guaranteed delivery with crash recovery

For use cases where a lost event is not acceptable, this library implements the **transactional outbox pattern**. The event is written to an `outbox_events` table inside the same DB transaction as your business data — atomically. A relay process then reads pending events and publishes them to Kafka.

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    outbox.send(
        OutboxMessage.to("orders.placed")
            .key(order.getId().toString())
            .payload(order)
            .build()
    );
    // Both rows commit atomically — or neither does
}
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

If the application crashes after the DB commits, the event is still in `outbox_events` and will be picked up on recovery. The DB is the source of truth — Kafka is derived from it.

---

## Which one to use

| | TransactionalKafkaTemplate | Durable Outbox |
|---|---|---|
| **Atomicity with DB** | Best-effort | Yes (atomic) |
| **Survives app crash** | No | Yes |
| **Schema changes needed** | No | Yes (`outbox_events` table) |
| **Operational complexity** | Low | Medium (relay process) |
| **Delivery guarantee** | Best-effort (at-least-once after commit, but may lose messages on crash) | At-least-once |
| **Best for** | Cache invalidation, non-critical events | Payments, orders, audit trails |

Start with `TransactionalKafkaTemplate` if you want zero infrastructure overhead and can tolerate a very small crash window. Move to the durable outbox when losing an event is not an option.

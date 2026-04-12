package io.github.springtxnkafka.outbox;

public enum OutboxEventStatus {
    PENDING,
    FAILED,
    DEAD,
    PUBLISHED
}

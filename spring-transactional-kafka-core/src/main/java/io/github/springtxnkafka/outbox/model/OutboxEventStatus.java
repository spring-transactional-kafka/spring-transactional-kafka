package io.github.springtxnkafka.outbox.model;

public enum OutboxEventStatus {
    PENDING,
    FAILED,
    DEAD,
    PUBLISHED
}

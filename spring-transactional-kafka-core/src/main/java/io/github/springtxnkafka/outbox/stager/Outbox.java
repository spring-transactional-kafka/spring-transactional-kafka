package io.github.springtxnkafka.outbox.stager;

import io.github.springtxnkafka.outbox.model.OutboxMessage;

public interface Outbox {
    void send(OutboxMessage message);
}
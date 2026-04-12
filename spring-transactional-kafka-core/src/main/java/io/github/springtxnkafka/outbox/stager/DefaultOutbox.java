package io.github.springtxnkafka.outbox.stager;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.model.OutboxMessage;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import io.github.springtxnkafka.outbox.serializer.OutboxPayloadSerializer;

import java.util.Objects;

public class DefaultOutbox implements Outbox {

    private final OutboxEventRepository repository;
    private final OutboxPayloadSerializer serializer;

    public DefaultOutbox(OutboxEventRepository repository, OutboxPayloadSerializer serializer) {
        this.repository = repository;
        this.serializer = serializer;
    }

    @Override
    public void send(OutboxMessage message) {
        String serializedPayload = serializer.serialize(message.getPayload());
        OutboxEvent event = OutboxEvent.from(message, serializedPayload);
        repository.save(event);
    }
}

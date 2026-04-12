package io.github.springtxnkafka.outbox.stager;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.model.OutboxMessage;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import io.github.springtxnkafka.outbox.serializer.OutboxPayloadSerializer;

public class DefaultOutboxEventStager implements OutboxEventStager {


    private OutboxEventRepository repository;
    private OutboxPayloadSerializer serializer;

    public DefaultOutboxEventStager(OutboxEventRepository repository, OutboxPayloadSerializer serializer) {
        this.repository = repository;
        this.serializer = serializer;
    }

    @Override
    public void stage(OutboxMessage message) {
        String serializedPayload = serializer.serialize(message.getPayload());
        OutboxEvent event = OutboxEvent.from(message, serializedPayload);
        repository.save(event);
    }
}

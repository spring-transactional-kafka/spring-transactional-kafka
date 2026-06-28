package io.github.springtxnkafka.outbox.stager;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.model.OutboxMessage;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import io.github.springtxnkafka.outbox.serializer.OutboxPayloadSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class DefaultOutbox implements Outbox {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutbox.class);

    private final OutboxEventRepository repository;
    private final OutboxPayloadSerializer serializer;

    public DefaultOutbox(OutboxEventRepository repository, OutboxPayloadSerializer serializer) {
        this.repository = repository;
        this.serializer = serializer;
    }

    @Override
    public void send(OutboxMessage message) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()){
            log.warn("outbox.send() called outside of a transaction - durablility guaranteed but atomicity is not");
        }
        String serializedPayload = serializer.serialize(message.getPayload());
        OutboxEvent event = OutboxEvent.from(message, serializedPayload);
        repository.save(event);
    }
}

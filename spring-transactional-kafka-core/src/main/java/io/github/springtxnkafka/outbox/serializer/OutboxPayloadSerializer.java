package io.github.springtxnkafka.outbox.serializer;

public interface OutboxPayloadSerializer {
    String serialize(Object payload);
}

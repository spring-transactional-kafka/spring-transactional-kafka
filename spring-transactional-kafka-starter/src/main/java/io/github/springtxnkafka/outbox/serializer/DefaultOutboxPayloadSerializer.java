package io.github.springtxnkafka.outbox.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.springtxnkafka.outbox.exception.OutboxSerializationException;

public class DefaultOutboxPayloadSerializer implements OutboxPayloadSerializer {

    private final ObjectMapper objectMapper;

    public DefaultOutboxPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(Object payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new OutboxSerializationException(payload.getClass(), e);
        }
    }
}

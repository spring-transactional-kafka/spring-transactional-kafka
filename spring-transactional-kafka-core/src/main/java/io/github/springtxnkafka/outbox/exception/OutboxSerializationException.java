package io.github.springtxnkafka.outbox.exception;

public class OutboxSerializationException extends RuntimeException {

    private final Class<?> payloadType;

    public OutboxSerializationException(Class<?> payloadType, Throwable cause) {
        super("Failed to serialize payload of type: " + (payloadType != null ? payloadType.getName() : "unknown"), cause);
        this.payloadType = payloadType;
    }

    public Class<?> getPayloadType() {
        return payloadType;
    }

}

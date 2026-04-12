package io.github.springtxnkafka.outbox.exception;

import io.github.springtxnkafka.outbox.model.OutboxEvent;

public class OutboxPublishException extends RuntimeException {

    public OutboxPublishException(OutboxEvent event, Throwable cause) {
        super(String.format(
                "Failed to publish outbox event id=%s to topic='%s'",
                event.getId(), event.getTopic()
        ), cause);
    }

}

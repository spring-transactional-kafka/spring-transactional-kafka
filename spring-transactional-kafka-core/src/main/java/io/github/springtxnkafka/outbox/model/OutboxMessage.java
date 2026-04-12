package io.github.springtxnkafka.outbox.model;

import io.github.springtxnkafka.outbox.stager.OutboxEventStager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a message to be staged for reliable delivery to Kafka.
 *
 * <p>Construct instances using the fluent builder API:
 * <pre>{@code
 * OutboxMessage.to("orders.placed")
 *     .key(order.getId().toString())
 *     .payload(order)
 *     .build();
 * }</pre>
 *
 * @see OutboxEventStager
 */
public class OutboxMessage {

    private static final Logger log = LoggerFactory.getLogger(OutboxMessage.class);

    private final String topic;
    private final String key;
    private final Object payload;

    private OutboxMessage(Builder builder) {
        this.topic = builder.topic;
        this.key = builder.key;
        this.payload = builder.payload;
    }

    public static Builder to(String topic) {
        return new Builder(topic);
    }

    public static class Builder {
        private final String topic;
        private String key;
        private Object payload;

        private Builder(String topic) {
            this.topic = topic;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        public OutboxMessage build() {
            if (topic == null || topic.isBlank()) {
                throw new IllegalArgumentException("topic must not be null or blank");
            }
            if (payload == null) {
                throw new IllegalArgumentException("payload must not be null");
            }
            if (key == null) {
                log.warn("OutboxMessage built without a key for topic '{}'", topic);
            }
            return new OutboxMessage(this);
        }
    }

    public String getTopic() { return topic; }
    public String getKey() { return key; }
    public Object getPayload() { return payload; }
}
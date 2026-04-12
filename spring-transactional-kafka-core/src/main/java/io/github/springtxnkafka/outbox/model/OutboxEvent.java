package io.github.springtxnkafka.outbox.model;


public class OutboxEvent {

    private Long id;
    private final String topic;
    private final String key;
    private final String payload;
    private OutboxEventStatus status;


    public OutboxEvent(Long id, String topic, String key, String payload, OutboxEventStatus status) {
        this.id = id;
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.status = status;
    }

    public static OutboxEvent from(OutboxMessage message, String serializedPayload){
        return new OutboxEvent(null,
                message.getTopic(),
                message.getKey(),
                serializedPayload,
                OutboxEventStatus.PENDING
        );
    }


    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public String getKey() { return key; }
    public String getPayload() { return payload; }
    public OutboxEventStatus getStatus() { return status; }
}

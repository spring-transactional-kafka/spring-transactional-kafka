package io.github.springtxnkafka.outbox.jpa;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.model.OutboxEventStatus;
import jakarta.persistence.*;

@Entity
@Table(name = "outbox_events")
public class JpaOutboxEvent  {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String topic;

    @Column(name = "message_key")
    private String key;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxEventStatus status;

    protected JpaOutboxEvent() {}

    private JpaOutboxEvent(String topic, String key, String payload, OutboxEventStatus status) {
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.status = status;
    }

    public static JpaOutboxEvent from(OutboxEvent event) {
        return new JpaOutboxEvent(event.getTopic(), event.getKey(), event.getPayload(), event.getStatus());
    }

    public OutboxEvent toDomain() {
        return new OutboxEvent(id, topic, key, payload, status);
    }

    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public String getKey() { return key; }
    public String getPayload() { return payload; }
    public OutboxEventStatus getStatus() { return status; }

    public void markFailed() { this.status = OutboxEventStatus.FAILED; }
    public void markPublished() { this.status = OutboxEventStatus.PUBLISHED;}
}
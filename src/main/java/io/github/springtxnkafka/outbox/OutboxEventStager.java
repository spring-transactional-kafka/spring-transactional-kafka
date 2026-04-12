package io.github.springtxnkafka.outbox;

public interface OutboxEventStager {

    void stage(OutboxEvent event);
}

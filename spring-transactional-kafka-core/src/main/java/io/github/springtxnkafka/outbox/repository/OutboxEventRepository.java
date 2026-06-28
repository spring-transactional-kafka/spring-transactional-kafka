package io.github.springtxnkafka.outbox.repository;

import io.github.springtxnkafka.outbox.model.OutboxEvent;

import java.util.List;

public interface OutboxEventRepository {

    void save(OutboxEvent event);
    List<OutboxEvent> findPending();
    void markPublished(OutboxEvent event);
    void markFailed(OutboxEvent event);

}

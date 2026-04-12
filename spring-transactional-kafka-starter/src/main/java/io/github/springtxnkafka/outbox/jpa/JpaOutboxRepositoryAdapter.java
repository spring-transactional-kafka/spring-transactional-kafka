package io.github.springtxnkafka.outbox.jpa;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;

import java.util.List;

public class JpaOutboxRepositoryAdapter implements OutboxEventRepository {

    private final JpaOutboxRepository repository;

    public JpaOutboxRepositoryAdapter(JpaOutboxRepository repository) {
        this.repository = repository;
    }

    @Override
    public void save(OutboxEvent event) {
        repository.save(JpaOutboxEvent.from(event));
    }

    @Override
    public List<OutboxEvent> findPending(int limit) {
        return List.of();
    }

}

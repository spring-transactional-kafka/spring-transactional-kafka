package io.github.springtxnkafka.outbox.jpa;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.model.OutboxEventStatus;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public class JpaOutboxRepositoryAdapter implements OutboxEventRepository {

    private final JpaOutboxRepository repository;
    private final int batchSize;

    public JpaOutboxRepositoryAdapter(JpaOutboxRepository repository, int batchSize) {
        this.repository = repository;
        this.batchSize = batchSize;
    }

    @Override
    public void save(OutboxEvent event) {
        repository.save(JpaOutboxEvent.from(event));
    }

    @Override
    public List<OutboxEvent> findPending() {
        return repository.findPending(batchSize)
                .stream()
                .map(JpaOutboxEvent::toDomain)
                .toList();
    }

    @Override
    @Transactional
    public void markPublished(OutboxEvent event) {
        repository.updateStatus(event.getId(), OutboxEventStatus.PUBLISHED);
    }

    @Override
    @Transactional
    public void markFailed(OutboxEvent event) {
        repository.updateStatus(event.getId(), OutboxEventStatus.FAILED);
    }
}
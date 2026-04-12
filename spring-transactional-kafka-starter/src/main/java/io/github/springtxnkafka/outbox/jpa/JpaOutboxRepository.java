package io.github.springtxnkafka.outbox.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JpaOutboxRepository extends JpaRepository<JpaOutboxEvent, Long> {

    List<JpaOutboxEvent> findPending(int limit);

}

package io.github.springtxnkafka.outbox.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

public interface JpaOutboxRepository extends JpaRepository<JpaOutboxEvent, Long> {



}

package io.github.springtxnkafka.outbox.jpa;

import io.github.springtxnkafka.outbox.model.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface JpaOutboxRepository extends JpaRepository<JpaOutboxEvent, Long> {

    @Query(
            value = "SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY id ASC FETCH FIRST :limit ROWS ONLY FOR UPDATE SKIP LOCKED",
            nativeQuery = true
    )
    List<JpaOutboxEvent> findPending(@Param("limit") int limit);

    @Modifying
    @Query("UPDATE JpaOutboxEvent e SET e.status = :status WHERE e.id = :id")
    void updateStatus(@Param("id") Long id, @Param("status") OutboxEventStatus status);

}

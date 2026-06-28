package io.github.springtxnkafka.outbox.relay;

import io.github.springtxnkafka.outbox.model.OutboxEvent;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

public class DefaultOutboxRelay implements OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutboxRelay.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DefaultOutboxRelay(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    @Scheduled(fixedDelayString = "${spring.txn.kafka.outbox.poll-interval-ms:5000}")
    public void poll() {
        List<OutboxEvent> pending = repository.findPending();
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(new ProducerRecord<>(event.getTopic(), event.getKey(), event.getPayload())).get();
                repository.markPublished(event);
            } catch (Exception e) {
                log.error("Failed to publish outbox event {}", event.getId(), e);
                repository.markFailed(event);
            }
        }
    }
}
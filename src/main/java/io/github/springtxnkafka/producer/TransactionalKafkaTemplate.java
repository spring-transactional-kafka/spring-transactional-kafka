package io.github.springtxnkafka.producer;

import io.github.springtxnkafka.sync.KafkaAfterCommitSynchronization;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.List;

public class TransactionalKafkaTemplate<K, V> {

    private static final Logger log = LoggerFactory.getLogger(TransactionalKafkaTemplate.class);

    private final KafkaTemplate<K, V> kafkaTemplate;

    public TransactionalKafkaTemplate(KafkaTemplate<K, V> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, V value) {
        send(new ProducerRecord<>(topic, value));
    }

    public void send(String topic, K key, V value) {
        send(new ProducerRecord<>(topic, key, value));
    }


    public void send(ProducerRecord<K, V> record) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            log.debug("Active transaction detected — deferring Kafka record for topic '{}'", record.topic());
            pendingRecords().add(record);
        } else {
            log.debug("No active transaction — publishing Kafka record immediately to topic '{}'", record.topic());
            kafkaTemplate.send(record);
        }
    }

    @SuppressWarnings("unchecked")
    private List<ProducerRecord<K, V>> pendingRecords() {
        List<ProducerRecord<K, V>> pending =
                (List<ProducerRecord<K, V>>) TransactionSynchronizationManager.getResource(this);

        if (null == pending) {
            pending = new ArrayList<>();
            TransactionSynchronizationManager.bindResource(this, pending);
            TransactionSynchronizationManager.registerSynchronization(
                    new KafkaAfterCommitSynchronization<>(kafkaTemplate, pending, this));
            log.debug("Registered KafkaAfterCommitSynchronization for current transaction");
        }

        return pending;
    }
}
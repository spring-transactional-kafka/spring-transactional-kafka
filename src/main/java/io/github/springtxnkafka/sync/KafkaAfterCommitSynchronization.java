package io.github.springtxnkafka.sync;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

public class KafkaAfterCommitSynchronization<K, V> implements TransactionSynchronization {
    private static final Logger log = LoggerFactory.getLogger(KafkaAfterCommitSynchronization.class);

    private static final String COMMITTED = "COMMITTED";
    private static final String ROLLED_BACK = "ROLLED_BACK";
    private static final String UNKNOWN = "UNKNOWN";

    private static final Map<Integer, String> STATUS_CODE_TO_STRING = Map.of(
            STATUS_COMMITTED, COMMITTED,
            STATUS_ROLLED_BACK, ROLLED_BACK,
            STATUS_UNKNOWN, UNKNOWN
    );

    private final KafkaTemplate<K, V> kafkaTemplate;
    private final List<ProducerRecord<K, V>> pendingRecords;
    private final Object resourceKey;

    public KafkaAfterCommitSynchronization(
            KafkaTemplate<K, V> kafkaTemplate,
            List<ProducerRecord<K, V>> pendingRecords,
            Object resourceKey) {
        this.kafkaTemplate = kafkaTemplate;
        this.pendingRecords = pendingRecords;
        this.resourceKey = resourceKey;
    }


    @Override
    public void afterCommit() {
        if (CollectionUtils.isEmpty(pendingRecords)) {
            log.debug("No pending Kafka records to publish after commit");
            return;
        }
        log.debug("Transaction committed — publishing {} Kafka record(s)", pendingRecords.size());
        for (ProducerRecord<K, V> record : pendingRecords) {
            try {
                kafkaTemplate.send(record);
            } catch (Exception ex) {
                log.error("Failed to publish deferred Kafka record to topic '{}': {}", record.topic(), ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void afterCompletion(int status) {
        TransactionSynchronizationManager.unbindResourceIfPossible(resourceKey);
        pendingRecords.clear();
        log.debug("Transaction completed with status {}", STATUS_CODE_TO_STRING.getOrDefault(status, UNKNOWN));
    }
}
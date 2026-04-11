package io.github.springtxnkafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TransactionalKafkaTemplateTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private TransactionalKafkaTemplate<String, String> template;

    @BeforeEach
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        template = new TransactionalKafkaTemplate<>(kafkaTemplate);
    }

    @Test
    void sendsImmediatelyWhenNoTransactionActive() {
        template.send("my-topic", "key", "value");

        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void defersPublishUntilAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            template.send("my-topic", "key", "value");

            // Within the transaction — nothing sent yet
            verify(kafkaTemplate, never()).send(any(ProducerRecord.class));

            // Simulate commit
            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(TransactionSynchronization::afterCommit);

            verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
        } finally {
            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(s -> s.afterCompletion(0));
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void discardsMessagesOnRollback() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            template.send("my-topic", "key", "value");

            // Simulate rollback — afterCommit is NOT called
            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(s -> s.afterCompletion(1));

            verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
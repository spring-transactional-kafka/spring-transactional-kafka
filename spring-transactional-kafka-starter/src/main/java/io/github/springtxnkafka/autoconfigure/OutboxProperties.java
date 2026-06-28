package io.github.springtxnkafka.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.txn.kafka.outbox")
public class OutboxProperties {

    private int batchSize = 100;

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
}
package io.github.springtxnkafka.autoconfigure;

import io.github.springtxnkafka.producer.TransactionalKafkaTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
public class TransactionalKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public <K, V> TransactionalKafkaTemplate<K, V> transactionalKafkaTemplate(KafkaTemplate<K, V> kafkaTemplate) {
        return new TransactionalKafkaTemplate<>(kafkaTemplate);
    }


}
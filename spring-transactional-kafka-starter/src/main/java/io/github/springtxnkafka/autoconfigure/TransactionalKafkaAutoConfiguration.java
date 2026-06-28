package io.github.springtxnkafka.autoconfigure;

import io.github.springtxnkafka.outbox.jpa.JpaOutboxRepository;
import io.github.springtxnkafka.outbox.jpa.JpaOutboxRepositoryAdapter;
import io.github.springtxnkafka.outbox.relay.DefaultOutboxRelay;
import io.github.springtxnkafka.outbox.relay.OutboxRelay;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import io.github.springtxnkafka.producer.TransactionalKafkaTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
public class TransactionalKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public <K, V> TransactionalKafkaTemplate<K, V> transactionalKafkaTemplate(KafkaTemplate<K, V> kafkaTemplate) {
        return new TransactionalKafkaTemplate<>(kafkaTemplate);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @EnableJpaRepositories(basePackages = "io.github.springtxnkafka.outbox.jpa")
    static class OutboxConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public OutboxEventRepository outboxEventRepository(JpaOutboxRepository jpaOutboxRepository,
                                                           OutboxProperties properties) {
            return new JpaOutboxRepositoryAdapter(jpaOutboxRepository, properties.getBatchSize());
        }

        @Bean
        @ConditionalOnMissingBean
        public OutboxRelay outboxRelay(OutboxEventRepository repository,
                                       KafkaTemplate<String, String> kafkaTemplate) {
            return new DefaultOutboxRelay(repository, kafkaTemplate);
        }
    }
}
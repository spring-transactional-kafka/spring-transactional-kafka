package io.github.springtxnkafka.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.springtxnkafka.outbox.jpa.JpaOutboxRepository;
import io.github.springtxnkafka.outbox.jpa.JpaOutboxRepositoryAdapter;
import io.github.springtxnkafka.outbox.relay.DefaultOutboxRelay;
import io.github.springtxnkafka.outbox.relay.OutboxRelay;
import io.github.springtxnkafka.outbox.repository.OutboxEventRepository;
import io.github.springtxnkafka.outbox.serializer.DefaultOutboxPayloadSerializer;
import io.github.springtxnkafka.outbox.serializer.OutboxPayloadSerializer;
import io.github.springtxnkafka.outbox.stager.DefaultOutbox;
import io.github.springtxnkafka.outbox.stager.Outbox;
import io.github.springtxnkafka.producer.TransactionalKafkaTemplate;
import jakarta.persistence.EntityManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
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
    static class OutboxConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public OutboxEventRepository outboxEventRepository(EntityManager em, OutboxProperties properties) {
            JpaOutboxRepository jpaRepo = new JpaRepositoryFactory(em).getRepository(JpaOutboxRepository.class);
            return new JpaOutboxRepositoryAdapter(jpaRepo, properties.getBatchSize());
        }

        @Bean
        @ConditionalOnMissingBean
        public OutboxPayloadSerializer outboxPayloadSerializer(ObjectMapper objectMapper) {
            return new DefaultOutboxPayloadSerializer(objectMapper);
        }

        @Bean
        @ConditionalOnMissingBean
        public Outbox outbox(OutboxEventRepository repository, OutboxPayloadSerializer serializer) {
            return new DefaultOutbox(repository, serializer);
        }

        @Bean
        @ConditionalOnMissingBean
        public OutboxRelay outboxRelay(OutboxEventRepository repository,
                                       KafkaTemplate<String, String> kafkaTemplate) {
            return new DefaultOutboxRelay(repository, kafkaTemplate);
        }
    }
}
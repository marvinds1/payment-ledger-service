package io.github.marvinds1.ledger.outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfig {

    @Bean
    NewTopic transferEventsTopic(OutboxProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }
}

package com.broadcast.dispatcher.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    private static final short REPLICATION_FACTOR = 1;

    @Bean
    public NewTopic rawTopic() {
        return TopicBuilder.name(Topics.RAW).partitions(12).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic smsTopic() {
        return TopicBuilder.name(Topics.SMS).partitions(6).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic emailTopic() {
        return TopicBuilder.name(Topics.EMAIL).partitions(4).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic pushTopic() {
        return TopicBuilder.name(Topics.PUSH).partitions(8).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic voiceTopic() {
        return TopicBuilder.name(Topics.VOICE).partitions(3).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic retryTopic() {
        return TopicBuilder.name(Topics.RETRY).partitions(6).replicas(REPLICATION_FACTOR).build();
    }

    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(Topics.DLT).partitions(3).replicas(REPLICATION_FACTOR).build();
    }
}

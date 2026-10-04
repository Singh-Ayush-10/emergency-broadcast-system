package com.broadcast.worker.config;

import com.broadcast.core.model.DeliveryTask;
import com.broadcast.core.model.TrackingEvent;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, DeliveryTask> deliveryTaskProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }
    @Bean
    public ProducerFactory<String, TrackingEvent> trackingEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, DeliveryTask> deliveryTaskKafkaTemplate(
            ProducerFactory<String, DeliveryTask> deliveryTaskProducerFactory) {
        return new KafkaTemplate<>(deliveryTaskProducerFactory);
    }
    @Bean
    public KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate(
            ProducerFactory<String, TrackingEvent> trackingEventProducerFactory) {
        return new KafkaTemplate<>(trackingEventProducerFactory);
    }
}

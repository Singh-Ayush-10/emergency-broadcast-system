package com.broadcast.worker.config;

import com.broadcast.core.model.DeliveryTask;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, DeliveryTask> deliveryTaskConsumerFactory(){
        Map<String,Object>props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.broadcast.core.model");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, DeliveryTask.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,DeliveryTask>deliveryTaskListenerFactory(
            ConsumerFactory<String,DeliveryTask>deliveryTaskConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<String,DeliveryTask>factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(deliveryTaskConsumerFactory);
        return  factory;
    }
}

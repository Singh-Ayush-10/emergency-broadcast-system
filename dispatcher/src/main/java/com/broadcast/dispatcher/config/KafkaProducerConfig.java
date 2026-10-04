package com.broadcast.dispatcher.config;

import com.broadcast.core.model.BroadCastEvent;
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

    private Map<String , Object>baseProducerProps(){
        Map<String,Object>props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JsonSerializer.class);
        return props;
    }
    @Bean
    public ProducerFactory<String, TrackingEvent>trackingEventProducerFactory(){
        return new DefaultKafkaProducerFactory<>(baseProducerProps());
    }
    @Bean
    public KafkaTemplate<String,TrackingEvent>trackingEventKafkaTemplate(ProducerFactory<String,TrackingEvent>trackingEventProducerFactory){
        return new KafkaTemplate<>(trackingEventProducerFactory);
    }
    @Bean
    public ProducerFactory<String, DeliveryTask>deliveryTaskProducerFactory(){
        return new DefaultKafkaProducerFactory<>(baseProducerProps());
    }
    @Bean
    public KafkaTemplate<String , DeliveryTask> deliveryTaskKafkaTemplate(ProducerFactory<String,DeliveryTask>deliveryTaskProducerFactory){
        return new KafkaTemplate<>(deliveryTaskProducerFactory);
    }
    @Bean
    public ProducerFactory<String, BroadCastEvent> broadCastEventProducerFactory(){
        return new DefaultKafkaProducerFactory<>(baseProducerProps());
    }
    @Bean
    public KafkaTemplate<String , BroadCastEvent> broadCastEventKafkaTemplate(ProducerFactory<String, BroadCastEvent>broadCastEventProducerFactory){
        return new KafkaTemplate<>(broadCastEventProducerFactory);
    }

}

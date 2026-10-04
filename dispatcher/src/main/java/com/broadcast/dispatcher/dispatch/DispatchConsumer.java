package com.broadcast.dispatcher.dispatch;

import com.broadcast.core.model.*;
import com.broadcast.dispatcher.config.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DispatchConsumer {

    private final KafkaTemplate<String, DeliveryTask>deliveryTaskKafkaTemplate;
    private final KafkaTemplate<String, TrackingEvent>trackingEventKafkaTemplate;

    public DispatchConsumer(KafkaTemplate<String, DeliveryTask> deliveryTaskKafkaTemplate, KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate) {
        this.deliveryTaskKafkaTemplate = deliveryTaskKafkaTemplate;
        this.trackingEventKafkaTemplate = trackingEventKafkaTemplate;
    }
    @KafkaListener(
            topics = "broadcast.emergency.raw",
            groupId = "dispatcher-group",
            containerFactory = "broadcastEventListenerFactory")
    public void onBroadcastEvent(BroadCastEvent event){
        for (String userId : event.getUserIds()) {
            dispatchToChannel(event, userId, ChannelType.PUSh);
        }
    }

    private void dispatchToChannel(BroadCastEvent event, String userId, ChannelType channel) {
        String recipient = FallbackChannelResolver.recipientFor(userId, channel);

        DeliveryTask task = new DeliveryTask(
                event.getBroadcastId(), userId, recipient, event.getMessage(), channel);

        String topic = FallbackChannelResolver.topicFor(channel);
        deliveryTaskKafkaTemplate.send(topic, userId, task);

        TrackingEvent pendingEvent = new TrackingEvent(
                event.getBroadcastId(), userId,channel, DeliveryStatus.PENDING,null
        );
        trackingEventKafkaTemplate.send(Topics.TRACKING_EVENTS,userId,pendingEvent);
    }
}

package com.broadcast.tracking;

import com.broadcast.core.model.TrackingEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TrackingEventConsumer {
    private final DeliveryTrackerService trackerService;


    public TrackingEventConsumer(DeliveryTrackerService trackerService) {
        this.trackerService = trackerService;
    }

    @KafkaListener(
            topics = "broadcast.emergency.tracking-events",
            groupId = "tracking-event-group",
            containerFactory = "trackingEventListenerFactory",
            concurrency = "6")
    public void onTrackingEvent(TrackingEvent event){
        switch(event.getStatus()){
            case PENDING -> trackerService.recordPending(
                    event.getBroadcastId(),event.getUserId(),event.getChannel());
            case DELIVERED -> trackerService.recordDelivered(
                    event.getBroadcastId(), event.getUserId(), event.getChannel(), event.getDetail());

            case RETRYING -> trackerService.recordRetrying(
                    event.getBroadcastId(), event.getUserId(), event.getChannel(), event.getDetail());

            case PERMANENTLY_FAILED -> trackerService.recordPermanentlyFailed(
                    event.getBroadcastId(), event.getUserId(), event.getChannel(), event.getDetail());
        }
    }
}

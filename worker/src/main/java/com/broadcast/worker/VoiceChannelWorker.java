package com.broadcast.worker;

import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryTask;
import com.broadcast.core.model.TrackingEvent;
import com.broadcast.provider.NotificationProvider;
import com.broadcast.worker.retry.IdempotencyService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class VoiceChannelWorker extends AbstractChannelWorker {

    public VoiceChannelWorker(@Qualifier("voiceProvider") NotificationProvider provider,
                              @Qualifier("voiceExecutor") ThreadPoolTaskExecutor executor,
                              KafkaTemplate<String, DeliveryTask> kafkaTemplate,
                              KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate,
                              IdempotencyService idempotencyService) {
        super(provider, executor, kafkaTemplate,trackingEventKafkaTemplate, idempotencyService);
    }

    @KafkaListener(
            topics = "broadcast.emergency.dispatch.voice",
            groupId = "voice-worker-group",
            containerFactory = "deliveryTaskListenerFactory",
            concurrency = "3")
    public void onDeliveryTask(DeliveryTask task) {
        handle(task);
    }

    @Override
    protected ChannelType channel() {
        return ChannelType.VOICE;
    }
}
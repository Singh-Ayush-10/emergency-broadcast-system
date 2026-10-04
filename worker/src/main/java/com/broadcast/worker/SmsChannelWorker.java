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

public class SmsChannelWorker extends AbstractChannelWorker{

    public SmsChannelWorker(@Qualifier("smsProvider") NotificationProvider provider,
                            @Qualifier("smsExecutor") ThreadPoolTaskExecutor executor,
                            KafkaTemplate<String, DeliveryTask> kafkaTemplate,
                            KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate,
                            IdempotencyService idempotencyService) {
        super(provider, executor, kafkaTemplate, trackingEventKafkaTemplate, idempotencyService);
    }

    @KafkaListener(
            topics = "broadcast.emergency.dispatch.sms",
            groupId = "sms-worker-group",
            containerFactory = "deliveryTaskListenerFactory",
            concurrency = "6")
    public void onDeliveryTask(DeliveryTask task){
        handle(task);
    }
    @Override
    protected ChannelType channel() {
        return ChannelType.SMS;
    }
}

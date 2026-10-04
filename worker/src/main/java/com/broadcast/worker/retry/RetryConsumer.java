package com.broadcast.worker.retry;

import com.broadcast.core.model.DeliveryTask;
import com.broadcast.worker.config.KafkaConsumerConfig;
import com.broadcast.worker.dispatch.FallbackChannelResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class RetryConsumer {
    private static final Logger log = LoggerFactory.getLogger(RetryConsumer.class);

    private final KafkaTemplate<String, DeliveryTask>kafkaTemplate;
    private final ScheduledExecutorService retryScheduler;

    public RetryConsumer(KafkaTemplate<String, DeliveryTask> kafkaTemplate, ScheduledExecutorService retryScheduler) {
        this.kafkaTemplate = kafkaTemplate;
        this.retryScheduler = retryScheduler;
    }


    @KafkaListener(
            topics = "broadcast.emergency.retry",
            groupId = "retry-worker-group",
            containerFactory = "deliveryTaskListenerFactory",
            concurrency = "6")
    public void onRetryTask(DeliveryTask task){
        long delayMillis = computeDelayMillis(task.getNotBeforeTimestamp());

        retryScheduler.schedule(()->republish(task),delayMillis, TimeUnit.MILLISECONDS);
    }
    private long computeDelayMillis(Instant notBeforeTimestamp){
        if(notBeforeTimestamp == null){
            return 0;
        }
        Duration remaning = Duration.between(Instant.now(),notBeforeTimestamp );
        return  Math.max(0,remaning.toMillis());
    }
    private void republish(DeliveryTask task){
        String topic = FallbackChannelResolver.topicFor(task.getChannel());
        log.info("Republishing retry attempt {} for user {} on channel {}",
                task.getAttemptCount(), task.getUserId(), task.getChannel());
        kafkaTemplate.send(topic, task.getUserId(), task);
    }
}

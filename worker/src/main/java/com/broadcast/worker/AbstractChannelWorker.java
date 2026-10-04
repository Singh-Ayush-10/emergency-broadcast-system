package com.broadcast.worker;

import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryStatus;
import com.broadcast.core.model.DeliveryTask;
import com.broadcast.core.model.TrackingEvent;
import com.broadcast.core.model.exception.PermanentDeliveryException;
import com.broadcast.core.model.exception.TransientDeliveryException;
import com.broadcast.provider.DeliveryResult;
import com.broadcast.provider.NotificationProvider;
import com.broadcast.worker.config.Topics;
import com.broadcast.worker.dispatch.FallbackChannelResolver;
import com.broadcast.worker.retry.IdempotencyService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.time.Instant;


public abstract class AbstractChannelWorker {

    private static final Logger log = LoggerFactory.getLogger(AbstractChannelWorker.class);
    private static final int MAX_RETRY_ATTEMPTS = 4;

    protected final NotificationProvider provider;
    protected  final ThreadPoolTaskExecutor executor;
    protected final KafkaTemplate<String, DeliveryTask>kafkaTemplate;
    protected final KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate;

    protected final IdempotencyService idempotencyService;

    public AbstractChannelWorker(NotificationProvider provider, ThreadPoolTaskExecutor executor, KafkaTemplate<String, DeliveryTask> kafkaTemplate, KafkaTemplate<String, TrackingEvent> trackingEventKafkaTemplate, IdempotencyService idempotencyService) {
        this.provider = provider;
        this.executor = executor;
        this.kafkaTemplate = kafkaTemplate;
        this.trackingEventKafkaTemplate = trackingEventKafkaTemplate;
        this.idempotencyService = idempotencyService;
    }
    protected  void handle(DeliveryTask task){
        executor.execute(()->processTask(task));
    }
    private void processTask(DeliveryTask task){
        if (!idempotencyService.markProcessingIfAbsent(task.idempotencyKey())){
            log.debug("Skipping duplicate delivery for {}",task.idempotencyKey());
            return;
        }
        try{
            DeliveryResult result  = provider.send(task.getRecipient(), task.getMessage());
            log.info("Delivered: broadcast={} user={} channel={} providerMessageId={}",
                    task.getBroadcastId(), task.getUserId(), task.getChannel(), result.getProviderMessageId());

            publishTrackingEvent(task,DeliveryStatus.DELIVERED,result.getProviderMessageId());

        }catch (TransientDeliveryException e){
            handleTransientFailure(task , e);
        }catch (PermanentDeliveryException e){
            handlePermanentFailure(task,e);
        }

    }
    private void handleTransientFailure(DeliveryTask task, TransientDeliveryException e) {
        if (task.getAttemptCount() >= MAX_RETRY_ATTEMPTS) {
            log.warn("Retries exhausted for {}, routing to DLT", task.idempotencyKey());
            publishToDlt(task, "Retries exhausted: " + e.getMessage());
            publishTrackingEvent(task, DeliveryStatus.PERMANENTLY_FAILED, "Retries exhausted: " + e.getMessage());
            triggerFallback(task);
            return;
        }
        publishTrackingEvent(task, DeliveryStatus.RETRYING, e.getMessage());

        Instant notBefore = Instant.now().plus(backoffFor(task.getAttemptCount()));
        DeliveryTask retryTask = task.withIncrementedAttempt(notBefore);
        kafkaTemplate.send(Topics.RETRY, task.getUserId(), retryTask);
    }
    private void handlePermanentFailure(DeliveryTask task, PermanentDeliveryException e){
        publishToDlt(task, e.getMessage());
        publishTrackingEvent(task, DeliveryStatus.PERMANENTLY_FAILED, e.getMessage());
        triggerFallback(task);
    }
    private void publishToDlt(DeliveryTask task, String reason){
        log.error("Routing to DLT: broadcast={} user={} channel={} reason={}",
                task.getBroadcastId(), task.getUserId(), task.getChannel(), reason);
        kafkaTemplate.send(Topics.DLT,task.getUserId(),task);
    }



    private void triggerFallback(DeliveryTask task){
        FallbackChannelResolver.nextChannel(task.getChannel()).ifPresent(nextChannel->{
            log.info("Falling back from {} to {} for user {}", task.getChannel(), nextChannel, task.getUserId());
            String recipient = FallbackChannelResolver.recipientFor(task.getUserId(),nextChannel);
            DeliveryTask fallbackTask = new DeliveryTask(
                    task.getBroadcastId(), task.getUserId(), recipient, task.getMessage(), nextChannel);
            kafkaTemplate.send(FallbackChannelResolver.topicFor(nextChannel), task.getUserId(),fallbackTask);

            TrackingEvent pendingEvent = new TrackingEvent(
                    task.getBroadcastId(), task.getUserId(), nextChannel, DeliveryStatus.PENDING, null);
            trackingEventKafkaTemplate.send(Topics.TRACKING_EVENTS, task.getUserId(), pendingEvent);
        });
    }
    private void publishTrackingEvent(DeliveryTask task, DeliveryStatus status, String detail) {
        TrackingEvent event = new TrackingEvent(
                task.getBroadcastId(), task.getUserId(), task.getChannel(), status, detail);
        trackingEventKafkaTemplate.send(Topics.TRACKING_EVENTS, task.getUserId(), event);
    }

    private Duration backoffFor(int attemptCount){
        long seconds = (long)(5* Math.pow(3,attemptCount));
        return
                Duration.ofSeconds(seconds);
    }
    protected abstract ChannelType channel();
}


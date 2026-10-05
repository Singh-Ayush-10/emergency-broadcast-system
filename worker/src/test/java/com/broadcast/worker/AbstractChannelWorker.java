package com.broadcast.worker;


import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryStatus;
import com.broadcast.core.model.DeliveryTask;
import com.broadcast.core.model.TrackingEvent;
import com.broadcast.core.model.exception.PermanentDeliveryException;
import com.broadcast.core.model.exception.TransientDeliveryException;
import com.broadcast.provider.DeliveryResult;
import com.broadcast.provider.NotificationProvider;
import com.broadcast.worker.retry.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class AbstractChannelWorkerTest {

    private NotificationProvider provider;
    private ThreadPoolTaskExecutor executor;
    private KafkaTemplate<String, DeliveryTask> deliveryTaskTemplate;
    private KafkaTemplate<String, TrackingEvent> trackingEventTemplate;
    private IdempotencyService idempotencyService;
    private PushChannelWorker worker;

    @BeforeEach
    void setUp() {
        provider = mock(NotificationProvider.class);
        executor = mock(ThreadPoolTaskExecutor.class);
        deliveryTaskTemplate = mock(KafkaTemplate.class);
        trackingEventTemplate = mock(KafkaTemplate.class);
        idempotencyService = mock(IdempotencyService.class);

        // run submitted tasks synchronously, right now, on this thread
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            r.run();
            return null;
        }).when(executor).execute(any(Runnable.class));

        when(idempotencyService.markProcessingIfAbsent(anyString())).thenReturn(true);

        worker = new PushChannelWorker(provider, executor, deliveryTaskTemplate, trackingEventTemplate, idempotencyService);
    }

    private DeliveryTask task(int attemptCount) {
        DeliveryTask t = new DeliveryTask("b-1", "user-1", "device-token-user-1", "msg", ChannelType.PUSh);
        t.setAttemptCount(attemptCount);
        return t;
    }

    @Test
    void successfulDelivery_publishesDeliveredEvent_noRetryOrDlt() throws Exception {
        when(provider.send(anyString(), anyString())).thenReturn(DeliveryResult.success("device-token-user-1", "PUSH-MOCK-1"));

        worker.onDeliveryTask(task(0));

        ArgumentCaptor<TrackingEvent> captor = ArgumentCaptor.forClass(TrackingEvent.class);
        verify(trackingEventTemplate).send(eq("broadcast.emergency.tracking-events"), eq("user-1"), captor.capture());
        assertEquals(DeliveryStatus.DELIVERED, captor.getValue().getStatus());

        verify(deliveryTaskTemplate, never()).send(eq("broadcast.emergency.retry"), anyString(), any());
        verify(deliveryTaskTemplate, never()).send(eq("broadcast.emergency.DLT"), anyString(), any());
    }

    @Test
    void transientFailure_underRetryLimit_requeuesWithIncrementedAttempt() throws Exception {
        when(provider.send(anyString(), anyString())).thenThrow(new TransientDeliveryException("timeout"));

        worker.onDeliveryTask(task(1));

        ArgumentCaptor<DeliveryTask> captor = ArgumentCaptor.forClass(DeliveryTask.class);
        verify(deliveryTaskTemplate).send(eq("broadcast.emergency.retry"), eq("user-1"), captor.capture());
        assertEquals(2, captor.getValue().getAttemptCount());

        verify(deliveryTaskTemplate, never()).send(eq("broadcast.emergency.DLT"), anyString(), any());
    }

    @Test
    void transientFailure_retriesExhausted_routesToDltAndFallsBack() throws Exception {
        when(provider.send(anyString(), anyString())).thenThrow(new TransientDeliveryException("timeout"));

        worker.onDeliveryTask(task(4)); // already at MAX_RETRY_ATTEMPTS

        verify(deliveryTaskTemplate).send(eq("broadcast.emergency.DLT"), eq("user-1"), any());
        verify(deliveryTaskTemplate).send(eq("broadcast.emergency.dispatch.sms"), eq("user-1"), any());
        verify(deliveryTaskTemplate, never()).send(eq("broadcast.emergency.retry"), anyString(), any());
    }

    @Test
    void permanentFailure_routesToDltAndFallsBackToSms() throws Exception {
        when(provider.send(anyString(), anyString())).thenThrow(new PermanentDeliveryException("invalid recipient"));

        worker.onDeliveryTask(task(0));

        verify(deliveryTaskTemplate).send(eq("broadcast.emergency.DLT"), eq("user-1"), any());

        ArgumentCaptor<DeliveryTask> fallbackCaptor = ArgumentCaptor.forClass(DeliveryTask.class);
        verify(deliveryTaskTemplate).send(eq("broadcast.emergency.dispatch.sms"), eq("user-1"), fallbackCaptor.capture());
        assertEquals(ChannelType.SMS, fallbackCaptor.getValue().getChannel());
    }

    @Test
    void duplicateDelivery_skipsProviderCallEntirely() throws Exception {
        when(idempotencyService.markProcessingIfAbsent(anyString())).thenReturn(false);

        worker.onDeliveryTask(task(0));

        verify(provider, never()).send(anyString(), anyString());
        verifyNoInteractions(trackingEventTemplate);
    }
}
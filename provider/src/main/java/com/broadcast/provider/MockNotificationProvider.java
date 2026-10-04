package com.broadcast.provider;

import com.broadcast.core.model.exception.PermanentDeliveryException;
import com.broadcast.core.model.exception.TransientDeliveryException;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A stand-in for a real provider (Twilio, SendGrid, FCM) that mimics
 * real-world failure patterns instead of always succeeding, so the
 * retry/backoff and DLT/fallback logic can actually be exercised and
 * demoed without sending real messages.
 */
public class MockNotificationProvider implements NotificationProvider{

    private final String channelName;
    private final double transientFailureRate;
    private final double permanentFailureRate;
    private final int minLatencyMs;
    private final int maxLatencyMs;

    public MockNotificationProvider(String channelName, double transientFailureRate, double permanentFailureRate, int minLatencyMs, int maxLatencyMs) {
        this.channelName = channelName;
        this.transientFailureRate = transientFailureRate;
        this.permanentFailureRate = permanentFailureRate;
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
    }
    public void simulateNetworkLatency(){
        try{
            int dely = ThreadLocalRandom.current().nextInt(minLatencyMs,maxLatencyMs+1);
            Thread.sleep(dely);
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }


    @Override
    public DeliveryResult send(String recipient, String message) throws TransientDeliveryException, PermanentDeliveryException {
        simulateNetworkLatency();
        double roll = ThreadLocalRandom.current().nextDouble();

        if (roll < transientFailureRate) {
            throw new TransientDeliveryException(
                    "Simulated " + channelName + " timeout/rate-limit for " + recipient);
        }
        if (roll < transientFailureRate + permanentFailureRate) {
            throw new PermanentDeliveryException(
                    "Simulated " + channelName + " permanent failure (invalid recipient) for " + recipient);
        }

        String providerMessageId = channelName.toUpperCase() + "-MOCK-" + UUID.randomUUID();
        return DeliveryResult.success(recipient, providerMessageId);
    }
}

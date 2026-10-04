package com.broadcast.core.model;

import java.io.Serializable;
import java.time.Instant;
/**
 * One (broadcastId, userId, channel) unit of work. This is what the
 * dispatcher publishes to a channel topic, and what gets republished
 * to the retry topic on a transient failure.
 */
public class DeliveryTask implements Serializable {
    private String broadcastId;
    private String userId;
    private String recipient;

    public DeliveryTask() {

    }

    private String message;
    private ChannelType channel;
    private int attemptCount;
    private Instant notBeforeTimestamp;
    public DeliveryTask(String broadcastId, String userId, String recipient,
                        String message, ChannelType channel) {
        this.broadcastId = broadcastId;
        this.userId = userId;
        this.recipient = recipient;
        this.message = message;
        this.channel = channel;
        this.attemptCount = 0;
    }

    public DeliveryTask withIncrementedAttempt(Instant notBeforeTimestamp) {
        DeliveryTask next = new DeliveryTask(broadcastId, userId, recipient, message, channel);
        next.attemptCount = this.attemptCount + 1;
        next.notBeforeTimestamp = notBeforeTimestamp;
        return next;
    }

    public String getBroadcastId() {
        return broadcastId;
    }

    public void setBroadcastId(String broadcastId) {
        this.broadcastId = broadcastId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ChannelType getChannel() {
        return channel;
    }

    public void setChannel(ChannelType channel) {
        this.channel = channel;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getNotBeforeTimestamp() {
        return notBeforeTimestamp;
    }

    public void setNotBeforeTimestamp(Instant notBeforeTimestamp) {
        this.notBeforeTimestamp = notBeforeTimestamp;
    }

    /** Idempotency key for the Redis-backed dedupe check in the worker module. */
    public String idempotencyKey() {
        return broadcastId + ":" + userId + ":" + channel + ":" + attemptCount;
    }}

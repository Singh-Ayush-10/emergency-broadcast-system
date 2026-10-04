package com.broadcast.tracking;

import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryStatus;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "delivery_log",
        uniqueConstraints =  @UniqueConstraint(columnNames = {"broadcastId", "userId", "channel"}),
        indexes = {
                @Index(name = "idx_delivery_log_broadcast", columnList = "broadcastId"),
                @Index(name = "idx_delivery_log_lookup", columnList = "broadcastId,userId,channel")
        })
public class DeliveryLog {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false)
    private String broadcastId;

    @Column(nullable = false)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ChannelType channel;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status;

    private int attemptCount;

    @Column(length = 1000)
    private String lastError;

    private String providerMessageId;

    private Instant lastAttemptAt;

    protected DeliveryLog() {
    }

    public DeliveryLog(String broadcastId, String userId, ChannelType channel) {
        this.broadcastId = broadcastId;
        this.userId = userId;
        this.channel = channel;
        this.status = DeliveryStatus.PENDING;
        this.attemptCount = 0;
        this.lastAttemptAt = Instant.now();
    }
    public DeliveryLog(String broadcastId, String userId, ChannelType channel,
                       DeliveryStatus status, String detail, int attemptCount) {
        this.broadcastId = broadcastId;
        this.userId = userId;
        this.channel = channel;
        this.status = status;
        this.attemptCount = attemptCount;
        this.lastAttemptAt = Instant.now();

        if (status == DeliveryStatus.DELIVERED) {
            this.providerMessageId = detail;
        } else {
            this.lastError = detail;
        }
    }

    // getters unchanged from before — id, broadcastId, userId, channel,
    // status, attemptCount, lastError, providerMessageId, lastAttemptAt
    public String getId() { return id; }
    public String getBroadcastId() { return broadcastId; }
    public String getUserId() { return userId; }
    public ChannelType getChannel() { return channel; }
    public DeliveryStatus getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public String getLastError() { return lastError; }
    public String getProviderMessageId() { return providerMessageId; }
    public Instant getLastAttemptAt() { return lastAttemptAt; }

}

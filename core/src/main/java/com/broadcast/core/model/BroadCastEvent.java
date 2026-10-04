package com.broadcast.core.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Published to broadcast.emergency.raw when an emergency is triggered.
 * Partitioned by regionId so dispatch for one region never blocks another.
 */

public class BroadCastEvent {

    public BroadCastEvent() {
    }

    private String broadcastId;
    private String regionId;
    private String message;
    private List<String>userIds;
    private Instant createdAt;

    public BroadCastEvent(@jakarta.validation.constraints.NotBlank List<String> userIds, String message, @jakarta.validation.constraints.NotEmpty String regionId) {
        this.broadcastId = UUID.randomUUID().toString();
        this.userIds = userIds;
        this.message = message;
        this.regionId = regionId;
        this.createdAt = Instant.now();
    }

    public void setBroadcastId(String broadcastId) {
        this.broadcastId = broadcastId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setUserIds(List<String> userIds) {
        this.userIds = userIds;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getBroadcastId() {
        return broadcastId;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }


}

package com.broadcast.core.model;

public class TrackingEvent {

    private String broadcastId;
    private String userId;
    private ChannelType channel;
    private DeliveryStatus status;
    private String detail;

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

    public ChannelType getChannel() {
        return channel;
    }

    public void setChannel(ChannelType channel) {
        this.channel = channel;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public void setStatus(DeliveryStatus status) {
        this.status = status;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public TrackingEvent(){

    }
    public TrackingEvent(String broadcastId, String userid, ChannelType channel,
                         DeliveryStatus status, String detail){
        this.broadcastId = broadcastId;
        this.channel = channel;
        this.detail = detail;
        this.status = status;
        this.userId = userid;
    }
}

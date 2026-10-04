package com.broadcast.worker.config;

public final class Topics {
    private Topics() {
    }

    public static final String TRACKING_EVENTS = "broadcast.emergency.tracking-events";
    public static final String SMS = "broadcast.emergency.dispatch.sms";
    public static final String EMAIL = "broadcast.emergency.dispatch.email";
    public static final String PUSH = "broadcast.emergency.dispatch.push";
    public static final String VOICE = "broadcast.emergency.dispatch.voice";
    public static final String RETRY = "broadcast.emergency.retry";
    public static final String DLT = "broadcast.emergency.DLT";
}

package com.broadcast.worker.dispatch;

import com.broadcast.core.model.ChannelType;
import com.broadcast.worker.config.Topics;

import java.util.Optional;

public final class FallbackChannelResolver {
    private FallbackChannelResolver() {
    }

    public static Optional<ChannelType> nextChannel(ChannelType current) {
        return switch (current) {
            case PUSh -> Optional.of(ChannelType.SMS);
            case SMS -> Optional.of(ChannelType.EMAIL);
            case EMAIL -> Optional.of(ChannelType.VOICE);
            case VOICE -> Optional.empty();
        };
    }

    public static String topicFor(ChannelType channel) {
        return switch (channel) {
            case SMS -> Topics.SMS;
            case EMAIL -> Topics.EMAIL;
            case PUSh -> Topics.PUSH;
            case VOICE -> Topics.VOICE;
        };
    }

    public static String recipientFor(String userId, ChannelType channel) {
        return switch (channel) {
            case SMS, VOICE -> "+91-" + Math.abs(userId.hashCode() % 9000000000L + 1000000000L);
            case EMAIL -> userId + "@example.com";
            case PUSh -> "device-token-" + userId;
        };
    }
}

package com.broadcast.worker.config;

import com.broadcast.provider.MockNotificationProvider;
import com.broadcast.provider.NotificationProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProviderConfig {

    @Bean
    public NotificationProvider smsProvider(
            @Value("${notification.mock.sms.transient-failure-rate:0.05}") double transientRate,
            @Value("${notification.mock.sms.permanent-failure-rate:0.03}") double permanentRate,
            @Value("${notification.mock.sms.min-latency-ms:80}") int minLatency,
            @Value("${notification.mock.sms.max-latency-ms:200}") int maxLatency) {
        return new MockNotificationProvider("sms", transientRate, permanentRate, minLatency, maxLatency);
    }

    @Bean
    public NotificationProvider emailProvider(
            @Value("${notification.mock.email.transient-failure-rate:0.03}") double transientRate,
            @Value("${notification.mock.email.permanent-failure-rate:0.02}") double permanentRate,
            @Value("${notification.mock.email.min-latency-ms:100}") int minLatency,
            @Value("${notification.mock.email.max-latency-ms:250}") int maxLatency) {
        return new MockNotificationProvider("email", transientRate, permanentRate, minLatency, maxLatency);
    }

    @Bean
    public NotificationProvider pushProvider(
            @Value("${notification.mock.push.transient-failure-rate:0.04}") double transientRate,
            @Value("${notification.mock.push.permanent-failure-rate:0.01}") double permanentRate,
            @Value("${notification.mock.push.min-latency-ms:50}") int minLatency,
            @Value("${notification.mock.push.max-latency-ms:150}") int maxLatency) {
        return new MockNotificationProvider("push", transientRate, permanentRate, minLatency, maxLatency);
    }

    @Bean
    public NotificationProvider voiceProvider(
            @Value("${notification.mock.voice.transient-failure-rate:0.08}") double transientRate,
            @Value("${notification.mock.voice.permanent-failure-rate:0.05}") double permanentRate,
            @Value("${notification.mock.voice.min-latency-ms:1500}") int minLatency,
            @Value("${notification.mock.voice.max-latency-ms:3000}") int maxLatency) {
        return new MockNotificationProvider("voice", transientRate, permanentRate, minLatency, maxLatency);
    }
}
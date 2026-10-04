package com.broadcast.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
@Configuration
public class ThreadPoolConfig {
    private ThreadPoolTaskExecutor buildExecutor(String threadNamePrefix, int corePoolSize, int maxPoolSize, int queueCapacity){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;

    }
    @Bean
    public ThreadPoolTaskExecutor smsExecutor() {
        return buildExecutor("sms-worker-", 50, 50, 200);
    }

    @Bean
    public ThreadPoolTaskExecutor emailExecutor() {
        return buildExecutor("email-worker-", 100, 100, 400);
    }

    @Bean
    public ThreadPoolTaskExecutor pushExecutor() {
        return buildExecutor("push-worker-", 50, 50, 200);
    }

    @Bean
    public ThreadPoolTaskExecutor voiceExecutor() {
        return buildExecutor("voice-worker-", 20, 20, 100);
    }

    @Bean
    public ScheduledExecutorService retryScheduler(){
        return Executors.newScheduledThreadPool(6);
    }

}

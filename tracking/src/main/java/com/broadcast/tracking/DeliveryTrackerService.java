package com.broadcast.tracking;

import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


@Service
public class DeliveryTrackerService {
    private static  final Logger log = LoggerFactory.getLogger(DeliveryTrackerService.class);
    private static final int QUEUE_CAPACITY = 100_000;
    private static final int MAX_BATCH_SIZE = 500;

    private final DeliveryLogRepository repository;
    private final BlockingQueue<DeliveryLog> pendingQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    public DeliveryTrackerService(DeliveryLogRepository repository) {
        this.repository = repository;
    }
    /**
     * Never writes to the DB directly - just enqueues. Returns immediately,
     * so the caller (dispatcher's fan-out loop) is never blocked waiting
     * on a database round-trip per user.
     */
    public void recordPending(String broadcastId, String userId, ChannelType channel) {
        DeliveryLog entry = new DeliveryLog(broadcastId, userId, channel);
        boolean accepted = pendingQueue.offer(entry);
        if(!accepted){
            log.warn("Pending queue full (capacity {}), dropping entry for broadcast={} user={} channel={}",
                    QUEUE_CAPACITY, broadcastId, userId, channel);
        }
    }
    /**
     * Runs on a fixed schedule, drains whatever has queued up since the
     * last run, and flushes it in bounded-size batches via saveAll().
     * This is the only place that actually talks to Postgres for inserts.
     */
    @Scheduled(fixedDelayString = "${tracking.flush-interval-ms:500}")
    public void flushPendingQueue(){
        if(pendingQueue.isEmpty()){
            return;
        }
        List<DeliveryLog>batch = new ArrayList<>(MAX_BATCH_SIZE);
        int totalFlushed = 0;

        while (pendingQueue.drainTo(batch,MAX_BATCH_SIZE)>0){
            repository.saveAll(batch);
            totalFlushed+= batch.size();
            batch.clear();
        }

        if(totalFlushed > 0){
            log.info("Flushed {} pending delivery log entries to the database", totalFlushed);
        }
    }
    @Transactional
    public void recordDelivered(String broadcastId, String userId, ChannelType channel, String providerMessageId) {
        int updated = repository.markDelivered(broadcastId, userId, channel, providerMessageId, Instant.now());
        if (updated == 0) {
            log.warn("markDelivered found no row (race with pending flush) - inserting directly for broadcast={} user={} channel={}",
                    broadcastId, userId, channel);
            insertDirect(broadcastId, userId, channel, DeliveryStatus.DELIVERED, providerMessageId, 0);
        }
    }
    @Transactional
    public void recordRetrying(String broadcastId, String userId, ChannelType channel, String error) {
        int updated = repository.markRetrying(broadcastId, userId, channel, error, Instant.now());
        if (updated == 0) {
            log.warn("markRetrying found no row (race with pending flush) - inserting directly for broadcast={} user={} channel={}",
                    broadcastId, userId, channel);
            insertDirect(broadcastId, userId, channel, DeliveryStatus.RETRYING, error, 1);
        }
    }

    @Transactional
    public void recordPermanentlyFailed(String broadcastId, String userId, ChannelType channel, String error) {
        int updated = repository.markPermanentlyFailed(broadcastId, userId, channel, error, Instant.now());
        if (updated == 0) {
            log.warn("markPermanentlyFailed found no row (race with pending flush) - inserting directly for broadcast={} user={} channel={}",
                    broadcastId, userId, channel);
            insertDirect(broadcastId, userId, channel, DeliveryStatus.PERMANENTLY_FAILED, error, 1);
        }
    }

    private void insertDirect(String broadcastId, String userId, ChannelType channel,
                              DeliveryStatus status, String detail, int attemptCount) {
        repository.save(new DeliveryLog(broadcastId, userId, channel, status, detail, attemptCount));
    }

    public Map<DeliveryStatus, Long> summarize(String broadcastId) {
        List<DeliveryLog> logs = repository.findByBroadcastId(broadcastId);
        return logs.stream().collect(Collectors.groupingBy(DeliveryLog::getStatus, Collectors.counting()));
    }

    public List<DeliveryLog> findAll(String broadcastId) {
        return repository.findByBroadcastId(broadcastId);
    }

}

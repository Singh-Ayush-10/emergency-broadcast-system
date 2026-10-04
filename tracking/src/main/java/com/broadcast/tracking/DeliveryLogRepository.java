package com.broadcast.tracking;

import com.broadcast.core.model.ChannelType;
import com.broadcast.core.model.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface DeliveryLogRepository extends JpaRepository<DeliveryLog,String> {
    List<DeliveryLog> findByBroadcastId(String broadcastId);

    List<DeliveryLog> findByStatusInAndLastAttemptAtBefore(
            List<DeliveryStatus> statuses, Instant threshold);

    @Modifying
    @Query("""
        UPDATE DeliveryLog d
        SET d.status = 'DELIVERED', d.providerMessageId = :providerMessageId, d.lastAttemptAt = :now
        WHERE d.broadcastId = :broadcastId AND d.userId = :userId AND d.channel = :channel
        """)
    int markDelivered(@Param("broadcastId") String broadcastId,
                      @Param("userId") String userId,
                      @Param("channel") ChannelType channel,
                      @Param("providerMessageId") String providerMessageId,
                      @Param("now") Instant now);

    @Modifying
    @Query("""
        UPDATE DeliveryLog d
        SET d.status = 'RETRYING', d.attemptCount = d.attemptCount + 1,
            d.lastError = :error, d.lastAttemptAt = :now
        WHERE d.broadcastId = :broadcastId AND d.userId = :userId AND d.channel = :channel
        """)
    int markRetrying(@Param("broadcastId") String broadcastId,
                     @Param("userId") String userId,
                     @Param("channel") ChannelType channel,
                     @Param("error") String error,
                     @Param("now") Instant now);

    @Modifying
    @Query("""
        UPDATE DeliveryLog d
        SET d.status = 'PERMANENTLY_FAILED', d.attemptCount = d.attemptCount + 1,
            d.lastError = :error, d.lastAttemptAt = :now
        WHERE d.broadcastId = :broadcastId AND d.userId = :userId AND d.channel = :channel
        """)
    int markPermanentlyFailed(@Param("broadcastId") String broadcastId,
                              @Param("userId") String userId,
                              @Param("channel") ChannelType channel,
                              @Param("error") String error,
                              @Param("now") Instant now);
}

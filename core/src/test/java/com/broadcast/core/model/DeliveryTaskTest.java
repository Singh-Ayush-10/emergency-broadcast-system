package com.broadcast.core.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

public class DeliveryTaskTest {

    private DeliveryTask newTask(){
        return new DeliveryTask("b-1","user-1","device-token-user-1","msg", ChannelType.PUSh);

    }
    @Test
    void idempotencyKey_differsBetweenAttempts(){
        DeliveryTask first = newTask();
        DeliveryTask retry = first.withIncrementedAttempt(Instant.now().plusSeconds(5));

        assertNotEquals(first.idempotencyKey(),retry.idempotencyKey());
    }
    @Test
    void idempotencyKey_sameForRedeliveryOfSameAttempt(){
        assertEquals(newTask().idempotencyKey(),newTask().idempotencyKey());
    }
    @Test
    void withIncrementAttempt_setsAttemptCountAndBackOffTimestamp(){
        Instant notBefore = Instant.now().plusSeconds(15);

        DeliveryTask retry = newTask().withIncrementedAttempt(notBefore);
        assertEquals(1,retry.getAttemptCount());
        assertEquals(notBefore,retry.getNotBeforeTimestamp());
    }
    @Test
    void withIncrementedAttempt_doesNotMutateOriginal() {
        DeliveryTask original = newTask();

        original.withIncrementedAttempt(Instant.now().plusSeconds(5));

        assertEquals(0, original.getAttemptCount());
        assertNull(original.getNotBeforeTimestamp());
    }
}

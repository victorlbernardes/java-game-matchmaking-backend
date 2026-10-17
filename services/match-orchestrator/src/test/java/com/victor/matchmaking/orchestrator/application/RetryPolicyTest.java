package com.victor.matchmaking.orchestrator.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * The retry timing is the difference between a bounded retry and a hot loop against a
 * database that is already down.
 */
class RetryPolicyTest {

    @Test
    void waitsBeforeEveryAttemptExceptTheFirst() {
        RetryPolicy policy = new RetryPolicy(4, Duration.ofMillis(250));

        assertEquals(Duration.ZERO, policy.backoffBefore(1));
        assertEquals(Duration.ofMillis(250), policy.backoffBefore(2));
        assertEquals(Duration.ofMillis(500), policy.backoffBefore(3));
        assertEquals(Duration.ofMillis(1_000), policy.backoffBefore(4));
    }

    @Test
    void theDefaultIsThreeAttemptsAndUnderASecondOfWaiting() {
        assertEquals(3, RetryPolicy.DEFAULT.maxAttempts());
        Duration total = RetryPolicy.DEFAULT.backoffBefore(2).plus(RetryPolicy.DEFAULT.backoffBefore(3));
        assertEquals(Duration.ofMillis(750), total);
    }

    @Test
    void refusesAPolicyThatWouldNeverRetryOrWaitForever() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, Duration.ofMillis(-1)));
    }
}
